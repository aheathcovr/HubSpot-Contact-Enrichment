"""
The service's side of "find people by job title" on the company card: /app/company/find, /app/company/candidates and
/app/company/add, and the worker that does a queued search or add.

A rep sends job titles and a search is queued (nothing is written to HubSpot by it); the card then reads the people it
found, the rep chooses some, and /app/company/add queues one add per person. These tests drive the real routes with a
signed request and fake HubSpot, run store and Cloud Tasks, and pin what matters: nothing happens unsigned, bad titles
are refused before anything is read, the same search within a day is the earlier run and costs nothing, a person not in
the stored results can never be added, the cap is all or nothing, and a run that cannot be queued gives its place back.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_app_title_endpoints.py" -v
"""

import json
import os
import sys
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app_run_store
import company_titles
import contact_objectives
import webhook_server
from hubspot_client import HubSpotError
from test_app_endpoints import _NOW, _USER, _clock, _headers, client, queued, store  # noqa: F401

_FACILITY = "Individual Facility / Child"
_FIND = "f" * 32
_PAT = {"candidate_id": "aaaaaaaaaaaa", "full_name": "Pat Lee", "title": "Medical Director", "linkedin_url": "https://www.linkedin.com/in/pat-lee-example",
        "updated_at": "2026-09-01", "linked_contact_id": ""}
_SAM = {"candidate_id": "bbbbbbbbbbbb", "full_name": "Sam Ortiz", "title": "Dietitian", "linkedin_url": "", "updated_at": "", "linked_contact_id": "901"}


@pytest.fixture
def hs(monkeypatch):
    h = MagicMock()
    h.get_company_properties.return_value = {"name": "Elm Court Care Center", "hierarchy_type": _FACILITY}
    monkeypatch.setattr(webhook_server, "_hubspot", lambda: h)
    return h


def _call(client, method, path, body=None, user=_USER, **kw):
    full = f"{path}{'&' if '?' in path else '?'}{user}"
    raw = b"" if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    url = "http://testserver" + full
    if method == "GET":
        return client.get(full, headers=_headers("GET", url, **kw))
    return client.post(full, content=raw, headers=_headers("POST", url, raw, **kw))


def _find(client, titles=("Medical Director",), company="500", **kw):
    return _call(client, "POST", "/app/company/find", {"companyId": company, "titles": list(titles)}, **kw)


def _candidates(client, run=_FIND, company="500", **kw):
    return _call(client, "GET", f"/app/company/candidates?companyId={company}&runId={run}", **kw)


def _add(client, candidates=("aaaaaaaaaaaa",), run=_FIND, company="500", **kw):
    return _call(client, "POST", "/app/company/add", {"companyId": company, "runId": run, "candidateIds": list(candidates)}, **kw)


def _search_run(store, run_id=_FIND, company="500", status="done", people=(_PAT, _SAM), titles=("Medical Director",), minutes_ago=5, save=True, **over):
    store.create_run({"run_id": run_id, "contact_id": "", "objective": "find_by_title", "titles": list(titles), "titles_key": company_titles.titles_key(list(titles)),
                      "requested_by_id": "77", "requested_by": "rep@example.org", "portal_id": "1234", "company_id": company, "status": status,
                      "created_at": _NOW - timedelta(minutes=minutes_ago), **over})
    if save and status == "done":
        store.save_candidates(run_id, company, list(people), _NOW)


def _add_run(store, run_id, candidate="aaaaaaaaaaaa", status="queued", company="500", source=_FIND, minutes_ago=1):
    store.create_run({"run_id": run_id, "contact_id": "", "objective": "add_candidate", "source_run_id": source, "candidate_id": candidate,
                      "requested_by_id": "77", "requested_by": "rep@example.org", "portal_id": "1234", "company_id": company, "status": status,
                      "created_at": _NOW - timedelta(minutes=minutes_ago)})


# ── Nobody gets in unsigned ───────────────────────────────────────────────────

@pytest.mark.parametrize("call", ["find", "candidates", "add"])
def test_an_unsigned_request_is_refused_and_nothing_is_read_or_done(client, store, hs, queued, call):
    _search_run(store)
    if call == "find":
        response = client.post(f"/app/company/find?{_USER}", json={"companyId": "500", "titles": ["Medical Director"]})
    elif call == "candidates":
        response = client.get(f"/app/company/candidates?companyId=500&runId={_FIND}&{_USER}")
    else:
        response = client.post(f"/app/company/add?{_USER}", json={"companyId": "500", "runId": _FIND, "candidateIds": ["aaaaaaaaaaaa"]})

    assert response.status_code == 401 and hs.get_company_properties.call_count == 0 and queued == []


@pytest.mark.parametrize("call", ["find", "candidates", "add"])
def test_a_request_signed_with_the_wrong_secret_is_refused(client, store, hs, queued, call):
    _search_run(store)
    response = {"find": _find, "candidates": _candidates, "add": _add}[call](client, secret="wrong")

    assert response.status_code == 401 and queued == []


# ── Asking for a search ───────────────────────────────────────────────────────

def test_a_search_is_queued_for_the_titles_with_the_company_and_the_rep(client, store, hs, queued):
    response = _find(client, titles=("Medical Director", "Dietitian"))
    body = response.json()

    assert response.status_code == 202 and body["ok"] is True and body["reused"] is False
    assert body["run"]["objective"] == "find_by_title" and body["run"]["status"] == "queued" and body["run"]["companyId"] == "500"
    assert body["run"]["contactId"] == "" and body["quota"] == {"cap": 25, "used": 1, "remaining": 24}
    record = store.get_run(body["run"]["runId"])
    assert record["titles"] == ["Medical Director", "Dietitian"] and record["company_id"] == "500"
    assert record["requested_by_id"] == "77" and record["requested_by"] == "rep@example.org" and queued == [body["run"]["runId"]]


def test_the_titles_are_cleaned_before_they_are_kept(client, store, hs, queued):
    body = _find(client, titles=("  Medical   Director ", "medical director", "")).json()

    assert store.get_run(body["run"]["runId"])["titles"] == ["Medical Director"]


@pytest.mark.parametrize("body, message", [
    ({"companyId": "500"}, "Choose at least one job title."),
    ({"companyId": "500", "titles": []}, "Choose at least one job title."),
    ({"companyId": "500", "titles": "Medical Director"}, "Choose at least one job title."),
    ({"companyId": "500", "titles": [f"T{n}" for n in range(6)]}, "Choose at most 5 job titles."),
    ({"companyId": "500", "titles": ["x" * 81]}, "A job title can be at most 80 characters."),
    ({"companyId": "500", "titles": [7]}, "Job titles must be plain text."),
])
def test_bad_titles_are_refused_before_anything_is_read(client, store, hs, queued, body, message):
    response = _call(client, "POST", "/app/company/find", body)

    assert response.status_code == 400 and response.json()["error"] == message
    assert hs.get_company_properties.call_count == 0 and queued == []


@pytest.mark.parametrize("body", [b"not json", ["a"], {"titles": ["x y"]}, {"companyId": "abc", "titles": ["x y"]}, {"companyId": True, "titles": ["x y"]}])
def test_an_ill_formed_search_request_is_a_bad_request(client, store, hs, queued, body):
    assert _call(client, "POST", "/app/company/find", body).status_code == 400 and queued == []


def test_a_company_hubspot_does_not_have_is_a_404_and_nothing_is_spent(client, store, hs, queued):
    hs.get_company_properties.side_effect = HubSpotError("not found", 404)

    response = _find(client)

    assert response.status_code == 404 and response.json()["error"] == "HubSpot has no such company." and store.used_today("77", "2026-10-26") == 0


def test_a_hubspot_outage_is_a_502_without_its_details(client, store, hs, queued):
    hs.get_company_properties.side_effect = HubSpotError("boom: secret detail", 500)

    response = _find(client)

    assert response.status_code == 502 and "secret detail" not in response.text and queued == []


def test_the_same_search_within_a_day_is_the_earlier_run_in_any_order_or_case_and_costs_nothing(client, store, hs, queued):
    first = _find(client, titles=("Medical Director", "Dietitian")).json()["run"]["runId"]

    again = _find(client, titles=("dietitian", "MEDICAL DIRECTOR")).json()

    assert again["reused"] is True and again["run"]["runId"] == first and again["quota"]["used"] == 1 and len(queued) == 1


def test_a_search_for_other_titles_is_a_new_run(client, store, hs, queued):
    _find(client, titles=("Medical Director",))

    assert _find(client, titles=("Dietitian",)).json()["reused"] is False and len(queued) == 2


def test_a_search_a_day_old_is_not_repeated(client, store, hs, queued):
    _search_run(store, minutes_ago=25 * 60, status="done")

    assert _find(client).json()["reused"] is False


def test_a_failed_search_does_not_stand_in_for_a_new_one(client, store, hs, queued):
    _search_run(store, status="failed")

    assert _find(client).json()["reused"] is False


def test_another_companys_search_does_not_stand_in_for_this_one(client, store, hs, queued):
    _search_run(store, company="600")

    assert _find(client, company="500").json()["reused"] is False


def test_a_search_needs_a_place_in_the_reps_cap(client, store, hs, queued):
    for _ in range(25):
        store.take_place("77", "2026-10-26", 25, _NOW)

    response = _find(client)

    assert response.status_code == 429 and response.json()["quota"]["remaining"] == 0 and queued == []


def test_a_search_that_cannot_be_queued_is_marked_failed_and_gives_its_place_back(client, store, hs, monkeypatch):
    def down(run_id):
        raise RuntimeError("queue is down")

    monkeypatch.setattr(webhook_server, "_enqueue_app_run", down)

    response = _find(client)

    assert response.status_code == 502 and store.used_today("77", "2026-10-26") == 0
    assert [r["status"] for r in store.runs_for_company("500")] == ["failed"]


# ── Reading the people a search found ─────────────────────────────────────────

def test_a_finished_search_gives_the_people_for_the_card(client, store, hs):
    _search_run(store)

    response = _candidates(client)
    body = response.json()

    assert response.status_code == 200 and body["ok"] is True and body["ready"] is True and body["run"]["runId"] == _FIND
    assert body["candidates"] == [
        {"candidateId": "aaaaaaaaaaaa", "name": "Pat Lee", "title": "Medical Director", "linkedinUrl": "https://www.linkedin.com/in/pat-lee-example",
         "updatedAt": "2026-09-01", "linkedContactId": None, "added": None},
        {"candidateId": "bbbbbbbbbbbb", "name": "Sam Ortiz", "title": "Dietitian", "linkedinUrl": None, "updatedAt": None, "linkedContactId": "901", "added": None},
    ]


def test_a_search_still_going_is_not_ready_and_has_no_people_yet(client, store, hs):
    _search_run(store, status="running")

    body = _candidates(client).json()

    assert body["ready"] is False and body["candidates"] == [] and body["run"]["status"] == "running"


def test_a_search_that_failed_is_not_ready(client, store, hs):
    _search_run(store, status="failed", outcome="error", message="FullEnrich has no credits left, so people cannot be searched for.")

    body = _candidates(client).json()

    assert body["ready"] is False and body["run"]["message"].startswith("FullEnrich has no credits")


def test_a_person_that_was_asked_to_be_added_shows_how_that_is_going_and_the_latest_wins(client, store, hs):
    _search_run(store)
    _add_run(store, "1" * 32, status="failed", minutes_ago=3)
    _add_run(store, "2" * 32, status="running", minutes_ago=1)

    body = _candidates(client).json()

    assert body["candidates"][0]["added"]["runId"] == "2" * 32 and body["candidates"][0]["added"]["status"] == "running"
    assert body["candidates"][1]["added"] is None


def test_an_add_made_from_another_search_is_not_shown_on_this_one(client, store, hs):
    _search_run(store)
    _add_run(store, "1" * 32, source="e" * 32, status="running")

    assert _candidates(client).json()["candidates"][0]["added"] is None


def test_results_that_have_expired_are_gone(client, store, hs):
    _search_run(store, save=False)

    response = _candidates(client)

    assert response.status_code == 410 and response.json()["error"] == "Those search results are no longer available. Search again."


@pytest.mark.parametrize("run, company", [("0" * 32, "500"), (_FIND, "600"), ("bad", "500")])
def test_a_search_that_is_not_this_companys_or_does_not_exist_is_not_found(client, store, hs, run, company):
    _search_run(store)

    assert _candidates(client, run=run, company=company).status_code in (400, 404)


def test_a_run_that_is_not_a_search_is_not_found(client, store, hs):
    store.create_run({"run_id": _FIND, "contact_id": "1", "objective": "find_email", "requested_by_id": "77", "requested_by": "", "portal_id": "1",
                      "company_id": "500", "status": "done", "created_at": _NOW})

    assert _candidates(client).status_code == 404


# ── Adding the people the rep chose ───────────────────────────────────────────

def test_each_person_chosen_gets_a_queued_add_that_remembers_where_it_came_from(client, store, hs, queued):
    _search_run(store)

    response = _add(client, candidates=("aaaaaaaaaaaa", "bbbbbbbbbbbb"))
    body = response.json()

    assert response.status_code == 202 and body["ok"] is True and body["started"] == 2 and len(body["batchId"]) == 32
    assert [(i["candidateId"], i["objective"], i["status"]) for i in body["items"]] == [
        ("aaaaaaaaaaaa", "add_candidate", "started"), ("bbbbbbbbbbbb", "add_candidate", "started")]
    for item in body["items"]:
        record = store.get_run(item["run"]["runId"])
        assert record["objective"] == "add_candidate" and record["company_id"] == "500" and record["source_run_id"] == _FIND
        assert record["candidate_id"] == item["candidateId"] and record["batch_id"] == body["batchId"] and record["contact_id"] == ""
        assert record["requested_by_id"] == "77" and record["status"] == "queued"
    assert sorted(queued) == sorted(i["run"]["runId"] for i in body["items"]) and body["quota"]["used"] == 2


def test_a_person_who_is_not_in_the_stored_results_is_never_added(client, store, hs, queued):
    _search_run(store)

    response = _add(client, candidates=("aaaaaaaaaaaa", "cccccccccccc"))
    body = response.json()

    assert [(i["candidateId"], i["status"]) for i in body["items"]] == [("aaaaaaaaaaaa", "started"), ("cccccccccccc", "skipped")]
    assert body["items"][1]["reason"] == "Not in these search results." and len(queued) == 1 and body["quota"]["used"] == 1


def test_a_person_already_being_added_is_the_earlier_run_and_costs_nothing(client, store, hs, queued):
    _search_run(store)
    _add_run(store, "1" * 32, status="running")

    body = _add(client, candidates=("aaaaaaaaaaaa", "bbbbbbbbbbbb")).json()

    assert [(i["candidateId"], i["status"]) for i in body["items"]] == [("aaaaaaaaaaaa", "reused"), ("bbbbbbbbbbbb", "started")]
    assert body["items"][0]["run"]["runId"] == "1" * 32 and body["quota"]["used"] == 1 and len(queued) == 1


def test_an_add_made_from_another_search_is_not_the_earlier_run_for_this_one(client, store, hs, queued):
    _search_run(store)
    _add_run(store, "1" * 32, source="e" * 32, status="running")

    assert _add(client).json()["items"][0]["status"] == "started"


def test_a_person_whose_add_failed_can_be_added_again(client, store, hs, queued):
    _search_run(store)
    _add_run(store, "1" * 32, status="failed")

    assert _add(client).json()["items"][0]["status"] == "started"


def test_a_person_listed_twice_is_added_once(client, store, hs, queued):
    _search_run(store)

    body = _add(client, candidates=("aaaaaaaaaaaa", "aaaaaaaaaaaa")).json()

    assert len(body["items"]) == 1 and len(queued) == 1


def test_when_nothing_can_be_started_it_says_so(client, store, hs, queued):
    _search_run(store)

    response = _add(client, candidates=("cccccccccccc",))

    assert response.status_code == 409 and response.json()["ok"] is False and queued == []


def test_adds_that_need_more_places_than_are_left_start_nothing(client, store, hs, queued):
    _search_run(store)
    for _ in range(24):
        store.take_place("77", "2026-10-26", 25, _NOW)

    response = _add(client, candidates=("aaaaaaaaaaaa", "bbbbbbbbbbbb"))

    assert response.status_code == 429 and queued == [] and response.json()["quota"]["used"] == 24


def test_an_add_that_cannot_be_queued_is_marked_failed_and_gives_its_place_back(client, store, hs, monkeypatch):
    _search_run(store)
    calls = []

    def enqueue(run_id):
        calls.append(run_id)
        if len(calls) == 2:
            raise RuntimeError("queue is down")

    monkeypatch.setattr(webhook_server, "_enqueue_app_run", enqueue)

    body = _add(client, candidates=("aaaaaaaaaaaa", "bbbbbbbbbbbb")).json()

    assert [i["status"] for i in body["items"]] == ["started", "failed"] and body["started"] == 1 and body["quota"]["used"] == 1


@pytest.mark.parametrize("body", [
    {"runId": _FIND, "candidateIds": ["aaaaaaaaaaaa"]},
    {"companyId": "500", "candidateIds": ["aaaaaaaaaaaa"]},
    {"companyId": "500", "runId": "bad", "candidateIds": ["aaaaaaaaaaaa"]},
    {"companyId": "500", "runId": _FIND},
    {"companyId": "500", "runId": _FIND, "candidateIds": []},
    {"companyId": "500", "runId": _FIND, "candidateIds": "aaaaaaaaaaaa"},
    {"companyId": "500", "runId": _FIND, "candidateIds": ["not an id!"]},
    {"companyId": "500", "runId": _FIND, "candidateIds": [f"{n:012x}" for n in range(11)]},
    ["not", "an", "object"],
])
def test_an_ill_formed_add_is_refused_before_anything_is_read(client, store, hs, queued, body):
    _search_run(store)

    response = _call(client, "POST", "/app/company/add", body)

    assert response.status_code == 400 and queued == []


def test_an_add_for_a_search_that_is_not_this_companys_is_not_found(client, store, hs, queued):
    _search_run(store, company="600")

    assert _add(client, company="500").status_code == 404 and queued == []


def test_an_add_for_a_search_that_has_not_finished_is_a_conflict(client, store, hs, queued):
    _search_run(store, status="running")

    response = _add(client)

    assert response.status_code == 409 and "not finished" in response.json()["error"] and queued == []


def test_an_add_for_results_that_have_expired_is_gone(client, store, hs, queued):
    _search_run(store, save=False)

    assert _add(client).status_code == 410 and queued == []


# ── The worker does a queued search or add ────────────────────────────────────

def test_a_queued_search_is_done_by_the_title_search_and_ends_with_its_outcome(client, store, hs, queued, monkeypatch):
    seen = []

    def fake(run, *, hs, store, now):
        seen.append((run["objective"], run["company_id"], run["titles"], now))
        return contact_objectives.Outcome("done", "found", "Found 2 people. Choose who to add.")

    monkeypatch.setattr(company_titles, "run_find_by_title", fake)
    run_id = _find(client).json()["run"]["runId"]

    assert webhook_server._execute_app_run(run_id) == "done"

    assert seen == [("find_by_title", "500", ["Medical Director"], _NOW)]
    assert store.get_run(run_id)["status"] == "done" and store.get_run(run_id)["message"] == "Found 2 people. Choose who to add."


def test_a_queued_add_is_done_by_the_add_and_the_contact_it_made_is_put_on_the_run(client, store, hs, queued, monkeypatch):
    _search_run(store)
    monkeypatch.setattr(company_titles, "run_add_candidate", lambda run, *, hs, store: contact_objectives.Outcome(
        "done", "found", "Created a new contact and linked it to Elm Court Care Center.", ["contact"], contact_id="901"))
    run_id = _add(client).json()["items"][0]["run"]["runId"]

    assert webhook_server._execute_app_run(run_id) == "done"

    record = store.get_run(run_id)
    assert record["contact_id"] == "901" and record["wrote"] == ["contact"] and record["status"] == "done"
    assert app_run_store.run_view(record)["contactId"] == "901"


def test_a_queued_contact_run_is_still_done_by_the_contact_objectives(client, store, hs, monkeypatch):
    seen = []
    monkeypatch.setattr(contact_objectives, "run_objective", lambda objective, contact_id, *, hs, requested_by: (
        seen.append((objective, contact_id)) or contact_objectives.Outcome("done", "found", "Email added.", ["email"])))
    store.create_run({"run_id": "c" * 32, "contact_id": "42", "objective": "find_email", "requested_by_id": "77", "requested_by": "rep@example.org",
                      "portal_id": "1", "created_at": _NOW})

    assert webhook_server._execute_app_run("c" * 32) == "done" and seen == [("find_email", "42")]
    assert store.get_run("c" * 32)["contact_id"] == "42"
