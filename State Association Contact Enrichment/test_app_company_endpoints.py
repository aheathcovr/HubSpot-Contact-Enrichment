"""
The service's side of the company card's "pick contacts" feature: /app/company/status and /app/company/run.

A rep ticks some of a company's contacts on the company card and chooses Find email, Find mobile, Verify email or
Enrich. The service plans the runs (contact_objectives.plan_for), checks each contact really is linked to that company,
takes one place in the rep's daily cap for each run, and queues them as the same runs the contact card makes. These
tests drive the real routes with a signed request and fake HubSpot, run store and Cloud Tasks, and pin what matters:
nothing happens unsigned, a contact that is not linked to the company is never run, a repeat spends nothing, the cap is
all or nothing, and a run that cannot be queued gives its place back.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_app_company_endpoints.py" -v
"""

import json
import os
import sys
from datetime import timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app_run_store
import webhook_server
from hubspot_client import HubSpotError
from test_app_endpoints import _CONTACT, _NOW, _SECRET, _USER, _clock, _headers, client, queued, store  # noqa: F401

_FACILITY = "Individual Facility / Child"
_CORPORATION = "Corporation / Operator"
_NOTHING_MISSING = {"email": "pat.lee@example.org", "email_verification_result": "ok", "mobilephone": "405-555-0101"}


def _contact(**over):
    return {**_CONTACT, **over}


@pytest.fixture
def hs(monkeypatch):
    """A HubSpot with company 500 (a facility) and three contacts linked to it, all missing an email and a mobile."""
    from unittest.mock import MagicMock

    h = MagicMock()
    h.contacts = {"1": _contact(), "2": _contact(firstname="Sam", lastname="Ortiz"), "3": _contact(firstname="Kim", lastname="Day")}
    h.links = {}
    h.get_contact_for_app.side_effect = lambda cid: dict(h.contacts[cid])
    h.get_contact_company_ids.side_effect = lambda cid: h.links.get(cid, ["500"])
    h.get_company_properties.return_value = {"name": "Elm Court Care Center", "hierarchy_type": _FACILITY}
    monkeypatch.setattr(webhook_server, "_hubspot", lambda: h)
    return h


def _get_status(client, company="500", user=_USER, **kw):
    path = f"/app/company/status?companyId={company}&{user}"
    return client.get(path, headers=_headers("GET", "http://testserver" + path, **kw))


def _start(client, body, user=_USER, **kw):
    path = f"/app/company/run?{user}"
    raw = json.dumps(body).encode() if not isinstance(body, bytes) else body
    return client.post(path, content=raw, headers=_headers("POST", "http://testserver" + path, raw, **kw))


def _go(client, objective="find_email", contacts=("1", "2"), company="500", **kw):
    return _start(client, {"companyId": company, "contactIds": list(contacts), "objective": objective}, **kw)


def _items(response):
    return [(i["contactId"], i["objective"], i["status"]) for i in response.json()["items"]]


# ── Nobody gets in unsigned ───────────────────────────────────────────────────

@pytest.mark.parametrize("call", ["status", "start"])
def test_an_unsigned_request_is_refused_and_nothing_is_read_or_done(client, store, hs, queued, call):
    if call == "status":
        response = client.get(f"/app/company/status?companyId=500&{_USER}")
    else:
        response = client.post(f"/app/company/run?{_USER}", json={"companyId": "500", "contactIds": ["1"], "objective": "find_email"})

    assert response.status_code == 401
    assert hs.get_company_properties.call_count == 0 and hs.get_contact_for_app.call_count == 0 and queued == []


@pytest.mark.parametrize("call", ["status", "start"])
def test_a_request_signed_with_the_wrong_secret_is_refused(client, store, hs, queued, call):
    response = _get_status(client, secret="wrong") if call == "status" else _go(client, secret="wrong")

    assert response.status_code == 401 and queued == []


# ── Status ────────────────────────────────────────────────────────────────────

def test_the_status_says_what_kind_of_company_it_is_what_can_be_asked_and_what_is_left(client, store, hs):
    body = _get_status(client).json()

    assert body["ok"] is True
    assert body["company"] == {"id": "500", "name": "Elm Court Care Center", "kind": "facility"}
    assert [o["id"] for o in body["objectives"]] == ["find_email", "find_mobile", "verify_email", "enrich"]
    assert body["limits"] == {"maxContacts": 10, "maxTitles": 5}
    assert body["titlePresets"] == ["Administrator", "Executive Director", "Director of Nursing"]
    assert body["quota"] == {"cap": 25, "used": 0, "remaining": 25}
    assert body["runs"] == []


def test_a_corporations_title_presets_are_the_corporate_titles(client, store, hs):
    import config

    hs.get_company_properties.return_value = {"name": "Elm Holdings", "hierarchy_type": _CORPORATION}

    assert _get_status(client).json()["titlePresets"] == config.W2_CORPORATE_TITLES


@pytest.mark.parametrize("hierarchy, kind", [(_FACILITY, "facility"), (_CORPORATION, "corporation"), ("", "other"), ("Something else", "other")])
def test_the_kind_of_company_comes_from_its_company_type(client, store, hs, hierarchy, kind):
    hs.get_company_properties.return_value = {"name": "Elm", "hierarchy_type": hierarchy}

    assert _get_status(client).json()["company"]["kind"] == kind


def test_the_status_shows_the_runs_started_from_this_company_newest_first_and_no_others(client, store, hs):
    for run_id, minutes, company in [("a" * 32, 0, "500"), ("b" * 32, 1, "500"), ("c" * 32, 2, "600")]:
        store.create_run({"run_id": run_id, "contact_id": "1", "objective": "find_email", "requested_by_id": "77", "requested_by": "rep@example.org",
                          "portal_id": "1234", "company_id": company, "created_at": _NOW + timedelta(minutes=minutes)})

    runs = _get_status(client).json()["runs"]

    assert [r["runId"] for r in runs] == ["b" * 32, "a" * 32]
    assert runs[0]["companyId"] == "500" and runs[0]["contactId"] == "1"


def test_the_status_shows_this_reps_quota(client, store, hs):
    for _ in range(4):
        store.take_place("77", "2026-10-26", 25, _NOW)

    assert _get_status(client).json()["quota"] == {"cap": 25, "used": 4, "remaining": 21}


@pytest.mark.parametrize("company", ["abc", "50%200", "", "5" * 21])
def test_a_company_id_that_is_not_digits_is_a_bad_request(client, store, hs, company):
    response = _get_status(client, company=company)

    assert response.status_code == 400 and hs.get_company_properties.call_count == 0


def test_a_company_hubspot_does_not_have_is_a_404(client, store, hs):
    hs.get_company_properties.side_effect = HubSpotError("not found", 404)

    response = _get_status(client)

    assert response.status_code == 404 and response.json()["error"] == "HubSpot has no such company."


def test_a_hubspot_outage_is_a_502_without_its_details(client, store, hs):
    hs.get_company_properties.side_effect = HubSpotError("boom: secret detail", 500)

    response = _get_status(client)

    assert response.status_code == 502 and "secret detail" not in response.text


# ── Asking: what is accepted ──────────────────────────────────────────────────

def test_a_body_that_is_not_json_is_a_bad_request(client, store, hs, queued):
    response = _start(client, b"not json")

    assert response.status_code == 400 and queued == []


@pytest.mark.parametrize("body", [
    {"contactIds": ["1"], "objective": "find_email"},
    {"companyId": "abc", "contactIds": ["1"], "objective": "find_email"},
    {"companyId": "500", "objective": "find_email"},
    {"companyId": "500", "contactIds": [], "objective": "find_email"},
    {"companyId": "500", "contactIds": "1", "objective": "find_email"},
    {"companyId": "500", "contactIds": ["1", "x"], "objective": "find_email"},
    {"companyId": "500", "contactIds": [1, True], "objective": "find_email"},
    {"companyId": "500", "contactIds": ["1"]},
    {"companyId": "500", "contactIds": ["1"], "objective": "delete_contact"},
    ["not", "an", "object"],
])
def test_an_ill_formed_request_is_refused_before_anything_is_read(client, store, hs, queued, body):
    response = _start(client, body)

    assert response.status_code == 400 and hs.get_contact_for_app.call_count == 0 and queued == []


def test_more_than_ten_contacts_at_once_is_refused(client, store, hs, queued):
    response = _go(client, contacts=[str(n) for n in range(1, 12)])

    assert response.status_code == 400 and "10" in response.json()["error"] and queued == []


def test_ten_contacts_are_accepted(client, store, hs, queued):
    for n in range(4, 11):
        hs.contacts[str(n)] = _contact()

    response = _go(client, contacts=[str(n) for n in range(1, 11)])

    assert response.status_code == 202 and len(queued) == 10


def test_a_contact_listed_twice_is_run_once(client, store, hs, queued):
    response = _go(client, contacts=["1", "1", "2"])

    assert response.status_code == 202 and _items(response) == [("1", "find_email", "started"), ("2", "find_email", "started")] and len(queued) == 2


def test_contact_ids_may_be_numbers_as_hubspot_gives_them(client, store, hs, queued):
    response = _start(client, {"companyId": 500, "contactIds": [1, 2], "objective": "find_email"})

    assert response.status_code == 202 and len(queued) == 2


# ── Asking: what happens ──────────────────────────────────────────────────────

def test_each_picked_contact_gets_a_queued_run_that_remembers_its_company_batch_and_rep(client, store, hs, queued):
    response = _go(client, "find_email", contacts=("1", "2"))
    body = response.json()

    assert response.status_code == 202 and body["ok"] is True and body["started"] == 2
    assert _items(response) == [("1", "find_email", "started"), ("2", "find_email", "started")]
    batch = body["batchId"]
    assert len(batch) == 32
    for item in body["items"]:
        record = store.get_run(item["run"]["runId"])
        assert record["company_id"] == "500" and record["batch_id"] == batch and record["objective"] == "find_email"
        assert record["requested_by_id"] == "77" and record["requested_by"] == "rep@example.org" and record["status"] == "queued"
    assert sorted(queued) == sorted(i["run"]["runId"] for i in body["items"])


def test_every_run_takes_one_place_in_the_reps_cap(client, store, hs, queued):
    body = _go(client, contacts=("1", "2", "3")).json()

    assert body["quota"] == {"cap": 25, "used": 3, "remaining": 22}


def test_the_rep_is_the_one_the_signed_query_names_not_one_in_the_body(client, store, hs, queued):
    body = {"companyId": "500", "contactIds": ["1"], "objective": "find_email", "userId": "1", "requestedBy": "boss@example.org"}

    response = _start(client, body)

    record = store.get_run(response.json()["items"][0]["run"]["runId"])
    assert record["requested_by_id"] == "77" and record["requested_by"] == "rep@example.org"


def test_enrich_plans_each_contact_on_its_own(client, store, hs, queued):
    hs.contacts["2"] = _contact(firstname="Sam", lastname="Ortiz", email="sam.ortiz@example.org", email_verification_result="ok")
    hs.contacts["3"] = _contact(firstname="Kim", lastname="Day", **_NOTHING_MISSING)

    response = _go(client, "enrich", contacts=("1", "2", "3"))
    body = response.json()

    assert response.status_code == 202
    assert _items(response) == [
        ("1", "find_email", "started"), ("1", "find_mobile", "started"),
        ("2", "find_mobile", "started"),
        ("3", "enrich", "skipped"),
    ]
    assert body["items"][3]["reason"] == "Nothing is missing." and body["items"][3]["run"] is None
    assert body["started"] == 3 and body["quota"]["used"] == 3 and len(queued) == 3


def test_enrich_never_stores_enrich_as_a_run_only_the_steps(client, store, hs, queued):
    _go(client, "enrich", contacts=("1",))

    assert sorted(store.get_run(rid)["objective"] for rid in queued) == ["find_email", "find_mobile"]


def test_a_contact_the_objective_is_closed_to_is_skipped_with_the_reason_and_costs_nothing(client, store, hs, queued):
    hs.contacts["2"] = _contact(firstname="Sam", lastname="Ortiz", email="sam.ortiz@example.org")

    response = _go(client, "find_email", contacts=("1", "2"))
    body = response.json()

    assert _items(response) == [("1", "find_email", "started"), ("2", "find_email", "skipped")]
    assert body["items"][1]["reason"] == "Already has an email. Use Verify email."
    assert body["quota"]["used"] == 1 and len(queued) == 1


def test_a_contact_not_linked_to_the_company_is_never_run(client, store, hs, queued):
    hs.links["2"] = ["999"]

    response = _go(client, contacts=("1", "2"))
    body = response.json()

    assert _items(response) == [("1", "find_email", "started"), ("2", "find_email", "skipped")]
    assert body["items"][1]["reason"] == "Not linked to this company." and len(queued) == 1
    assert store.recent_runs("2") == []


def test_a_contact_hubspot_cannot_read_is_reported_and_the_others_go_ahead(client, store, hs, queued):
    original = hs.get_contact_for_app.side_effect

    def read(cid):
        if cid == "1":
            raise HubSpotError("boom: secret detail", 500)
        return original(cid)

    hs.get_contact_for_app.side_effect = read

    response = _go(client, contacts=("1", "2"))
    body = response.json()

    assert _items(response) == [("1", "find_email", "failed"), ("2", "find_email", "started")]
    assert "secret detail" not in response.text and body["quota"]["used"] == 1


def test_the_same_thing_asked_of_the_same_contact_within_a_day_is_the_earlier_run_and_costs_nothing(client, store, hs, queued):
    first = _go(client, contacts=("1",)).json()["items"][0]["run"]["runId"]

    again = _go(client, contacts=("1", "2")).json()

    assert [(i["contactId"], i["status"]) for i in again["items"]] == [("1", "reused"), ("2", "started")]
    assert again["items"][0]["run"]["runId"] == first
    assert again["quota"]["used"] == 2 and len(queued) == 2


def test_a_run_from_the_contact_card_counts_as_the_earlier_run_too(client, store, hs, queued):
    store.create_run({"run_id": "e" * 32, "contact_id": "1", "objective": "find_email", "requested_by_id": "5", "requested_by": "other@example.org",
                      "portal_id": "1234", "created_at": _NOW - timedelta(hours=2)})

    response = _go(client, contacts=("1",))

    assert response.status_code == 202 and _items(response) == [("1", "find_email", "reused")] and queued == []


def test_a_failed_earlier_run_does_not_stand_in_for_a_new_one(client, store, hs, queued):
    store.create_run({"run_id": "e" * 32, "contact_id": "1", "objective": "find_email", "requested_by_id": "5", "requested_by": "",
                      "portal_id": "1234", "created_at": _NOW - timedelta(hours=2), "status": "failed"})

    assert _items(_go(client, contacts=("1",))) == [("1", "find_email", "started")]


def test_when_nothing_can_be_started_and_nothing_was_reused_it_says_so_with_the_reasons(client, store, hs, queued):
    for cid in ("1", "2"):
        hs.contacts[cid] = _contact(email="x@example.org")

    response = _go(client, "find_email", contacts=("1", "2"))

    assert response.status_code == 409 and response.json()["ok"] is False
    assert [i["status"] for i in response.json()["items"]] == ["skipped", "skipped"]
    assert queued == [] and response.json()["quota"]["used"] == 0


# ── The cap ───────────────────────────────────────────────────────────────────

def test_a_request_that_needs_more_places_than_are_left_starts_nothing(client, store, hs, queued):
    for _ in range(23):
        store.take_place("77", "2026-10-26", 25, _NOW)

    response = _go(client, contacts=("1", "2", "3"))
    body = response.json()

    assert response.status_code == 429 and body["ok"] is False
    assert body["quota"] == {"cap": 25, "used": 23, "remaining": 2}
    assert "3" in body["error"] and "2" in body["error"]
    assert queued == [] and store.runs_for_company("500") == []


def test_a_rep_with_no_runs_left_is_told_so_not_how_many_the_request_needed(client, store, hs, queued):
    for _ in range(25):
        store.take_place("77", "2026-10-26", 25, _NOW)

    response = _go(client, contacts=("1", "2"))

    assert response.status_code == 429 and response.json()["error"] == "You have used all 25 runs for today."


def test_a_request_that_needs_exactly_the_places_left_goes_ahead(client, store, hs, queued):
    for _ in range(22):
        store.take_place("77", "2026-10-26", 25, _NOW)

    response = _go(client, contacts=("1", "2", "3"))

    assert response.status_code == 202 and response.json()["quota"]["remaining"] == 0


def test_the_cap_is_counted_for_this_rep_only(client, store, hs, queued):
    for _ in range(25):
        store.take_place("88", "2026-10-26", 25, _NOW)

    assert _go(client, contacts=("1",)).status_code == 202


def test_a_run_that_cannot_be_queued_is_marked_failed_and_gives_its_place_back(client, store, hs, monkeypatch):
    calls = []

    def enqueue(run_id):
        calls.append(run_id)
        if len(calls) == 2:
            raise RuntimeError("queue is down")

    monkeypatch.setattr(webhook_server, "_enqueue_app_run", enqueue)

    response = _go(client, contacts=("1", "2", "3"))
    body = response.json()

    assert response.status_code == 202
    assert [i["status"] for i in body["items"]] == ["started", "failed", "started"]
    assert body["items"][1]["run"]["status"] == "failed" and "could not be queued" in body["items"][1]["run"]["message"]
    assert body["started"] == 2 and body["quota"]["used"] == 2


def test_when_nothing_can_be_queued_the_answer_is_a_502_and_nothing_is_spent(client, store, hs, monkeypatch):
    def down(run_id):
        raise RuntimeError("queue is down")

    monkeypatch.setattr(webhook_server, "_enqueue_app_run", down)

    response = _go(client, contacts=("1", "2"))

    assert response.status_code == 502 and response.json()["quota"]["used"] == 0


# ── A queued run is the same run the contact card makes ───────────────────────

def test_a_queued_company_run_is_done_by_the_same_worker_as_a_contact_run(client, store, hs, queued, monkeypatch):
    import contact_objectives

    seen = []
    monkeypatch.setattr(contact_objectives, "run_objective", lambda objective, contact_id, *, hs, requested_by: (
        seen.append((objective, contact_id, requested_by)) or contact_objectives.Outcome("done", "found", "Email added.", ["email"])))
    run_id = _go(client, contacts=("1",)).json()["items"][0]["run"]["runId"]

    assert webhook_server._execute_app_run(run_id) == "done"

    assert seen == [("find_email", "1", "rep@example.org")]
    assert store.get_run(run_id)["status"] == "done" and app_run_store.run_view(store.get_run(run_id))["companyId"] == "500"
