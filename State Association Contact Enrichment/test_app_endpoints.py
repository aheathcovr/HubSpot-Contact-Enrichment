"""
The service's side of the HubSpot app's contact card: /app/contact/status, /app/contact/run and /app/run.

The card calls the first two with hubspot.fetch, which HubSpot signs; /app/run is the Cloud Task that does the work.
These tests drive the real routes with a signed request and fake HubSpot, run store and Cloud Tasks, and pin what
matters: nothing is done for an unsigned request, the rep's identity comes only from the signed query, a repeat request
spends nothing, the cap holds, and a run that cannot be queued gives its place back.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_app_endpoints.py" -v
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app_run_store
import contact_objectives
import hubspot_request_auth as auth
import webhook_server
from hubspot_client import HubSpotError

_SECRET = "test-app-secret"
_NOW = datetime(2026, 10, 26, 16, 0, 0, tzinfo=timezone.utc)
_USER = "userId=77&userEmail=rep%40example.org&portalId=1234&appId=99"
_CONTACT = {
    "firstname": "Pat", "lastname": "Lee", "jobtitle": "Administrator", "email": "", "mobilephone": "", "phone": "",
    "hs_linkedin_url": "", "email_verification_result": "", "email_verification_date": "",
}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("HUBSPOT_WEBHOOK_SECRET", "unused-by-these-tests")   # the server will not start without it
    monkeypatch.setenv("HUBSPOT_APP_CLIENT_SECRET", _SECRET)
    monkeypatch.delenv("HUBSPOT_PORTAL_ID", raising=False)
    monkeypatch.delenv("APP_DAILY_RUN_CAP", raising=False)
    webhook_server._tasks_client = None
    with TestClient(webhook_server.app) as c:
        yield c
    webhook_server._tasks_client = None


@pytest.fixture
def store(monkeypatch):
    s = app_run_store.MemoryRunStore()
    monkeypatch.setattr(webhook_server, "_run_store", lambda: s)
    return s


@pytest.fixture
def hs(monkeypatch):
    h = MagicMock()
    h.get_contact_for_app.return_value = dict(_CONTACT)
    monkeypatch.setattr(webhook_server, "_hubspot", lambda: h)
    return h


@pytest.fixture
def queued(monkeypatch):
    ids = []
    monkeypatch.setattr(webhook_server, "_enqueue_app_run", lambda run_id: ids.append(run_id))
    return ids


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr(webhook_server, "_utcnow", lambda: _NOW)


def _headers(method, url, body=b"", secret=_SECRET, ts=None):
    timestamp = str(int(time.time() * 1000)) if ts is None else str(ts)
    return {
        "X-HubSpot-Signature-v3": auth.compute_signature_v3(secret, method, url, body, timestamp),
        "X-HubSpot-Request-Timestamp": timestamp,
        "Content-Type": "application/json",
    }


def _get_status(client, contact="42", user=_USER, **kw):
    path = f"/app/contact/status?contactId={contact}&{user}"
    return client.get(path, headers=_headers("GET", "http://testserver" + path, **kw))


def _start(client, body, user=_USER, **kw):
    path = f"/app/contact/run?{user}"
    raw = json.dumps(body).encode() if not isinstance(body, bytes) else body
    return client.post(path, content=raw, headers=_headers("POST", "http://testserver" + path, raw, **kw))


def _go(client, objective="find_email", contact="42", **kw):
    return _start(client, {"contactId": contact, "objective": objective}, **kw)


# ── Nobody gets in unsigned ───────────────────────────────────────────────────

@pytest.mark.parametrize("call", ["status", "start"])
def test_an_unsigned_request_is_refused_and_nothing_is_read_or_done(client, store, hs, queued, call):
    if call == "status":
        response = client.get(f"/app/contact/status?contactId=42&{_USER}")
    else:
        response = client.post(f"/app/contact/run?{_USER}", json={"contactId": "42", "objective": "find_email"})

    assert response.status_code == 401
    assert hs.get_contact_for_app.call_count == 0 and queued == [] and store.recent_runs("42") == []


@pytest.mark.parametrize("call", ["status", "start"])
def test_a_request_signed_with_the_wrong_secret_is_refused(client, store, hs, queued, call):
    response = _get_status(client, secret="wrong") if call == "status" else _go(client, secret="wrong")

    assert response.status_code == 401 and queued == []


def test_a_stale_signature_is_refused(client, store, hs):
    response = _get_status(client, ts=int(time.time() * 1000) - 10 * 60 * 1000)

    assert response.status_code == 401


def test_changing_who_asked_after_signing_is_refused(client, store, hs, queued):
    path = f"/app/contact/run?{_USER}"
    body = json.dumps({"contactId": "42", "objective": "find_email"}).encode()
    headers = _headers("POST", "http://testserver" + path, body)

    response = client.post(path.replace("userId=77", "userId=1"), content=body, headers=headers)

    assert response.status_code == 401 and queued == []


@pytest.mark.parametrize("call", ["status", "start"])
def test_a_service_with_no_client_secret_answers_503(client, store, hs, queued, monkeypatch, call):
    monkeypatch.delenv("HUBSPOT_APP_CLIENT_SECRET")

    response = _get_status(client) if call == "status" else _go(client)

    assert response.status_code == 503 and response.json()["ok"] is False and queued == []


def test_a_request_from_another_portal_is_refused_when_one_is_expected(client, store, hs, monkeypatch):
    monkeypatch.setenv("HUBSPOT_PORTAL_ID", "9999")

    assert _get_status(client).status_code == 403


def test_the_expected_portal_gets_in(client, store, hs, monkeypatch):
    monkeypatch.setenv("HUBSPOT_PORTAL_ID", "1234")

    assert _get_status(client).status_code == 200


def test_the_work_endpoint_refuses_anything_without_a_cloud_tasks_token(client, store, hs):
    response = client.post("/app/run", json={"run_id": "a" * 32})

    assert response.status_code == 401


def test_a_signed_hubspot_request_cannot_trigger_the_work_endpoint(client, store, hs):
    body = json.dumps({"run_id": "a" * 32}).encode()

    response = client.post("/app/run", content=body, headers=_headers("POST", "http://testserver/app/run", body))

    assert response.status_code == 401


# ── Status ────────────────────────────────────────────────────────────────────

def test_status_shows_the_contact_the_objectives_and_the_quota(client, store, hs):
    hs.get_contact_for_app.return_value = {**_CONTACT, "mobilephone": "405-555-0101"}

    body = _get_status(client).json()

    assert body["ok"] is True
    assert body["contact"] == {"id": "42", "name": "Pat Lee", "title": "Administrator", "email": None, "mobile": "405-555-0101", "emailCheck": None, "emailCheckedOn": None}
    assert [(o["id"], o["available"]) for o in body["objectives"]] == [("find_email", True), ("find_mobile", False), ("verify_email", False)]
    assert body["quota"] == {"cap": 25, "used": 0, "remaining": 25}
    assert body["runs"] == []


def test_status_lists_the_recent_runs_on_this_contact_newest_first(client, store, hs):
    store.create_run({"run_id": "a" * 32, "contact_id": "42", "objective": "find_email", "requested_by_id": "88", "requested_by": "other@example.org", "portal_id": "1234", "created_at": _NOW - timedelta(minutes=5)})
    store.create_run({"run_id": "b" * 32, "contact_id": "42", "objective": "verify_email", "requested_by_id": "77", "requested_by": "rep@example.org", "portal_id": "1234", "created_at": _NOW - timedelta(minutes=1)})
    store.create_run({"run_id": "c" * 32, "contact_id": "99", "objective": "find_email", "requested_by_id": "77", "requested_by": "rep@example.org", "portal_id": "1234", "created_at": _NOW})

    runs = _get_status(client).json()["runs"]

    assert [r["runId"] for r in runs] == ["b" * 32, "a" * 32]
    assert runs[1]["requestedBy"] == "other@example.org"


def test_status_shows_at_most_five_runs(client, store, hs):
    for i in range(8):
        store.create_run({"run_id": f"{i:032x}", "contact_id": "42", "objective": "find_email", "requested_by_id": "77", "requested_by": "", "portal_id": "1", "created_at": _NOW - timedelta(minutes=i)})

    assert len(_get_status(client).json()["runs"]) == 5


def test_status_counts_this_reps_runs_against_the_cap(client, store, hs, monkeypatch):
    monkeypatch.setenv("APP_DAILY_RUN_CAP", "10")
    store.take_place("77", "2026-10-26", 10, _NOW)
    store.take_place("77", "2026-10-26", 10, _NOW)
    store.take_place("88", "2026-10-26", 10, _NOW)

    assert _get_status(client).json()["quota"] == {"cap": 10, "used": 2, "remaining": 8}


@pytest.mark.parametrize("contact", ["", "abc", "4%202", "../1", "1" * 30])   # "4%202" is "4 2", already encoded as a URL needs
def test_status_refuses_a_contact_id_that_is_not_digits(client, store, hs, contact):
    response = _get_status(client, contact=contact)

    assert response.status_code == 400 and hs.get_contact_for_app.call_count == 0


def test_status_says_so_when_hubspot_has_no_such_contact(client, store, hs):
    hs.get_contact_for_app.side_effect = HubSpotError("GET failed: 404", status_code=404)

    assert _get_status(client).status_code == 404


def test_status_says_so_when_hubspot_cannot_be_reached(client, store, hs):
    hs.get_contact_for_app.side_effect = HubSpotError("GET failed: 500", status_code=500)

    response = _get_status(client)

    assert response.status_code == 502 and response.json()["ok"] is False


def test_status_writes_nothing(client, store, hs, queued):
    _get_status(client)

    assert hs.update_contact.call_count == 0 and queued == [] and store.used_today("77", "2026-10-26") == 0


# ── Starting a run ────────────────────────────────────────────────────────────

def test_a_signed_request_starts_a_run_that_is_queued(client, store, hs, queued):
    response = _go(client, "find_email")

    assert response.status_code == 202
    body = response.json()
    assert body["ok"] is True and body["reused"] is False
    assert body["run"]["status"] == "queued" and body["run"]["objective"] == "find_email" and body["run"]["contactId"] == "42"
    assert body["run"]["requestedBy"] == "rep@example.org"
    assert body["quota"] == {"cap": 25, "used": 1, "remaining": 24}
    assert queued == [body["run"]["runId"]]
    record = store.get_run(body["run"]["runId"])
    assert record["requested_by_id"] == "77" and record["portal_id"] == "1234" and record["created_at"] == _NOW


def test_who_asked_comes_from_the_signed_query_never_from_the_body(client, store, hs, queued):
    body = {"contactId": "42", "objective": "find_email", "userId": "1", "userEmail": "boss@example.org", "requestedBy": "boss@example.org"}

    response = _start(client, body)

    record = store.get_run(response.json()["run"]["runId"])
    assert record["requested_by_id"] == "77" and record["requested_by"] == "rep@example.org"


@pytest.mark.parametrize("body", [
    {"contactId": "abc", "objective": "find_email"},
    {"contactId": "", "objective": "find_email"},
    {"objective": "find_email"},
    {"contactId": "42"},
    {"contactId": "42", "objective": "delete_contact"},
    {"contactId": "42", "objective": ""},
    {"contactId": 42, "objective": ["find_email"]},
    [],
    "text",
])
def test_a_request_that_does_not_say_which_contact_and_objective_is_refused(client, store, hs, queued, body):
    response = _start(client, body)

    assert response.status_code == 400 and queued == [] and store.used_today("77", "2026-10-26") == 0


def test_a_body_that_is_not_json_is_refused(client, store, hs, queued):
    response = _start(client, b"not json")

    assert response.status_code == 400 and queued == []


def test_a_contact_id_sent_as_a_number_is_taken_as_that_id(client, store, hs, queued):
    response = _start(client, {"contactId": 42, "objective": "find_email"})

    assert response.status_code == 202 and response.json()["run"]["contactId"] == "42"


def test_an_objective_that_is_not_open_is_refused_with_the_reason_and_costs_nothing(client, store, hs, queued):
    hs.get_contact_for_app.return_value = {**_CONTACT, "email": "pat.lee@example.org"}

    response = _go(client, "find_email")

    assert response.status_code == 409 and "Verify email" in response.json()["error"]
    assert queued == [] and store.used_today("77", "2026-10-26") == 0 and store.recent_runs("42") == []


def test_asking_again_for_the_same_thing_returns_the_earlier_run_and_spends_nothing(client, store, hs, queued):
    first = _go(client, "find_email").json()

    again = _go(client, "find_email")

    assert again.status_code == 202 and again.json()["reused"] is True
    assert again.json()["run"]["runId"] == first["run"]["runId"]
    assert queued == [first["run"]["runId"]]
    assert again.json()["quota"]["used"] == 1


def test_another_rep_asking_for_the_same_thing_is_given_the_same_run(client, store, hs, queued):
    first = _go(client, "find_email").json()

    other = _go(client, "find_email", user="userId=88&userEmail=other%40example.org&portalId=1234&appId=99").json()

    assert other["reused"] is True and other["run"]["runId"] == first["run"]["runId"]
    assert store.used_today("88", "2026-10-26") == 0


def test_a_different_objective_on_the_same_contact_is_a_new_run(client, store, hs, queued):
    _go(client, "find_email")
    hs.get_contact_for_app.return_value = {**_CONTACT, "email": "pat.lee@example.org"}

    second = _go(client, "verify_email")

    assert second.status_code == 202 and second.json()["reused"] is False and len(queued) == 2


def test_a_run_that_failed_is_not_repeated_so_the_rep_can_try_again(client, store, hs, queued):
    first = _go(client, "find_email").json()["run"]["runId"]
    store.claim_run(first, _NOW)
    store.finish_run(first, _NOW, status="failed", outcome="error", message="Seamless did not answer.", wrote=[])

    again = _go(client, "find_email").json()

    assert again["reused"] is False and again["run"]["runId"] != first and len(queued) == 2


def test_a_run_from_a_few_hours_ago_is_repeated(client, store, hs, queued):
    earlier = "b" * 32
    store.create_run({"run_id": earlier, "contact_id": "42", "objective": "find_email", "requested_by_id": "77", "requested_by": "", "portal_id": "1", "created_at": _NOW - timedelta(hours=23)})

    again = _go(client, "find_email").json()

    assert again["reused"] is True and again["run"]["runId"] == earlier and queued == []


def test_a_run_older_than_a_day_is_not_repeated(client, store, hs, queued):
    store.create_run({"run_id": "a" * 32, "contact_id": "42", "objective": "find_email", "requested_by_id": "77", "requested_by": "", "portal_id": "1", "created_at": _NOW - timedelta(hours=25)})

    again = _go(client, "find_email").json()

    assert again["reused"] is False


# ── The cap ───────────────────────────────────────────────────────────────────

def test_a_rep_who_has_used_the_cap_is_told_so_and_nothing_is_queued(client, store, hs, queued, monkeypatch):
    monkeypatch.setenv("APP_DAILY_RUN_CAP", "2")
    assert _go(client, "find_email", contact="1").status_code == 202
    assert _go(client, "find_email", contact="2").status_code == 202

    third = _go(client, "find_email", contact="3")

    assert third.status_code == 429
    assert third.json()["quota"] == {"cap": 2, "used": 2, "remaining": 0}
    assert "2" in third.json()["error"] and len(queued) == 2 and store.recent_runs("3") == []


def test_each_rep_has_a_cap_of_their_own(client, store, hs, queued, monkeypatch):
    monkeypatch.setenv("APP_DAILY_RUN_CAP", "1")
    _go(client, "find_email", contact="1")

    other = _go(client, "find_email", contact="2", user="userId=88&userEmail=other%40example.org&portalId=1234&appId=99")

    assert other.status_code == 202


def test_the_cap_is_per_utc_day(client, store, hs, queued, monkeypatch):
    monkeypatch.setenv("APP_DAILY_RUN_CAP", "1")
    _go(client, "find_email", contact="1")
    monkeypatch.setattr(webhook_server, "_utcnow", lambda: _NOW + timedelta(days=1))

    assert _go(client, "find_email", contact="2").status_code == 202


@pytest.mark.parametrize("value", ["", "abc", "-3", "0"])
def test_a_cap_that_is_not_a_positive_number_means_the_default(client, store, hs, queued, monkeypatch, value):
    monkeypatch.setenv("APP_DAILY_RUN_CAP", value)

    assert _get_status(client).json()["quota"]["cap"] == 25


# ── Things going wrong while starting ─────────────────────────────────────────

def test_a_run_that_cannot_be_queued_gives_its_place_back_and_is_marked_failed(client, store, hs, monkeypatch):
    def boom(run_id):
        raise RuntimeError("Cloud Tasks is down")
    monkeypatch.setattr(webhook_server, "_enqueue_app_run", boom)

    response = _go(client, "find_email")

    assert response.status_code == 502 and "could not be queued" in response.json()["error"].lower()
    assert store.used_today("77", "2026-10-26") == 0
    (run,) = store.recent_runs("42")
    assert run["status"] == "failed" and run["outcome"] == "error"


def test_a_failed_queueing_does_not_block_trying_again(client, store, hs, monkeypatch):
    monkeypatch.setattr(webhook_server, "_enqueue_app_run", lambda run_id: (_ for _ in ()).throw(RuntimeError("down")))
    _go(client, "find_email")
    queued = []
    monkeypatch.setattr(webhook_server, "_enqueue_app_run", lambda run_id: queued.append(run_id))

    again = _go(client, "find_email")

    assert again.status_code == 202 and again.json()["reused"] is False and len(queued) == 1


def test_starting_says_so_when_hubspot_has_no_such_contact(client, store, hs, queued):
    hs.get_contact_for_app.side_effect = HubSpotError("GET failed: 404", status_code=404)

    assert _go(client).status_code == 404 and queued == []


def test_starting_says_so_when_hubspot_cannot_be_reached(client, store, hs, queued):
    hs.get_contact_for_app.side_effect = HubSpotError("GET failed: 500", status_code=500)

    assert _go(client).status_code == 502 and queued == []


# ── Doing the work (Cloud Tasks) ──────────────────────────────────────────────

@pytest.fixture
def tasks_ok(monkeypatch):
    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)


def _make(store, run_id="a" * 32, objective="find_email", by="rep@example.org"):
    store.create_run({"run_id": run_id, "contact_id": "42", "objective": objective, "requested_by_id": "77", "requested_by": by, "portal_id": "1234", "created_at": _NOW})
    return run_id


def test_the_work_endpoint_runs_the_objective_and_records_how_it_went(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)
    seen = {}

    def fake(objective, contact_id, *, hs, requested_by):
        seen.update(objective=objective, contact_id=contact_id, requested_by=requested_by, hs=hs)
        return contact_objectives.Outcome("done", "found", "Email added. MillionVerifier says it is good.", ["email", "email_verification_result"])
    monkeypatch.setattr(webhook_server.contact_objectives, "run_objective", fake)

    response = client.post("/app/run", json={"run_id": run_id})

    assert response.status_code == 200
    assert seen == {"objective": "find_email", "contact_id": "42", "requested_by": "rep@example.org", "hs": hs}
    run = store.get_run(run_id)
    assert run["status"] == "done" and run["outcome"] == "found" and run["wrote"] == ["email", "email_verification_result"]
    assert run["message"].startswith("Email added") and run["started_at"] == _NOW and run["finished_at"] == _NOW


def test_a_rep_with_no_email_on_record_is_named_by_user_id(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store, by="")
    seen = {}
    monkeypatch.setattr(webhook_server.contact_objectives, "run_objective",
                        lambda objective, contact_id, *, hs, requested_by: seen.update(by=requested_by) or contact_objectives.Outcome("done", "skipped", "x"))

    client.post("/app/run", json={"run_id": run_id})

    assert seen["by"] == "HubSpot user 77"


def test_a_run_delivered_twice_is_done_once(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)
    calls = []
    monkeypatch.setattr(webhook_server.contact_objectives, "run_objective",
                        lambda *a, **k: calls.append(1) or contact_objectives.Outcome("done", "found", "x", []))

    first = client.post("/app/run", json={"run_id": run_id})
    second = client.post("/app/run", json={"run_id": run_id})

    assert first.status_code == 200 and second.status_code == 200 and len(calls) == 1


def test_a_run_nobody_made_is_answered_200_so_cloud_tasks_does_not_retry(client, store, hs, tasks_ok):
    response = client.post("/app/run", json={"run_id": "f" * 32})

    assert response.status_code == 200 and response.json()["status"] == "skipped"


@pytest.mark.parametrize("body", [{}, {"run_id": ""}, {"run_id": "not-a-run"}, {"run_id": 5}, []])
def test_the_work_endpoint_refuses_a_body_without_a_run_id(client, store, hs, tasks_ok, body):
    assert client.post("/app/run", json=body).status_code == 400


def test_a_crash_in_the_work_ends_the_run_as_failed_and_still_answers_200(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)

    def boom(*a, **k):
        raise RuntimeError("secret detail")
    monkeypatch.setattr(webhook_server.contact_objectives, "run_objective", boom)

    response = client.post("/app/run", json={"run_id": run_id})

    assert response.status_code == 200
    run = store.get_run(run_id)
    assert run["status"] == "failed" and run["outcome"] == "error" and "secret detail" not in run["message"]


def test_the_whole_path_from_start_to_finish_with_the_real_objective(client, store, hs, tasks_ok, queued, monkeypatch):
    """Start a Verify email run, deliver its task, and read the result back through status."""
    hs.get_contact_for_app.return_value = {**_CONTACT, "email": "pat.lee@example.org"}
    hs.get_contact_company_ids.return_value = []
    monkeypatch.setattr(contact_objectives, "verify_email_millionverifier", lambda email: {"result": "ok", "status": "VERIFIED", "quality": "good"})
    run_id = _go(client, "verify_email").json()["run"]["runId"]

    assert queued == [run_id]
    assert client.post("/app/run", json={"run_id": run_id}).status_code == 200

    run = _get_status(client).json()["runs"][0]
    assert run["status"] == "done" and run["outcome"] == "verified" and "good" in run["message"]
    assert run["wrote"] == ["email_verification_result", "email_verification_date"]
    assert hs.update_contact.call_args.args[0] == "42"
