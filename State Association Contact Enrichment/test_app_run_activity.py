"""
The trail of a HubSpot-app run, as the service worker and the status routes handle it.

While a run is being done, whatever the vendor code notes (run_trail.note) is stored on the run record at once, so a card
that asks again a moment later sees it; the status routes send it with the run; and storing a step can never fail a run.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_app_run_activity.py" -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import contact_objectives
import run_trail
import webhook_server
from test_app_endpoints import _NOW, _clock, _get_status, _make, client, hs, queued, store, tasks_ok  # noqa: F401  (fixtures)


def _work(monkeypatch, body):
    """Make the objective do *body(objective, contact_id)*, and return what it is to be called with."""
    monkeypatch.setattr(webhook_server.contact_objectives, "run_objective",
                        lambda objective, contact_id, *, hs, requested_by: body(objective, contact_id))


def _texts(store, run_id):
    return [s["text"] for s in store.get_run(run_id)["steps"]]


def test_what_the_vendor_code_notes_is_stored_on_the_run(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)

    def body(objective, contact_id):
        run_trail.note("fullenrich", "Asked FullEnrich for a work email", "info")
        run_trail.note("millionverifier", "MillionVerifier says the address is ok", "ok")
        return contact_objectives.Outcome("done", "found", "Email added.", ["email"])
    _work(monkeypatch, body)

    client.post("/app/run", json={"run_id": run_id})

    steps = store.get_run(run_id)["steps"]
    assert [(s["vendor"], s["result"], s["text"]) for s in steps] == [
        ("fullenrich", "info", "Asked FullEnrich for a work email"),
        ("millionverifier", "ok", "MillionVerifier says the address is ok"),
    ]


def test_a_step_is_stored_while_the_run_is_still_going_not_when_it_ends(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)
    seen = {}

    def body(objective, contact_id):
        run_trail.note("fullenrich", "Asked FullEnrich", "info")
        mid = store.get_run(run_id)
        seen.update(status=mid["status"], steps=[s["text"] for s in mid["steps"]])
        return contact_objectives.Outcome("done", "not_found", "Nothing found.", [])
    _work(monkeypatch, body)

    client.post("/app/run", json={"run_id": run_id})

    assert seen == {"status": "running", "steps": ["Asked FullEnrich"]}


def test_each_run_keeps_only_its_own_steps(client, store, hs, tasks_ok, monkeypatch):
    first, second = _make(store, run_id="a" * 32), _make(store, run_id="b" * 32)
    _work(monkeypatch, lambda objective, contact_id: run_trail.note("service", f"for {objective}") or contact_objectives.Outcome("done", "found", "x", []))

    client.post("/app/run", json={"run_id": first})
    client.post("/app/run", json={"run_id": second})

    assert _texts(store, first) == ["for find_email"] and _texts(store, second) == ["for find_email"]


def test_a_note_made_after_the_run_ended_is_not_added_to_it(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)
    _work(monkeypatch, lambda objective, contact_id: contact_objectives.Outcome("done", "found", "x", []))

    client.post("/app/run", json={"run_id": run_id})
    run_trail.note("service", "later, from something else")

    assert _texts(store, run_id) == []


def test_a_store_that_cannot_take_a_step_does_not_fail_the_run(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)

    def broken(run_id, step):
        raise RuntimeError("Firestore is down")
    monkeypatch.setattr(store, "append_step", broken)
    _work(monkeypatch, lambda objective, contact_id: run_trail.note("fullenrich", "x") or contact_objectives.Outcome("done", "found", "Email added.", ["email"]))

    response = client.post("/app/run", json={"run_id": run_id})

    assert response.status_code == 200
    assert store.get_run(run_id)["status"] == "done" and store.get_run(run_id)["outcome"] == "found"


def test_a_crash_leaves_a_failed_step_that_says_where_it_stopped_without_the_error_text(client, store, hs, tasks_ok, monkeypatch):
    run_id = _make(store)

    def body(objective, contact_id):
        run_trail.note("fullenrich", "Asked FullEnrich")
        raise RuntimeError("secret detail jane.doe@example.com")
    _work(monkeypatch, body)

    client.post("/app/run", json={"run_id": run_id})

    steps = store.get_run(run_id)["steps"]
    assert steps[0]["text"] == "Asked FullEnrich"
    assert steps[-1]["result"] == "failed" and steps[-1]["vendor"] == "service"
    assert "secret detail" not in str(steps) and "jane.doe" not in str(steps)
    # Exactly the fixed sentence: a longer one would be cut at 160 characters, hiding the very text this guards against.
    assert steps[-1]["text"] == "The run stopped on an unexpected error. Nothing was changed."


def test_the_status_route_sends_each_runs_steps(client, store, hs, queued):
    store.create_run({"run_id": "a" * 32, "contact_id": "42", "objective": "find_email", "requested_by_id": "77", "requested_by": "rep@example.org",
                      "portal_id": "1234", "created_at": _NOW})
    store.append_step("a" * 32, {"at": "2026-10-26T16:00:01.000Z", "vendor": "fullenrich", "result": "empty", "text": 'Searched "Administrator": 0 people'})

    runs = _get_status(client).json()["runs"]

    assert runs[0]["steps"] == [{"at": "2026-10-26T16:00:01.000Z", "vendor": "fullenrich", "result": "empty", "text": 'Searched "Administrator": 0 people'}]
