"""
What Seamless, MillionVerifier and BounceBan say back, as the trail of a run records it.

Each vendor client leaves a step when a vendor answers: a status, a result word, a credit balance, the reason a person or
an address was not used. These tests run the real clients against the same fakes their own tests use and pin that the
step says what happened in words for a rep, that a call that could not be made is marked as failed (not as "nothing
found"), and that no name, email address or phone number is in any step.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_vendor_trail.py" -v
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bounceban_client as bb
import email_verifier as ev
import run_trail
import seamless_client as sl
from test_bounceban_client import server as bb_server  # noqa: F401  (fixture: a local BounceBan that counts what it is asked)
from test_email_verifier import _mock_response
from test_seamless_client import _contact, _lookup, server as seamless_server  # noqa: F401  (fixture: a local Seamless)

PERSONAL = ("jane", "doe", "@", "330", "valleyview.org/in")


def recorded(call, vendor):
    steps, box = [], {}
    with run_trail.recording(steps.append):
        box["result"] = call()
        box["failures"] = run_trail.failures(vendor)
    for s in steps:
        assert not any(p in s["text"].lower() for p in PERSONAL), s["text"]
    return steps, box


def texts(steps):
    return [s["text"] for s in steps]


def last(steps):
    return steps[-1]


# ── Seamless ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def test_seamless_with_no_key_says_it_was_not_asked(monkeypatch):
    monkeypatch.delenv("SEAMLESS_API_KEY", raising=False)

    steps, _ = recorded(lambda: sl.lookup_person("Jane", "Doe", domain="valleyview.org"), "seamless")

    assert steps[0]["vendor"] == "seamless" and steps[0]["result"] == "skipped" and "not set up" in steps[0]["text"]


def test_seamless_without_a_name_or_a_facility_says_what_it_needs(seamless_server):
    seamless_server()

    steps, _ = recorded(lambda: sl.lookup_person("Jane", "", domain="valleyview.org"), "seamless")

    assert steps[0]["result"] == "skipped" and "needs a name" in steps[0]["text"]


def test_a_lookup_says_it_asked_who_it_asked_about_and_the_credits_left(seamless_server):
    seamless_server(credits_header="900")

    steps, _ = recorded(lambda: _lookup(), "seamless")

    assert any(t.startswith("Asked Seamless") and "valleyview.org" in t for t in texts(steps))
    assert "Seamless credits left: 900" in texts(steps)


def test_a_lookup_that_took_a_phone_and_an_email_says_how_confident_it_was(seamless_server):
    seamless_server()

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["result"]["phone"] and box["failures"] == 0
    assert last(steps)["result"] == "ok"
    assert "phone accepted" in last(steps)["text"] and "confidence" in last(steps)["text"]


def test_a_person_who_has_moved_on_is_empty_and_says_where_to_look(seamless_server):
    seamless_server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done",
                                                      "contact": _contact(companyName="Some Other Care", companyDomain="other.example")}]}])

    steps, _ = recorded(lambda: _lookup(), "seamless")

    assert last(steps)["result"] == "empty" and "no longer at this organisation" in last(steps)["text"] and "Some Other Care" in last(steps)["text"]


def test_a_different_person_coming_back_is_said_and_nothing_is_used(seamless_server):
    seamless_server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact(firstName="Priya", lastName="Venkataraman")}]}])

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["result"] is None
    assert last(steps)["result"] == "empty" and "different person" in last(steps)["text"]
    assert not any("priya" in t.lower() or "venkat" in t.lower() for t in texts(steps))


@pytest.mark.parametrize("status", [401, 403])
def test_a_rejected_key_is_a_failure_that_says_how_long_seamless_is_paused(seamless_server, status):
    seamless_server(submit_statuses=[status])

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["result"] is None and box["failures"] == 1
    assert last(steps)["result"] == "failed" and f"HTTP {status}" in last(steps)["text"] and "paused for 10 minutes" in last(steps)["text"]


def test_the_next_lookup_while_paused_says_it_is_paused_and_asks_nothing(seamless_server):
    fake = seamless_server(submit_statuses=[401])
    _lookup()
    before = len(fake.submits)

    steps, _ = recorded(lambda: _lookup(), "seamless")

    assert len(fake.submits) == before
    assert steps[0]["result"] == "skipped" and "paused" in steps[0]["text"]


def test_no_credits_is_a_failure_that_says_so(seamless_server):
    seamless_server(submit_statuses=[422])

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["failures"] == 1 and last(steps)["result"] == "failed" and "out of credits" in last(steps)["text"]


def test_a_seamless_server_error_is_a_failure_with_its_status(seamless_server):
    seamless_server(submit_statuses=[500])

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["failures"] == 1 and "HTTP 500" in last(steps)["text"]


def test_a_rate_limited_submit_says_it_is_waiting_and_then_goes_on(seamless_server):
    seamless_server(submit_statuses=[429, 202], reset_in=3)

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert any("rate limited" in t and "waiting" in t for t in texts(steps))
    assert box["result"] is not None


def test_a_research_that_never_finishes_is_a_failure_that_says_the_credit_is_spent(seamless_server, monkeypatch):
    seamless_server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "pending"}]}])
    monkeypatch.setattr(sl, "_POLL_TIMEOUT_S", 10.0)
    monkeypatch.setattr(sl, "_POLL_INTERVAL_S", 5.0)

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["result"] is None and box["failures"] == 1
    assert "not finished after 10 s" in last(steps)["text"] and "credit is spent" in last(steps)["text"]


def test_a_research_that_ended_with_nothing_says_the_status_it_ended_with(seamless_server):
    seamless_server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "notfound"}]}])

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert box["failures"] == 0 and last(steps)["result"] == "empty" and "notfound" in last(steps)["text"]


def test_a_remembered_answer_says_it_was_remembered_and_asks_nothing(seamless_server):
    fake = seamless_server()
    _lookup()
    before = len(fake.submits)

    steps, box = recorded(lambda: _lookup(), "seamless")

    assert len(fake.submits) == before and box["result"] is not None
    assert "earlier" in steps[0]["text"] and steps[0]["result"] == "info"


def test_a_balance_under_the_floor_is_said_and_nothing_is_spent(seamless_server):
    fake = seamless_server(credits_header="10")
    _lookup()
    sl._answers.clear()        # the answer is remembered; the balance is what is being tested
    before = len(fake.submits)

    steps, _ = recorded(lambda: _lookup(), "seamless")

    assert len(fake.submits) == before
    assert steps[0]["result"] == "skipped" and "10 credits left" in steps[0]["text"]


# ── MillionVerifier ──────────────────────────────────────────────────────────────────────────────────────────────────

def _verify(response=None, error=None, key="test-key"):
    steps, box = [], {}
    with patch("email_verifier._session.get") as get, patch.dict(os.environ, {"MILLIONVERIFIER_API_KEY": key} if key else {}, clear=False):
        if not key:
            os.environ.pop("MILLIONVERIFIER_API_KEY", None)
        if error is not None:
            get.side_effect = error
        else:
            get.return_value = response
        with run_trail.recording(steps.append):
            box["result"] = ev.verify_email_millionverifier("jane.doe@facility.com")
            box["failures"] = run_trail.failures("millionverifier")
    for s in steps:
        assert not any(p in s["text"].lower() for p in PERSONAL), s["text"]
    return steps, box


@pytest.mark.parametrize("result, quality, words, tag", [
    ("ok", "good", "deliverable", "ok"),
    ("invalid", "bad", "not deliverable", "ok"),
    ("disposable", "bad", "disposable", "ok"),
    ("catch_all", "risky", "accepts every address", "info"),
    ("unknown", "risky", "would not answer", "info"),
])
def test_the_result_word_is_put_in_words_for_a_rep(result, quality, words, tag, monkeypatch):
    monkeypatch.delenv("BOUNCEBAN_API_KEY", raising=False)

    steps, _ = _verify(_mock_response(result, quality))

    step = next(s for s in steps if s["vendor"] == "millionverifier")
    assert words in step["text"] and step["result"] == tag


def test_the_credits_left_and_the_detail_come_with_the_result(monkeypatch):
    monkeypatch.delenv("BOUNCEBAN_API_KEY", raising=False)

    steps, _ = _verify(_mock_response("invalid", "bad", subresult="no_mailbox", credits=1234))

    text = steps[0]["text"]
    assert "1,234 credits left" in text and "no mailbox" in text


def test_a_check_with_no_key_says_it_was_not_made():
    steps, box = _verify(None, key="")

    assert steps[0]["result"] == "skipped" and "not set up" in steps[0]["text"] and box["failures"] == 0


def test_a_refusal_from_the_account_is_a_failure_with_the_vendors_reason():
    steps, box = _verify(_mock_response("", "", error="insufficient credits"))

    assert box["failures"] == 1 and steps[0]["result"] == "failed"
    assert "refused" in steps[0]["text"] and "insufficient credits" in steps[0]["text"]


def test_a_request_that_could_not_be_made_is_a_failure_with_the_reason():
    class ReadTimeout(Exception):
        pass

    steps, box = _verify(error=ReadTimeout("read timed out"))

    assert box["failures"] == 1 and steps[0]["result"] == "failed" and "timed out" in steps[0]["text"]


def test_an_address_that_is_not_one_leaves_no_step(monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "test-key")
    steps = []
    with run_trail.recording(steps.append):
        ev.verify_email_millionverifier("not-an-address")

    assert steps == []


# ── BounceBan ────────────────────────────────────────────────────────────────────────────────────────────────────────

def _second(address="jane.doe@facility.com"):
    return recorded(lambda: bb.second_opinion(address), "bounceban")


def test_bounceban_with_no_key_says_there_was_no_second_opinion(monkeypatch):
    monkeypatch.delenv("BOUNCEBAN_API_KEY", raising=False)

    steps, _ = _second()

    assert steps[0]["vendor"] == "bounceban" and steps[0]["result"] == "skipped" and "not set up" in steps[0]["text"]


@pytest.mark.parametrize("result, score, words, tag", [
    ("deliverable", 100, "deliverable (score 100)", "ok"),
    ("deliverable", 90, "not guaranteed (score 90)", "info"),
    ("undeliverable", 0, "not deliverable", "ok"),
    ("unknown", 0, "could not settle", "empty"),
])
def test_bounceban_says_what_it_decided_and_how_sure_it_was(bb_server, result, score, words, tag):
    bb_server(result=result, score=score)

    steps, box = _second()

    assert box["failures"] == 0 and last(steps)["result"] == tag and words in last(steps)["text"]


def test_the_credits_bounceban_has_left_are_said(bb_server, monkeypatch):
    bb_server()
    monkeypatch.setattr(bb, "_ask", lambda address, key: {"result": "deliverable", "score": 100, "status": "success", "credits_remaining": 4321})

    steps, _ = _second()

    assert "4,321 credits left" in last(steps)["text"]


@pytest.mark.parametrize("status, reason", [(401, "bad or missing API key"), (403, "out of credits"), (405, "account blocked")])
def test_a_stop_status_is_a_failure_that_says_why_and_that_it_is_paused(bb_server, status, reason):
    bb_server(statuses=[status])

    steps, box = _second()

    assert box["failures"] == 1 and last(steps)["result"] == "failed"
    assert reason in last(steps)["text"] and "paused for 10 minutes" in last(steps)["text"]


def test_the_next_ask_while_paused_says_it_is_paused(bb_server):
    bb_server(statuses=[401])
    bb.second_opinion("jane.doe@facility.com")

    steps, _ = _second("someone.else@facility.com")

    assert steps[0]["result"] == "skipped" and "paused" in steps[0]["text"]


def test_a_check_that_times_out_twice_is_a_failure(bb_server):
    bb_server(statuses=[408, 408])

    steps, box = _second()

    assert box["failures"] == 1 and "did not finish" in last(steps)["text"]
    assert any("slow" in t for t in texts(steps))


def test_a_bounceban_server_error_is_a_failure_with_its_status(bb_server):
    bb_server(statuses=[500])

    steps, box = _second()

    assert box["failures"] == 1 and "HTTP 500" in last(steps)["text"]


def test_a_remembered_answer_says_it_was_remembered(bb_server):
    bb_server()
    bb.second_opinion("jane.doe@facility.com")

    steps, box = _second()

    assert box["result"] is not None and "earlier" in steps[0]["text"] and steps[0]["result"] == "info"


def test_millionverifier_asking_bounceban_leaves_both_steps_in_order(bb_server, monkeypatch):
    bb_server(result="deliverable", score=100)

    steps, box = _verify(_mock_response("catch_all", "risky"))

    assert [s["vendor"] for s in steps][:2] == ["millionverifier", "bounceban"]
    assert box["result"]["second_opinion"] == "bounceban"


# ── Outside a run ────────────────────────────────────────────────────────────────────────────────────────────────────

def test_the_clients_record_nothing_outside_a_run(seamless_server, bb_server):
    seamless_server()
    bb_server()

    assert _lookup() is not None
    assert bb.second_opinion("jane.doe@facility.com") is not None
    assert run_trail.failures() == 0
