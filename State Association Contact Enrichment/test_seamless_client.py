"""
Seamless: phone numbers, and an email when FullEnrich finds none.

Seamless is the phone source. The outbound repo measured why: a phone rides in the
same research credit that returns the address (a 96%-confidence mobile for one
credit), where FullEnrich charges ten for a phone. Its emails are weaker: composed
from the corporate pattern when Seamless has no observed one, 74% of them came back
invalid, and a loose company match returned real, verified mailboxes at strangers. So
the email is a fallback, and everything it returns is held to the same identity and
verification gates as any vendor address.

The API is two-phase and asynchronous (submit, then poll), so the fake server below
speaks both phases and records every request. A person is researched BY NAME against
an exact company domain: a search by name is matched strictly and returns nothing for
people who are demonstrably in the index.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_seamless_client.py" -v
"""

import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import seamless_client as sl


def _contact(**over) -> dict:
    """A researched contact as Seamless returns it, at the facility by default."""
    base = {
        "firstName": "Jane", "lastName": "Doe", "title": "Administrator",
        "companyName": "Valley View Care Center", "companyDomain": "valleyview.org",
        "email1": "jane.doe@valleyview.org", "email1TotalAI": "98%", "email1EmailAI": "valid",
        "contactPhone1": "3307020110", "contactPhone1TotalAI": "96%", "contactPhone1DataType": "mobile",
        "lIProfileUrl": "https://www.linkedin.com/in/janedoe",
    }
    base.update(over)
    return base


class _FakeSeamless:
    """Local stand-in for api.seamless.ai: submit research, then poll for it."""

    def __init__(self, polls=None, submit_statuses=(200,), credits_header="900", reset_in=0):
        # polls: one entry per GET poll, in order, then the last repeats.
        #   a dict is the JSON body, an int is a bare HTTP status.
        self.polls = list(polls) if polls is not None else [{"success": True, "data": [
            {"requestId": "r1", "status": "done", "contact": _contact()}]}]
        self.submit_statuses = list(submit_statuses)
        self.credits_header = credits_header
        self.reset_in = reset_in
        self.submits: list[dict] = []
        self.poll_count = 0
        self.auth: list[dict] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, payload):
                body = json.dumps(payload).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("X-PublicAPI-Credits", outer.credits_header)
                    if status == 429:
                        import time as _t
                        self.send_header("X-RateLimit-Reset", str(int(_t.time()) + outer.reset_in))
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                outer.submits.append(json.loads(self.rfile.read(length) or b"{}"))
                outer.auth.append({"Token": self.headers.get("Token"), "Authorization": self.headers.get("Authorization")})
                status = outer.submit_statuses.pop(0) if outer.submit_statuses else 200
                if status == 200:
                    return self._send(200, {"requestIds": ["r1"]})
                if status == 422:
                    return self._send(422, {"error": "insufficientCredits"})
                return self._send(status, {"error": f"status {status}"})

            def do_GET(self):
                if not urlparse(self.path).path.endswith("/poll"):
                    return self._send(404, {})
                outer.poll_count += 1
                entry = outer.polls[min(outer.poll_count - 1, len(outer.polls) - 1)]
                if isinstance(entry, int):
                    return self._send(entry, {"error": f"status {entry}"})
                self._send(200, entry)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.base = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=lambda: self._server.serve_forever(poll_interval=0.01), daemon=True).start()

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def server(monkeypatch):
    started: list[_FakeSeamless] = []
    slept: list[float] = []

    def start(**kwargs) -> _FakeSeamless:
        s = _FakeSeamless(**kwargs)
        started.append(s)
        monkeypatch.setattr(sl, "_BASE_URL", s.base)
        return s

    monkeypatch.setenv("SEAMLESS_API_KEY", "sl-key")
    monkeypatch.setattr(sl, "_sleep", slept.append)
    monkeypatch.setattr(sl, "_MIN_REQUEST_INTERVAL_S", 0.0)
    sl.reset_state()
    start.slept = slept
    yield start
    sl.reset_state()
    for s in started:
        s.close()


def _lookup(**over):
    args = dict(domain="valleyview.org", company="Valley View Care Center")
    args.update(over)
    return sl.lookup_person("Jane", "Doe", **args)


# ── Asking ────────────────────────────────────────────────────────────────────

def test_without_a_key_nothing_is_asked(monkeypatch):
    monkeypatch.delenv("SEAMLESS_API_KEY", raising=False)

    assert sl.lookup_person("Jane", "Doe", domain="valleyview.org") is None


def test_a_person_is_researched_by_name_against_an_exact_domain_with_the_token_header(server):
    fake = server()

    _lookup()

    assert fake.submits == [{"contacts": [{"contactName": "Jane Doe", "domain": "valleyview.org"}]}]
    assert fake.auth[0]["Token"] == "sl-key"
    assert fake.auth[0]["Authorization"] is None            # Bearer is the OAuth flow and 401s here


def test_the_company_name_is_used_only_when_there_is_no_domain(server):
    fake = server()

    sl.lookup_person("Jane", "Doe", company="Valley View Care Center")

    assert fake.submits[0]["contacts"][0] == {"contactName": "Jane Doe", "companyName": "Valley View Care Center"}


def test_it_waits_for_research_to_finish_and_never_resubmits(server):
    fake = server(polls=[
        {"success": False, "data": [{"requestId": "r1", "status": "queued"}]},
        {"success": False, "data": [{"requestId": "r1", "status": "researching"}]},
        {"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact()}]},
    ])

    result = _lookup()

    assert result["phone"] == "330-702-0110"
    assert len(fake.submits) == 1                           # the credit is spent once
    assert fake.poll_count == 3


def test_research_that_never_finishes_is_given_up_on_without_a_second_purchase(server):
    fake = server(polls=[{"success": False, "data": [{"requestId": "r1", "status": "researching"}]}])

    assert _lookup() is None
    assert len(fake.submits) == 1


# ── Who came back ─────────────────────────────────────────────────────────────

def test_a_different_person_is_not_accepted(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done",
                                              "contact": _contact(firstName="Bob", lastName="Smith")}]}])

    assert _lookup() is None


def test_a_diminutive_given_name_still_matches_the_same_person(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done",
                                              "contact": _contact(firstName="Jennifer")}]}])

    assert sl.lookup_person("Jen", "Doe", domain="valleyview.org")["phone"] == "330-702-0110"


def test_a_person_now_at_another_organisation_yields_no_data_and_says_where(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done",
                                              "contact": _contact(companyName="Acme Staffing", companyDomain="acmestaffing.com")}]}])

    result = _lookup()

    assert result["moved"] is True
    assert result["company"] == "Acme Staffing"
    assert result["phone"] == "" and result["email"] == ""


def test_an_organisation_that_cannot_be_confirmed_yields_no_data(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done",
                                              "contact": _contact(companyName="", companyDomain="")}]}])

    result = _lookup()

    assert result["moved"] is False
    assert result["phone"] == "" and result["email"] == ""


def test_the_operators_own_other_domain_counts_as_the_same_organisation(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done",
                                              "contact": _contact(companyName="Genesis Healthcare", companyDomain="genesishcc.com")}]}])

    result = _lookup(domain="edenpark.org", company="Eden Park Nursing Center", operator_name="Genesis Healthcare")

    assert result["moved"] is False
    assert result["phone"] == "330-702-0110"


def test_a_duplicate_stub_is_followed_to_the_original_research(server):
    fake = server(polls=[
        {"success": True, "data": [{"requestId": "r1", "status": "duplicate", "originalRequestId": "r0"}]},
        {"success": True, "data": [{"requestId": "r0", "status": "done", "contact": _contact()}]},
    ])

    result = _lookup()

    assert result["phone"] == "330-702-0110"
    assert len(fake.submits) == 1


# ── The phone ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("over, expected", [
    ({}, "330-702-0110"),
    ({"contactPhone1": "3307020110129"}, "330-702-0110 x129"),          # extension glued on, no separator
    ({"contactPhone1": "+1 (330) 702-0110"}, "330-702-0110"),
    ({"contactPhone1": "13307020110"}, "330-702-0110"),
])
def test_phone_numbers_are_formatted_for_dialling(server, over, expected):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact(**over)}]}])

    assert _lookup()["phone"] == expected


@pytest.mark.parametrize("over, reason", [
    ({"contactPhone1DataType": "main"}, "switchboard"),                 # the facility line, shared by everyone there
    ({"isDncAll": True}, "do-not-call"),
    ({"contactPhone1IsDnc": True}, "do-not-call"),
    ({"isDncSome": True}, "do-not-call"),                               # some number is barred; which one is unsaid
    ({"contactPhone1": "441189778600"}, "US"),                          # not a North American number
    ({"contactPhone1": "12345"}, "US"),
    ({"contactPhone1TotalAI": "40%"}, "confidence"),
])
def test_a_phone_that_cannot_be_dialled_is_rejected_with_the_reason(server, over, reason):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact(**over)}]}])

    result = _lookup()

    assert result["phone"] == ""
    assert reason in result["phone_rejected"]
    assert result["email"] == "jane.doe@valleyview.org"               # the email is judged on its own


def test_an_unscored_phone_is_accepted_because_absence_of_a_score_is_not_a_low_score(server):
    contact = _contact()
    del contact["contactPhone1TotalAI"]
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": contact}]}])

    assert _lookup()["phone"] == "330-702-0110"


# ── The email ─────────────────────────────────────────────────────────────────

def test_the_best_scoring_email_is_chosen_not_the_first(server):
    # A real row carried email1 at 0% and email2 at 98% on the same person.
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact(
        email1="guess@valleyview.org", email1TotalAI="0%", email2="jane.doe@valleyview.org", email2TotalAI="98%")}]}])

    result = _lookup()

    assert result["email"] == "jane.doe@valleyview.org"
    assert result["email_confidence"] == 98.0


def test_an_email_below_the_confidence_floor_is_rejected_but_the_phone_is_kept(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact(email1TotalAI="30%")}]}])

    result = _lookup()

    assert result["email"] == ""
    assert "confidence" in result["email_rejected"]
    assert result["phone"] == "330-702-0110"


def test_an_email_seamless_itself_calls_invalid_is_rejected(server):
    server(polls=[{"success": True, "data": [{"requestId": "r1", "status": "done", "contact": _contact(email1EmailAI="invalid")}]}])

    assert _lookup()["email"] == ""


# ── Limits, credits and failures ──────────────────────────────────────────────

@pytest.mark.parametrize("status", [401, 403])
def test_a_rejected_key_stops_asking_and_says_so_once(server, caplog, status):
    fake = server(submit_statuses=[status])

    with caplog.at_level(logging.WARNING):
        assert _lookup() is None
        assert sl.lookup_person("John", "Roe", domain="valleyview.org") is None

    assert len(fake.submits) == 1
    flagged = [r for r in caplog.records if getattr(r, "json_fields", {}).get("alert") == "seamless_unavailable"]
    assert len(flagged) == 1


def test_running_out_of_credits_arrives_as_a_422_and_stops_asking(server, caplog):
    fake = server(submit_statuses=[422])

    with caplog.at_level(logging.WARNING):
        assert _lookup() is None
        assert sl.lookup_person("John", "Roe", domain="valleyview.org") is None

    assert len(fake.submits) == 1
    assert [r for r in caplog.records if getattr(r, "json_fields", {}).get("alert") == "seamless_unavailable"]


def test_a_429_on_submit_waits_for_the_reset_the_api_reports_then_repeats(server):
    fake = server(submit_statuses=[429, 200], reset_in=7)

    result = _lookup()

    assert result["phone"] == "330-702-0110"
    assert len(fake.submits) == 2                           # a 429 was not processed, so repeating is safe
    assert any(5 <= w <= 9 for w in server.slept)


def test_a_balance_below_the_floor_stops_further_spending_and_says_so(server, caplog):
    fake = server(credits_header="10")

    with caplog.at_level(logging.WARNING):
        first = _lookup()                                   # this one was already paid for and is used
        second = sl.lookup_person("John", "Roe", domain="valleyview.org")

    assert first["phone"] == "330-702-0110"
    assert second is None
    assert len(fake.submits) == 1
    assert [r for r in caplog.records if getattr(r, "json_fields", {}).get("alert") == "seamless_credits_low"]


def test_an_unreachable_service_never_raises(monkeypatch):
    monkeypatch.setenv("SEAMLESS_API_KEY", "sl-key")
    monkeypatch.setattr(sl, "_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(sl, "_sleep", lambda s: None)
    sl.reset_state()

    assert sl.lookup_person("Jane", "Doe", domain="valleyview.org") is None


def test_the_same_person_is_researched_once_per_window(server):
    fake = server()

    _lookup()
    _lookup()

    assert len(fake.submits) == 1
