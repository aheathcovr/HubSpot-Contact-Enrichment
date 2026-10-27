"""
Tests for fullenrich_client's paid calls: what they ask for, and how often.

FullEnrich charges per field found: a work email is 1 credit, a personal email 3
and a phone 10. It also states that simultaneous requests defeat its own
de-duplication, so a POST that is repeated after the response was lost (a read
timeout) or after a 5xx can bill the same contact twice.

These tests run the real functions against a throwaway local HTTP server and
count the requests that arrive, rather than mocking the session: the property
under test is "how many times did the vendor hear from us".

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_fullenrich_client.py" -v
"""

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fullenrich_client as fe


class _FakeFullEnrich:
    """Local stand-in for app.fullenrich.com that records every request."""

    def __init__(self, post_status=200, post_delay=0.0, poll_statuses=(200,), post_statuses=None,
                 retry_after=None, rate_limit_message="", credits_balance=100, credits_status=200):
        self.post_status = post_status      # HTTP status for POSTs
        self.post_statuses = list(post_statuses) if post_statuses else []   # one per POST, in order, then post_status
        self.post_delay = post_delay        # seconds to sit on a POST before replying
        self.poll_statuses = list(poll_statuses)   # one HTTP status per GET poll, in order
        self.retry_after = retry_after      # Retry-After header on a 429
        self.rate_limit_message = rate_limit_message   # body text on a 429, e.g. "Try again in 1m"
        self.credits_balance = credits_balance
        self.credits_status = credits_status
        self.posts: list[dict] = []         # JSON body of every POST received
        self.gets: list[str] = []           # path of every status poll received
        self.credit_gets = 0                # balance checks received
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):   # keep test output pristine
                pass

            def _reply(self, status, payload):
                if status == 429 and outer.rate_limit_message:
                    payload = {"error": outer.rate_limit_message}
                body = json.dumps(payload).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    if status == 429 and outer.retry_after is not None:
                        self.send_header("Retry-After", str(outer.retry_after))
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass    # the client gave up on a deliberately slow POST

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                outer.posts.append(json.loads(self.rfile.read(length) or b"{}"))
                if outer.post_delay:
                    time.sleep(outer.post_delay)
                status = outer.post_statuses.pop(0) if outer.post_statuses else outer.post_status
                self._reply(status, {"enrichment_id": "enr-1", "people": []})

            def do_GET(self):
                if self.path.endswith("/credits"):
                    outer.credit_gets += 1
                    return self._reply(outer.credits_status, {"balance": outer.credits_balance})
                outer.gets.append(self.path)
                status = outer.poll_statuses.pop(0) if outer.poll_statuses else 200
                if status != 200:
                    return self._reply(status, {"error": "unavailable"})
                self._reply(200, {
                    "status": "FINISHED",
                    "data": [{
                        "contact_info": {
                            "most_probable_work_email": {
                                "email": "jane.doe@facility.com", "status": "DELIVERABLE",
                            },
                        },
                    }],
                })

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.base = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def fake(monkeypatch):
    """Factory: start a fake vendor and point the client's URLs and key at it."""
    started: list[_FakeFullEnrich] = []

    def start(**kwargs) -> _FakeFullEnrich:
        server = _FakeFullEnrich(**kwargs)
        started.append(server)
        monkeypatch.setattr(fe, "_ENRICH_URL", f"{server.base}/bulk")
        monkeypatch.setattr(fe, "_SEARCH_URL", f"{server.base}/search")
        monkeypatch.setattr(fe, "_CREDITS_URL", f"{server.base}/credits")
        return server

    monkeypatch.setenv("FULLENRICH_API_KEY", "test-key")
    monkeypatch.setattr(fe, "_POLL_INTERVAL", 0)
    fe._credits_cache.clear()
    yield start
    fe._credits_cache.clear()
    for server in started:
        server.close()


# ── What a paid enrichment asks for ───────────────────────────────────────────

def test_enrich_requests_work_email_only(fake):
    server = fake()

    result = fe.enrich_contact_info("Jane", "Doe", company_name="Valley View", domain="facility.com")

    assert len(server.posts) == 1
    assert server.posts[0]["data"][0]["enrich_fields"] == ["contact.work_emails"]
    assert result["email"] == "jane.doe@facility.com"
    assert result["email_status"] == "DELIVERABLE"


def test_enrich_result_keeps_the_keys_callers_read(fake):
    fake()

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    # Callers still read personal_email and phone; they are simply never bought.
    assert result["personal_email"] == ""
    assert result["phone"] == ""


# ── Paid POSTs are never repeated ─────────────────────────────────────────────

def test_enrich_post_is_not_repeated_after_a_server_error(fake):
    server = fake(post_status=503)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 1
    assert result["email"] == ""


def test_enrich_post_is_not_repeated_after_a_read_timeout(fake, monkeypatch):
    # The server receives the request, then sits on it past the client's timeout:
    # the request reached the vendor and only the response was lost.
    server = fake(post_delay=1.0)
    monkeypatch.setattr(fe, "_TIMEOUT", 0.3)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 1
    assert result["email"] == ""


def test_people_search_post_is_not_repeated_after_a_server_error(fake):
    server = fake(post_status=503)

    people = fe._search_one_title(api_key="test-key", title="Administrator", domain="facility.com")

    assert len(server.posts) == 1
    assert people == []


# ── Reads are still retried ───────────────────────────────────────────────────

def test_status_poll_is_still_retried_after_a_transient_error(fake):
    # Polling is a free GET; losing a finished, already-paid enrichment to one
    # 503 is the failure retries exist to prevent.
    server = fake(poll_statuses=[503, 200])

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 1
    assert len(server.gets) == 2
    assert result["email"] == "jane.doe@facility.com"


# ── A 429 on a submit was not processed, so it is waited out and repeated ──────

@pytest.fixture
def waits(monkeypatch):
    """Record every wait instead of sleeping; keep only the rate-limit waits (poll interval is 0)."""
    recorded: list[float] = []
    monkeypatch.setattr(fe, "_sleep", recorded.append)
    return lambda: [w for w in recorded if w]


@pytest.fixture
def plain_session(monkeypatch):
    """A session with no urllib3 retries, so a 429 on a GET reaches our own handling at once."""
    import requests
    monkeypatch.setattr(fe, "_session", requests.Session())


def test_a_rate_limited_submit_waits_out_the_limit_and_is_not_lost(fake, waits):
    server = fake(post_statuses=[429, 429, 200], retry_after=7)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 3
    assert waits() == [7.0, 7.0]
    assert result["email"] == "jane.doe@facility.com"


def test_a_submit_that_stays_rate_limited_gives_up_after_a_few_tries(fake, waits):
    server = fake(post_status=429, retry_after=1)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == fe._RATE_LIMIT_ATTEMPTS
    assert result["email"] == ""


def test_the_wait_follows_a_try_again_message_in_the_body_but_never_beyond_the_cap(fake, waits):
    fake(post_statuses=[429, 200], rate_limit_message="Rate limit exceeded. Try again in 1m")

    fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert waits() == [fe._RATE_LIMIT_MAX_WAIT_S]       # 1m = 60s, which is the cap


def test_people_search_waits_out_a_rate_limit_the_same_way(fake, waits):
    server = fake(post_statuses=[429, 200], retry_after=3)

    fe._search_one_title(api_key="test-key", title="Administrator", domain="facility.com")

    assert len(server.posts) == 2
    assert waits() == [3.0]


def test_the_wait_is_read_from_the_header_then_the_body_then_a_default():
    from types import SimpleNamespace as R
    assert fe._retry_after_seconds(R(headers={"Retry-After": "12"}, text="")) == 12.0
    assert fe._retry_after_seconds(R(headers={}, text='{"error":"Try again in 30s"}')) == 30.0
    assert fe._retry_after_seconds(R(headers={}, text="Try again in 2m")) == 120.0
    assert fe._retry_after_seconds(R(headers={}, text="slow down")) == fe._RATE_LIMIT_DEFAULT_WAIT_S
    assert fe._retry_after_seconds(R(headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, text="")) == fe._RATE_LIMIT_DEFAULT_WAIT_S


# ── A 429 on a poll must not abandon an enrichment that is already paid for ────

def test_a_rate_limited_status_poll_waits_and_still_returns_the_paid_result(fake, waits, plain_session):
    server = fake(poll_statuses=[429, 429, 200], retry_after=5)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 1                       # never resubmitted
    assert len(server.gets) == 3
    assert waits() == [5.0, 5.0]
    assert result["email"] == "jane.doe@facility.com"


def test_a_poll_that_stays_rate_limited_gives_up_at_the_cap_rather_than_forever(fake, waits, plain_session):
    server = fake(poll_statuses=[429] * 100, retry_after=60)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 1
    assert len(server.gets) == 4                        # waits of 60, 60, 60 are within the 180s cap; the 4th is not
    assert result["email"] == ""


# ── The balance is checked before spending ─────────────────────────────────────

def test_no_lookup_is_attempted_when_the_balance_is_zero(fake, caplog):
    server = fake(credits_balance=0)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert server.posts == []
    assert result["email"] == ""
    assert "credits exhausted" in caplog.text.lower()


def test_people_search_is_not_attempted_when_the_balance_is_zero(fake):
    server = fake(credits_balance=0)

    people = fe.search_facility_contacts("Valley View", "Bristol", "VA", ["Administrator"], domain="facility.com")

    assert server.posts == []
    assert people == []


def test_a_failing_balance_check_does_not_block_the_lookup(fake, plain_session):
    server = fake(credits_status=500)

    result = fe.enrich_contact_info("Jane", "Doe", domain="facility.com")

    assert len(server.posts) == 1
    assert result["email"] == "jane.doe@facility.com"


def test_the_balance_is_checked_once_per_window_not_once_per_lookup(fake):
    server = fake()

    fe.enrich_contact_info("Jane", "Doe", domain="facility.com")
    fe.enrich_contact_info("John", "Roe", domain="facility.com")

    assert server.credit_gets == 1


# ── Whether a title search can be done at all (the HubSpot app asks before it promises a result) ─────────────────

def test_a_title_search_cannot_be_done_without_an_api_key(monkeypatch):
    monkeypatch.delenv("FULLENRICH_API_KEY", raising=False)

    assert fe.search_unavailable_reason() == "FullEnrich is not set up on the service, so people cannot be searched for."


def test_a_title_search_cannot_be_done_when_the_credits_are_gone(monkeypatch):
    monkeypatch.setenv("FULLENRICH_API_KEY", "test-key")
    monkeypatch.setattr(fe, "_credits_exhausted", lambda api_key: True)

    assert fe.search_unavailable_reason() == "FullEnrich has no credits left, so people cannot be searched for."


def test_a_title_search_can_be_done_with_a_key_and_credits(monkeypatch):
    monkeypatch.setenv("FULLENRICH_API_KEY", "test-key")
    monkeypatch.setattr(fe, "_credits_exhausted", lambda api_key: False)

    assert fe.search_unavailable_reason() is None
