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

    def __init__(self, post_status=200, post_delay=0.0, poll_statuses=(200,)):
        self.post_status = post_status      # HTTP status for POSTs
        self.post_delay = post_delay        # seconds to sit on a POST before replying
        self.poll_statuses = list(poll_statuses)   # one HTTP status per GET poll, in order
        self.posts: list[dict] = []         # JSON body of every POST received
        self.gets: list[str] = []           # path of every GET received
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):   # keep test output pristine
                pass

            def _reply(self, status, payload):
                body = json.dumps(payload).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
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
                self._reply(outer.post_status, {"enrichment_id": "enr-1", "people": []})

            def do_GET(self):
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
        return server

    monkeypatch.setenv("FULLENRICH_API_KEY", "test-key")
    monkeypatch.setattr(fe, "_POLL_INTERVAL", 0)
    yield start
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
