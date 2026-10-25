"""
BounceBan as the second opinion behind MillionVerifier.

MillionVerifier answers honestly where the mail server does (direct Microsoft 365
rejects unknown recipients), and cannot answer behind a server that accepts
everyone: a catch-all, or a Proofpoint/Barracuda gateway in front of a Microsoft
tenant. The outbound repo measured MillionVerifier `ok` bouncing 0 of 305, and
`catch_all` bouncing 4.7%, with 80% of its unresolved addresses sitting behind such
gateways. BounceBan resolves those without sending mail, so it is asked about
exactly the answers MillionVerifier could not settle (`unknown`, `catch_all`) and
nothing else. Its four answers are translated into MillionVerifier's five words
before anything leaves the module, because the portal property and the outbound
loaders read exactly those words.

Billing rules these tests pin (from the outbound repo's notes): the waterfall
endpoint is used because a 408 retried within 30 minutes is free, the key goes in the
Authorization header raw (no "Bearer"), 401/403/405 mean stop, and each address is
asked at most once.

The service runs without a BounceBan key; the second opinion is simply skipped.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_bounceban_client.py" -v
"""

import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bounceban_client as bb
import email_verifier as ev


class _FakeBounceBan:
    """Local stand-in for api-waterfall.bounceban.com that records every request."""

    def __init__(self, statuses=(200,), result="deliverable", score=100):
        self.statuses = list(statuses)      # one HTTP status per request, in order, then 200
        self.result, self.score = result, score
        self.requests: list[dict] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                parsed = urlparse(self.path)
                outer.requests.append({
                    "params": {k: v[0] for k, v in parse_qs(parsed.query).items()},
                    "authorization": self.headers.get("Authorization"),
                })
                status = outer.statuses.pop(0) if outer.statuses else 200
                if status == 200:
                    payload = {"email": "x@y.com", "result": outer.result, "score": outer.score, "status": "success",
                               "id": "chk-1", "credits_consumed": 1}
                elif status == 408:
                    payload = {"id": "chk-1", "error": "timeout"}
                else:
                    payload = {"error": f"status {status}"}
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/v1/verify/single"
        threading.Thread(target=lambda: self._server.serve_forever(poll_interval=0.01), daemon=True).start()

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def server(monkeypatch):
    """Factory: start a fake BounceBan with a key set, sleeps recorded, and no remembered state."""
    started: list[_FakeBounceBan] = []
    slept: list[float] = []

    def start(**kwargs) -> _FakeBounceBan:
        s = _FakeBounceBan(**kwargs)
        started.append(s)
        monkeypatch.setattr(bb, "_WATERFALL_URL", s.url)
        return s

    monkeypatch.setenv("BOUNCEBAN_API_KEY", "bb-key")
    monkeypatch.setattr(bb, "_sleep", slept.append)
    bb.reset_state()
    start.slept = slept
    yield start
    bb.reset_state()
    for s in started:
        s.close()


# ── Its four answers, in MillionVerifier's five words ─────────────────────────

@pytest.mark.parametrize("row, expected", [
    ({"result": "deliverable", "score": 100, "status": "success"}, "ok"),
    ({"result": "deliverable", "score": 99,  "status": "success"}, "ok"),         # the band BounceBan guarantees
    ({"result": "deliverable", "score": 98,  "status": "success"}, "catch_all"),
    ({"result": "deliverable", "status": "success"},               "catch_all"),   # no score is not a good score
    ({"result": "risky",       "score": 60,  "status": "success"}, "unknown"),
    ({"result": "undeliverable", "score": 0, "status": "success"}, "invalid"),
    ({"result": "unknown",     "status": "success"},               "unknown"),
    ({"result": "deliverable", "score": 100, "status": "timeout"}, "unknown"),     # an unfinished check decides nothing
    ({"result": "something-new", "status": "success"},             "unknown"),     # never raises, never trusts
])
def test_translation(row, expected):
    assert bb.translate(row) == expected


# ── Asking ────────────────────────────────────────────────────────────────────

def test_without_a_key_nothing_is_asked(monkeypatch):
    monkeypatch.delenv("BOUNCEBAN_API_KEY", raising=False)

    assert bb.second_opinion("jane@facility.com") is None


def test_the_key_goes_in_the_authorization_header_raw_and_the_waterfall_endpoint_is_used(server):
    fake = server()

    answer = bb.second_opinion("jane@facility.com")

    assert answer["result"] == "ok"
    assert fake.requests[0]["authorization"] == "bb-key"            # not "Bearer bb-key"
    assert fake.requests[0]["params"]["email"] == "jane@facility.com"
    assert "api-waterfall.bounceban.com" in bb._DEFAULT_WATERFALL_URL


def test_a_408_is_retried_once_because_that_retry_is_free_then_given_up_on(server):
    fake = server(statuses=[408, 408, 408])

    answer = bb.second_opinion("jane@facility.com")

    assert answer is None
    assert len(fake.requests) == bb._ATTEMPTS == 2


def test_a_408_followed_by_an_answer_uses_the_answer(server):
    fake = server(statuses=[408, 200], result="undeliverable", score=0)

    assert bb.second_opinion("jane@facility.com")["result"] == "invalid"
    assert len(fake.requests) == 2


def test_a_429_is_waited_out_and_repeated(server):
    fake = server(statuses=[429, 200])

    answer = bb.second_opinion("jane@facility.com")

    assert answer["result"] == "ok"
    assert len(fake.requests) == 2
    assert server.slept == [2.0]


@pytest.mark.parametrize("status", [401, 403, 405])
def test_a_key_credit_or_block_failure_stops_asking_and_says_so(server, caplog, status):
    fake = server(statuses=[status])

    with caplog.at_level(logging.WARNING):
        first = bb.second_opinion("jane@facility.com")
        second = bb.second_opinion("john@facility.com")     # a different address, same broken account

    assert first is None and second is None
    assert len(fake.requests) == 1                          # not hammered
    flagged = [r for r in caplog.records if getattr(r, "json_fields", {}).get("alert") == "bounceban_unavailable"]
    assert len(flagged) == 1


def test_a_server_error_on_one_address_does_not_stop_the_next(server):
    fake = server(statuses=[500, 200])

    assert bb.second_opinion("jane@facility.com") is None
    assert bb.second_opinion("john@facility.com")["result"] == "ok"
    assert len(fake.requests) == 2


def test_an_unreachable_service_never_raises(monkeypatch):
    monkeypatch.setenv("BOUNCEBAN_API_KEY", "bb-key")
    monkeypatch.setattr(bb, "_WATERFALL_URL", "http://127.0.0.1:1/v1/verify/single")   # nothing listens
    bb.reset_state()

    assert bb.second_opinion("jane@facility.com") is None


def test_a_settled_answer_is_not_bought_twice(server):
    fake = server()

    bb.second_opinion("jane@facility.com")
    bb.second_opinion("Jane@Facility.com")

    assert len(fake.requests) == 1


def test_an_unknown_answer_is_asked_again_next_time(server):
    fake = server(result="unknown", score=0)

    bb.second_opinion("jane@facility.com")
    bb.second_opinion("jane@facility.com")

    assert len(fake.requests) == 2


def test_a_low_balance_is_reported_from_the_answer_with_no_extra_call(server, caplog):
    fake = server()
    fake_answer = {"email": "x@y.com", "result": "deliverable", "score": 100, "status": "success",
                   "id": "chk-1", "credits_consumed": 1, "credits_remaining": 40}
    with patch.object(bb._session, "get") as get:
        get.return_value.status_code = 200
        get.return_value.json.return_value = fake_answer
        with caplog.at_level(logging.WARNING):
            answer = bb.second_opinion("jane@facility.com")

    assert answer["result"] == "ok"
    flagged = [r for r in caplog.records if getattr(r, "json_fields", {}).get("alert") == "bounceban_credits_low"]
    assert len(flagged) == 1
    assert get.call_count == 1                              # the balance came with the answer
    assert fake.requests == []


def test_a_healthy_balance_is_not_reported(server, caplog):
    answer = {"email": "x@y.com", "result": "deliverable", "score": 100, "status": "success", "credits_remaining": 6257}
    with patch.object(bb._session, "get") as get:
        get.return_value.status_code = 200
        get.return_value.json.return_value = answer
        with caplog.at_level(logging.WARNING):
            bb.second_opinion("jane@facility.com")

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


# ── Behind MillionVerifier ────────────────────────────────────────────────────

def _mv_says(result, quality="risky"):
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"result": result, "quality": quality, "free": False, "role": False,
                                  "didyoumean": "", "error": ""}
    return patch.object(ev._session, "get", return_value=response)


@pytest.fixture
def mv_key(monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "mv-key")


def test_a_catch_all_that_bounceban_confirms_is_upgraded_to_verified(server, mv_key):
    fake = server(result="deliverable", score=100)
    with _mv_says("catch_all"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["result"] == "ok"
    assert result["status"] == "VERIFIED"
    assert result["second_opinion"] == "bounceban"
    assert result["mv_result"] == "catch_all"
    assert len(fake.requests) == 1


def test_an_unknown_that_bounceban_says_is_undeliverable_becomes_invalid(server, mv_key):
    server(result="undeliverable", score=0)
    with _mv_says("unknown"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["result"] == "invalid"
    assert result["status"] == "INVALID"


def test_when_bounceban_is_itself_inconclusive_millionverifiers_answer_stands(server, mv_key):
    server(result="risky", score=50)
    with _mv_says("catch_all"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["result"] == "catch_all"              # not downgraded to unknown
    assert result["status"] == "RISKY"
    assert "second_opinion" not in result


def test_an_ok_from_millionverifier_is_never_second_guessed(server, mv_key):
    fake = server()
    with _mv_says("ok", quality="good"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["status"] == "VERIFIED"
    assert fake.requests == []
    assert "second_opinion" not in result


def test_an_invalid_from_millionverifier_is_never_second_guessed(server, mv_key):
    fake = server(result="deliverable", score=100)
    with _mv_says("invalid", quality="bad"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["status"] == "INVALID"
    assert fake.requests == []


def test_when_bounceban_is_unavailable_millionverifiers_answer_stands(server, mv_key):
    server(statuses=[403])
    with _mv_says("unknown"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["result"] == "unknown"
    assert result["status"] == "UNKNOWN"
    assert "second_opinion" not in result


def test_without_a_bounceban_key_millionverifier_behaves_exactly_as_before(monkeypatch, mv_key):
    monkeypatch.delenv("BOUNCEBAN_API_KEY", raising=False)
    with _mv_says("catch_all"):
        result = ev.verify_email_millionverifier("jane@facility.com")

    assert result["result"] == "catch_all"
    assert result["status"] == "RISKY"


# ── The wording in the notes ──────────────────────────────────────────────────

def test_a_second_opinion_is_shown_on_the_note():
    import enrichment_processor as ep
    line = ep._email_verification_line("jane@facility.com", {
        "status": "VERIFIED", "result": "ok", "quality": "risky", "second_opinion": "bounceban",
        "mv_result": "catch_all", "bounceban_score": 100,
    })

    assert "BounceBan" in line
    assert "catch_all" in line


def test_unknown_no_longer_claims_microsoft_365_blocks_verification():
    """The outbound repo measured MillionVerifier as accurate on direct Microsoft 365; the
    unresolved addresses sit behind gateways. The old wording told reps an unknown was
    "likely valid", which is not something verification supports."""
    import enrichment_processor as ep
    line = ep._email_verification_line("jane@facility.com", {"status": "UNKNOWN", "result": "unknown"})
    added = ep._email_added_line("jane@facility.com", {"email_status": "DELIVERABLE"}, {"status": "UNKNOWN"})

    for text in (line, added):
        assert "Microsoft 365" not in text and "M365" not in text
        assert "likely valid" not in text


def test_the_note_shows_why_an_address_is_invalid():
    import enrichment_processor as ep
    line = ep._email_verification_line("ghost@facility.com", {"status": "INVALID", "result": "invalid", "subresult": "no_mailbox"})

    assert "no_mailbox" in line
