"""
BounceBan: the second opinion behind MillionVerifier, for what it cannot decide.

Ported from the outbound repo's covr_utils/messaging/bounceban.py, which was built
around measured behaviour. MillionVerifier answers honestly where the mail server
does (direct Microsoft 365 rejects unknown recipients), and cannot answer behind a
server that accepts everyone: a catch-all, or a Proofpoint/Barracuda gateway in front
of a Microsoft tenant. There `ok` bounced 0 of 305 and `catch_all` bounced 4.7%, and
80% of unresolved addresses sat behind such gateways. BounceBan resolves those without
sending mail.

Single public function: second_opinion(email) -> dict | None

    {"result": "ok" | "catch_all" | "unknown" | "invalid", "score": int, "bounceban_result": str}

BounceBan's four answers are translated into MillionVerifier's own words before
anything leaves this module, because the portal's email_verification_result property
and the outbound loaders read exactly those words:

    deliverable, score >= 99   -> ok         (the band BounceBan guarantees)
    deliverable, score <  99   -> catch_all
    risky                      -> unknown
    undeliverable              -> invalid
    unknown / unfinished       -> unknown

Billing rules this is built around (outbound repo's notes on the BounceBan API):
  * Only the WATERFALL endpoint is used. The standard single endpoint bills every
    request, even for a repeated address; the waterfall one answers 408 when a check is
    slow, and re-sending within 30 minutes is free.
  * The key goes in the Authorization header as is, not "Bearer <key>".
  * 401 (bad key), 403 (out of credits) and 405 (account blocked) mean nothing will
    work: stop asking for ten minutes and say so once, rather than failing every address.
  * `risky` is always charged, and a settled answer is remembered so an address is never
    bought twice in a window. `unknown` is not remembered: it is asked again next time.

Never raises. BOUNCEBAN_API_KEY unset means the second opinion is simply skipped, so the
service runs the same without it.
"""

import logging
import os
import threading
import time

import requests

logger = logging.getLogger(__name__)

_DEFAULT_WATERFALL_URL = "https://api-waterfall.bounceban.com/v1/verify/single"
_WATERFALL_URL = _DEFAULT_WATERFALL_URL

_API_TIMEOUT_S = 45          # how long BounceBan holds the connection before answering 408
_ATTEMPTS = 2                # the first ask, plus one retry (free within 30 minutes after a 408)
_RATE_LIMIT_RETRIES = 3
_GUARANTEED_SCORE = 99
_STOP_STATUSES = {401: "bad or missing API key", 403: "out of credits", 405: "account blocked"}
_STOP_FOR_S = 600
_ANSWER_TTL_S = 6 * 3600

# No automatic retries: what is repeated is decided here, because billing differs by call.
_session = requests.Session()
_sleep = time.sleep          # replaced in tests so waits are recorded, not slept

_lock = threading.Lock()
_answers: dict = {}          # address -> (monotonic time, answer)
_stopped_until = 0.0         # monotonic time before which nothing is asked


def reset_state() -> None:
    """Forget remembered answers and any stop. For tests."""
    global _stopped_until
    with _lock:
        _answers.clear()
        _stopped_until = 0.0


def _score(row: dict) -> int:
    try:
        return int(float(row.get("score")))
    except (TypeError, ValueError):
        return -1


def translate(row: dict) -> str:
    """MillionVerifier's word for one BounceBan answer. Anything unrecognised is `unknown`."""
    if str(row.get("status") or "success").strip().lower() != "success":
        return "unknown"
    result = str(row.get("result") or "").strip().lower()
    if result == "deliverable":
        return "ok" if _score(row) >= _GUARANTEED_SCORE else "catch_all"
    if result == "undeliverable":
        return "invalid"
    return "unknown"          # risky, unknown, or a word BounceBan has not used before


def _ask(address: str, key: str):
    """One address's raw answer, or None. Handles 408, 429 and the stop statuses."""
    global _stopped_until
    asked = limited = 0
    while asked < _ATTEMPTS:
        try:
            resp = _session.get(
                _WATERFALL_URL,
                params={"email": address, "timeout": _API_TIMEOUT_S, "mode": "regular"},
                headers={"Authorization": key},
                timeout=_API_TIMEOUT_S + 30,
            )
        except Exception as exc:
            logger.warning("BounceBan request failed for %r: %s", address, exc)
            return None
        code = resp.status_code
        if code == 200:
            try:
                return resp.json()
            except Exception as exc:
                logger.warning("BounceBan returned unreadable JSON for %r: %s", address, exc)
                return None
        if code in _STOP_STATUSES:
            with _lock:
                _stopped_until = time.monotonic() + _STOP_FOR_S
            logger.warning(
                "BounceBan unavailable (%d: %s): second opinions are skipped for %d minutes",
                code, _STOP_STATUSES[code], _STOP_FOR_S // 60,
                extra={"json_fields": {"alert": "bounceban_unavailable", "status": code}},
            )
            return None
        if code == 408:
            asked += 1          # slow check; the retry inside the 30-minute window is free
            continue
        if code == 429 and limited < _RATE_LIMIT_RETRIES:
            limited += 1
            _sleep(float(min(2 ** limited, 30)))
            continue
        logger.warning("BounceBan returned %d for %r", code, address)
        return None
    logger.info("BounceBan did not finish checking %r in time", address)
    return None


def second_opinion(email: str):
    """
    BounceBan's answer for *email* in MillionVerifier's words, or None when there is no
    key, no usable answer, or the service is unavailable.
    """
    key = os.getenv("BOUNCEBAN_API_KEY", "").strip()
    if not key or not email or "@" not in email:
        return None
    address = email.strip().lower()

    now = time.monotonic()
    with _lock:
        if now < _stopped_until:
            return None
        cached = _answers.get(address)
        if cached and now - cached[0] < _ANSWER_TTL_S:
            return cached[1]

    row = _ask(address, key)
    if row is None:
        return None
    answer = {
        "result": translate(row),
        "score": _score(row),
        "bounceban_result": str(row.get("result") or ""),
    }
    if answer["result"] != "unknown":
        with _lock:
            _answers[address] = (now, answer)
    return answer
