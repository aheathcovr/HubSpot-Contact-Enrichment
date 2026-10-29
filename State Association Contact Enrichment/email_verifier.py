"""
MillionVerifier email validation helper.

Single public function: verify_email_millionverifier(email) -> dict

Returned dict always contains these keys:
    result    : "ok" | "catch_all" | "unknown" | "invalid" | "disposable" | "skipped" | "error"
                (the API's own words, passed through unchanged; "" if it sent none)
    quality   : "good" | "risky" | "bad" | ""
    status    : "VERIFIED" | "INVALID" | "UNKNOWN" | "RISKY" | "UNVERIFIED" | "ERROR"
    free      : bool  (True = free email provider, e.g. gmail.com)
    role      : bool  (True = role-based address, e.g. admin@, info@)
    didyoumean: str   (typo suggestion from API, or "")

The caller uses `status` for gating logic:
    VERIFIED  → email confirmed deliverable (API: ok); write to HubSpot
    INVALID   → definitively bad (API: invalid, or disposable); suppress email
                from HubSpot writes
    RISKY     → catch-all domain (accepts any address, so the check proves
                nothing; API: catch_all); write but flag
    UNKNOWN   → the mail server would not answer (API: unknown); write but flag
    UNVERIFIED→ API key not configured; write but flag
    ERROR     → network failure, or the API refused the request (no credits,
                bad key, blocked IP — HTTP 200 with the reason in `error`);
                write but flag

If MILLIONVERIFIER_API_KEY is unset, returns status="UNVERIFIED" immediately
(no HTTP call made). All exceptions are caught; errors return status="ERROR".

Second opinion. MillionVerifier cannot settle an address behind a catch-all or a mail
gateway: it says `catch_all` or `unknown`. For exactly those two answers, and only
when BOUNCEBAN_API_KEY is set, BounceBan is asked (see bounceban_client). If it settles
the address (ok / invalid, or catch_all for what MillionVerifier left unknown) its
answer, already in MillionVerifier's words, replaces the result, and the dict gains
`second_opinion="bounceban"`, `mv_result` (what MillionVerifier said) and
`bounceban_score`. An `ok` or `invalid` from MillionVerifier is never second-guessed.
"""

import logging
import os
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import bounceban_client
import run_trail

logger = logging.getLogger(__name__)

_API_URL = "https://api.millionverifier.com/api/v3/"
# How long MillionVerifier waits on the recipient's mail server before giving up with
# `unknown`. The API accepts 2-60s (default 20). 10s timed slow servers out into
# `unknown`s that a longer wait settles, and `unknown` is the answer that then costs a
# BounceBan credit. The outbound repo uses the 60s maximum for the same reason. Our own
# wait must outlast it.
_TIMEOUT = 60
_REQUEST_SLACK = 20

# MillionVerifier puts the account's remaining credits on every response. They do not
# renew, so say so before they run out, at most once every ten minutes.
_LOW_CREDITS = 200
_WARNING_INTERVAL_S = 600
_last_low_credits_warning = None     # monotonic time, or None if never warned
_last_test_mode_warning = None


def _build_session() -> requests.Session:
    """Shared, retrying, connection-pooled session — see fullenrich_client.py."""
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_session = _build_session()

# Keys are the words the v3 single API actually returns. This table once held
# "valid" and "catchall", which the API never sends, so every deliverable and
# catch-all address fell through to UNKNOWN.
_RESULT_TO_STATUS: dict[str, str] = {
    "ok":         "VERIFIED",
    "catch_all":  "RISKY",
    "invalid":    "INVALID",
    "disposable": "INVALID",
    "unknown":    "UNKNOWN",
    "unverified": "UNKNOWN",     # MillionVerifier's own word for "could not verify"
}

# The two answers MillionVerifier gives when the mail server will not settle an address.
_UNSETTLED = frozenset({"unknown", "catch_all"})

_UNVERIFIED: dict = {
    "result":     "skipped",
    "quality":    "",
    "status":     "UNVERIFIED",
    "free":       False,
    "role":       False,
    "didyoumean": "",
}

_ERROR: dict = {
    "result":     "error",
    "quality":    "",
    "status":     "ERROR",
    "free":       False,
    "role":       False,
    "didyoumean": "",
}


def _warn_throttled(last, message, *args, level=logging.WARNING, alert, **fields):
    """Log at most once per _WARNING_INTERVAL_S. Returns the new "last warned" time."""
    now = time.monotonic()
    if last is not None and now - last < _WARNING_INTERVAL_S:
        return last
    logger.log(level, message, *args, extra={"json_fields": {"alert": alert, **fields}})
    return now


def _report_account_state(data: dict) -> None:
    """Credits left and test mode, both of which ride on every response."""
    global _last_low_credits_warning, _last_test_mode_warning
    credits = data.get("credits")
    if isinstance(credits, (int, float)) and not isinstance(credits, bool) and credits < _LOW_CREDITS:
        _last_low_credits_warning = _warn_throttled(
            _last_low_credits_warning,
            "MillionVerifier has %s credits left and they do not renew", credits,
            alert="millionverifier_credits_low", credits=credits,
        )
    if data.get("livemode") is False:
        _last_test_mode_warning = _warn_throttled(
            _last_test_mode_warning,
            "MillionVerifier answered in test mode: this is a demo key, so these results are not real",
            level=logging.ERROR, alert="millionverifier_test_mode",
        )


# What each result word means to a rep, and whether it settles anything (run_trail.py says what a step's result is).
_SAYS = {
    "ok":         ("the address is deliverable", "ok"),
    "invalid":    ("the address is not deliverable", "ok"),
    "disposable": ("the address is a disposable one", "ok"),
    "catch_all":  ("the domain accepts every address (catch-all), so the check proves nothing", "info"),
    "unknown":    ("the mail server would not answer", "info"),
}


def verify_email_millionverifier(email: str) -> dict:
    """
    Validate *email* against the MillionVerifier API.

    Returns a result dict with keys: result, quality, status, free, role, didyoumean.
    Never raises — all failures produce status="ERROR" or status="UNVERIFIED".
    """
    api_key = os.getenv("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key:
        logger.debug("MILLIONVERIFIER_API_KEY not set — skipping email validation")
        run_trail.note("millionverifier", "MillionVerifier is not set up on the service, so the address was not checked.", "skipped")
        return dict(_UNVERIFIED)

    if not email or "@" not in email:
        return dict(_UNVERIFIED)

    try:
        resp = _session.get(
            _API_URL,
            params={"api": api_key, "email": email.strip(), "timeout": _TIMEOUT},
            timeout=_TIMEOUT + _REQUEST_SLACK,  # our wait must outlast MV's own
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("MillionVerifier request failed for %r: %s", email, exc)
        run_trail.note("millionverifier", f"MillionVerifier could not be reached: {run_trail.why(exc)}.", "failed")
        return dict(_ERROR)

    # An account-level refusal arrives as HTTP 200 with an empty result.
    if data.get("error"):
        logger.warning("MillionVerifier refused %r: %s", email, data["error"])
        run_trail.note("millionverifier", f"MillionVerifier refused the check: {data['error']}.", "failed")
        return dict(_ERROR)

    mv_result  = str(data.get("result",  "")).lower()
    mv_quality = str(data.get("quality", "")).lower()

    if mv_result not in _RESULT_TO_STATUS:
        logger.warning(
            "MillionVerifier returned unrecognised result %r for %r — treating as UNKNOWN",
            mv_result, email,
        )

    _report_account_state(data)

    out = {
        "result":     mv_result,
        "quality":    mv_quality,
        "subresult":  str(data.get("subresult", "") or ""),    # e.g. no_mailbox vs no_domain
        "status":     _RESULT_TO_STATUS.get(mv_result, "UNKNOWN"),
        "free":       bool(data.get("free", False)),
        "role":       bool(data.get("role", False)),
        "didyoumean": str(data.get("didyoumean", "") or ""),
    }

    says, settled = _SAYS.get(mv_result, ("it could not verify the address", "info"))
    detail = f" ({out['subresult'].replace('_', ' ')})" if out["subresult"] else ""
    credits = data.get("credits")
    left = f" {credits:,.0f} credits left." if isinstance(credits, (int, float)) and not isinstance(credits, bool) else ""
    run_trail.note("millionverifier", f"MillionVerifier says {says}{detail}.{left}", settled)

    if mv_result in _UNSETTLED:
        second = bounceban_client.second_opinion(email)
        # BounceBan's own "unknown" adds nothing: keep MillionVerifier's answer, which
        # may be the more informative one (catch_all vs unknown).
        if second and second["result"] != "unknown":
            out.update(
                result=second["result"],
                status=_RESULT_TO_STATUS.get(second["result"], "UNKNOWN"),
                mv_result=mv_result,
                second_opinion="bounceban",
                bounceban_score=second["score"],
            )
    return out
