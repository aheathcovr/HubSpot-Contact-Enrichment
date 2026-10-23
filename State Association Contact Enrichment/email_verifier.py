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
"""

import logging
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_API_URL = "https://api.millionverifier.com/api/v3/"
_TIMEOUT = 10  # MV API's own internal timeout (seconds); requests timeout = _TIMEOUT + 2


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
}

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


def verify_email_millionverifier(email: str) -> dict:
    """
    Validate *email* against the MillionVerifier API.

    Returns a result dict with keys: result, quality, status, free, role, didyoumean.
    Never raises — all failures produce status="ERROR" or status="UNVERIFIED".
    """
    api_key = os.getenv("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key:
        logger.debug("MILLIONVERIFIER_API_KEY not set — skipping email validation")
        return dict(_UNVERIFIED)

    if not email or "@" not in email:
        return dict(_UNVERIFIED)

    try:
        resp = _session.get(
            _API_URL,
            params={"api": api_key, "email": email.strip(), "timeout": _TIMEOUT},
            timeout=_TIMEOUT + 2,  # requests-level timeout slightly longer than MV's internal one
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("MillionVerifier request failed for %r: %s", email, exc)
        return dict(_ERROR)

    # An account-level refusal arrives as HTTP 200 with an empty result.
    if data.get("error"):
        logger.warning("MillionVerifier refused %r: %s", email, data["error"])
        return dict(_ERROR)

    mv_result  = str(data.get("result",  "")).lower()
    mv_quality = str(data.get("quality", "")).lower()

    if mv_result not in _RESULT_TO_STATUS:
        logger.warning(
            "MillionVerifier returned unrecognised result %r for %r — treating as UNKNOWN",
            mv_result, email,
        )

    return {
        "result":     mv_result,
        "quality":    mv_quality,
        "status":     _RESULT_TO_STATUS.get(mv_result, "UNKNOWN"),
        "free":       bool(data.get("free", False)),
        "role":       bool(data.get("role", False)),
        "didyoumean": str(data.get("didyoumean", "") or ""),
    }
