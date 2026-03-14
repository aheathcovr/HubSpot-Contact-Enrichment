"""
MillionVerifier email validation helper.

Single public function: verify_email_millionverifier(email) -> dict

Returned dict always contains these keys:
    result    : "valid" | "invalid" | "unknown" | "catchall" | "skipped" | "error"
    quality   : "good" | "ok" | "bad" | ""
    status    : "VERIFIED" | "INVALID" | "UNKNOWN" | "RISKY" | "UNVERIFIED" | "ERROR"
    free      : bool  (True = free/disposable email provider)
    role      : bool  (True = role-based address, e.g. admin@, info@)
    didyoumean: str   (typo suggestion from API, or "")

The caller uses `status` for gating logic:
    VERIFIED  → email confirmed deliverable; write to HubSpot
    INVALID   → definitively bad; suppress email from HubSpot writes
    RISKY     → catchall domain (domain accepts all addresses); write but flag
    UNKNOWN   → could not determine; write but flag
    UNVERIFIED→ API key not configured; write but flag
    ERROR     → network/API failure; write but flag

If MILLIONVERIFIER_API_KEY is unset, returns status="UNVERIFIED" immediately
(no HTTP call made). All exceptions are caught; errors return status="ERROR".
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

_API_URL = "https://api.millionverifier.com/api/v3/"
_TIMEOUT = 10  # MV API's own internal timeout (seconds); requests timeout = _TIMEOUT + 2

_RESULT_TO_STATUS: dict[str, str] = {
    "valid":    "VERIFIED",
    "invalid":  "INVALID",
    "unknown":  "UNKNOWN",
    "catchall": "RISKY",
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
        resp = requests.get(
            _API_URL,
            params={"api": api_key, "email": email.strip(), "timeout": _TIMEOUT},
            timeout=_TIMEOUT + 2,  # requests-level timeout slightly longer than MV's internal one
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("MillionVerifier request failed for %r: %s", email, exc)
        return dict(_ERROR)

    mv_result  = str(data.get("result",  "")).lower()
    mv_quality = str(data.get("quality", "")).lower()

    return {
        "result":     mv_result,
        "quality":    mv_quality,
        "status":     _RESULT_TO_STATUS.get(mv_result, "UNKNOWN"),
        "free":       bool(data.get("free", False)),
        "role":       bool(data.get("role", False)),
        "didyoumean": str(data.get("didyoumean", "") or ""),
    }
