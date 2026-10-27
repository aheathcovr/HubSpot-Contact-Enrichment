"""
Who is asking: checking a request that HubSpot's hubspot.fetch signed.

An app card calls this service with hubspot.fetch. HubSpot signs the request with the app's client secret
(X-HubSpot-Signature-v3 and X-HubSpot-Request-Timestamp) and adds userId, userEmail, portalId and appId to the
query string. The signature covers the URI, so a rep's identity in the query cannot be forged without the secret.

The v3 signature is the base64 HMAC-SHA256, keyed with the client secret, of
    method + URI + body + timestamp
where the URI has these URL-encoded characters decoded first: %3A %2F %3F %40 %21 %24 %27 %28 %29 %2A %2C %3B.
A request older than five minutes is refused. (The webhook's own check does not decode: its URL has no query.)

No HubSpot, no web framework, no clock of its own in here beyond time.time(), so every case is testable.
"""

import base64
import hashlib
import hmac
import time
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

# HubSpot's published list of what is decoded in the URI before it is signed.
_DECODED = {
    "%3A": ":", "%2F": "/", "%3F": "?", "%40": "@", "%21": "!", "%24": "$",
    "%27": "'", "%28": "(", "%29": ")", "%2A": "*", "%2C": ",", "%3B": ";",
}

MAX_AGE_MS = 5 * 60 * 1000


class AuthError(Exception):
    """A request that is not accepted, with the HTTP status to answer it with."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass(frozen=True)
class Identity:
    """Who asked, as HubSpot said in the signed query string."""
    user_id: str
    email: str
    portal_id: str
    app_id: str


def decode_signed_uri(url: str) -> str:
    """The URI as HubSpot signs it: the listed percent-encodings decoded, the rest left as they came."""
    for encoded, decoded in _DECODED.items():
        url = url.replace(encoded, decoded)
    return url


def compute_signature_v3(secret: str, method: str, url: str, body: bytes, timestamp: str) -> str:
    message = method.upper() + decode_signed_uri(url) + (body or b"").decode("utf-8", errors="replace") + str(timestamp)
    digest = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _identity(url: str) -> Identity:
    query = parse_qs(urlsplit(url).query, keep_blank_values=True)
    first = lambda name: (query.get(name) or [""])[0]
    user_id = first("userId").strip()
    if not user_id.isdigit():
        raise AuthError(401, "The request does not say who asked.")
    return Identity(user_id=user_id, email=first("userEmail").strip(), portal_id=first("portalId").strip(), app_id=first("appId").strip())


def authenticate(
    *, secret: str, method: str, url: str, body: bytes, signature: str | None, timestamp: str | None,
    expected_portal: str = "", now_ms: float | None = None,
) -> Identity:
    """
    The rep who made a signed request. Raises AuthError: 503 when the service has no client secret (it
    fails closed), 401 for a missing, stale or wrong signature or a request that does not say who asked,
    403 for a request from another portal than the expected one.
    """
    if not secret:
        raise AuthError(503, "The service is not set up for the HubSpot app.")
    if not (signature or "").strip():
        raise AuthError(401, "Missing signature.")
    try:
        sent_at = int(str(timestamp).strip())
    except (TypeError, ValueError):
        raise AuthError(401, "Missing or unreadable timestamp.") from None
    now = time.time() * 1000 if now_ms is None else now_ms
    if abs(now - sent_at) > MAX_AGE_MS:
        raise AuthError(401, "The request is too old.")
    expected = compute_signature_v3(secret, method, url, body, str(sent_at))
    if not hmac.compare_digest(expected, signature.strip()):
        raise AuthError(401, "Invalid signature.")
    who = _identity(url)
    if expected_portal and who.portal_id != expected_portal:
        raise AuthError(403, "This request is from another HubSpot account.")
    return who
