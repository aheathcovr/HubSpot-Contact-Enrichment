"""
Who is asking: checking a request that HubSpot's hubspot.fetch signed.

HubSpot signs each fetch from an app card (X-HubSpot-Signature-v3: base64 HMAC-SHA256, with the app's client
secret, of method + URI + body + timestamp) and adds userId, userEmail, portalId and appId to the query string.
Because the URI is part of what is signed, the rep's identity in the query cannot be forged. These tests check the
signature against HubSpot's own published example, then every way a request can be refused.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_hubspot_request_auth.py" -v
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import hubspot_request_auth as auth

SECRET = "test-client-secret"
BASE = "https://svc.example.run.app/app/contact/status"


def _signed(*, method="GET", path_and_query="?contactId=42&userId=77&userEmail=rep%40example.org&portalId=1234&appId=99",
            body=b"", secret=SECRET, signer=None, ts=None, base=BASE):
    """A request as HubSpot would send it: the URL, the body, and the two headers. *signer* is the secret that
    signed it when that is not the one the service holds."""
    url = base + path_and_query
    timestamp = str(int(time.time() * 1000)) if ts is None else str(ts)
    signature = auth.compute_signature_v3(signer or secret, method, url, body, timestamp)
    return {"secret": secret, "method": method, "url": url, "body": body, "signature": signature, "timestamp": timestamp}


def _check(req, **over):
    args = {**req, **over}
    return auth.authenticate(**args)


# ── The signature itself ──────────────────────────────────────────────────────

def test_the_signature_matches_the_example_in_hubspots_documentation():
    body = (
        '[{"eventId":531833541,"subscriptionId":3923621,"portalId":48807704,"appId":16111050,'
        '"occurredAt":1752613920733,"subscriptionType":"contact.creation","attemptNumber":0,"objectId":138017612137,'
        '"changeFlag":"CREATED","changeSource":"CRM_UI","sourceId":"userId:76023669"}]'
    ).encode()

    signature = auth.compute_signature_v3(
        "cfc68c0b-4b4e-4ef8-b764-95350e4ea479", "POST", "https://webhook.site/335453f5-94b3-49d9-b684-a55354d4b8df",
        body, "1752613922216",
    )

    assert signature == "gbj1XPRvUt0noT7i7fXfTzOD4sLzQmf0VT28ZYq0EYg="


@pytest.mark.parametrize("encoded, decoded", [
    ("%3A", ":"), ("%2F", "/"), ("%3F", "?"), ("%40", "@"), ("%21", "!"), ("%24", "$"),
    ("%27", "'"), ("%28", "("), ("%29", ")"), ("%2A", "*"), ("%2C", ","), ("%3B", ";"),
])
def test_the_characters_hubspot_lists_are_decoded_before_signing(encoded, decoded):
    assert auth.decode_signed_uri(f"https://h/p?x=a{encoded}b") == f"https://h/p?x=a{decoded}b"


@pytest.mark.parametrize("kept", ["%20", "%25", "%26", "%3D", "%2B", "%23"])
def test_other_encoded_characters_are_left_as_they_came(kept):
    assert auth.decode_signed_uri(f"https://h/p?x=a{kept}b") == f"https://h/p?x=a{kept}b"


def test_an_email_in_the_query_is_signed_with_its_at_sign_decoded():
    url = BASE + "?userEmail=rep%40example.org"

    signature = auth.compute_signature_v3(SECRET, "GET", url, b"", "1")

    assert signature == auth.compute_signature_v3(SECRET, "GET", BASE + "?userEmail=rep@example.org", b"", "1")


# ── A good request ────────────────────────────────────────────────────────────

def test_a_signed_request_says_who_asked():
    who = _check(_signed())

    assert who == auth.Identity(user_id="77", email="rep@example.org", portal_id="1234", app_id="99")


def test_a_post_with_a_body_is_checked_against_the_body():
    req = _signed(method="POST", path_and_query="?userId=77&portalId=1234", body=b'{"contactId":"42","objective":"find_email"}')

    assert _check(req).user_id == "77"


def test_a_request_without_an_email_is_still_a_request_from_that_user():
    req = _signed(path_and_query="?userId=77&portalId=1234")

    assert _check(req) == auth.Identity(user_id="77", email="", portal_id="1234", app_id="")


def test_a_timestamp_four_minutes_old_is_accepted():
    req = _signed(ts=int(time.time() * 1000) - 4 * 60 * 1000)

    assert _check(req).user_id == "77"


# ── Refusals ──────────────────────────────────────────────────────────────────

def _refused(req, code, **over):
    with pytest.raises(auth.AuthError) as exc:
        _check(req, **over)
    assert exc.value.status_code == code
    return exc.value


def test_a_service_with_no_client_secret_answers_503_and_checks_nothing():
    err = _refused(_signed(), 503, secret="")

    assert "not set up" in err.message


@pytest.mark.parametrize("missing", ["", None, "   "])
def test_a_request_with_no_signature_is_refused(missing):
    _refused(_signed(), 401, signature=missing)


def test_a_wrong_signature_is_refused():
    _refused(_signed(), 401, signature="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")


def test_a_signature_made_with_another_secret_is_refused():
    _refused(_signed(signer="someone-elses-secret"), 401)


def test_changing_who_asked_breaks_the_signature():
    req = _signed()

    _refused(req, 401, url=req["url"].replace("userId=77", "userId=1"))


def test_changing_the_body_breaks_the_signature():
    req = _signed(method="POST", body=b'{"objective":"verify_email"}')

    _refused(req, 401, body=b'{"objective":"find_mobile"}')


def test_changing_the_method_breaks_the_signature():
    _refused(_signed(method="GET"), 401, method="POST")


@pytest.mark.parametrize("age_ms", [6 * 60 * 1000, 60 * 60 * 1000])
def test_an_old_timestamp_is_refused(age_ms):
    _refused(_signed(ts=int(time.time() * 1000) - age_ms), 401)


def test_a_timestamp_far_in_the_future_is_refused():
    _refused(_signed(ts=int(time.time() * 1000) + 10 * 60 * 1000), 401)


@pytest.mark.parametrize("bad", ["", "soon", "12.5", None])
def test_a_timestamp_that_is_not_a_number_is_refused(bad):
    req = _signed()

    _refused(req, 401, timestamp=bad)


@pytest.mark.parametrize("query", ["?portalId=1234", "?userId=&portalId=1234", "?userId=abc&portalId=1234", "?userId=7%207"])
def test_a_request_that_does_not_say_who_asked_is_refused(query):
    err = _refused(_signed(path_and_query=query), 401)

    assert "who" in err.message.lower()


def test_a_request_for_another_portal_is_refused_when_one_portal_is_expected():
    _refused(_signed(), 403, expected_portal="9999")


def test_any_portal_is_accepted_when_none_is_expected():
    assert _check(_signed()).portal_id == "1234"


def test_the_expected_portal_is_accepted():
    assert _check(_signed(), expected_portal="1234").portal_id == "1234"


def test_the_signature_is_checked_before_the_portal():
    """Nothing in an unsigned request is trusted, not even to say it is from the wrong portal."""
    _refused(_signed(signer="other"), 401, expected_portal="9999")
