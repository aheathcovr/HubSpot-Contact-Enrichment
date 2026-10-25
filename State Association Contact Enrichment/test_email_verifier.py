#!/usr/bin/env python3
"""
Unit tests for email_verifier.verify_email_millionverifier().

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_email_verifier.py" -v
or:
    python3 "State Association Contact Enrichment/test_email_verifier.py"
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Ensure key is absent before importing so module-level state is clean
os.environ.pop("MILLIONVERIFIER_API_KEY", None)

from email_verifier import verify_email_millionverifier


def _mock_response(
    result: str, quality: str, free=False, role=False, didyoumean="", error="",
    subresult=None, credits=None, livemode=None,
):
    """
    Shape of a MillionVerifier v3 single-verification reply.

    The result words are the API's own (confirmed against its demo keys
    2026-10-23): ok, catch_all, unknown, invalid, disposable. An API-level
    failure (no credits, bad key, blocked IP) is still HTTP 200, with an empty
    `result` and the reason in `error`.
    """
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "result":     result,
        "quality":    quality,
        "free":       free,
        "role":       role,
        "didyoumean": didyoumean,
        "error":      error,
    }
    for key, value in (("subresult", subresult), ("credits", credits), ("livemode", livemode)):
        if value is not None:
            mock_resp.json.return_value[key] = value
    return mock_resp


class TestVerifyEmailMillionverifier(unittest.TestCase):

    def setUp(self):
        os.environ.pop("MILLIONVERIFIER_API_KEY", None)

    def tearDown(self):
        os.environ.pop("MILLIONVERIFIER_API_KEY", None)

    # ── 1: valid email ────────────────────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_valid_email_returns_verified(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("ok", "good")

        result = verify_email_millionverifier("jane@facility.com")

        self.assertEqual(result["status"],  "VERIFIED")
        self.assertEqual(result["quality"], "good")
        self.assertEqual(result["result"],  "ok")
        mock_get.assert_called_once()
        # API key must appear in the call args
        call_kwargs = str(mock_get.call_args)
        self.assertIn("test-key", call_kwargs)

    # ── 2: invalid email ──────────────────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_invalid_email_returns_invalid(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("invalid", "bad")

        result = verify_email_millionverifier("bad@nxdomain.invalid")

        self.assertEqual(result["status"],  "INVALID")
        self.assertEqual(result["quality"], "bad")
        mock_get.assert_called_once()

    # ── 3: catch_all → RISKY ──────────────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_catch_all_returns_risky(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("catch_all", "risky")

        result = verify_email_millionverifier("anyone@catchall.org")

        self.assertEqual(result["status"], "RISKY")

    # ── 3b: disposable → INVALID (suppressed like a dead address) ─────────────

    @patch("email_verifier._session.get")
    def test_disposable_returns_invalid_and_keeps_the_api_word(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("disposable", "bad")

        result = verify_email_millionverifier("x@mailinator.com")

        # Callers suppress on status == "INVALID"; a throwaway mailbox is never
        # worth cold-emailing. The raw word stays in `result` for the note.
        self.assertEqual(result["status"], "INVALID")
        self.assertEqual(result["result"], "disposable")

    # ── 3c: unknown → UNKNOWN ─────────────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_unknown_returns_unknown(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("unknown", "risky")

        result = verify_email_millionverifier("someone@blocks-probes.org")

        self.assertEqual(result["status"], "UNKNOWN")

    # ── 3d: API-level failure is HTTP 200 with an empty result ────────────────

    @patch("email_verifier._session.get")
    def test_api_error_body_returns_error_status(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("", "", error="Insufficient credits")

        result = verify_email_millionverifier("someone@company.com")

        # Not UNKNOWN: "the provider would not answer" and "we could not ask"
        # are different, and a spent account must be visible in the note.
        self.assertEqual(result["status"], "ERROR")

    # ── 3e: a word the API has not used before is not silently trusted ────────

    @patch("email_verifier._session.get")
    def test_unrecognised_result_word_returns_unknown(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("deliverable", "good")

        result = verify_email_millionverifier("someone@company.com")

        self.assertEqual(result["status"], "UNKNOWN")

    # ── 4: missing API key → UNVERIFIED, no HTTP call ────────────────────────

    @patch("email_verifier._session.get")
    def test_missing_api_key_returns_unverified_without_http_call(self, mock_get):
        result = verify_email_millionverifier("someone@company.com")

        self.assertEqual(result["status"], "UNVERIFIED")
        mock_get.assert_not_called()

    # ── 5: network error → ERROR ──────────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_network_error_returns_error_status(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.side_effect = ConnectionError("Network unreachable")

        result = verify_email_millionverifier("someone@company.com")

        self.assertEqual(result["status"], "ERROR")

    # ── 6: empty/malformed email → UNVERIFIED, no HTTP call ──────────────────

    @patch("email_verifier._session.get")
    def test_empty_email_no_http_call(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"

        result = verify_email_millionverifier("")

        self.assertEqual(result["status"], "UNVERIFIED")
        mock_get.assert_not_called()

    @patch("email_verifier._session.get")
    def test_no_at_sign_no_http_call(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"

        result = verify_email_millionverifier("notanemail")

        self.assertEqual(result["status"], "UNVERIFIED")
        mock_get.assert_not_called()

    # ── 7: free/role fields surfaced ──────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_free_and_role_fields_returned(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("ok", "good", free=True, role=True)

        result = verify_email_millionverifier("info@gmail.com")

        self.assertTrue(result["free"])
        self.assertTrue(result["role"])

    # ── 8: typo suggestion surfaced ───────────────────────────────────────────

    @patch("email_verifier._session.get")
    def test_didyoumean_returned(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response(
            "invalid", "bad", didyoumean="user@gmail.com"
        )

        result = verify_email_millionverifier("user@gmai.com")

        self.assertEqual(result["didyoumean"], "user@gmail.com")


    # ── 9: use the longest timeout MillionVerifier allows ─────────────────────

    @patch("email_verifier._session.get")
    def test_asks_for_the_longest_timeout_millionverifier_allows(self, mock_get):
        # The API accepts 2-60s (default 20): how long it waits on the recipient's mail
        # server before giving up with `unknown`. 10s timed out slow servers into
        # `unknown` that a longer wait would have settled; the outbound repo uses 60.
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("ok", "good")

        verify_email_millionverifier("jane@facility.com")

        self.assertEqual(mock_get.call_args.kwargs["params"]["timeout"], 60)
        self.assertGreater(mock_get.call_args.kwargs["timeout"], 60)    # our own wait outlasts theirs

    # ── 10: the detail MillionVerifier returns and we used to drop ────────────

    @patch("email_verifier._session.get")
    def test_subresult_is_passed_through(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("invalid", "bad", subresult="no_mailbox")

        result = verify_email_millionverifier("ghost@facility.com")

        self.assertEqual(result["subresult"], "no_mailbox")

    @patch("email_verifier._session.get")
    def test_unverified_is_a_real_result_word_and_is_unknown_without_a_warning(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("unverified", "")

        with self.assertNoLogs("email_verifier", level="WARNING"):
            result = verify_email_millionverifier("jane@facility.com")

        self.assertEqual(result["status"], "UNKNOWN")

    # ── 11: the balance rides on every response ───────────────────────────────

    @patch("email_verifier._session.get")
    def test_low_credits_are_reported_once_per_window_with_an_alert_tag(self, mock_get):
        import email_verifier
        email_verifier._last_low_credits_warning = None
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("ok", "good", credits=50)

        with self.assertLogs("email_verifier", level="WARNING") as logged:
            verify_email_millionverifier("a@facility.com")
            verify_email_millionverifier("b@facility.com")

        flagged = [r for r in logged.records if getattr(r, "json_fields", {}).get("alert") == "millionverifier_credits_low"]
        self.assertEqual(len(flagged), 1)

    @patch("email_verifier._session.get")
    def test_a_healthy_balance_is_not_reported(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("ok", "good", credits=1880)

        with self.assertNoLogs("email_verifier", level="WARNING"):
            verify_email_millionverifier("jane@facility.com")

    # ── 12: a test key must never pass for a real answer ──────────────────────

    @patch("email_verifier._session.get")
    def test_a_test_mode_key_is_reported_as_an_error(self, mock_get):
        import email_verifier
        email_verifier._last_low_credits_warning = None
        os.environ["MILLIONVERIFIER_API_KEY"] = "API_KEY_FOR_OK"
        mock_get.return_value = _mock_response("ok", "good", credits=5000, livemode=False)

        with self.assertLogs("email_verifier", level="ERROR") as logged:
            verify_email_millionverifier("jane@facility.com")

        flagged = [r for r in logged.records if getattr(r, "json_fields", {}).get("alert") == "millionverifier_test_mode"]
        self.assertEqual(len(flagged), 1)


if __name__ == "__main__":
    unittest.main()
