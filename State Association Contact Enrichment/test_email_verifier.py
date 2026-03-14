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


def _mock_response(result: str, quality: str, free=False, role=False, didyoumean=""):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "result":     result,
        "quality":    quality,
        "free":       free,
        "role":       role,
        "didyoumean": didyoumean,
    }
    return mock_resp


class TestVerifyEmailMillionverifier(unittest.TestCase):

    def setUp(self):
        os.environ.pop("MILLIONVERIFIER_API_KEY", None)

    def tearDown(self):
        os.environ.pop("MILLIONVERIFIER_API_KEY", None)

    # ── 1: valid email ────────────────────────────────────────────────────────

    @patch("email_verifier.requests.get")
    def test_valid_email_returns_verified(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("valid", "good")

        result = verify_email_millionverifier("jane@facility.com")

        self.assertEqual(result["status"],  "VERIFIED")
        self.assertEqual(result["quality"], "good")
        self.assertEqual(result["result"],  "valid")
        mock_get.assert_called_once()
        # API key must appear in the call args
        call_kwargs = str(mock_get.call_args)
        self.assertIn("test-key", call_kwargs)

    # ── 2: invalid email ──────────────────────────────────────────────────────

    @patch("email_verifier.requests.get")
    def test_invalid_email_returns_invalid(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("invalid", "bad")

        result = verify_email_millionverifier("bad@nxdomain.invalid")

        self.assertEqual(result["status"],  "INVALID")
        self.assertEqual(result["quality"], "bad")
        mock_get.assert_called_once()

    # ── 3: catchall → RISKY ───────────────────────────────────────────────────

    @patch("email_verifier.requests.get")
    def test_catchall_returns_risky(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("catchall", "ok")

        result = verify_email_millionverifier("anyone@catchall.org")

        self.assertEqual(result["status"], "RISKY")

    # ── 4: missing API key → UNVERIFIED, no HTTP call ────────────────────────

    @patch("email_verifier.requests.get")
    def test_missing_api_key_returns_unverified_without_http_call(self, mock_get):
        result = verify_email_millionverifier("someone@company.com")

        self.assertEqual(result["status"], "UNVERIFIED")
        mock_get.assert_not_called()

    # ── 5: network error → ERROR ──────────────────────────────────────────────

    @patch("email_verifier.requests.get")
    def test_network_error_returns_error_status(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.side_effect = ConnectionError("Network unreachable")

        result = verify_email_millionverifier("someone@company.com")

        self.assertEqual(result["status"], "ERROR")

    # ── 6: empty/malformed email → UNVERIFIED, no HTTP call ──────────────────

    @patch("email_verifier.requests.get")
    def test_empty_email_no_http_call(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"

        result = verify_email_millionverifier("")

        self.assertEqual(result["status"], "UNVERIFIED")
        mock_get.assert_not_called()

    @patch("email_verifier.requests.get")
    def test_no_at_sign_no_http_call(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"

        result = verify_email_millionverifier("notanemail")

        self.assertEqual(result["status"], "UNVERIFIED")
        mock_get.assert_not_called()

    # ── 7: free/role fields surfaced ──────────────────────────────────────────

    @patch("email_verifier.requests.get")
    def test_free_and_role_fields_returned(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response("valid", "good", free=True, role=True)

        result = verify_email_millionverifier("info@gmail.com")

        self.assertTrue(result["free"])
        self.assertTrue(result["role"])

    # ── 8: typo suggestion surfaced ───────────────────────────────────────────

    @patch("email_verifier.requests.get")
    def test_didyoumean_returned(self, mock_get):
        os.environ["MILLIONVERIFIER_API_KEY"] = "test-key"
        mock_get.return_value = _mock_response(
            "invalid", "bad", didyoumean="user@gmail.com"
        )

        result = verify_email_millionverifier("user@gmai.com")

        self.assertEqual(result["didyoumean"], "user@gmail.com")


if __name__ == "__main__":
    unittest.main()
