"""
Seamless is asked for a phone while FullEnrich is still looking for the email.

A contact with no phone is researched at Seamless whatever FullEnrich finds, so waiting
for FullEnrich first only put two vendor waits end to end (each polls for up to 90 s).
Starting the Seamless call first costs nothing extra and the contact waits for the
slower of the two. What changes is only WHEN the call starts; the calls made, what is
written and every gate (identity, MillionVerifier, never overwriting) are those of
test_seamless_wiring.py, which also runs through this path.

The cases that do not overlap matter as much. A contact that already has a phone is
asked at Seamless only when FullEnrich finds no email, and only after it: starting that
call early would pay for a lookup FullEnrich's answer makes unnecessary.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_seamless_overlap.py" -v
"""

import os
import sys
import threading
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep
from test_seamless_wiring import _NO_FE, _OK, _existing, _new, _sl, _written


class _Run:
    """The vendors, recording what overlapped with what."""

    def __init__(self):
        self.order: list[str] = []
        self.seamless_started = threading.Event()
        self.fullenrich_started = threading.Event()
        self.seamless_was_running_during_fullenrich: bool | None = None
        self.fullenrich_result = dict(_NO_FE)

    def fullenrich(self, **kwargs):
        self.order.append("fullenrich")
        self.fullenrich_started.set()
        # Is the Seamless call already underway while FullEnrich is "working"?
        self.seamless_was_running_during_fullenrich = self.seamless_started.wait(1)
        return dict(self.fullenrich_result)

    def seamless(self, *args, **kwargs):
        self.order.append("seamless")
        self.seamless_started.set()
        return _sl()


@pytest.fixture
def run():
    r = _Run()
    with patch.object(ep, "enrich_contact_info", side_effect=r.fullenrich) as fe, \
         patch.object(ep, "verify_email_millionverifier", return_value=dict(_OK)), \
         patch.object(ep, "seamless_lookup", side_effect=r.seamless) as sl:
        r.fe_mock, r.sl_mock = fe, sl
        yield r


# ── The overlap ───────────────────────────────────────────────────────────────

def test_an_existing_contact_missing_both_is_researched_at_seamless_while_fullenrich_works(run):
    _existing({"email": "", "phone": ""})

    assert run.seamless_was_running_during_fullenrich is True


def test_a_new_contact_missing_both_is_researched_at_seamless_while_fullenrich_works(run):
    _new(email_to_use="", found_phone="")

    assert run.seamless_was_running_during_fullenrich is True


def test_the_overlap_costs_no_extra_lookup(run):
    _existing({"email": "", "phone": ""})
    assert run.sl_mock.call_count == 1 and run.fe_mock.call_count == 1

    run.sl_mock.reset_mock(); run.fe_mock.reset_mock()
    _new(email_to_use="", found_phone="")
    assert run.sl_mock.call_count == 1 and run.fe_mock.call_count == 1


def test_what_the_overlapped_call_returned_is_still_screened_and_written(run):
    hs = _existing({"email": "", "phone": ""})

    written = _written(hs)
    assert written["phone"] == "330-702-0110"
    assert written["email"] == "jane.doe@facility.com"      # nobody else found one: the fallback applies
    assert written["email_verification_result"] == "ok"


def test_a_fullenrich_email_still_wins_over_an_overlapped_seamless_email(run):
    run.fullenrich_result = {"email": "found@facility.com", "email_status": "DELIVERABLE",
                             "personal_email": "", "phone": ""}

    hs = _existing({"email": "", "phone": ""})

    assert _written(hs)["email"] == "found@facility.com"
    assert _written(hs)["phone"] == "330-702-0110"


# ── No overlap where it would cost a lookup ───────────────────────────────────

def test_a_contact_with_a_phone_is_not_researched_at_seamless_until_fullenrich_has_failed(run):
    """Seamless is only an email fallback for this contact. Asking it early would pay for a lookup that
    a FullEnrich email makes unnecessary."""
    run.fullenrich_result = {"email": "found@facility.com", "email_status": "DELIVERABLE",
                             "personal_email": "", "phone": ""}

    _existing({"email": "", "phone": "5551234567"})

    assert run.sl_mock.call_count == 0


def test_a_contact_with_a_phone_is_researched_after_fullenrich_finds_no_email(run):
    _existing({"email": "", "phone": "5551234567"})

    assert run.order == ["fullenrich", "seamless"]
    assert run.seamless_was_running_during_fullenrich is False    # it had not been started yet


def test_a_new_contact_with_a_phone_is_not_researched_at_seamless_until_fullenrich_has_failed(run):
    run.fullenrich_result = {"email": "found@facility.com", "email_status": "DELIVERABLE",
                             "personal_email": "", "phone": ""}

    _new(email_to_use="", found_phone="5551234567")
    assert run.sl_mock.call_count == 0

    run.fullenrich_result = dict(_NO_FE)
    run.order.clear(); run.seamless_started.clear()
    _new(email_to_use="", found_phone="5551234567")
    assert run.order == ["fullenrich", "seamless"]


def test_a_contact_with_an_email_and_no_phone_makes_one_seamless_call_and_no_fullenrich_call(run):
    _existing({"email": "jane@facility.com", "phone": ""})

    assert run.order == ["seamless"]
    assert run.fe_mock.call_count == 0


def test_a_contact_with_both_makes_no_vendor_call_at_all(run):
    _existing({"email": "jane@facility.com", "phone": "5551234567"})

    assert run.order == []
