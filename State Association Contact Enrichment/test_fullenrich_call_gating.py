"""
FullEnrich enrichment is only requested for work emails (see
fullenrich_client.enrich_contact_info), so it must only be called for a contact
who is actually missing one. A contact that has an email but no phone used to
trigger the lookup too, because the same call also bought a phone; now it would
spend a credit and be able to return nothing.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_fullenrich_call_gating.py" -v
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep

_VERIFIED = {"status": "VERIFIED", "quality": "good", "result": "ok"}
_FE_FOUND_EMAIL = {
    "email": "jane.doe@facility.com", "email_status": "DELIVERABLE",
    "personal_email": "", "phone": "",
}


@pytest.fixture
def enrich():
    """FullEnrich returns a work email; MillionVerifier confirms it."""
    with patch.object(ep, "enrich_contact_info", return_value=dict(_FE_FOUND_EMAIL)) as fe, \
         patch.object(ep, "verify_email_millionverifier", return_value=dict(_VERIFIED)):
        yield fe


def _enrich_existing_contact(existing_props: dict):
    hs = MagicMock()
    hs.get_contact_properties.return_value = {"jobtitle": "Administrator", **existing_props}
    ep._enrich_and_associate_existing_contact(
        hs=hs, matched_contact_id="42", matched_detail="matched by name",
        found_name="Jane Doe", found_title="Administrator", found_email="",
        found_phone="", found_linkedin="", facility_name="Valley View",
        facility_domain="facility.com", email_validation={}, email_to_use="",
        data_freshness_value="", research_reasoning="", source_path="sonar_pro",
        _base={},
    )
    return hs


def _create_new_contact(email_to_use: str, found_phone: str):
    hs = MagicMock()
    hs.create_contact.return_value = "999"
    ep._create_and_enrich_new_contact(
        hs=hs, first="Jane", last="Doe", found_title="Administrator",
        found_name="Jane Doe", email_to_use=email_to_use, found_phone=found_phone,
        found_linkedin="", facility_name="Valley View", facility_domain="facility.com",
        target_company_id="1", data_freshness_value="", research_reasoning="",
        _base={},
    )
    return hs


def _written_properties(hs) -> dict:
    """Every property sent to HubSpot for the contact, merged."""
    merged: dict = {}
    for call in hs.update_contact.call_args_list:
        merged.update(call.args[1])
    return merged


# ── Existing HubSpot contact ──────────────────────────────────────────────────

def test_existing_contact_with_email_but_no_phone_is_not_looked_up(enrich):
    _enrich_existing_contact({"email": "jane@facility.com", "phone": ""})

    enrich.assert_not_called()


def test_existing_contact_without_email_is_looked_up_once_and_gets_the_email(enrich):
    hs = _enrich_existing_contact({"email": "", "phone": "5551234567"})

    enrich.assert_called_once()
    assert _written_properties(hs)["email"] == "jane.doe@facility.com"


# ── Newly created contact ─────────────────────────────────────────────────────

def test_new_contact_with_email_but_no_phone_is_not_looked_up(enrich):
    _create_new_contact(email_to_use="jane@facility.com", found_phone="")

    enrich.assert_not_called()


def test_new_contact_without_email_is_looked_up_once_and_gets_the_email(enrich):
    hs = _create_new_contact(email_to_use="", found_phone="5551234567")

    enrich.assert_called_once()
    assert _written_properties(hs)["email"] == "jane.doe@facility.com"
