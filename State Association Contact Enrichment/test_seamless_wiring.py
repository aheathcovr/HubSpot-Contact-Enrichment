"""
Where Seamless fits in the contact flow.

  Phone:  Definitive / the existing HubSpot record first, then Seamless. Never FullEnrich
          (it charges ten credits for a phone; the service asks it for email only).
  Email:  Definitive / existing, then FullEnrich, then Seamless as a fallback. Whatever
          Seamless returns goes through the same gates as any vendor address: held unless
          it is at the facility or its operator, then verified.

One Seamless call serves both gaps. A contact that already has an email and a phone is
never researched, and an existing value is never overwritten.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_seamless_wiring.py" -v
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep

_OK = {"result": "ok", "status": "VERIFIED", "quality": "good"}
_NO_FE = {"email": "", "email_status": "", "personal_email": "", "phone": ""}


def _sl(**over) -> dict:
    base = {
        "status": "done", "moved": False, "company": "Valley View Care Center",
        "company_domain": "facility.com", "title": "Administrator", "linkedin": "",
        "phone": "330-702-0110", "phone_type": "mobile", "phone_confidence": 96.0, "phone_rejected": "",
        "email": "jane.doe@facility.com", "email_confidence": 98.0, "email_rejected": "",
    }
    base.update(over)
    return base


@pytest.fixture
def vendors():
    """FullEnrich finds nothing, MillionVerifier says ok, Seamless returns a good row."""
    with patch.object(ep, "enrich_contact_info", return_value=dict(_NO_FE)) as fe, \
         patch.object(ep, "verify_email_millionverifier", return_value=dict(_OK)) as mv, \
         patch.object(ep, "seamless_lookup", return_value=_sl()) as seamless:
        yield type("V", (), {"fe": fe, "mv": mv, "seamless": seamless})


def _written(hs) -> dict:
    merged: dict = {}
    for call in hs.update_contact.call_args_list:
        merged.update(call.args[1])
    return merged


def _note(hs) -> str:
    return " ".join(str(c.args[1]) for c in hs.create_note_on_contact.call_args_list)


def _existing(existing_props, *, found_phone="", base=None, operator_name="Valley View Holdings"):
    hs = MagicMock()
    hs.get_contact_properties.return_value = {"jobtitle": "Administrator", **existing_props}
    action_base = base if base is not None else {}
    ep._enrich_and_associate_existing_contact(
        hs=hs, matched_contact_id="42", matched_detail="matched by name", found_name="Jane Doe",
        found_title="Administrator", found_email="", found_phone=found_phone, found_linkedin="",
        facility_name="Valley View", facility_domain="facility.com", email_validation={}, email_to_use="",
        data_freshness_value="", research_reasoning="", source_path="sonar_pro", _base=action_base,
        operator_name=operator_name,
    )
    return hs


def _new(*, email_to_use="", found_phone="", base=None):
    hs = MagicMock()
    hs.create_contact.return_value = "999"
    ep._create_and_enrich_new_contact(
        hs=hs, first="Jane", last="Doe", found_title="Administrator", found_name="Jane Doe",
        email_to_use=email_to_use, found_phone=found_phone, found_linkedin="",
        facility_name="Valley View", facility_domain="facility.com", target_company_id="1",
        data_freshness_value="", research_reasoning="", _base=base if base is not None else {},
        operator_name="Valley View Holdings",
    )
    return hs


# ── When Seamless is asked ────────────────────────────────────────────────────

def test_a_contact_with_an_email_and_no_phone_is_researched_for_the_phone_alone(vendors):
    hs = _existing({"email": "jane@facility.com", "phone": ""})

    vendors.seamless.assert_called_once()
    assert vendors.seamless.call_args.args[:2] == ("Jane", "Doe")
    assert vendors.seamless.call_args.kwargs["domain"] == "facility.com"
    assert vendors.seamless.call_args.kwargs["company"] == "Valley View"
    assert vendors.seamless.call_args.kwargs["operator_name"] == "Valley View Holdings"
    assert _written(hs)["phone"] == "330-702-0110"
    assert "email" not in _written(hs)                  # the email it already has is left alone
    vendors.fe.assert_not_called()                      # FullEnrich is for email only, and there is one


def test_a_contact_that_already_has_both_is_never_researched(vendors):
    _existing({"email": "jane@facility.com", "phone": "5551234567"})

    vendors.seamless.assert_not_called()


def test_one_call_serves_both_gaps(vendors):
    _existing({"email": "", "phone": ""})

    assert vendors.seamless.call_count == 1


# ── The phone ─────────────────────────────────────────────────────────────────

def test_the_phone_is_written_noted_and_credited_to_seamless(vendors):
    base: dict = {}
    hs = _existing({"email": "jane@facility.com", "phone": ""}, base=base)

    assert _written(hs)["phone"] == "330-702-0110"
    assert base["found_phone"] == "330-702-0110"
    assert base["found_phone_source"] == "Seamless"
    assert "Seamless" in _note(hs) and "330-702-0110" in _note(hs)


def test_an_existing_phone_is_never_overwritten(vendors):
    hs = _existing({"email": "jane@facility.com", "phone": "5551234567"})

    assert "phone" not in _written(hs)


def test_a_definitive_phone_is_not_replaced_by_seamless(vendors):
    hs = _existing({"email": "", "phone": ""}, found_phone="5559998888")

    assert _written(hs)["phone"] == "5559998888"


def test_a_rejected_phone_is_explained_on_the_note_and_not_written(vendors):
    vendors.seamless.return_value = _sl(phone="", phone_rejected="the facility's main line (a switchboard, not a direct dial)")

    hs = _existing({"email": "jane@facility.com", "phone": ""})

    assert "phone" not in _written(hs)
    assert "switchboard" in _note(hs)


# ── The email: a fallback behind FullEnrich ───────────────────────────────────

def test_seamless_supplies_an_email_when_nobody_else_found_one_and_it_is_verified_and_recorded(vendors):
    hs = _existing({"email": "", "phone": ""})

    written = _written(hs)
    assert written["email"] == "jane.doe@facility.com"
    assert written["email_verification_result"] == "ok"
    vendors.mv.assert_called_once_with("jane.doe@facility.com")
    assert "Seamless" in _note(hs)


def test_a_fullenrich_email_wins_and_seamless_is_not_used_for_email(vendors):
    vendors.fe.return_value = {"email": "found@facility.com", "email_status": "DELIVERABLE", "personal_email": "", "phone": ""}

    hs = _existing({"email": "", "phone": ""})

    assert _written(hs)["email"] == "found@facility.com"
    assert _written(hs)["phone"] == "330-702-0110"      # still takes the phone
    verified = [c.args[0] for c in vendors.mv.call_args_list]
    assert "jane.doe@facility.com" not in verified      # Seamless's email is not even verified


def test_a_seamless_email_at_an_unrelated_company_is_held_not_verified(vendors):
    vendors.seamless.return_value = _sl(email="jane.doe@strangerco.com")

    hs = _existing({"email": "", "phone": ""})

    assert "email" not in _written(hs)
    vendors.mv.assert_not_called()
    assert "held" in _note(hs).lower() and "strangerco.com" in _note(hs)
    assert _written(hs)["phone"] == "330-702-0110"      # the phone is judged separately


def test_a_seamless_email_millionverifier_calls_invalid_is_suppressed(vendors):
    vendors.mv.return_value = {"result": "invalid", "status": "INVALID", "quality": "bad"}

    hs = _existing({"email": "", "phone": ""})

    assert "email" not in _written(hs)
    assert "suppressed" in _note(hs).lower()


def test_an_existing_email_is_never_replaced_by_seamless(vendors):
    hs = _existing({"email": "jane@facility.com", "phone": ""})

    assert "email" not in _written(hs) or _written(hs)["email"] == "jane@facility.com"


# ── Someone who has moved ─────────────────────────────────────────────────────

def test_a_person_who_has_moved_yields_nothing_and_the_note_says_where(vendors):
    vendors.seamless.return_value = _sl(moved=True, company="Acme Staffing", phone="", email="",
                                        phone_rejected="no longer at this organisation",
                                        email_rejected="no longer at this organisation")

    hs = _existing({"email": "jane@facility.com", "phone": ""})

    assert "phone" not in _written(hs)
    assert "Acme Staffing" in _note(hs)


# ── Seamless unavailable ──────────────────────────────────────────────────────

def test_when_seamless_has_nothing_the_run_is_unchanged(vendors):
    vendors.seamless.return_value = None

    hs = _existing({"email": "jane@facility.com", "phone": ""})

    assert "phone" not in _written(hs)
    assert "most_recent_enrichment_date" in _written(hs)   # the run still completes and stamps


# ── A newly created contact ───────────────────────────────────────────────────

def test_a_new_contact_gets_its_phone_from_seamless(vendors):
    base: dict = {}
    hs = _new(email_to_use="jane@facility.com", base=base)

    assert _written(hs)["phone"] == "330-702-0110"
    assert base["found_phone_source"] == "Seamless"
    vendors.fe.assert_not_called()


def test_a_new_contact_with_nothing_gets_a_verified_seamless_email_as_a_fallback(vendors):
    hs = _new(email_to_use="")

    assert _written(hs)["email"] == "jane.doe@facility.com"
    assert _written(hs)["phone"] == "330-702-0110"
    assert vendors.seamless.call_count == 1


def test_a_new_contact_that_has_both_is_never_researched(vendors):
    _new(email_to_use="jane@facility.com", found_phone="5551234567")

    vendors.seamless.assert_not_called()


# ── LinkedIn rides along on the same credit ───────────────────────────────────

def test_a_linkedin_url_comes_along_when_the_contact_has_none(vendors):
    vendors.seamless.return_value = _sl(linkedin="https://www.linkedin.com/in/janedoe")

    hs = _existing({"email": "jane@facility.com", "phone": "", "hs_linkedin_url": ""})

    assert _written(hs)["hs_linkedin_url"] == "https://www.linkedin.com/in/janedoe"
    assert "Seamless" in _note(hs)


def test_an_existing_linkedin_url_is_not_replaced(vendors):
    vendors.seamless.return_value = _sl(linkedin="https://www.linkedin.com/in/someone-else")

    hs = _existing({"email": "jane@facility.com", "phone": "", "hs_linkedin_url": "https://www.linkedin.com/in/janedoe"})

    assert "hs_linkedin_url" not in _written(hs)
