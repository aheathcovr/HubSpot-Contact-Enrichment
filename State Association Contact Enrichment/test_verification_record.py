"""
Recording the verification answer on the contact, and holding off-domain vendor emails.

Two properties already exist in the HubSpot portal for exactly this and the
outbound pipeline reads them: `email_verification_result` (text) and
`email_verification_date` (date). They let a rep, and the lead loaders that decide
which mailbox a send comes from, see what MillionVerifier said instead of
re-buying it. This service verified every address and then threw the answer away,
keeping it only in a note.

The value written is MillionVerifier's own word (ok / catch_all / unknown /
invalid / disposable), the vocabulary the outbound code hard-codes.

A vendor-found address is held, not written, when it is not at the facility, its
operator, or an obvious affiliate. A held address is never verified either:
paying to confirm a stranger's mailbox is wasted credit.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_verification_record.py" -v
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep


def _mv(result: str, status: str, quality: str = "good") -> dict:
    return {"result": result, "status": status, "quality": quality}


@pytest.fixture
def mv():
    """MillionVerifier answers `ok` unless a test overrides it."""
    with patch.object(ep, "verify_email_millionverifier", return_value=_mv("ok", "VERIFIED")) as verify:
        yield verify


@pytest.fixture
def fullenrich():
    """FullEnrich finds one work email; tests set .return_value["email"]."""
    found = {"email": "jane.doe@facility.com", "email_status": "DELIVERABLE", "personal_email": "", "phone": ""}
    with patch.object(ep, "enrich_contact_info", return_value=found) as enrich:
        yield enrich


def _written(hs) -> dict:
    merged: dict = {}
    for call in hs.update_contact.call_args_list:
        merged.update(call.args[1])
    return merged


def _existing(existing_props, *, found_email="", email_validation=None, operator_name="",
              facility_name="Valley View", facility_domain="facility.com"):
    hs = MagicMock()
    hs.get_contact_properties.return_value = {"jobtitle": "Administrator", **existing_props}
    ep._enrich_and_associate_existing_contact(
        hs=hs, matched_contact_id="42", matched_detail="matched by name", found_name="Jane Doe",
        found_title="Administrator", found_email=found_email, found_phone="", found_linkedin="",
        facility_name=facility_name, facility_domain=facility_domain,
        email_validation=email_validation or {}, email_to_use=found_email,
        data_freshness_value="", research_reasoning="", source_path="sonar_pro", _base={},
        operator_name=operator_name,
    )
    return hs


def _new(*, email_to_use="", base=None, operator_name="", facility_name="Valley View",
         facility_domain="facility.com"):
    hs = MagicMock()
    hs.create_contact.return_value = "999"
    ep._create_and_enrich_new_contact(
        hs=hs, first="Jane", last="Doe", found_title="Administrator", found_name="Jane Doe",
        email_to_use=email_to_use, found_phone="5551234567", found_linkedin="",
        facility_name=facility_name, facility_domain=facility_domain, target_company_id="1",
        data_freshness_value="", research_reasoning="", _base=base if base is not None else {},
        operator_name=operator_name,
    )
    return hs


def _note(hs) -> str:
    return " ".join(str(c.args[1]) for c in hs.create_note_on_contact.call_args_list)


# ── Recording the answer ──────────────────────────────────────────────────────

def test_an_existing_contacts_address_gets_its_result_recorded(mv):
    hs = _existing({"email": "jane@facility.com", "phone": "5551234567"})

    written = _written(hs)
    assert written["email_verification_result"] == "ok"
    assert written["email_verification_date"].isdigit()      # epoch ms, as the date properties take


def test_a_catch_all_is_recorded_as_catch_all_not_as_ok(mv):
    mv.return_value = _mv("catch_all", "RISKY", "risky")

    hs = _existing({"email": "jane@facility.com", "phone": "5551234567"})

    assert _written(hs)["email_verification_result"] == "catch_all"


def test_an_invalid_existing_address_is_flagged_on_the_record_but_not_replaced(mv):
    mv.return_value = _mv("invalid", "INVALID", "bad")

    hs = _existing({"email": "jane@facility.com", "phone": "5551234567"})

    written = _written(hs)
    assert written["email_verification_result"] == "invalid"
    assert "email" not in written


def test_a_found_address_written_to_a_contact_with_none_has_its_result_recorded(mv):
    hs = _existing(
        {"email": "", "phone": "5551234567"},
        found_email="jane@facility.com",
        email_validation=_mv("catch_all", "RISKY", "risky"),
    )

    written = _written(hs)
    assert written["email"] == "jane@facility.com"
    assert written["email_verification_result"] == "catch_all"


def test_the_result_of_a_different_address_is_not_recorded_against_the_contacts_own(mv):
    # The contact keeps jane@facility.com; the researched address was only checked.
    hs = _existing(
        {"email": "jane@facility.com", "phone": "5551234567"},
        found_email="other@facility.com",
        email_validation=_mv("ok", "VERIFIED"),
    )

    assert "email_verification_result" not in _written(hs)


def test_a_fullenrich_address_that_is_written_has_its_result_recorded(mv, fullenrich):
    mv.return_value = _mv("catch_all", "RISKY", "risky")

    hs = _existing({"email": "", "phone": "5551234567"})

    written = _written(hs)
    assert written["email"] == "jane.doe@facility.com"
    assert written["email_verification_result"] == "catch_all"


def test_a_new_contact_is_created_with_the_result_of_the_address_it_carries():
    hs = _new(email_to_use="jane@facility.com", base={"email_validation_result": "ok"})

    props = hs.create_contact.call_args.args[0]
    assert props["email"] == "jane@facility.com"
    assert props["email_verification_result"] == "ok"
    assert props["email_verification_date"].isdigit()


def test_nothing_is_recorded_when_verification_did_not_actually_run(mv):
    # No API key, or the API refused: there is no answer to record, and writing
    # "skipped" or "error" into a field the outbound loaders read would mislead them.
    mv.return_value = {"result": "skipped", "status": "UNVERIFIED", "quality": ""}

    hs = _existing({"email": "jane@facility.com", "phone": "5551234567"})

    assert "email_verification_result" not in _written(hs)
    assert "email_verification_date" not in _written(hs)


# ── Holding off-domain vendor emails ──────────────────────────────────────────

def test_a_fullenrich_address_at_an_unrelated_company_is_held_not_written_or_verified(mv, fullenrich):
    fullenrich.return_value["email"] = "jane.doe@strangerco.com"

    hs = _existing({"email": "", "phone": "5551234567"})

    assert "email" not in _written(hs)
    mv.assert_not_called()
    assert "held" in _note(hs).lower()
    assert "strangerco.com" in _note(hs)


def test_a_fullenrich_address_at_the_facility_domain_is_written(mv, fullenrich):
    hs = _existing({"email": "", "phone": "5551234567"})

    assert _written(hs)["email"] == "jane.doe@facility.com"


def test_a_fullenrich_address_at_the_operators_own_domain_is_written(mv, fullenrich):
    fullenrich.return_value["email"] = "jane.doe@genesishcc.com"

    hs = _existing(
        {"email": "", "phone": "5551234567"},
        facility_name="Eden Park Nursing Center", facility_domain="edenpark.org",
        operator_name="Genesis Healthcare",
    )

    assert _written(hs)["email"] == "jane.doe@genesishcc.com"


def test_the_same_holds_for_a_newly_created_contact(mv, fullenrich):
    fullenrich.return_value["email"] = "jane.doe@strangerco.com"

    hs = _new(email_to_use="")

    assert "email" not in _written(hs)
    mv.assert_not_called()
    assert "held" in _note(hs).lower()


def test_a_researched_vendor_address_off_the_domain_is_held_before_it_is_verified(mv):
    """The path where FullEnrich found the person (people search) and the address."""
    hs = MagicMock()
    hs.search_contacts_by_email.return_value = []
    hs.search_contacts_by_name.return_value = []
    hs.create_contact.return_value = "999"
    with patch.object(ep, "enrich_contact_info", return_value={"email": "", "email_status": "", "personal_email": "", "phone": ""}):
        ep._process_found_contact(
            hs=hs, found_name="Jane Doe", found_title="Administrator",
            found_email="jane.doe@strangerco.com", found_phone="", confidence="medium",
            pre_populated_contact_ids="", target_company_id="1", facility_name="Valley View",
            research_findings="FullEnrich", source_path="fullenrich", facility_domain="facility.com",
        )

    mv.assert_not_called()
    assert "email" not in hs.create_contact.call_args.args[0]


def test_an_address_from_definitive_is_not_second_guessed_by_the_gate(mv):
    hs = MagicMock()
    hs.search_contacts_by_email.return_value = []
    hs.search_contacts_by_name.return_value = []
    hs.create_contact.return_value = "999"
    with patch.object(ep, "enrich_contact_info", return_value={"email": "", "email_status": "", "personal_email": "", "phone": ""}):
        ep._process_found_contact(
            hs=hs, found_name="Jane Doe", found_title="Administrator",
            found_email="jane.doe@parent-operator.com", found_phone="", confidence="high",
            pre_populated_contact_ids="", target_company_id="1", facility_name="Valley View",
            research_findings="", source_path="definitive_healthcare", facility_domain="facility.com",
        )

    assert hs.create_contact.call_args.args[0]["email"] == "jane.doe@parent-operator.com"
