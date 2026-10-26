"""
A found person is only attached to an existing HubSpot contact when it is safe to.

_process_found_contact used to take the first contact with the same name and attach it, whoever
that contact worked for. These tests drive it the way a run does, with the real case of 2026-10-25
as the centrepiece: a person found at The Meridian by a vendor's people search, and a HubSpot
contact of the same name linked only to a customer's facility.

What it must do:
  - leave the other company's contact completely alone (no link, no title or email change);
  - make a separate contact instead, and say why on that contact's note;
  - still reuse a contact that is already linked to this facility or its operator, one linked to no
    company, one with the same email, and one that two independent sources place here;
  - when it cannot tell what a contact is linked to, make the separate contact rather than guess.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_contact_reuse_guard.py" -v
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep
from hubspot_client import HubSpotError

_FAC, _OP, _BEECHNUT = "28153294136", "48193149800", "28187803295"
_SEAN = "190698808613"
_NOTHING = {"email": "", "email_status": "", "personal_email": "", "phone": ""}
_OK = {"result": "ok", "status": "VERIFIED", "quality": "good"}


def _hs(*, candidates, links, names=None):
    """A HubSpot whose name search returns *candidates* and whose contacts are linked as *links* says."""
    hs = MagicMock()
    hs.search_contacts_by_email.return_value = []
    hs.search_contacts_by_name.return_value = candidates
    hs.create_contact.return_value = "999"
    hs.get_contact_properties.return_value = {"email": "", "phone": "", "jobtitle": "Administrator",
                                              "hs_linkedin_url": "", "most_recent_enrichment_date": ""}
    hs.get_contact_company_ids.side_effect = lambda cid: links[cid] if not isinstance(links[cid], Exception) else (_ for _ in ()).throw(links[cid])
    hs.get_company_properties.side_effect = lambda cid: {"name": (names or {}).get(cid, f"company {cid}")}
    return hs


def _sean(*, email="", company=_BEECHNUT):
    return {"id": _SEAN, "firstname": "Sean", "lastname": "Buelow", "email": email, "associatedcompanyid": company}


def _run(hs, *, source_path="fullenrich_people_search", found_email="", dh_verified=False, pre_populated="",
         related=(_FAC, _OP), facility_domain="meridiangalveston.com", found_name="Sean Buelow"):
    with patch.object(ep, "enrich_contact_info", return_value=dict(_NOTHING)), \
         patch.object(ep, "seamless_lookup", return_value=None), \
         patch.object(ep, "verify_email_millionverifier", return_value=dict(_OK)):
        return ep._process_found_contact(
            hs=hs, found_name=found_name, found_title="Executive Director", found_email=found_email,
            found_phone="", confidence="medium", pre_populated_contact_ids=pre_populated,
            target_company_id=_FAC, facility_name="The Meridian", research_findings="",
            source_path=source_path, facility_domain=facility_domain, dh_verified=dh_verified,
            operator_name="RJ Meridian Care", related_company_ids=related,
        )


def _linked(hs, contact_id):
    return any(c.args[:2] == (contact_id, _FAC) for c in hs.associate_contact_to_company.call_args_list)


def _touched(hs, contact_id):
    return any(c.args[0] == contact_id for c in hs.update_contact.call_args_list)


def _notes(hs):
    return " ".join(str(c.args[1]) for c in hs.create_note_on_contact.call_args_list)


# ── The case that went wrong ──────────────────────────────────────────────────

def test_a_contact_of_the_same_name_linked_only_to_another_company_is_left_alone():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]}, names={_BEECHNUT: "Focused Care At Beechnut"})

    action = _run(hs)

    assert not _linked(hs, _SEAN) and not _touched(hs, _SEAN)
    assert action.action == "created_new" and action.contact_id == "999"


def test_the_separate_contact_says_why_it_was_not_merged_and_with_whom():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]}, names={_BEECHNUT: "Focused Care At Beechnut"})

    _run(hs)

    note = _notes(hs)
    assert "Not merged" in note and _SEAN in note and "Focused Care At Beechnut" in note


def test_the_separate_contact_is_linked_to_this_facility():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]})

    _run(hs)

    assert _linked(hs, "999")


# ── When it is safe to reuse ──────────────────────────────────────────────────

@pytest.mark.parametrize("links", [[_FAC], [_OP], [_BEECHNUT, _FAC], [_BEECHNUT, _OP]])
def test_a_contact_already_linked_to_this_facility_or_its_operator_is_reused(links):
    hs = _hs(candidates=[_sean()], links={_SEAN: links})

    action = _run(hs)

    assert _linked(hs, _SEAN)
    hs.create_contact.assert_not_called()
    assert action.action != "created_new"


def test_a_contact_linked_to_no_company_is_reused():
    hs = _hs(candidates=[_sean(company="")], links={_SEAN: []})

    _run(hs)

    assert _linked(hs, _SEAN)
    hs.create_contact.assert_not_called()


def test_a_contact_with_the_same_email_is_the_same_person_wherever_it_is_linked():
    hs = _hs(candidates=[_sean(email="sbuelow@meridiangalveston.com")], links={_SEAN: [_BEECHNUT]})

    _run(hs, found_email="SBuelow@MeridianGalveston.com")

    assert _linked(hs, _SEAN)
    hs.create_contact.assert_not_called()


def test_two_independent_sources_are_enough_to_reuse_a_contact_linked_elsewhere():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]})

    _run(hs, source_path="state_association_bq_table", found_email="sbuelow@meridiangalveston.com")

    assert _linked(hs, _SEAN)
    hs.create_contact.assert_not_called()


def test_definitive_confirming_a_person_a_vendor_found_makes_two_sources():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]})

    _run(hs, source_path="fullenrich_people_search", dh_verified=True)

    assert _linked(hs, _SEAN)


def test_the_existing_contacts_own_address_at_the_facility_domain_counts_as_a_second_source():
    hs = _hs(candidates=[_sean(email="sbuelow@meridiangalveston.com")], links={_SEAN: [_BEECHNUT]})

    _run(hs, source_path="fullenrich_people_search")

    assert _linked(hs, _SEAN)


def test_one_source_twice_over_is_still_one_source():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]})

    _run(hs, source_path="definitive_healthcare", dh_verified=True)

    assert not _linked(hs, _SEAN) and hs.create_contact.called


def test_the_roster_alone_is_not_enough_for_a_contact_linked_elsewhere():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]})

    _run(hs, source_path="state_association_bq_table")

    assert not _linked(hs, _SEAN) and hs.create_contact.called


# ── Candidates, ids and failures ──────────────────────────────────────────────

def test_a_later_candidate_that_is_linked_here_is_used_when_the_first_is_not():
    other = {"id": "555", "firstname": "Sean", "lastname": "Buelow", "email": "", "associatedcompanyid": _FAC}
    hs = _hs(candidates=[_sean(), other], links={_SEAN: [_BEECHNUT], "555": [_FAC]})

    _run(hs)

    assert _linked(hs, "555") and not _linked(hs, _SEAN)
    hs.create_contact.assert_not_called()


def test_a_prepopulated_id_from_the_hubspot_copy_is_held_to_the_same_rule():
    hs = _hs(candidates=[], links={_SEAN: [_BEECHNUT]})

    _run(hs, pre_populated=_SEAN)

    assert not _linked(hs, _SEAN) and hs.create_contact.called


def test_a_prepopulated_id_already_linked_here_is_reused():
    hs = _hs(candidates=[], links={_SEAN: [_FAC]})

    _run(hs, pre_populated=_SEAN)

    assert _linked(hs, _SEAN)
    hs.create_contact.assert_not_called()


def test_when_hubspot_will_not_say_what_a_contact_is_linked_to_a_separate_contact_is_made():
    hs = _hs(candidates=[_sean()], links={_SEAN: HubSpotError("GET failed: 500", status_code=500)})

    action = _run(hs)

    assert not _linked(hs, _SEAN) and not _touched(hs, _SEAN)
    assert action.action == "created_new"
    assert "Not merged" in _notes(hs)


def test_a_company_name_that_cannot_be_read_does_not_stop_the_note():
    hs = _hs(candidates=[_sean()], links={_SEAN: [_BEECHNUT]})
    hs.get_company_properties.side_effect = HubSpotError("GET failed: 404", status_code=404)

    _run(hs)

    assert _BEECHNUT in _notes(hs)


def test_with_no_candidate_at_all_a_new_contact_is_made_as_before():
    hs = _hs(candidates=[], links={})

    action = _run(hs)

    assert action.action == "created_new" and "Not merged" not in _notes(hs)


def test_the_facility_the_run_is_for_always_counts_as_related_even_if_the_caller_names_no_others():
    hs = _hs(candidates=[_sean(company=_FAC)], links={_SEAN: [_FAC]})

    _run(hs, related=())

    assert _linked(hs, _SEAN)
