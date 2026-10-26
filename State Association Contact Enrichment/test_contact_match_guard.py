"""
When may a person the run just found be treated as a contact HubSpot already has?

On 2026-10-25 a test run found a person at one facility and, on the name alone, took the HubSpot
contact of that name, which belonged to a customer's account at another operator, linked it to the
new facility and changed its title and email. The name was the only thing checked.

The rule: an existing contact is reused when it is the same email, or it is already linked to this
facility or its operator, or it is linked to no company at all. A contact linked ONLY to other
companies is reused only when two independent kinds of evidence place the person at this facility.
Otherwise the run makes a separate contact and says why on its note; a person can merge two
records later, but nobody can easily undo a wrong overwrite.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_contact_match_guard.py" -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import contact_match_guard as g


def _signals(**over):
    base = dict(source_path="", dh_verified=False, found_email_at_domain=False, existing_email_at_domain=False)
    base.update(over)
    return g.signals_for(**base)


# ── What counts as a kind of evidence ─────────────────────────────────────────

@pytest.mark.parametrize("source_path, kind", [
    ("state_association_bq_table", "licensing_roster"),
    ("definitive_healthcare", "definitive"),
    ("definitive_healthcare:executives", "definitive"),
    ("fullenrich", "vendor_search"),
    ("fullenrich_people_search", "vendor_search"),
    ("sonar_pro", "web_research"),
    ("sonar_pro_facility_sweep", "web_research"),
    ("sonar_pro_discovery", "web_research"),
])
def test_where_a_person_was_found_is_one_kind_of_evidence(source_path, kind):
    assert _signals(source_path=source_path) == {kind}


@pytest.mark.parametrize("source_path", ["", "existing_contact", "something_new"])
def test_a_source_that_places_no_one_at_the_facility_is_no_evidence(source_path):
    assert _signals(source_path=source_path) == set()


def test_definitive_confirming_the_person_is_evidence_even_when_they_were_found_elsewhere():
    assert _signals(source_path="sonar_pro", dh_verified=True) == {"web_research", "definitive"}


def test_definitive_twice_is_still_one_kind_of_evidence():
    assert _signals(source_path="definitive_healthcare", dh_verified=True) == {"definitive"}


def test_an_address_at_the_facilitys_or_operators_domain_is_evidence():
    assert _signals(found_email_at_domain=True) == {"email_at_facility_domain"}


def test_the_existing_contacts_own_address_at_the_domain_is_evidence():
    assert _signals(existing_email_at_domain=True) == {"existing_email_at_facility_domain"}


# ── The decision ──────────────────────────────────────────────────────────────

def _decide(candidate=("OTHER",), related=("FACILITY", "OPERATOR"), signals=(), same_email=False):
    return g.decide(candidate_company_ids=set(candidate), related_company_ids=set(related), signals=set(signals),
                    same_email=same_email)


def test_the_same_email_is_the_same_person_wherever_they_are_linked():
    d = _decide(same_email=True)

    assert d.reuse and "email" in d.reason


@pytest.mark.parametrize("linked", [("FACILITY",), ("OPERATOR",), ("OTHER", "FACILITY"), ("OTHER", "OPERATOR")])
def test_a_contact_already_linked_to_this_facility_or_its_operator_is_reused_without_further_evidence(linked):
    d = _decide(candidate=linked)

    assert d.reuse and "already linked" in d.reason


def test_a_contact_linked_to_no_company_is_reused():
    d = _decide(candidate=())

    assert d.reuse and "no company" in d.reason


def test_a_contact_linked_only_elsewhere_is_reused_when_two_kinds_of_evidence_agree():
    d = _decide(signals=("licensing_roster", "vendor_search"))

    assert d.reuse and "2" in d.reason


def test_a_contact_linked_only_elsewhere_is_not_reused_on_one_kind_of_evidence():
    d = _decide(signals=("vendor_search",))

    assert not d.reuse
    assert "OTHER" in d.reason and "vendor" in d.reason


def test_a_contact_linked_only_elsewhere_is_not_reused_on_no_evidence():
    d = _decide(signals=())

    assert not d.reuse and "OTHER" in d.reason


def test_ids_are_compared_as_text_whatever_they_arrive_as():
    d = g.decide(candidate_company_ids={28153294136}, related_company_ids={"28153294136"}, signals=set())

    assert d.reuse


def test_every_other_company_is_named_in_the_reason():
    d = _decide(candidate=("OTHER", "ANOTHER"), signals=("vendor_search",))

    assert "OTHER" in d.reason and "ANOTHER" in d.reason


def test_the_reason_says_how_much_more_evidence_was_needed():
    d = _decide(signals=("vendor_search",))

    assert "two" in d.reason.lower()
