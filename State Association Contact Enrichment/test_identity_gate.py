"""
Is an address a vendor found actually at the organisation we asked about?

MillionVerifier answers "is this mailbox real". It cannot answer "is this the
right person". A vendor that matches company NAME loosely, or finds a person who
has since moved, returns a real, deliverable mailbox at a different company, and
verification passes it. The outbound repo measured 15 of 29 "sendable" Seamless
contacts being strangers this way, and FullEnrich returns the person's CURRENT
employer's address, which is a stranger's whenever they have left.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_identity_gate.py" -v
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest

from identity_gate import bare_domain, same_org_domain, same_org_or_affiliate


# ── same_org_domain: the strict test ──────────────────────────────────────────

def test_the_same_host_matches_and_so_does_a_subdomain():
    assert same_org_domain("jane@valleyview.org", "valleyview.org")
    assert same_org_domain("jane@mail.valleyview.org", "valleyview.org")
    assert same_org_domain("jane@valleyview.org", "www.valleyview.org/about")


@pytest.mark.parametrize("website", [
    "https://valleyview.org/",
    "https://www.valleyview.org/about?x=1#top",
    "HTTP://ValleyView.org",
    "valleyview.org:8080/contact",
    "https://user@valleyview.org",
])
def test_a_website_url_counts_as_its_host_because_that_is_what_hubspot_stores(website):
    assert same_org_domain("jane@valleyview.org", website)
    assert same_org_domain("jane@mail.valleyview.org", website)
    assert not same_org_domain("jane@other.org", website)


@pytest.mark.parametrize("value", ["https://", "http:///x", "N/A", "none", "localhost", "valley view.org", "", None])
def test_a_value_that_is_not_a_host_name_matches_nothing(value):
    assert not same_org_domain("jane@valleyview.org", value)
    assert not same_org_domain("jane@https", value)


@pytest.mark.parametrize("value, host", [
    ("https://www.valleyview.org/about", "valleyview.org"),
    ("HTTP://ValleyView.org:8443/x?y=1#z", "valleyview.org"),
    ("www.valleyview.org", "valleyview.org"),
    ("valleyview.org", "valleyview.org"),
    ("  valleyview.org.  ", "valleyview.org"),
    ("https://valleyview.org?utm_source=x", "valleyview.org"),
    ("valleyview.org#top", "valleyview.org"),
    ("https://", ""),
    ("N/A", ""),
    ("", ""),
    (None, ""),
])
def test_bare_domain_is_the_host_or_nothing(value, host):
    assert bare_domain(value) == host


def test_two_agencies_under_one_public_suffix_are_two_different_organisations():
    assert not same_org_domain("jane@dor.ms.gov", "msva.ms.gov")


def test_unknown_is_never_a_match():
    assert not same_org_domain("jane@valleyview.org", "")
    assert not same_org_domain("jane@valleyview.org", None)
    assert not same_org_domain("", "valleyview.org")
    assert not same_org_domain("not-an-address", "valleyview.org")


# ── same_org_or_affiliate: the operator's own other domain ────────────────────

def test_an_operator_that_mails_from_a_different_domain_matches_on_a_distinctive_word():
    # Warm Hearth Village publishes at retire.org and mails from warmhearth.org.
    assert same_org_or_affiliate("jane@warmhearth.org", "retire.org", "Warm Hearth Village")


def test_generic_trade_words_never_match_on_their_own():
    assert not same_org_or_affiliate("jane@seniorliving.com", "other.org", "Senior Living Services")


def test_a_stranger_at_an_unrelated_company_does_not_match():
    assert not same_org_or_affiliate("jane@pjt.com", "msva.ms.gov", "Mississippi State Veterans Homes")


def test_public_sector_and_academic_hosts_never_match_on_a_name_fragment():
    assert not same_org_or_affiliate("jane@valleyview.gov", "other.org", "Valley View")
    assert not same_org_or_affiliate("jane@valleyview.edu", "other.org", "Valley View")


def test_an_address_with_no_host_never_matches():
    assert not same_org_or_affiliate("", "valleyview.org", "Valley View")
