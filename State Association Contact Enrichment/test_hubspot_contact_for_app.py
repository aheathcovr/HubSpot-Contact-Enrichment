"""
What the HubSpot app's contact card needs to know about a contact.

The card shows the contact's email (and how it checked out) and mobile number, and the service decides from them
which objectives are open: Find email only for a contact with no email, Find mobile only for one with no mobile.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_hubspot_contact_for_app.py" -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hubspot_client import HubSpotClient, HubSpotError


class _Recorded(HubSpotClient):
    def __init__(self, page):
        super().__init__(api_key="test-key")
        self.page = page
        self.calls = []

    def _get(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        if isinstance(self.page, Exception):
            raise self.page
        return self.page


_ALL = {
    "firstname": "Pat", "lastname": "Lee", "jobtitle": "Administrator", "email": "pat.lee@example.org",
    "mobilephone": "405-555-0101", "phone": "405-555-0100", "hs_linkedin_url": "https://www.linkedin.com/in/pat-lee",
    "email_verification_result": "ok", "email_verification_date": "2026-10-21",
}


def test_it_returns_what_the_card_and_the_objectives_need():
    client = _Recorded({"id": "42", "properties": dict(_ALL)})

    got = client.get_contact_for_app("42")

    assert got == {
        "firstname": "Pat", "lastname": "Lee", "jobtitle": "Administrator", "email": "pat.lee@example.org",
        "mobilephone": "405-555-0101", "phone": "405-555-0100", "hs_linkedin_url": "https://www.linkedin.com/in/pat-lee",
        "email_verification_result": "ok", "email_verification_date": "2026-10-21",
    }


def test_it_asks_for_exactly_those_properties_of_that_contact():
    client = _Recorded({"properties": {}})

    client.get_contact_for_app("42")

    (path, params), = client.calls
    assert path == "/crm/v3/objects/contacts/42"
    assert set(params["properties"].split(",")) == set(_ALL)


@pytest.mark.parametrize("empty", [None, ""])
def test_a_property_with_no_value_is_an_empty_string(empty):
    client = _Recorded({"properties": {**_ALL, "email": empty, "mobilephone": empty}})

    got = client.get_contact_for_app("42")

    assert got["email"] == "" and got["mobilephone"] == ""


def test_a_contact_with_no_properties_at_all_is_all_empty_strings():
    got = _Recorded({}).get_contact_for_app("42")

    assert set(got) == set(_ALL) and all(v == "" for v in got.values())


def test_surrounding_spaces_are_taken_off():
    got = _Recorded({"properties": {**_ALL, "firstname": "  Pat ", "email": " pat.lee@example.org "}}).get_contact_for_app("42")

    assert got["firstname"] == "Pat" and got["email"] == "pat.lee@example.org"


def test_a_failed_request_is_not_hidden():
    with pytest.raises(HubSpotError):
        _Recorded(HubSpotError("GET failed", status_code=500)).get_contact_for_app("42")
