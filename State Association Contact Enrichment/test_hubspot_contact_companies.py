"""
Every company a contact is linked to, not only the primary one.

The mirror's `associatedcompanyid` and the name search's `associatedcompanyid` are the PRIMARY
company only. A contact can be linked to several (Sean Buelow was linked to The Meridian and to a
customer's facility at once), and whether one of them is the facility being enriched is exactly what
decides whether the contact may be reused.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_hubspot_contact_companies.py" -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hubspot_client import HubSpotClient, HubSpotError


class _Recorded(HubSpotClient):
    def __init__(self, *pages):
        super().__init__(api_key="test-key")
        self.pages = list(pages)
        self.calls = []

    def _get(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return page


def test_every_linked_company_is_returned_as_text():
    client = _Recorded({"results": [{"toObjectId": 28153294136, "associationTypes": []},
                                    {"toObjectId": "28187803295", "associationTypes": []}]})

    assert client.get_contact_company_ids("190698808613") == ["28153294136", "28187803295"]


def test_it_asks_the_contacts_company_associations():
    client = _Recorded({"results": []})

    client.get_contact_company_ids("190698808613")

    assert client.calls[0][0] == "/crm/v4/objects/contacts/190698808613/associations/companies"


def test_a_contact_linked_to_nothing_has_no_companies():
    assert _Recorded({"results": []}).get_contact_company_ids("1") == []
    assert _Recorded({}).get_contact_company_ids("1") == []


def test_later_pages_are_followed():
    client = _Recorded(
        {"results": [{"toObjectId": 1}], "paging": {"next": {"after": "abc"}}},
        {"results": [{"toObjectId": 2}]},
    )

    assert client.get_contact_company_ids("1") == ["1", "2"]
    assert client.calls[1][1].get("after") == "abc"


def test_a_row_with_no_company_id_is_skipped():
    assert _Recorded({"results": [{"associationTypes": []}, {"toObjectId": 5}]}).get_contact_company_ids("1") == ["5"]


def test_a_failed_request_is_not_hidden():
    with pytest.raises(HubSpotError):
        _Recorded(HubSpotError("GET failed: 500", status_code=500)).get_contact_company_ids("1")
