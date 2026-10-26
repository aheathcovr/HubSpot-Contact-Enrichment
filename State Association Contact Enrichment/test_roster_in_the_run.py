"""
The state roster inside a run: the cheapest source, asked first, and good enough to stop there.

When the roster names a facility's licensed administrator the run has its answer: no web search is
paid for and no vendor is asked to discover anyone. What it does not carry is a phone or an email:
those columns are the facility's, and passing them on would have a facility main line looked up as
if it were a person's own number.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_roster_in_the_run.py" -v
"""

import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep
import roster

_PROPS = {"name": "The Meridian", "city": "Galveston", "state": "TX", "facility_type": "SNF",
          "hierarchy_type": ep.CHILD_HIERARCHY_TYPE, "website": "meridiangalveston.com"}


def _contact():
    return roster.row_to_contact({
        "administrator": "SEAN L BUELOW", "source_url": "https://apps.hhs.texas.gov/providers",
        "extracted_date": "2026-10-25", "source_state": "TX", "phone": "409-763-6437", "email": "info@x.com",
    })


@pytest.fixture
def world(monkeypatch):
    w = type("World", (), {})()
    w.asked = []
    w.lookups = []
    monkeypatch.setattr(ep, "query_state_association_bq_table",
                        lambda *a, **k: w.asked.append((a, k)) or _contact())
    monkeypatch.setattr(ep, "lookup_hubspot_contact_enhanced",
                        lambda **kw: w.lookups.append(kw) or ([], "name_not_found"))
    monkeypatch.setattr(ep, "call_openrouter",
                        lambda *a, **k: pytest.fail("the roster named someone: no web search should be paid for"))
    monkeypatch.setattr(ep, "search_facility_contacts",
                        lambda *a, **k: pytest.fail("the roster named someone: no vendor should be asked to find anyone"))
    return w


def _run():
    return ep._run_workflow1_single_facility(
        bq_client=MagicMock(), hs=MagicMock(), company_id="28153294136", props=dict(_PROPS),
        parent_corp_name="RJ Meridian Care", missing=pd.DataFrame([{"hubspot_id": "28153294136"}]),
    )


def test_a_roster_that_names_the_administrator_ends_the_search(world):
    results = _run()

    assert len(results) == 1
    assert results[0]["found_name"] == "Sean Buelow" and results[0]["found_title"] == "Administrator"
    assert results[0]["source_path"] == "state_association_bq_table"


def test_the_roster_is_asked_for_the_facilitys_kind_as_well_as_its_name(world):
    _run()

    (args, _), = world.asked
    assert args[1:] == ("The Meridian", "Galveston", "TX", "SNF")


def test_no_phone_or_email_is_offered_to_the_hubspot_match_as_the_persons(world):
    _run()

    (lookup,) = world.lookups
    assert lookup["full_name"] == "Sean Buelow"
    assert lookup["email"] == "" and lookup["phone"] == ""


def test_where_and_when_the_roster_said_so_travels_with_the_result(world):
    results = _run()

    assert results[0]["source_date"] == "2026-10-25" and results[0]["source_state"] == "TX"
    assert results[0]["research_findings"] == "https://apps.hhs.texas.gov/providers"
