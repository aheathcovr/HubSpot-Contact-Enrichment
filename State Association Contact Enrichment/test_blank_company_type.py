"""
A company whose Company Type is blank (or neither Facility nor Corporation) gets a note that asks for the type, and
nothing else.

The service researches a facility's leaders and a corporation's executives differently, so it has to know which one a
company is. Until now a blank type still ended a run the same way as any other: a note full of empty counts and a
research process that never ran, and then the enrichment date stamped, which the 30-day guard (and the HubSpot card)
treat as "enriched" for the next month. On 2026-10-28 the first run from the app did exactly that, and the rep could not
ask again. About one company in eleven has no Company Type.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_blank_company_type.py" -v
"""

import os
import sys
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep

BLANK = {"name": "Maple Grove Care", "hierarchy_type": ""}
UNSET = {"name": "Maple Grove Care"}
# Company Type's other options in HubSpot: Partner, Staffing Agency, Other (stored as these values).
PARTNER = {"name": "Acme Partners", "hierarchy_type": "Partner"}
OTHER = {"name": "Acme Staffing", "hierarchy_type": "Staffing Agency"}

# Everything that costs a vendor call or touches the research path: any of them being called is a failure.
RESEARCH_STEPS = (
    "_get_bigquery_client", "check_hubspot_mirror_freshness", "find_facilities_missing_leadership",
    "_run_workflow1_single_facility", "workflow_2_research_contacts", "search_corporation_website_for_executives",
    "search_facility_contacts", "_process_found_contact",
)


@pytest.fixture
def run(monkeypatch):
    """run_enrichment with HubSpot faked and every research step recording that it was reached."""
    hs = MagicMock()
    state = SimpleNamespace(hs=hs, props=dict(BLANK), reached=[])
    hs.get_company_properties.side_effect = lambda company_id: dict(state.props)
    monkeypatch.setattr(ep, "_get_hubspot_client", lambda: hs)

    def reached(name):
        def step(*args, **kwargs):
            state.reached.append(name)
            return [] if name != "check_hubspot_mirror_freshness" else None
        return step

    for name in RESEARCH_STEPS:
        monkeypatch.setattr(ep, name, reached(name))
    return state


def _note(state) -> str:
    return state.hs.create_note_on_company.call_args.args[1]


def _updates(state) -> list:
    return [call.args[1] for call in state.hs.update_company.call_args_list]


@pytest.mark.parametrize("props", [BLANK, UNSET, PARTNER, OTHER])
def test_a_company_the_service_cannot_classify_gets_no_research_at_all(run, props):
    run.props = dict(props)

    ep.run_enrichment("42")

    assert run.reached == []


@pytest.mark.parametrize("props", [BLANK, UNSET, PARTNER, OTHER])
def test_the_enrichment_date_is_left_alone_and_the_request_is_reset(run, props):
    run.props = dict(props)

    ep.run_enrichment("42")

    assert _updates(run) == [{"request_to_enrich": "false"}]


@pytest.mark.parametrize("props", [BLANK, UNSET])
def test_a_blank_type_is_asked_for_in_the_note(run, props):
    run.props = dict(props)

    ep.run_enrichment("42")

    note = _note(run)
    assert "no Company Type" in note
    assert "Facility" in note and "Corporation" in note
    assert "No research was done" in note


@pytest.mark.parametrize("props", [PARTNER, OTHER])
def test_another_type_is_named_as_not_one_the_service_enriches(run, props):
    run.props = dict(props)

    ep.run_enrichment("42")

    note = _note(run)
    assert "not Facility or Corporation" in note
    assert "no Company Type" not in note
    assert "No research was done" in note


def test_the_note_does_not_claim_a_research_process_or_counts_that_never_happened(run):
    ep.run_enrichment("42")

    note = _note(run)
    assert "Research Process" not in note
    assert "researched" not in note
    assert "Definitive" not in note and "Sonar" not in note


def test_the_note_says_the_date_was_left_so_the_request_can_be_made_again_at_once(run):
    ep.run_enrichment("42")

    assert "request enrichment again" in _note(run)
    assert "left alone" in _note(run)


def test_a_company_name_in_the_note_is_escaped(run):
    run.props = {"name": "<b>Elm</b> & Co", "hierarchy_type": ""}

    ep.run_enrichment("42")

    assert "&lt;b&gt;Elm&lt;/b&gt; &amp; Co" in _note(run)
    assert "<b>Elm</b>" not in _note(run)


def test_the_missing_type_is_asked_for_even_when_the_company_was_stamped_recently(run):
    # A company stamped by the old behaviour, within 30 days: the 30-day guard must not hide the real reason.
    run.props = {**BLANK, "revops_enrichment_date": str(int(time.time() * 1000))}

    ep.run_enrichment("42")

    assert "no Company Type" in _note(run)
    assert _updates(run) == [{"request_to_enrich": "false"}]


def test_a_note_that_cannot_be_written_still_resets_the_request(run):
    run.hs.create_note_on_company.side_effect = ep.HubSpotError("HubSpot is down")

    ep.run_enrichment("42")

    assert _updates(run) == [{"request_to_enrich": "false"}]


def test_a_reset_that_fails_is_tried_twice_and_never_raises(run):
    run.hs.update_company.side_effect = ep.HubSpotError("HubSpot is down")

    ep.run_enrichment("42")

    assert run.hs.update_company.call_count == 2


@pytest.mark.parametrize("hierarchy_type", [ep.CHILD_HIERARCHY_TYPE, ep.CORP_HIERARCHY_TYPE])
def test_a_facility_and_a_corporation_still_go_on_to_research_and_are_stamped(run, hierarchy_type):
    run.props = {"name": "Elm Court", "hierarchy_type": hierarchy_type}

    ep.run_enrichment("42")

    assert "_get_bigquery_client" in run.reached
    assert "revops_enrichment_date" in _updates(run)[-1]
    assert "no Company Type" not in _note(run)
