"""
A corporation run asks FullEnrich for its executives while Definitive and Sonar work,
instead of after them.

The FullEnrich people search does not depend on what the other two found: it ran last
only because it was written last. What it returns is still filtered against the names
Definitive and Sonar produced, and it is still asked exactly once per run, so credits
and results are unchanged. Only the waiting is.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_corporation_run_overlap.py" -v
"""

import os
import sys
import threading
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep


def _dh(name: str) -> dict:
    return {"workflow": "2 - Unassociated Contact", "contact_name": name, "found_name": name,
            "title": "CFO", "found_title": "CFO", "research_confidence": "high", "source_path": "definitive"}


def _fe(name: str) -> dict:
    return {"full_name": name, "title": "Chief Operating Officer", "linkedin_url": "", "updated_at": ""}


@pytest.fixture
def corporation_run(monkeypatch):
    """run_enrichment on a corporation, with every collaborator replaced and recorded."""
    hs = MagicMock()
    hs.get_company_properties.return_value = {
        "name": "Acme Care", "hierarchy_type": ep.CORP_HIERARCHY_TYPE, "definitive_healthcare_id": "123",
        "website": "acme-care.com", "state": "NC", "facility_type": "Skilled Nursing",
    }
    state = type("S", (), {})()
    state.fe_started = threading.Event()
    state.fe_calls = []
    state.dh_overlapped = None
    state.dh_results = [_dh("Jane Doe")]
    state.sonar_names = []
    state.fe_results = [_fe("Jane Doe"), _fe("John Roe")]
    state.notified = {}

    def workflow_2(**kwargs):
        # Definitive's step: is the FullEnrich search already underway while this one runs?
        state.dh_overlapped = state.fe_started.wait(3)
        return list(state.dh_results)

    def sonar_discovery(corporation_name, corporate_domain, facility_type, known_names):
        known_names.update(n.lower() for n in state.sonar_names)   # the real one adds the names it accepts, lowercased
        return [_dh(n) for n in state.sonar_names]

    def fullenrich(**kwargs):
        state.fe_calls.append(kwargs)
        state.fe_started.set()
        return list(state.fe_results)

    monkeypatch.setattr(ep, "_get_hubspot_client", lambda: hs)
    monkeypatch.setattr(ep, "_get_bigquery_client", lambda: MagicMock())
    monkeypatch.setattr(ep, "workflow_2_research_contacts", workflow_2)
    monkeypatch.setattr(ep, "search_corporation_website_for_executives", sonar_discovery)
    monkeypatch.setattr(ep, "search_facility_contacts", fullenrich)
    monkeypatch.setattr(ep, "_process_found_contact", lambda **kw: MagicMock(action="created_new", contact_name=kw["found_name"]))
    monkeypatch.setattr(ep, "_build_note", lambda **kw: state.notified.update(kw) or "note")
    state.hs = hs
    return state


def test_the_fullenrich_search_is_underway_while_definitive_works(corporation_run):
    ep.run_enrichment("42")

    assert corporation_run.dh_overlapped is True, "FullEnrich was only asked after Definitive had finished"


def test_the_fullenrich_search_is_asked_exactly_once_with_the_same_arguments(corporation_run):
    ep.run_enrichment("42")

    assert len(corporation_run.fe_calls) == 1
    call = corporation_run.fe_calls[0]
    assert call["company_name"] == "Acme Care"
    assert call["domain"] == "acme-care.com"
    assert call["state"] == "NC"
    assert call["corporate_mode"] is True
    assert call["titles"] == ep.W2_CORPORATE_TITLES


def test_what_fullenrich_finds_is_still_filtered_against_definitive_and_sonar(corporation_run):
    corporation_run.sonar_names = ["Sam Poe"]
    corporation_run.fe_results = [_fe("Jane Doe"), _fe("Sam Poe"), _fe("John Roe")]   # Definitive's, Sonar's, new

    ep.run_enrichment("42")

    added = [r["contact_name"] for r in corporation_run.notified["results"]
             if r.get("source_path") == "fullenrich_people_search"]
    assert added == ["John Roe"]


def test_the_run_still_finishes_and_stamps_the_company(corporation_run):
    ep.run_enrichment("42")

    corporation_run.hs.update_company.assert_called()
    stamped = corporation_run.hs.update_company.call_args.args[1]
    assert stamped["request_to_enrich"] == "false" and "revops_enrichment_date" in stamped
