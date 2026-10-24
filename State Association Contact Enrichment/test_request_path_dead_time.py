"""
The research calls that serve a single record do not sleep after they answer.

W1_RATE_LIMIT_SECONDS (1.5 s) and W2_RATE_LIMIT_SECONDS (3 s) space the calls of the
batch loops, where many records go through one process and the pause is taken after the
semaphore is released. In the service each call below is one request's only call for
its record: the sleep spaced nothing and was pure waiting, 1.5 s of a 25 s run on
2026-09-24. The per-contact and per-facility batch workers keep their pauses.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_request_path_dead_time.py" -v
"""

import json
import os
import sys
import time
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep
import state_association_matcher as sam

_NO_CONTACTS = json.dumps({"contacts": []})
_NOT_FOUND = json.dumps({"found_name": "", "found_title": "", "confidence": "not_found"})


@pytest.fixture
def slept(monkeypatch):
    """Every time.sleep the code under test asks for, without waiting."""
    calls: list[float] = []
    monkeypatch.setattr(time, "sleep", calls.append)
    return calls


def test_the_corporate_website_sweep_does_not_sleep_after_its_one_call(monkeypatch, slept):
    monkeypatch.setattr(sam, "call_openrouter", lambda *a, **k: _NO_CONTACTS)

    found = sam.search_corporation_website_for_executives(
        corporation_name="Acme Care", corporate_domain="https://www.acme-care.com",
        facility_type="Skilled Nursing", known_names=set(),
    )

    assert found == []
    assert slept == []


def test_the_facility_website_sweep_does_not_sleep_after_its_one_call(monkeypatch, slept):
    monkeypatch.setattr(sam, "call_openrouter", lambda *a, **k: _NO_CONTACTS)

    found = sam.search_facility_website_for_leadership(
        facility_name="Valley View", domain="valleyview.example.com",
        city="Tarboro", state="NC", facility_type="Skilled Nursing",
    )

    assert found == []
    assert slept == []


def test_the_single_facility_research_does_not_sleep_after_its_one_call(monkeypatch, slept):
    monkeypatch.setattr(ep, "query_state_association_bq_table", lambda *a, **k: None)
    monkeypatch.setattr(ep, "call_openrouter", lambda *a, **k: _NOT_FOUND)
    monkeypatch.setattr(ep, "search_facility_contacts", lambda *a, **k: [])
    props = {"name": "Valley View", "city": "Tarboro", "state": "NC", "facility_type": "Skilled Nursing",
             "hierarchy_type": ep.CHILD_HIERARCHY_TYPE, "website": ""}

    results = ep._run_workflow1_single_facility(
        bq_client=MagicMock(), hs=MagicMock(), company_id="1", props=props, parent_corp_name="Acme Care",
        missing=pd.DataFrame([{"hubspot_id": "1"}]),
    )

    assert results and results[0]["research_confidence"] == "not_found"
    assert slept == []
