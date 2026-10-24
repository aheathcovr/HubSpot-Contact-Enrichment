"""
The HubSpot BigQuery mirror the service matches contacts against.

Contact matching, the "which facilities lack leadership" check and company lookups
all read a copy of HubSpot that an Airbyte sync keeps in BigQuery. That sync
froze: `HubSpot_Airbyte` last synced 2026-09-29 04:01 UTC and was 25 days stale on
2026-10-24, so 2,425 contacts that exist in HubSpot were invisible to the service
(it could create a duplicate of any of them). `HubSpot_Airbyte_Self_Hosted` was
current, with every column the service reads. Mirrors have frozen and thawed
before, so the fix is two parts: read the current one, and say so loudly when
whichever one is configured goes stale.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_hubspot_mirror.py" -v
"""

import logging
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep
import state_association_matcher as m


class _FakeClient:
    """Answers the freshness query with a canned age, or raises."""

    def __init__(self, minutes_old=None, error=None):
        self.minutes_old = minutes_old
        self.error = error
        self.queries: list[str] = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        if self.error:
            raise self.error
        rows = [SimpleNamespace(minutes_old=self.minutes_old)]
        return SimpleNamespace(result=lambda: rows)


@pytest.fixture(autouse=True)
def _fresh_cache():
    m._mirror_age_cache.clear()
    yield
    m._mirror_age_cache.clear()


# ── Which mirror is read ──────────────────────────────────────────────────────

def test_the_service_reads_the_current_mirror_by_default():
    assert m.HUBSPOT_CONTACTS_TABLE.endswith(".HubSpot_Airbyte_Self_Hosted.contacts")
    assert m.HUBSPOT_COMPANIES_TABLE.endswith(".HubSpot_Airbyte_Self_Hosted.companies")


# ── The staleness check ───────────────────────────────────────────────────────

def test_a_fresh_mirror_reports_its_age_and_logs_no_error(caplog):
    client = _FakeClient(minutes_old=90)

    with caplog.at_level(logging.INFO):
        hours = m.check_hubspot_mirror_freshness(client)

    assert hours == pytest.approx(1.5)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_a_stale_mirror_logs_an_error_tagged_for_alerting(caplog):
    client = _FakeClient(minutes_old=596 * 60)

    with caplog.at_level(logging.INFO):
        hours = m.check_hubspot_mirror_freshness(client)

    assert hours == pytest.approx(596.0)
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "596" in errors[0].getMessage()
    assert errors[0].json_fields["alert"] == "hubspot_mirror_stale"


def test_the_limit_is_36_hours():
    assert m.check_hubspot_mirror_freshness(_FakeClient(minutes_old=35 * 60)) == pytest.approx(35.0)
    m._mirror_age_cache.clear()
    with patch.object(m.logger, "error") as error:
        m.check_hubspot_mirror_freshness(_FakeClient(minutes_old=37 * 60))
    error.assert_called_once()


def test_a_mirror_with_no_sync_timestamp_is_stale_not_fresh(caplog):
    with caplog.at_level(logging.INFO):
        hours = m.check_hubspot_mirror_freshness(_FakeClient(minutes_old=None))

    assert hours is None
    assert [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_a_failing_check_never_raises_and_does_not_claim_the_mirror_is_stale(caplog):
    client = _FakeClient(error=RuntimeError("403 Access Denied"))

    with caplog.at_level(logging.INFO):
        hours = m.check_hubspot_mirror_freshness(client)

    assert hours is None
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [r for r in caplog.records if r.levelno == logging.WARNING]


def test_the_check_is_cached_so_it_runs_once_per_window_not_once_per_enrichment():
    client = _FakeClient(minutes_old=60)

    m.check_hubspot_mirror_freshness(client)
    m.check_hubspot_mirror_freshness(client)
    m.check_hubspot_mirror_freshness(client)

    assert len(client.queries) == 1


# ── The company note ──────────────────────────────────────────────────────────

def test_a_stale_mirror_is_reported_in_the_company_note():
    hs = MagicMock()
    hs.get_company_properties.return_value = {"name": "Valley View", "hierarchy_type": "Something Else"}
    with patch.object(ep, "_get_hubspot_client", return_value=hs), \
         patch.object(ep, "_get_bigquery_client", return_value=MagicMock()), \
         patch.object(ep, "check_hubspot_mirror_freshness", return_value=596.0):
        ep.run_enrichment("12345")

    note = hs.create_note_on_company.call_args.args[1]
    assert "mirror" in note.lower()
    assert "596" in note
