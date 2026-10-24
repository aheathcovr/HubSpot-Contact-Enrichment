"""
The Definitive Healthcare tables the service reads.

`definitive_healthcare.December SNF AND ALF Contact Full Feed` no longer exists
in BigQuery (checked 2026-10-24: the other tables in the dataset were refreshed
2026-10-14, this one is gone). Three functions still queried it:

  * verify_contact_in_definitive  checks Executives, then falls through to Full
    Feed ONLY when the person is not found, so it hit the missing table exactly
    when the honest answer was "not found" and reported "lookup unavailable
    (query error)" instead. The service lost its negative signal.
  * query_dh_facility_contacts and load_corporate_level_contacts each ran a
    query that failed and was swallowed as a WARNING, on every run.

These tests use a stand-in BigQuery client that behaves like production: it
raises "Not found" for the dead table and answers for the live ones.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_definitive_sources.py" -v
"""

import logging
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import state_association_matcher as m

_DEAD_TABLE = "December SNF AND ALF Contact Full Feed"


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return list(self._rows)


class FakeBigQuery:
    """Answers per table; raises for the table that no longer exists."""

    def __init__(self, rows_by_table: dict[str, list[dict]]):
        self.rows_by_table = rows_by_table
        self.tables_queried: list[str] = []

    def query(self, sql, job_config=None):
        table = re.search(r"definitive_healthcare\.([^`]+)`", sql).group(1)
        self.tables_queried.append(table)
        if table == _DEAD_TABLE:
            raise RuntimeError(f"404 Not found: Table definitive_healthcare.{table} was not found in location US")
        return _Result(self.rows_by_table.get(table, []))


def _facility_row(first="Jane", last="Doe", title="administrator"):
    return {
        "executive_id": "1", "first_name": first, "last_name": last, "full_name": f"{first} {last}",
        "title": title, "email": "jane@facility.com", "direct_phone": "5551234567",
        "location_phone": "", "linkedin_url": "", "last_update": "2026-09-01", "source_table": "executives",
    }


# ── verify_contact_in_definitive ──────────────────────────────────────────────

def test_a_contact_found_in_executives_is_confirmed():
    bq = FakeBigQuery({"Executives": [{
        "dh_name": "Jane Doe", "dh_title": "administrator", "dh_email": "jane@facility.com",
        "dh_last_update": "2026-09-01",
    }]})

    result = m.verify_contact_in_definitive(bq, "Jane", "Doe", "561557")

    assert result["confirmed"] is True
    assert result["source_table"] == "executives"


def test_a_contact_not_in_executives_is_reported_not_found_rather_than_a_query_error():
    bq = FakeBigQuery({"Executives": []})

    result = m.verify_contact_in_definitive(bq, "Jane", "Doe", "561557")

    assert result["confirmed"] is False
    assert result["error"] == ""           # a clean "queried, not found", not "query_failed"
    assert _DEAD_TABLE not in bq.tables_queried


# ── query_dh_facility_contacts ────────────────────────────────────────────────

def test_facility_contacts_come_from_executives_without_touching_the_dead_table(caplog):
    bq = FakeBigQuery({"Executives": [_facility_row()]})

    with caplog.at_level(logging.WARNING):
        contacts = m.query_dh_facility_contacts(bq, "561557")

    assert [c["full_name"] for c in contacts] == ["Jane Doe"]
    assert contacts[0]["direct_phone"] == "5551234567"
    assert _DEAD_TABLE not in bq.tables_queried
    assert not [r for r in caplog.records if "query failed" in r.getMessage()]


# ── load_corporate_level_contacts (workflow 2) ────────────────────────────────

def _corporate_row(source, first, last, executive_id):
    return {
        "EXECUTIVE_ID": executive_id, "hospital_id": 9, "EXECUTIVE_NAME": f"{first} {last}",
        "FIRST_NAME": first, "LAST_NAME": last, "title": "chief executive officer",
        "EMAIL": f"{first.lower()}@operator.com", "DIRECT_PHONE": None, "LOCATION_PHONE": None,
        "LINKEDIN_PROFILE": "", "HOSPITAL_NAME": "Operator", "LAST_UPDATE": "2026-09-01",
        "source_table": source,
    }


def test_corporate_contacts_come_from_the_live_tables_without_touching_the_dead_one(caplog):
    bq = FakeBigQuery({
        "Executives": [_corporate_row("executives", "Ann", "Lee", "10")],
        "Corporate Executives": [_corporate_row("corporate_executives", "Bo", "Kim", "11")],
    })

    with caplog.at_level(logging.WARNING):
        df = m.load_corporate_level_contacts(bq, 12345)

    assert sorted(df["LAST_NAME"]) == ["Kim", "Lee"]
    assert _DEAD_TABLE not in bq.tables_queried
    assert not [r for r in caplog.records if "query failed" in r.getMessage()]


def test_the_dead_table_is_no_longer_configured():
    assert "full_feed" not in m.DH_TABLES
    assert all("Full Feed" not in table for table in m.DH_TABLES.values())
