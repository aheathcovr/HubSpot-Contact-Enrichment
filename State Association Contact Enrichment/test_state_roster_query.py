"""
Looking a facility up in the state licensing roster.

query_state_association_bq_table is the cheapest source in a run and the first one asked. It never
returned anyone: it filtered on a `state` column that is empty for Texas (the state is in
`source_state`), looked for name columns that do not exist, took any copy of the facility out of
the several the table keeps, and passed the facility's own phone and email on as the person's.

The query itself cannot be run here, so these tests pin what it asks for (the state test, the
window, no filter that hides a vacancy) and what it does with the rows it gets back (roster.py
decides which to believe).

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_state_roster_query.py" -v
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import state_association_matcher as sam


class _Job:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _FakeBigQuery:
    """Answers each query with the next list of rows, and records what was asked."""

    def __init__(self, *answers, error=None):
        self.answers = list(answers)
        self.error = error
        self.calls: list[dict] = []

    def query(self, sql, job_config=None):
        self.calls.append({"sql": sql, "params": {p.name: p.value for p in (job_config.query_parameters or [])}})
        if self.error:
            raise self.error
        return _Job(self.answers.pop(0) if self.answers else [])


def _now_minus(days):
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)


def _row(**over):
    row = {
        "facility_id": "110349", "license_number": None, "facility_name": "THE MERIDIAN", "street_address": None,
        "city": None, "source_state": "TX", "administrator": "SEAN L BUELOW", "executive_director": None,
        "phone": "409-763-6437", "email": "info@meridiangalveston.com",
        "source_url": "https://apps.hhs.texas.gov/providers", "extracted_date": _now_minus(1),
    }
    row.update(over)
    return row


@pytest.fixture(autouse=True)
def _defaults(monkeypatch):
    monkeypatch.delenv("STATE_ROSTER_MAX_AGE_DAYS", raising=False)
    monkeypatch.delenv("STATE_ROSTER_STATES", raising=False)


def test_a_facility_in_the_roster_gives_its_licensed_administrator():
    client = _FakeBigQuery([_row()])

    contact = sam.query_state_association_bq_table(client, "The Meridian", "Galveston", "TX")

    assert contact["found_name"] == "Sean Buelow" and contact["found_title"] == "Administrator"
    assert contact["source_path"] == "state_association_bq_table"


def test_the_facilitys_phone_and_email_are_not_passed_on_as_the_persons():
    contact = sam.query_state_association_bq_table(_FakeBigQuery([_row()]), "The Meridian", "Galveston", "TX")

    assert contact["found_phone"] == "" and contact["found_email"] == ""


def test_the_state_is_tested_on_source_state_when_the_state_column_is_empty():
    client = _FakeBigQuery([_row()])

    sam.query_state_association_bq_table(client, "The Meridian", "Galveston", "TX")

    sql = client.calls[0]["sql"]
    assert "source_state" in sql and "COALESCE" in sql
    assert client.calls[0]["params"]["state"] == "TX"


def test_a_state_given_by_name_is_looked_up_by_its_abbreviation():
    client = _FakeBigQuery([_row()])

    sam.query_state_association_bq_table(client, "The Meridian", "Galveston", "Texas")

    assert client.calls[0]["params"]["state"] == "TX"


def test_the_facility_is_matched_on_its_name_without_case_or_punctuation():
    client = _FakeBigQuery([_row()])

    sam.query_state_association_bq_table(client, "The Meridian", "", "TX")

    assert client.calls[0]["params"]["name_key"] == "themeridian"
    assert "REGEXP_REPLACE" in client.calls[0]["sql"]


def test_the_query_does_not_hide_a_vacancy_by_asking_only_for_rows_that_name_someone():
    client = _FakeBigQuery([_row()])

    sam.query_state_association_bq_table(client, "The Meridian", "", "TX")

    assert "administrator IS NOT NULL" not in client.calls[0]["sql"]


def test_the_query_only_looks_at_loads_inside_the_age_window():
    client = _FakeBigQuery([_row()])

    sam.query_state_association_bq_table(client, "The Meridian", "", "TX")

    assert "extracted_date" in client.calls[0]["sql"] and client.calls[0]["params"]["window_days"] == 120


def test_the_age_window_can_be_changed_by_the_environment(monkeypatch):
    monkeypatch.setenv("STATE_ROSTER_MAX_AGE_DAYS", "45")
    client = _FakeBigQuery([_row()])

    sam.query_state_association_bq_table(client, "The Meridian", "", "TX")

    assert client.calls[0]["params"]["window_days"] == 45


def test_only_the_newest_copy_of_the_facility_is_believed():
    client = _FakeBigQuery([_row(administrator="OLD PERSON", extracted_date=_now_minus(60)),
                            _row(administrator="SEAN L BUELOW", extracted_date=_now_minus(1))])

    contact = sam.query_state_association_bq_table(client, "The Meridian", "", "TX")

    assert contact["found_name"] == "Sean Buelow"


def test_a_newest_copy_that_says_vacant_is_no_one_and_does_not_fall_back_to_an_older_name():
    client = _FakeBigQuery([_row(administrator="SEAN L BUELOW", extracted_date=_now_minus(60)),
                            _row(administrator="VACANT", extracted_date=_now_minus(1))])

    assert sam.query_state_association_bq_table(client, "The Meridian", "", "TX") is None
    assert len(client.calls) == 1                         # a facility that was found is not searched for more loosely


def test_a_facility_the_exact_name_misses_is_looked_for_by_a_name_that_contains_it():
    client = _FakeBigQuery([], [_row(facility_name="THE MERIDIAN AT GALVESTON")])

    contact = sam.query_state_association_bq_table(client, "The Meridian at Galveston Nursing", "", "TX")

    assert contact is not None and len(client.calls) == 2
    assert "STRPOS" in client.calls[1]["sql"]


def test_a_name_too_short_to_search_loosely_is_only_searched_exactly():
    client = _FakeBigQuery([])

    assert sam.query_state_association_bq_table(client, "Villa", "", "TX") is None
    assert len(client.calls) == 1


def test_a_loose_match_that_fits_two_facilities_is_not_guessed():
    client = _FakeBigQuery([], [_row(facility_id="1", facility_name="SUNRISE OF HOUSTON NORTH"),
                                _row(facility_id="2", facility_name="SUNRISE OF HOUSTON SOUTH")])

    assert sam.query_state_association_bq_table(client, "Sunrise of Houston", "", "TX") is None


@pytest.mark.parametrize("facility, state", [("", "TX"), ("The Meridian", ""), ("The Meridian", "Xyzland"), (None, "TX")])
def test_without_a_facility_and_a_state_nothing_is_asked(facility, state):
    client = _FakeBigQuery([_row()])

    assert sam.query_state_association_bq_table(client, facility, "Galveston", state) is None
    assert client.calls == []


def test_a_state_left_out_of_the_allowed_list_is_not_asked(monkeypatch):
    monkeypatch.setenv("STATE_ROSTER_STATES", "NC, pa")
    client = _FakeBigQuery([_row()])

    assert sam.query_state_association_bq_table(client, "The Meridian", "", "TX") is None
    assert client.calls == []
    assert sam.query_state_association_bq_table(_FakeBigQuery([_row(source_state="PA")]), "The Meridian", "", "PA")


def test_a_failed_query_is_no_answer_not_a_failed_run(caplog):
    client = _FakeBigQuery(error=RuntimeError("BigQuery is down"))

    with caplog.at_level(logging.WARNING):
        assert sam.query_state_association_bq_table(client, "The Meridian", "", "TX") is None

    assert "BigQuery is down" in caplog.text


def test_using_the_roster_is_logged_with_where_and_when_it_came_from(caplog):
    client = _FakeBigQuery([_row()])

    with caplog.at_level(logging.INFO):
        sam.query_state_association_bq_table(client, "The Meridian", "", "TX")

    assert any("roster" in m.lower() and "TX" in m and "Sean Buelow" in m and "The Meridian" in m for m in caplog.messages)


def test_the_kind_of_facility_separates_two_with_the_same_name():
    client = _FakeBigQuery([_row(administrator="SEAN L BUELOW", facility_id="4862", facility_type="SNF"),
                            _row(administrator="ARIS TAN", facility_id="107196", facility_type="ALF")])

    contact = sam.query_state_association_bq_table(client, "The Meridian", "", "TX", "SNF")

    assert contact["found_name"] == "Sean Buelow"
    assert "facility_type" in client.calls[0]["sql"]
