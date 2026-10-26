"""
Reading a state licensing roster row as a contact.

The roster (state_associations.state_association_snf_al_il_information) is loaded from state
health-department files. Its `administrator` column holds the licensed administrator of each
facility, and it is the one source we hold that names a person with legal standing. The service's
reader never returned anyone from it (it looked for columns that do not exist), so it settled
nothing. What a correct reader has to get right, from the real rows of 2026-10-25:

  - names arrive as "DANA R KESTREL", "TIEN, VINCE", " Megan  Loar", "Angela S. Compton";
  - some "names" are not one person: Alaska's are state staff email addresses, Hawaii's are two
    people with titles, and any state may say VACANT;
  - the table keeps a copy of every load, so only the NEWEST row of a facility counts, and a
    newest row that says vacant beats an older row that names someone;
  - an old row is not trusted, and a name that fits two facilities is not guessed at;
  - the roster's phone and email columns belong to the FACILITY, never to the administrator.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_roster.py" -v
"""

import os
import sys
from datetime import date, datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import roster

_TODAY = date(2026, 10, 25)


# ── Names ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("DANA R KESTREL", "Dana Kestrel"),                      # Texas: caps, middle initial
    ("TIEN, VINCE", "Vince Tien"),                         # California: LAST, FIRST
    (" Megan  Loar", "Megan Loar"),                        # Louisiana: stray and doubled spaces
    ("Zuccarelli, Patrick", "Patrick Zuccarelli"),         # Wisconsin
    ("Kuljanin-Thompson, Nikolina", "Nikolina Kuljanin-Thompson"),
    ("Angela S. Compton", "Angela Compton"),               # North Carolina: initial with a dot
    ("MARY SUE BREMERMAN", "Mary Sue Bremerman"),          # not an initial, kept
    ("O'BRIEN, PATRICK", "Patrick O'Brien"),
    ("O’BRIEN, PATRICK", "Patrick O'Brien"),               # curly apostrophe
    ("JOHN MCDONALD", "John McDonald"),
    ("DE LA CRUZ, MARIA", "Maria De La Cruz"),
    ("Alisa Racelo, Administrator", "Alisa Racelo"),       # Hawaii: one person with a role
    ("JANE DOE NHA", "Jane Doe"),
    ("Jane Doe, NHA", "Jane Doe"),
    ("Jane Doe R.N.", "Jane Doe"),
    ("Smith, John, NHA", "John Smith"),
    ("Jane Doe", "Jane Doe"),                              # already right: left as it is
    ("José Álvarez", "José Álvarez"),
    ("NANCY JONES", "Nancy Jones"),                         # starts like the placeholder NA
    ("NATALIE SMITH", "Natalie Smith"),
    ("OPENSHAW, JOHN", "John Openshaw"),                   # starts like the placeholder OPEN
    ("NOTTINGHAM, ALAN", "Alan Nottingham"),               # starts like NOT
    ("TBDELL, SUE", "Sue Tbdell"),
    ("VACANTI, ROSA", "Rosa Vacanti"),                     # starts like VACANT
])
def test_a_persons_name_is_read_as_first_and_last(raw, expected):
    assert roster.normalize_person_name(raw) == expected


@pytest.mark.parametrize("raw", [
    "Meagan.Adams@alaska.gov",                                                   # Alaska: a state employee's address
    "Keleka Ho’okano, Administrator; Bree Komagome, Chief Operating Officer",    # two people
    "Amy Lee, NHA and Chief Executive Officer",                                  # a person and a second role
    "John & Mary Smith",
    "Smith/Jones",
    "VACANT", "Vacant", "TBD", "None", "N/A", "UNKNOWN", "Position Vacant", "INTERIM", "Pending", "Not available",
    "ACME HEALTH CARE LLC", "Sunrise Senior Living", "Valley View Nursing Home", "Care Partners Management",
    "John", "J. Smith", "Joe Smith 2", "12345", "", "   ", None,
])
def test_something_that_is_not_one_persons_name_is_nothing(raw):
    assert roster.normalize_person_name(raw) == ""


# ── States ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("TX", "TX"), ("tx", "TX"), (" Texas ", "TX"), ("TEXAS", "TX"), ("Utah", "UT"),
    ("New Jersey", "NJ"), ("District of Columbia", "DC"), ("", ""), (None, ""), ("Xyzland", ""),
])
def test_a_state_name_or_abbreviation_becomes_the_abbreviation(raw, expected):
    assert roster.to_state_abbr(raw) == expected


# ── One row as a contact ──────────────────────────────────────────────────────

def _row(**over):
    row = {
        "facility_id": "110349", "facility_name": "THE MERIDIAN", "city": "GALVESTON", "state": None,
        "source_state": "TX", "administrator": "DANA R KESTREL", "executive_director": None,
        "phone": "409-763-6437", "email": "info@meridiangalveston.com",
        "source_url": "https://apps.hhs.texas.gov/providers", "extracted_date": datetime(2026, 10, 25, 9, 34, 56),
    }
    row.update(over)
    return row


def test_a_roster_row_becomes_the_licensed_administrator_with_where_it_came_from():
    contact = roster.row_to_contact(_row())

    assert contact["found_name"] == "Dana Kestrel"
    assert contact["found_title"] == "Administrator"
    assert contact["research_confidence"] == "high"
    assert contact["source_path"] == "state_association_bq_table"
    assert contact["research_findings"] == "https://apps.hhs.texas.gov/providers"
    assert contact["source_date"] == "2026-10-25" and contact["source_state"] == "TX"


def test_the_facilitys_own_phone_and_email_are_never_taken_as_the_administrators():
    contact = roster.row_to_contact(_row())

    assert contact["found_phone"] == "" and contact["found_email"] == ""


def test_an_executive_director_is_used_when_there_is_no_administrator():
    contact = roster.row_to_contact(_row(administrator="", executive_director="Pat Lee"))

    assert (contact["found_name"], contact["found_title"]) == ("Pat Lee", "Executive Director")


def test_the_administrator_wins_over_the_executive_director():
    contact = roster.row_to_contact(_row(executive_director="Pat Lee"))

    assert contact["found_name"] == "Dana Kestrel"


@pytest.mark.parametrize("administrator", [None, "", "VACANT", "Meagan.Adams@alaska.gov", "ACME LLC"])
def test_a_row_with_no_one_usable_is_no_contact(administrator):
    assert roster.row_to_contact(_row(administrator=administrator, executive_director=None)) is None


# ── Which row ─────────────────────────────────────────────────────────────────

def _snap(day, name, **over):
    return _row(administrator=name, extracted_date=datetime(2026, *day, 9, 0, 0), **over)


def test_only_the_newest_row_of_a_facility_counts():
    rows = [_snap((3, 21), "OLD PERSON"), _snap((10, 25), "DANA R KESTREL"), _snap((7, 1), "MIDDLE PERSON")]

    picked = roster.pick_roster_row(rows, city="Galveston", max_age_days=120, today=_TODAY)

    assert picked["administrator"] == "DANA R KESTREL"


def test_a_newest_row_that_says_vacant_beats_an_older_row_that_names_someone():
    rows = [_snap((7, 1), "DANA R KESTREL"), _snap((10, 25), "VACANT")]

    assert roster.pick_roster_row(rows, city="Galveston", max_age_days=120, today=_TODAY) is None


def test_a_row_older_than_the_limit_is_not_trusted():
    rows = [_snap((5, 1), "DANA R KESTREL")]                           # 177 days before 2026-10-25

    assert roster.pick_roster_row(rows, city="", max_age_days=120, today=_TODAY) is None
    assert roster.pick_roster_row(rows, city="", max_age_days=200, today=_TODAY) is not None


def test_two_facilities_with_one_name_are_not_guessed_between():
    rows = [_snap((10, 25), "ONE PERSON", facility_id="1", city="HOUSTON"),
            _snap((10, 25), "TWO PERSON", facility_id="2", city="DALLAS")]

    assert roster.pick_roster_row(rows, city="", max_age_days=120, today=_TODAY) is None


def test_the_city_picks_between_facilities_with_one_name():
    rows = [_snap((10, 25), "ONE PERSON", facility_id="1", city="HOUSTON"),
            _snap((10, 25), "TWO PERSON", facility_id="2", city="DALLAS")]

    picked = roster.pick_roster_row(rows, city="dallas", max_age_days=120, today=_TODAY)

    assert picked["administrator"] == "TWO PERSON"


def test_two_facilities_in_the_same_city_with_one_name_are_still_not_guessed_between():
    rows = [_snap((10, 25), "ONE PERSON", facility_id="1", city="DALLAS"),
            _snap((10, 25), "TWO PERSON", facility_id="2", city="DALLAS")]

    assert roster.pick_roster_row(rows, city="Dallas", max_age_days=120, today=_TODAY) is None


def test_a_city_the_roster_does_not_have_is_not_held_against_the_facility():
    rows = [_snap((10, 25), "DANA R KESTREL", city=None)]              # Texas rows have no city

    assert roster.pick_roster_row(rows, city="Galveston", max_age_days=120, today=_TODAY) is not None


def test_a_row_with_no_facility_id_is_still_told_apart_from_other_facilities():
    rows = [_snap((10, 25), "ONE PERSON", facility_id=None, license_number="L1"),
            _snap((10, 25), "TWO PERSON", facility_id=None, license_number="L2")]

    assert roster.pick_roster_row(rows, city="", max_age_days=120, today=_TODAY) is None


@pytest.mark.parametrize("stamp", ["2026-10-25 09:34:56", "2026-10-25", date(2026, 10, 25), datetime(2026, 10, 25, 9, 34)])
def test_the_extraction_date_may_be_text_a_date_or_a_timestamp(stamp):
    rows = [_row(extracted_date=stamp)]

    assert roster.pick_roster_row(rows, city="", max_age_days=5, today=_TODAY) is not None


def test_a_row_with_no_date_is_not_trusted():
    assert roster.pick_roster_row([_row(extracted_date=None)], city="", max_age_days=120, today=_TODAY) is None


def test_no_rows_is_no_row():
    assert roster.pick_roster_row([], city="", max_age_days=120, today=_TODAY) is None


# ── The kind of facility tells two with one name apart ────────────────────────
# Texas has two facilities called THE MERIDIAN: a nursing facility and an unrelated assisted
# living one, with no city on either row. HubSpot knows which one it is looking for.

def _two_meridians():
    return [_snap((10, 25), "DANA R KESTREL", facility_id="4862", facility_type="SNF"),
            _snap((10, 25), "ARIS TAN", facility_id="107196", facility_type="ALF")]


@pytest.mark.parametrize("hubspot_type, expected", [
    ("SNF", "DANA R KESTREL"), ("AL/IL/MC", "ARIS TAN"), ("ALF", "ARIS TAN"), ("assisted living", "ARIS TAN"),
])
def test_the_kind_of_facility_picks_between_facilities_with_one_name(hubspot_type, expected):
    picked = roster.pick_roster_row(_two_meridians(), city="", max_age_days=120, today=_TODAY, facility_type=hubspot_type)

    assert picked["administrator"] == expected


def test_with_no_kind_two_facilities_with_one_name_are_still_not_guessed_between():
    assert roster.pick_roster_row(_two_meridians(), city="", max_age_days=120, today=_TODAY) is None


def test_a_kind_nobody_recognises_does_not_pick_either():
    assert roster.pick_roster_row(_two_meridians(), city="", max_age_days=120, today=_TODAY, facility_type="Hospital") is None


def test_a_roster_row_with_no_kind_is_not_ruled_out_by_one():
    rows = [_snap((10, 25), "DANA R KESTREL", facility_id="4862", facility_type=None)]

    assert roster.pick_roster_row(rows, city="", max_age_days=120, today=_TODAY, facility_type="SNF") is not None


def test_a_facility_of_the_wrong_kind_is_not_used_even_when_it_is_the_only_one():
    rows = [_snap((10, 25), "ARIS TAN", facility_id="107196", facility_type="ALF")]

    assert roster.pick_roster_row(rows, city="", max_age_days=120, today=_TODAY, facility_type="SNF") is None
