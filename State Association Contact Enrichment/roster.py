"""
Reading a state licensing roster row as a contact.

The roster (state_associations.state_association_snf_al_il_information) is loaded from state
health-department files; its `administrator` column holds the licensed administrator of each
facility. This module turns its rows into the contact dict the pipeline uses, and decides which
row of a facility to believe. It has no BigQuery in it, so every rule can be tested on its own.

The rules come from the real rows of 2026-10-25 (see test_roster.py): names in four formats,
"names" that are state staff emails or two people with titles or the word VACANT, one copy of the
table per load, and phone and email columns that belong to the facility, not the person.
"""

import re
from datetime import date, datetime
from typing import Optional

SOURCE_PATH = "state_association_bq_table"

# A roster says VACANT or TBD where it has no one. Whole words only: NANCY starts like NA.
_PLACEHOLDER = re.compile(r"^(VACANT|NONE|TBD|NA|UNKNOWN|OPEN|PENDING|INTERIM|NOT|POSITION)\b")

# Whole words that mark a company or a facility, never a person.
_ORG_WORDS = frozenset({
    "LLC", "INC", "CORP", "CORPORATION", "LTD", "LP", "LLP", "HOLDINGS", "PARTNERS", "MANAGEMENT", "HEALTHCARE",
    "CARE", "CENTER", "CENTRE", "HOME", "HOMES", "NURSING", "REHAB", "REHABILITATION", "SENIOR", "LIVING",
    "VILLAGE", "MANOR", "GROUP", "SERVICES", "ASSOCIATES", "COMPANY", "FACILITY", "HEALTH",
})

# Trailing credentials and suffixes that are not part of the name.
_CREDENTIALS = frozenset({
    "NHA", "LNHA", "RN", "LPN", "MHA", "MBA", "MSN", "BSN", "PHD", "MD", "DON", "JR", "SR", "II", "III", "IV",
})

# What may follow a comma when the part before it is the whole name ("Alisa Racelo, Administrator").
_ROLES = frozenset({"ADMINISTRATOR", "ADMIN", "ED", "EXECUTIVE DIRECTOR", "DIRECTOR", "NURSING HOME ADMINISTRATOR"})

_STATES = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA", "COLORADO": "CO",
    "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT",
    "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM",
    "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
    "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
}
_ABBREVIATIONS = frozenset(_STATES.values())


def to_state_abbr(value) -> str:
    """'Texas', 'texas ' and 'TX' are all 'TX'; anything else is ''."""
    text = re.sub(r"\s+", " ", str(value or "")).strip().upper()
    if text in _ABBREVIATIONS:
        return text
    return _STATES.get(text, "")


# ── Names ─────────────────────────────────────────────────────────────────────

def _smart_title(token: str) -> str:
    """Title-case an all-caps or all-lowercase word; leave a word that already has mixed case alone."""
    if token != token.upper() and token != token.lower():
        return token

    def part(p: str) -> str:
        lower = p.lower()
        if lower.startswith("mc") and len(lower) > 2:
            return "Mc" + lower[2:].capitalize()
        if lower.startswith("o'") and len(lower) > 2:
            return "O'" + lower[2:].capitalize()
        return lower.capitalize()

    return "-".join(part(p) for p in token.split("-"))


def _is_word(token: str) -> bool:
    return any(ch.isalpha() for ch in token) and all(ch.isalpha() or ch in "'.-" for ch in token)


def _bare(token: str) -> str:
    return token.replace(".", "").upper()


def normalize_person_name(raw) -> str:
    """
    One person's name as "First Last", or "" when the text is not one person's name.

    Handles "SEAN L BUELOW", "TIEN, VINCE", " Megan  Loar", "Angela S. Compton" and "Jane Doe, NHA".
    Rejects email addresses, two people, a person plus a second role, placeholders such as VACANT,
    company and facility names, and anything with digits.
    """
    text = re.sub(r"\s+", " ", str(raw or "").replace("’", "'")).strip()
    if not text:
        return ""
    upper = text.upper()
    if any(ch in text for ch in "@;/()&+") or any(ch.isdigit() for ch in text):
        return ""
    if re.search(r"\bAND\b", upper) or _PLACEHOLDER.match(upper):
        return ""

    parts = [p.strip() for p in text.split(",")]
    if len(parts) == 1:
        name = parts[0]
    elif len(parts) == 2:
        second = parts[1]
        if second.upper() in _ROLES or _bare(second) in _CREDENTIALS:
            name = parts[0]
        else:
            name = f"{second} {parts[0]}"                       # "TIEN, VINCE"
    elif all(_bare(p) in _CREDENTIALS or p.upper() in _ROLES for p in parts[2:]):
        name = f"{parts[1]} {parts[0]}"                          # "Smith, John, NHA"
    else:
        return ""

    tokens = name.split()
    while tokens and _bare(tokens[-1]) in _CREDENTIALS:
        tokens.pop()
    tokens = [t for t in tokens if len(t.replace(".", "")) > 1]  # middle initials
    if len(tokens) < 2 or not all(_is_word(t) for t in tokens):
        return ""
    if any(_bare(t) in _ORG_WORDS for t in tokens):
        return ""
    return " ".join(_smart_title(t.strip(".")) if t.endswith(".") else _smart_title(t) for t in tokens)


# ── One row, and which row ────────────────────────────────────────────────────

def _as_date(value) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def row_to_contact(row: dict) -> Optional[dict]:
    """
    The contact a roster row names, or None.

    The licensed administrator, else the executive director. The row's `phone` and `email` are the
    FACILITY's, so they are never copied onto the person: a facility main line written as an
    administrator's phone would also make every contact at that number look like a match.
    """
    for column, title in (("administrator", "Administrator"), ("executive_director", "Executive Director")):
        name = normalize_person_name(row.get(column))
        if name:
            when = _as_date(row.get("extracted_date"))
            return {
                "found_name": name,
                "found_title": title,
                "found_phone": "",
                "found_email": "",
                "research_findings": str(row.get("source_url") or ""),
                "research_confidence": "high",
                "source_tier": "1",
                "source_path": SOURCE_PATH,
                "source_date": when.isoformat() if when else "",
                "source_state": str(row.get("source_state") or ""),
            }
    return None


def facility_group(value) -> str:
    """'SNF' for a skilled nursing facility, 'AL' for assisted, independent or memory care, '' if unknown."""
    tokens = set(re.sub(r"[^a-z]+", " ", str(value or "").lower()).split())
    if tokens & {"snf", "nf", "skilled", "nursing"}:
        return "SNF"
    if tokens & {"alf", "al", "il", "mc", "assisted", "independent", "memory"}:
        return "AL"
    return ""


def _facility_key(row: dict) -> tuple:
    for field in ("facility_id", "license_number"):
        value = str(row.get(field) or "").strip()
        if value:
            return (field, value)
    return ("name", str(row.get("facility_name") or "").strip().lower(), str(row.get("street_address") or "").strip().lower())


def pick_roster_row(
    rows: list, *, city: str, max_age_days: int, today: Optional[date] = None, facility_type: str = "",
) -> Optional[dict]:
    """
    The one row to believe for a facility, or None.

    The table keeps a copy of every load, so each facility is reduced to its NEWEST row; if that row
    names no one (vacant) an older row naming someone does not count. A row older than the limit, or
    with no date, is not trusted. If more than one facility fits, nothing is guessed: the kind of
    facility (a nursing facility and an assisted living one can share a name) and the city, where the
    roster has one, are what tell them apart.
    """
    today = today or date.today()
    newest: dict = {}
    for row in rows:
        when = _as_date(row.get("extracted_date"))
        if when is None:
            continue
        key = _facility_key(row)
        if key not in newest or when > newest[key][0]:
            newest[key] = (when, row)
    fresh = [row for when, row in newest.values() if (today - when).days <= max_age_days]

    kind = facility_group(facility_type)
    if kind:
        fresh = [r for r in fresh if facility_group(r.get("facility_type")) in ("", kind)]
    wanted = str(city or "").strip().lower()
    if wanted:
        fresh = [r for r in fresh if not str(r.get("city") or "").strip() or str(r.get("city")).strip().lower() == wanted]
    if len(fresh) != 1:
        return None
    return fresh[0] if row_to_contact(fresh[0]) else None
