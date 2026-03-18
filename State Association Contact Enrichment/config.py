"""
Centralized configuration for title keywords and enrichment constants.

All title-matching lists are defined here to prevent drift between modules
that use them (enrichment_processor.py, state_association_matcher.py, etc.).

Rules:
- When adding a new corporate title, update BOTH W2_CORPORATE_TITLES
  AND DH_W2_TITLE_PATTERN so FullEnrich fallback and BigQuery queries
  stay in sync.
- DH_CSUITE_KEYWORDS must be a strict subset of the titles covered by
  DH_W2_TITLE_PATTERN.
- DH_CSUITE_PATTERN is the authoritative fast-track check.  DH_CSUITE_KEYWORDS
  is kept for reference but is no longer used for matching — the regex avoids
  the substring false-positive problems of the old frozenset approach.
"""

import re

# ── Workflow routing: corporate vs. facility titles ────────────────────────────

# Keywords that identify a corporate-level executive (any substring match,
# case-insensitive). Used by enrichment_processor._is_corporate_title() to
# route found contacts to the parent corporation rather than the child facility.
CORPORATE_TITLE_KEYWORDS: frozenset[str] = frozenset({
    "chief executive",
    "ceo",
    "chief operating",
    "coo",
    "chief financial",
    "cfo",
    "vp of op",
    "vice president",
    "regional director",
    "regional vp",
    "president",
})

# ── Facility-level leadership ──────────────────────────────────────────────────

# Titles that identify facility-level leadership used as the FullEnrich
# People Search query in Workflow 1 when Sonar Pro + Definitive Healthcare
# both find nothing.
FACILITY_LEADERSHIP_TITLES: list[str] = [
    "Administrator",
    "Executive Director",
    "Director of Nursing",
]

# Set of lowercase title substrings that identify facility-level leaders
# we are searching for in Workflow 1 contact-coverage checks.
TARGET_TITLES: frozenset[str] = frozenset({"executive director", "administrator"})

# ── Corporate-level executive titles ──────────────────────────────────────────

# Titles searched by FullEnrich People Search in Workflow 2 fallback.
# Must mirror the intent of DH_W2_TITLE_PATTERN so the fallback searches
# for the same cohort as the primary Definitive Healthcare query.
W2_CORPORATE_TITLES: list[str] = [
    "Chief Executive Officer",
    "Chief Operating Officer",
    "Chief Financial Officer",
    "Chief Nursing Officer",
    "Chief Clinical Officer",
    "Chief Administrative Officer",
    "President",
    "Vice President of Operations",
    "Senior Vice President of Operations",
    "Senior Vice President",
    "Vice President",
    "Regional Vice President of Operations",
    "Regional Vice President",
    "Regional Director of Operations",
    "Owner",
]

# ── BigQuery RE2 pattern for Definitive Healthcare title filtering ─────────────

# Matches the same corporate-executive cohort as W2_CORPORATE_TITLES above,
# written as a BigQuery RE2 regex for use in REGEXP_CONTAINS() predicates.
# Update this whenever W2_CORPORATE_TITLES is modified.
DH_W2_TITLE_PATTERN: str = (
    r"chief executive|chief operating|chief financial|chief nursing|chief clinical"
    r"|chief administrative"
    r"|\bceo\b|\bcoo\b|\bcfo\b|\bcno\b|\bcco\b|\bcao\b"
    r"|senior vice president"
    r"|vice president"
    r"|regional director"
    r"|\bpresident\b"
    r"|\bowner\b"
)

# ── DH fast-track: C-suite bypass ─────────────────────────────────────────────

# Compiled regex for C-suite fast-track detection.
#
# Key correctness decisions vs. the old frozenset substring approach:
#
#   "president" — must NOT match "vice president" / "senior vice president" /
#                 "executive vice president".  Negative lookbehinds enforce this.
#                 Old code: "president" in title → matched all VP titles.
#
#   "cco"       — must NOT match the substring "cco" inside "Accounts".
#                 \b word-boundary prevents this.
#
#   "owner"     — must NOT match "Owner Of <side-business>".
#                 Negative lookahead (?!\s+of\b) excludes "Owner Of …" patterns.
#
# Use DH_CSUITE_PATTERN.search(title) — returns a match object or None.
DH_CSUITE_PATTERN: re.Pattern = re.compile(
    r"""
    \b(?:
        chief\s+(?:executive|operating|financial|nursing|clinical|administrative)
        | \bceo\b | \bcoo\b | \bcfo\b | \bcno\b | \bcco\b | \bcao\b
    )\b
    # "president" only when NOT preceded by "vice", "senior", "executive", or "exec"
    | (?<!vice\s)(?<!senior\s)(?<!executive\s)(?<!exec\s)\bpresident\b
    # "owner" as a standalone word, but NOT "owner of <something>"
    | \bowner\b(?!\s+of\b)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Title keywords that identify true C-suite executives — kept for reference
# and use in DH BQ query patterns.  Do NOT use for substring matching;
# use DH_CSUITE_PATTERN.search() instead.
DH_CSUITE_KEYWORDS: frozenset[str] = frozenset({
    "chief executive", "ceo",
    "chief operating",  "coo",
    "chief financial",  "cfo",
    "chief nursing",    "cno",
    "chief clinical",   "cco",
    "chief administrative", "cao",
    "president",
    "owner",
})

# How many months a DH LAST_UPDATE date can be in the past before the record
# is considered stale and worth re-verifying via Sonar Pro.
DH_FRESHNESS_MONTHS: int = 12
