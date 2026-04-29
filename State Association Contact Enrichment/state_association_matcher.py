#!/usr/bin/env python3
"""
State Association HubSpot Definitive Matching
===============================================

Two-workflow research tool for finding executive contacts at healthcare facilities
by searching state nursing / assisted-living / independent-living association websites
via OpenRouter (Perplexity Sonar).

Workflow 1 — HubSpot Facilities Missing Leadership Contacts
------------------------------------------------------------
  1. Find all HubSpot child companies whose hs_parent_company_id matches the input
     HubSpot corporation ID.
  2. For each child facility, check whether any HubSpot contact with title
     "Administrator" or "Executive Director" is associated with it.
  3. For each facility missing that leadership contact, call OpenRouter and ask it
     to search state association sites for an administrator or executive director.
  4. If a name is found, search HubSpot contacts by name and return the matching
     contact record ID (if that person already exists in HubSpot).

Workflow 2 — Unassociated Definitive Contacts
----------------------------------------------
  1. Query Definitive Healthcare tables for all HOSPITAL_IDs under that NETWORK_ID.
  2. Find contacts with title "Executive Director" or "Administrator" that are
     listed at the *corporation* (network) level only — i.e., their EXECUTIVE_ID
     does NOT also appear at a specific child facility.
  3. For each such contact, call OpenRouter and ask it to search the web to find
     any facility assignment for that person.

Output
------
  Two timestamped CSV files:
    state_assoc_facilities_<TIMESTAMP>.csv — Workflow 1 results (facilities)
    state_assoc_contacts_<TIMESTAMP>.csv   — Workflow 2 results (contacts)

Environment Variables
---------------------
  BQ_PROJECT_ID                BigQuery project (default: gen-lang-client-0844868008)
  BQ_LOCATION                  BigQuery location  (default: US)
  OPENROUTER_API_KEY           Required for OpenRouter / Perplexity calls
  OPENROUTER_MODEL             Model slug (default: perplexity/sonar-pro)
  GOOGLE_APPLICATION_CREDENTIALS  Path to GCP service account key file
"""

import csv
import json
import logging
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from config import (
    DH_W2_TITLE_PATTERN,
    TARGET_TITLES,
    DH_CSUITE_KEYWORDS as _DH_CSUITE_KEYWORDS,
    DH_CSUITE_PATTERN as _DH_CSUITE_PATTERN,
    DH_FRESHNESS_MONTHS as _DH_FRESHNESS_MONTHS,
)
from state_association.llm_client import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    OPENROUTER_MODEL,
    OPENROUTER_MODEL_TIER23,
    RATE_LIMIT_SECONDS,
    W1_RATE_LIMIT_SECONDS,
    W2_RATE_LIMIT_SECONDS,
    W2_TIMEOUT,
    RESEARCH_SYSTEM_PROMPT,
    W1_DISCOVERY_SYSTEM_PROMPT,
    W2_DISCOVERY_SYSTEM_PROMPT,
    W2_VERIFY_SYSTEM_PROMPT,
    _CONTACT_JSON_SCHEMA,
    _CONTACT_LIST_JSON_SCHEMA,
    _W1_MAX_WORKERS,
    _W1_SEMAPHORE,
    _OPENROUTER_SEMAPHORE,
    _W2_MAX_WORKERS,
    _W2_SEMAPHORE,
    call_openrouter,
    normalize_phone,
    parse_found_name,
)

# ─── PyYAML (for state association guide parsing) ─────────────────────────────
try:
    import yaml
except ImportError:
    raise ImportError(
        "PyYAML is required. Install with: pip install pyyaml"
    )

# ─── Optional .env loading ────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── BigQuery ─────────────────────────────────────────────────────────────────
try:
    from google.cloud import bigquery
    from utils.bigquery_client import init_bigquery_client
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    raise ImportError(
        "google-cloud-bigquery is required. Install with: pip install google-cloud-bigquery"
    )

logger = logging.getLogger(__name__)

# ─── URL helpers ──────────────────────────────────────────────────────────────

def _extract_bare_domain(url: str) -> str:
    """Return hostname from a URL, stripping scheme, www., port, and path.

    Examples:
        "https://www.example.com:8080/path" → "example.com"
        "example.com"                       → "example.com"
        ""                                  → ""
    """
    if not url:
        return ""
    from urllib.parse import urlparse
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    host = urlparse(url).hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host.lower()


# ─── Configuration ────────────────────────────────────────────────────────────
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "gen-lang-client-0844868008")
BQ_LOCATION   = os.getenv("BQ_LOCATION",   "US")

HUBSPOT_COMPANIES_TABLE = f"{BQ_PROJECT_ID}.HubSpot_Airbyte.companies"
HUBSPOT_CONTACTS_TABLE  = f"{BQ_PROJECT_ID}.HubSpot_Airbyte.contacts"

DH_TABLES = {
    "snf_overview": f"{BQ_PROJECT_ID}.definitive_healthcare.SNF Overview",
    "alf_overview": f"{BQ_PROJECT_ID}.definitive_healthcare.ALF Overview",
    "executives":   f"{BQ_PROJECT_ID}.definitive_healthcare.Executives",
    "corporation":  f"{BQ_PROJECT_ID}.definitive_healthcare.Corporate Executives",
    "full_feed":    f"{BQ_PROJECT_ID}.definitive_healthcare.December SNF AND ALF Contact Full Feed",
}

STATE_ASSOC_TABLE = f"{BQ_PROJECT_ID}.state_associations.state_association_snf_al_il_information"

# DH_W2_TITLE_PATTERN, TARGET_TITLES, _DH_CSUITE_KEYWORDS, _DH_FRESHNESS_MONTHS
# imported from config.py

# OPENROUTER_API_KEY, OPENROUTER_BASE_URL, OPENROUTER_MODEL, OPENROUTER_MODEL_TIER23,
# W1_RATE_LIMIT_SECONDS, W2_RATE_LIMIT_SECONDS, W2_TIMEOUT,
# RESEARCH_SYSTEM_PROMPT, W2_DISCOVERY_SYSTEM_PROMPT, W2_VERIFY_SYSTEM_PROMPT,
# _CONTACT_JSON_SCHEMA, _CONTACT_LIST_JSON_SCHEMA,
# _W1_SEMAPHORE, _W2_SEMAPHORE, call_openrouter, parse_found_name, normalize_phone
# imported from state_association.llm_client


def _is_csuite_title(title: str) -> bool:
    """
    Return True if *title* identifies a true C-suite executive that should bypass
    Sonar Pro (DH is authoritative; state association directories never list them).

    Uses DH_CSUITE_PATTERN (word-boundary regex) rather than the old frozenset
    substring approach, which produced false positives:
      - "president" matched all "vice president" / "senior vice president" titles
      - "cco" matched the substring in "Accounts" (Payable Manager)
      - "owner" matched "Owner Of <side-business>" side-business descriptions
    """
    if not title:
        return False
    return bool(_DH_CSUITE_PATTERN.search(title))


def _dh_record_is_fresh(last_update: str) -> bool:
    """
    Return True if the DH LAST_UPDATE value indicates the record is within
    _DH_FRESHNESS_MONTHS months of today.

    Handles two formats that DH uses:
      - ISO date strings: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
      - Human-readable relative strings: "15 days ago", "3 months ago",
        "Within the last year", "Within the last 6 months", etc.

    Returns False (treat as stale) if the value cannot be interpreted.
    """
    if not last_update:
        return False
    raw = str(last_update).strip().lower()

    # ── Human-readable relative strings DH uses ───────────────────────────────
    # "Within the last year" / "Within the last X months" → treat as fresh
    if raw.startswith("within the last"):
        return True

    # "X days ago" → fresh if X <= _DH_FRESHNESS_MONTHS * 30
    days_match = re.match(r"^(\d+)\s+days?\s+ago$", raw)
    if days_match:
        return int(days_match.group(1)) <= _DH_FRESHNESS_MONTHS * 30

    # "X months ago" → fresh if X <= _DH_FRESHNESS_MONTHS
    months_match = re.match(r"^(\d+)\s+months?\s+ago$", raw)
    if months_match:
        return int(months_match.group(1)) <= _DH_FRESHNESS_MONTHS

    # "X years ago" → always stale
    if re.match(r"^\d+\s+years?\s+ago$", raw):
        return False

    # ── ISO date strings ──────────────────────────────────────────────────────
    try:
        date_part = raw[:10]
        parts = date_part.split("-")
        if len(parts) != 3:
            return False
        record_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=_DH_FRESHNESS_MONTHS * 30)
        return record_date >= cutoff
    except (ValueError, IndexError):
        return False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

# Output filenames (timestamped so successive runs never overwrite each other)
_TS                = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CONTACTS    = f"state_assoc_contacts_{_TS}.csv"
OUTPUT_FACILITIES  = f"state_assoc_facilities_{_TS}.csv"

# ─── State association guide ──────────────────────────────────────────────────
# Path to the machine-readable guide (one level up from this script's directory)
_GUIDE_PATH = os.path.join(os.path.dirname(__file__), "..", "state_association_agent_guide.md")

_STATE_ABBREV_TO_NAME: dict[str, str] = {
    "AL": "Alabama",      "AK": "Alaska",        "AZ": "Arizona",      "AR": "Arkansas",
    "CA": "California",   "CO": "Colorado",       "CT": "Connecticut",  "DE": "Delaware",
    "FL": "Florida",      "GA": "Georgia",        "HI": "Hawaii",       "ID": "Idaho",
    "IL": "Illinois",     "IN": "Indiana",        "IA": "Iowa",         "KS": "Kansas",
    "KY": "Kentucky",     "LA": "Louisiana",      "ME": "Maine",        "MD": "Maryland",
    "MA": "Massachusetts","MI": "Michigan",       "MN": "Minnesota",    "MS": "Mississippi",
    "MO": "Missouri",     "MT": "Montana",        "NE": "Nebraska",     "NV": "Nevada",
    "NH": "New Hampshire","NJ": "New Jersey",     "NM": "New Mexico",   "NY": "New York",
    "NC": "North Carolina","ND": "North Dakota",  "OH": "Ohio",         "OK": "Oklahoma",
    "OR": "Oregon",       "PA": "Pennsylvania",   "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee",      "TX": "Texas",        "UT": "Utah",
    "VT": "Vermont",      "VA": "Virginia",       "WA": "Washington",   "WV": "West Virginia",
    "WI": "Wisconsin",    "WY": "Wyoming",
}

_GUIDE_CACHE: tuple[list[dict], list[dict]] | None = None


# call_openrouter, parse_found_name, normalize_phone — imported from state_association.llm_client


def lookup_hubspot_contact_enhanced(
    client: bigquery.Client,
    full_name: str,
    email: str = "",
    phone: str = "",
    facility_hubspot_id: str = "",
    parent_hubspot_id: str = "",
) -> tuple[list[dict], str]:
    """
    Search HubSpot contacts using multiple strategies with priority ordering.

    Priority order:
      1. Exact match on email (if provided)
      2. Exact match on phone (if provided)
      3. Exact match on name AND associated with the specific facility
      4. Exact match on name AND associated with the parent company
      5. Exact match on name only (any association)

    Returns (matches, match_strategy) where match_strategy describes how the match was found.
    Each match dict contains: contact_id, first_name, last_name, email, phone, job_title,
    associated_company_id, match_type.
    """
    parts = full_name.strip().split() if full_name else []
    if len(parts) < 2 and not email and not phone:
        return [], "no_search_criteria"

    first_name = parts[0] if parts else ""
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    normalized_phone = normalize_phone(phone)

    all_matches: list[dict] = []
    strategy_used = "not_found"

    # ── Strategy 1: Email lookup (highest confidence) ───────────────────────────
    if email and "@" in email:
        email_clean = email.strip().lower()
        query = f"""
        SELECT
            CAST(properties_hs_object_id AS STRING)      AS contact_id,
            properties_firstname                          AS first_name,
            properties_lastname                           AS last_name,
            properties_email                              AS email,
            properties_phone                              AS phone,
            properties_mobilephone                        AS mobile_phone,
            properties_jobtitle                           AS job_title,
            CAST(properties_associatedcompanyid AS STRING) AS associated_company_id
        FROM `{HUBSPOT_CONTACTS_TABLE}`
        WHERE (archived IS NOT TRUE OR archived IS NULL)
          AND LOWER(TRIM(properties_email)) = LOWER(@email_clean)
        LIMIT 5
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("email_clean", "STRING", email_clean),
        ])
        try:
            rows = client.query(query, job_config=job_config).to_dataframe()
            rows = rows.replace(["nan", "None", "<NA>"], "")
            for row in rows.to_dict("records"):
                row["match_type"] = "email_match"
                all_matches.append(row)
            if all_matches:
                return all_matches, "email_exact"
        except Exception as e:
            logger.warning(f"HubSpot email lookup failed: {e}")

    # ── Strategy 2: Phone lookup ────────────────────────────────────────────────
    if normalized_phone and len(normalized_phone) >= 10:
        # Search both phone and mobile_phone fields
        phone_suffix = normalized_phone[-10:]
        query = f"""
        SELECT
            CAST(properties_hs_object_id AS STRING)      AS contact_id,
            properties_firstname                          AS first_name,
            properties_lastname                           AS last_name,
            properties_email                              AS email,
            properties_phone                              AS phone,
            properties_mobilephone                        AS mobile_phone,
            properties_jobtitle                           AS job_title,
            CAST(properties_associatedcompanyid AS STRING) AS associated_company_id
        FROM `{HUBSPOT_CONTACTS_TABLE}`
        WHERE (archived IS NOT TRUE OR archived IS NULL)
          AND (
              REGEXP_REPLACE(properties_phone, r'[^0-9]', '') LIKE CONCAT('%', @phone_suffix, '%')
              OR REGEXP_REPLACE(properties_mobilephone, r'[^0-9]', '') LIKE CONCAT('%', @phone_suffix, '%')
          )
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("phone_suffix", "STRING", phone_suffix),
        ])
        try:
            rows = client.query(query, job_config=job_config).to_dataframe()
            rows = rows.replace(["nan", "None", "<NA>"], "")
            phone_matches = []
            for row in rows.to_dict("records"):
                row["match_type"] = "phone_match"
                phone_matches.append(row)
            if phone_matches:
                return phone_matches, "phone_exact"
        except Exception as e:
            logger.warning(f"HubSpot phone lookup failed: {e}")

    # ── Strategy 3-5: Name lookup with association priority ranked in SQL ───────
    if first_name and last_name:
        # CASE expression ranks rows so that matches at the specific facility come
        # first (1), then parent company (2), then any other company (3).
        # A single query replaces three sequential Python filter passes.
        # Both WHEN clauses are always emitted; empty-string params never match real IDs.
        name_query = f"""
        SELECT
            CAST(properties_hs_object_id AS STRING)       AS contact_id,
            properties_firstname                           AS first_name,
            properties_lastname                            AS last_name,
            properties_email                               AS email,
            properties_phone                               AS phone,
            properties_mobilephone                         AS mobile_phone,
            properties_jobtitle                            AS job_title,
            CAST(properties_associatedcompanyid AS STRING) AS associated_company_id,
            CASE
                WHEN @facility_id != '' AND CAST(properties_associatedcompanyid AS STRING) = @facility_id THEN 1
                WHEN @parent_id != '' AND CAST(properties_associatedcompanyid AS STRING) = @parent_id THEN 2
                ELSE 3
            END AS _priority
        FROM `{HUBSPOT_CONTACTS_TABLE}`
        WHERE (archived IS NOT TRUE OR archived IS NULL)
          AND LOWER(TRIM(properties_firstname)) = LOWER(@first_name)
          AND LOWER(TRIM(properties_lastname))  = LOWER(@last_name)
        ORDER BY _priority, contact_id
        LIMIT 20
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("first_name",  "STRING", first_name),
            bigquery.ScalarQueryParameter("last_name",   "STRING", last_name),
            bigquery.ScalarQueryParameter("facility_id", "STRING", facility_hubspot_id or ""),
            bigquery.ScalarQueryParameter("parent_id",   "STRING", parent_hubspot_id or ""),
        ])

        try:
            rows = client.query(name_query, job_config=job_config).to_dataframe()
            rows = rows.replace(["nan", "None", "<NA>"], "")

            if rows.empty:
                return [], "name_not_found"

            # All rows at the highest priority (lowest _priority value)
            best = int(rows["_priority"].min())
            top_rows = rows[rows["_priority"] == best].drop(columns=["_priority"])
            matches = top_rows.to_dict("records")

            strategy_map = {1: "name_facility", 2: "name_parent", 3: "name_any"}
            match_type_map = {
                1: "name_facility_match",
                2: "name_parent_match",
                3: "name_any_match",
            }
            for m in matches:
                m["match_type"] = match_type_map[best]

            return matches, strategy_map[best]

        except Exception as e:
            logger.warning(f"HubSpot name lookup failed for '{full_name}': {e}")
            return [], "name_query_error"

    return [], "no_criteria_matched"


def _format_hubspot_matches_enhanced(
    matches: list[dict],
    match_strategy: str,
    facility_name: str = "",
) -> tuple[str, str, str]:
    """
    Summarise enhanced HubSpot match results.

    Returns (hubspot_contact_ids, hubspot_match_detail, match_summary).
    match_summary is a human-readable description of the match quality.
    """
    if not matches:
        return "", "Not found in HubSpot", "No match found"

    ids = "|".join(m.get("contact_id", "") for m in matches if m.get("contact_id"))

    # Build detailed description
    details = []
    for m in matches:
        name = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip()
        contact_id = m.get("contact_id", "")
        title = m.get("job_title", "") or "N/A"
        company = m.get("associated_company_id", "") or "N/A"
        email = m.get("email", "") or ""
        phone = m.get("phone", "") or m.get("mobile_phone", "") or ""
        match_type = m.get("match_type", "unknown")

        detail = f"{name} [{contact_id}] title={title} company={company}"
        if email:
            detail += f" email={email}"
        if phone:
            detail += f" phone={phone}"
        detail += f" (match_type={match_type})"
        details.append(detail)

    detail_str = "; ".join(details)

    # Build match summary
    strategy_descriptions = {
        "email_exact": "✅ Exact email match - highest confidence",
        "phone_exact": "✅ Exact phone match - high confidence",
        "name_facility": "✅ Name match at facility - high confidence",
        "name_parent": "⚠️ Name match at parent company - may need facility reassignment",
        "name_any": "⚠️ Name match at other company - verify this is the same person",
        "name_not_found": "Not found in HubSpot",
    }
    summary = strategy_descriptions.get(match_strategy, f"Match strategy: {match_strategy}")

    return ids, detail_str, summary


def lookup_hubspot_contact_by_name(
    client: bigquery.Client,
    full_name: str,
) -> list[dict]:
    """
    Legacy function for backward compatibility.
    Search HubSpot contacts for a person by their full name (first + last).

    Returns a list of matching contact dicts (may be empty or have multiple hits).
    Each dict contains: contact_id, first_name, last_name, email, job_title,
    associated_company_id.
    """
    matches, _ = lookup_hubspot_contact_enhanced(client, full_name)
    return matches


def _format_hubspot_matches(matches: list[dict]) -> tuple[str, str]:
    """
    Summarise HubSpot name-match results into two CSV-friendly strings:
      - hubspot_contact_ids  (pipe-separated list of contact record IDs)
      - hubspot_match_detail (human-readable summary)
    """
    if not matches:
        return "", "Not found in HubSpot"

    ids    = "|".join(m.get("contact_id", "") for m in matches if m.get("contact_id"))
    detail = "; ".join(
        f"{m.get('first_name', '')} {m.get('last_name', '')} "
        f"[{m.get('contact_id', '')}] "
        f"title={m.get('job_title', '') or 'N/A'} "
        f"company={m.get('associated_company_id', '') or 'N/A'}"
        for m in matches
    )
    return ids, detail


# ─── State association guide helpers ─────────────────────────────────────────

def _load_association_guide(guide_path: str = _GUIDE_PATH) -> tuple[list[dict], list[dict]]:
    """
    Parse the state_association_agent_guide.md file and return (state_entries, fallback_sources).

    Extracts all fenced ```yaml blocks, safe-loads each, and separates entries with a
    top-level "state" key from the single "fallback_sources" block.
    Returns ([], []) on any file or parse error so callers degrade gracefully.
    """
    global _GUIDE_CACHE
    if _GUIDE_CACHE is not None:
        return _GUIDE_CACHE

    state_entries: list[dict] = []
    fallback_sources: list[dict] = []

    try:
        with open(guide_path, "r", encoding="utf-8") as f:
            content = f.read()

        blocks = re.findall(r"```yaml\s*(.*?)```", content, re.DOTALL)
        for raw in blocks:
            try:
                # Strip YAML document-separator lines (---) that appear inside fenced blocks
                raw_clean = "\n".join(
                    line for line in raw.strip().splitlines() if line.strip() != "---"
                )
                data = yaml.safe_load(raw_clean)
                if not isinstance(data, dict):
                    continue
                if "state" in data and "sources" in data:
                    state_entries.append(data)
                elif "fallback_sources" in data:
                    fallback_sources = data["fallback_sources"] or []
            except yaml.YAMLError as e:
                logger.warning(f"Guide YAML parse warning (block skipped): {e}")

    except FileNotFoundError:
        logger.warning(f"State association guide not found at {guide_path} — prompts will use generic sources")
    except Exception as e:
        logger.warning(f"Failed to load state association guide: {e} — prompts will use generic sources")

    _GUIDE_CACHE = (state_entries, fallback_sources)
    return _GUIDE_CACHE


def _normalize_state_name(state: str) -> str:
    """Convert a state abbreviation ('WA') or full name ('Washington') to the guide's canonical form."""
    if not state:
        return ""
    stripped = state.strip()
    if len(stripped) == 2:
        return _STATE_ABBREV_TO_NAME.get(stripped.upper(), stripped.title())
    return stripped.title()


def _lookup_guide_sources(
    state: str,
    facility_type: str,
) -> tuple[str, list[dict], list[dict]]:
    """
    Look up the state association sources from the guide for a given state and facility type.

    Returns (tier, matching_sources, fallback_sources) where:
      tier "1" = public contacts available
      tier "2" = login required to access contacts
      tier "3" = no named contacts in any public directory
    """
    state_entries, fallback_sources = _load_association_guide()
    normalized = _normalize_state_name(state)

    if not normalized:
        return "3", [], fallback_sources

    entry = next((e for e in state_entries if e.get("state") == normalized), None)
    if not entry:
        return "3", [], fallback_sources

    all_sources: list[dict] = entry.get("sources") or []

    # Normalize facility type for matching
    ftype = (facility_type or "").upper()
    # Treat CCRC and ILF as ALF/IL for association matching purposes
    if ftype in ("CCRC", "ILF"):
        ftype_alts = {"ALF", "IL"}
    else:
        ftype_alts = {ftype} if ftype else set()

    # Filter sources by type; fall back to all sources if no type-specific match
    if ftype_alts:
        type_matched = [
            s for s in all_sources
            if any(alt in (s.get("type") or "").upper() for alt in ftype_alts)
        ]
        sources = type_matched if type_matched else all_sources
    else:
        sources = all_sources

    # Determine tier
    for s in sources:
        if not s.get("login_required", False) and s.get("contacts_available"):
            return "1", sources, fallback_sources
    for s in sources:
        if s.get("login_required", False) and s.get("contacts_available"):
            return "2", sources, fallback_sources
    return "3", sources, fallback_sources


def _format_sources_for_prompt(
    tier: str,
    matching_sources: list[dict],
    fallback_sources: list[dict],
    state: str,
    facility_name: str = "",
    city: str = "",
    website: str = "",
) -> str:
    """
    Build the 'Please search the following sources:' bullet text for a research prompt.

    Tier 1: lead with specific verified association URLs + access notes.
    Tier 2: note login barrier, fall through to CMS + state DOH fallbacks.
    Tier 3: skip association search, lead directly with CMS + state DOH fallbacks.

    The state association site is always the primary source when available.
    The facility's own website is included as a location-verification step only —
    staff names are rarely listed publicly.
    """
    lines: list[str] = []
    state_label = state or "the relevant state"

    if tier == "1":
        tier_1_sources = [
            s for s in matching_sources
            if not s.get("login_required", False) and s.get("contacts_available")
        ]
        for s in tier_1_sources:
            name = s.get("association_name", "Association directory")
            url  = s.get("directory_url") or ""
            contacts = ", ".join(s.get("contacts_available") or [])
            notes = (s.get("access_notes") or "").strip()
            verified = s.get("verified", True)
            unverified_tag = " (unverified — confirm before scraping)" if not verified else ""

            entry = f"- {name}"
            if url:
                entry += f"\n    URL: {url}{unverified_tag}"
            if contacts:
                entry += f"\n    Contacts listed: {contacts}"

            pattern = s.get("url_pattern")
            if pattern:
                entry += (
                    f"\n    Direct facility page: {pattern}"
                    f" (replace {{slug}} with the facility name lowercased, spaces → hyphens)"
                )
            if notes:
                entry += f"\n    Access notes: {notes}"
            lines.append(entry)

        # Secondary fallbacks always appended for Tier 1
        lines.append(
            "- CMS Care Compare (https://www.medicare.gov/care-compare/)"
            " — search by facility name (covers all Medicare/Medicaid-certified SNFs)"
        )
        lines.append(
            f"- {state_label} Department of Health licensing database"
            f" — search \"{state_label} licensed nursing facilities list\""
        )
        # Facility website: use for location verification only, not staff discovery
        if website:
            lines.append(
                f"- Facility website ({website})"
                " — check for a 'Leadership', 'Staff', 'About Us', or 'Contact' page;"
                " staff names are sometimes listed but this is lower priority than the sources above"
            )
        elif facility_name:
            lines.append(
                f"- Facility's own website"
                f" — search \"{facility_name} {city} {state_label}\" to find the site, then check"
                " for a 'Leadership', 'Staff', or 'About Us' page;"
                " lower priority than the sources above"
            )
        lines.append(
            f"- LinkedIn — last resort only; search \"{facility_name} administrator\""
            f" or \"{facility_name} executive director\" if all above sources fail"
            if facility_name else
            "- LinkedIn — last resort; search facility name + administrator"
        )

    elif tier == "2":
        login_sources = [s for s in matching_sources if s.get("login_required", False)]
        for s in login_sources:
            name = s.get("association_name", "Association directory")
            lines.append(
                f"- NOTE: The {name} directory for {state_label} requires a member login"
                " and cannot be accessed directly. Skip this source."
            )
        lines.append(
            "- CMS Care Compare (https://www.medicare.gov/care-compare/)"
            " — search by facility name (covers all Medicare/Medicaid-certified SNFs)"
        )
        lines.append(
            f"- {state_label} Department of Health licensing database"
            f" — search \"{state_label} licensed nursing facilities list\""
        )
        if website:
            lines.append(
                f"- Facility website ({website})"
                " — check for a 'Leadership', 'Staff', 'About Us', or 'Contact' page;"
                " staff names are sometimes listed but this is lower priority than the sources above"
            )
        elif facility_name:
            lines.append(
                f"- Facility's own website"
                f" — search \"{facility_name} {city} {state_label}\" to find the site, then check"
                " for a 'Leadership', 'Staff', or 'About Us' page;"
                " lower priority than the sources above"
            )
        lines.append(
            f"- LinkedIn — last resort only; search \"{facility_name} administrator\""
            f" or \"{facility_name} executive director\" if all above sources fail"
            if facility_name else
            "- LinkedIn — last resort; search facility name + administrator"
        )

    else:  # Tier 3
        lines.append(
            "- CMS Care Compare (https://www.medicare.gov/care-compare/)"
            " — search by facility name (the state association directory does not list named contacts;"
            " CMS is the best public source for SNFs)"
        )
        lines.append(
            f"- {state_label} Department of Health licensing database"
            f" — search \"{state_label} licensed nursing facilities list\" for a downloadable"
            " Excel or CSV with administrator names"
        )
        if website:
            lines.append(
                f"- Facility website ({website})"
                " — check for a 'Leadership', 'Staff', 'About Us', or 'Contact' page;"
                " staff names are sometimes listed but this is lower priority than the sources above"
            )
        elif facility_name:
            lines.append(
                f"- Facility's own website"
                f" — search \"{facility_name} {city} {state_label}\" to find the site, then check"
                " for a 'Leadership', 'Staff', or 'About Us' page;"
                " lower priority than the sources above"
            )
        lines.append(
            f"- LinkedIn — last resort only; search \"{facility_name} administrator\""
            f" or \"{facility_name} executive director\" if all above sources fail"
            if facility_name else
            "- LinkedIn — last resort; search facility name + administrator"
        )
        if facility_name:
            lines.append(
                f"- The operator's corporate website — search for the {facility_name} community page"
                " and check for a staff or leadership section listing the Executive Director"
            )

    return "\n".join(lines)


def _build_contact_research_prompt(
    contact_name: str,
    title: str,
    corporation_name: str,
    state: str,
    facility_type: str,
) -> str:
    """Build the OpenRouter prompt for researching an unassociated contact."""
    state_context = f"in {state}" if state else "across the United States"
    type_desc = {
        "SNF":  "skilled nursing facility (SNF)",
        "ALF":  "assisted living facility (ALF)",
        "BOTH": "skilled nursing or assisted living facility",
    }.get(facility_type.upper() if facility_type else "", "long-term care facility")

    # Build a targeted source list from the guide when possible
    tier, matching_sources, fallback_sources = _lookup_guide_sources(state, facility_type)
    state_label = state or "the relevant state"

    if tier == "1":
        tier_1 = [
            s for s in matching_sources
            if not s.get("login_required", False) and s.get("contacts_available")
        ]
        assoc_lines = []
        for s in tier_1:
            name = s.get("association_name", "Association directory")
            url  = s.get("directory_url") or ""
            entry = f"- {name}" + (f" ({url})" if url else "")
            assoc_lines.append(entry)
        assoc_block = "\n".join(assoc_lines)
    else:
        assoc_block = (
            f"- {state_label} health care association member directory\n"
            f"- {state_label} assisted living or senior living association directory"
        )

    return f"""I need you to search publicly available state association websites and
directories to find information about the following person:

Contact Name: {contact_name}
Known Title:  {title}
Corporation:  {corporation_name}
Location:     {state_context}

Search these sources in order, stopping when you find a verified match:
{assoc_block}
- CMS Care Compare / Nursing Home Compare listings for {corporation_name} facilities
  (https://www.medicare.gov/care-compare/)
- LinkedIn or professional directories for "{contact_name}" + "{title}"

For each source you check, briefly note: source name → result (found / not found / inaccessible).

Return a JSON object with these fields:
- found_name:  full name as listed at the source, or empty string if not found
- found_title: exact job title as listed at the source, or empty string
- found_email: publicly listed email address, or empty string
- found_phone: publicly listed phone number, or empty string
- confidence:  "high" (official association/CMS listing), "medium" (facility/operator site),
               "low" (LinkedIn/news/possibly outdated), or "not_found" (no verified name)
- source_url:  exact URL where the name was found, or empty string
- source_name: plain-English name of that source (e.g. "Washington State Assisted Living Association member directory")"""


# Regex to identify regional/VP-level titles that should use the corporate domain
# Sonar Pro prompt rather than the state association prompt.
_REGIONAL_TITLE_RE = re.compile(
    r"\b(regional|vice president|\bvp\b|svp|senior vice president|director of operations)\b",
    re.IGNORECASE,
)


def _is_regional_title(title: str) -> bool:
    """Return True if *title* is a regional or VP-level role (not C-suite)."""
    return bool(_REGIONAL_TITLE_RE.search(title or ""))


def _build_corporate_contact_research_prompt(
    contact_name: str,
    title: str,
    corporation_name: str,
    corporate_domain: str,
    facility_type: str,
    linkedin_url: str = "",
) -> str:
    """
    Build a domain-targeted Sonar Pro prompt for stale VP / Regional Director
    records in Workflow 2.

    Unlike _build_contact_research_prompt (which searches state association
    directories), this prompt restricts the search to the corporation's own
    website and LinkedIn.  It is used when:
      - The DH record is stale (older than _DH_FRESHNESS_MONTHS months)
      - The title is NOT C-suite (those are fast-tracked directly from DH)
      - The title IS a regional or VP-level role

    When *linkedin_url* is provided (from DH or HubSpot), a "Step 0" is
    prepended that directs Sonar Pro to check that profile directly before
    doing any broader search.  This is the most reliable verification path
    because the person's own LinkedIn shows their current employer.

    The ``search_domain_filter`` key is injected into the OpenRouter payload
    (via extra_params) by the caller so Perplexity restricts its web search to
    the corporate domain and common investor-relations subdomains.
    """
    type_desc = {
        "SNF":  "skilled nursing facility (SNF) operator",
        "ALF":  "assisted living facility (ALF) operator",
        "BOTH": "skilled nursing and assisted living operator",
    }.get((facility_type or "").upper(), "long-term care operator")

    # Derive the bare domain for display in the prompt
    bare = _extract_bare_domain(corporate_domain)
    domain_display = bare or corporation_name

    is_regional = _is_regional_title(title)

    # If we have a LinkedIn URL for this specific person, make it Step 0 —
    # the most direct verification path.  Sonar Pro can load the profile and
    # confirm whether their current employer matches {corporation_name}.
    if linkedin_url:
        step0 = (
            f"0. Go directly to this LinkedIn profile: {linkedin_url}\n"
            f"   Check whether their current employer is {corporation_name} and whether their title "
            f"matches or is consistent with \"{title}\".\n"
            f"   If confirmed, you can stop here — return high or medium confidence based on the recency shown.\n"
        )
        step_offset = 1
    else:
        step0 = ""
        step_offset = 0

    if is_regional:
        search_instructions = (
            f"{step0}"
            f"{step_offset + 1}. {domain_display} — look for pages titled 'About Us', 'Leadership', 'Our Team',\n"
            f"   'Management', or 'Investor Relations' that list regional executives.\n"
            f"{step_offset + 2}. LinkedIn: search for \"{corporation_name}\" \"{title}\" to find the current person\n"
            f"   holding this role.  Prefer profiles showing a current position at {corporation_name}.\n"
            f"{step_offset + 3}. LinkedIn: try the variant search \"{corporation_name} regional\" to surface regional\n"
            f"   leadership not listed by exact title."
        )
    else:
        search_instructions = (
            f"{step0}"
            f"{step_offset + 1}. {domain_display} — look specifically for pages titled 'About Us', 'Leadership Team',\n"
            f"   'Management', 'Our Team', or 'Investor Relations / Governance' that list senior\n"
            f"   executives.  Common URL patterns: /about-us, /leadership-team, /management,\n"
            f"   /corporate-governance, /investor-relations/management.\n"
            f"{step_offset + 2}. LinkedIn: search for \"{corporation_name}\" \"{title}\" to confirm or supplement the\n"
            f"   corporate website finding."
        )

    return f"""I need you to verify whether {contact_name} is currently the {title} at {corporation_name},
a {type_desc}.

Contact on file in Definitive Healthcare (may be outdated):
  Name:  {contact_name}
  Title: {title}

Search these sources in order, stopping when you find a verified match:
{search_instructions}

For each source you check, briefly note: source name → result (found / not found / inaccessible).

Return a JSON object with these fields:
- found_name:  full name as listed at the source, or empty string if not found
- found_title: exact job title as listed at the source, or empty string
- found_email: publicly listed email address, or empty string
- found_phone: publicly listed phone number, or empty string
- confidence:  "high" (official corporate website or investor relations page),
               "medium" (operator news / press release or LinkedIn showing current role),
               "low" (LinkedIn showing unclear tenure or possibly outdated source),
               or "not_found" (no verified name located)
- source_url:  exact URL where the name was found, or empty string
- source_name: plain-English name of that source (e.g. "{domain_display} leadership page",
               "LinkedIn profile — {corporation_name}")
- reasoning:   1–3 sentence explanation of what you found and why you assigned that confidence"""


def _build_facility_research_prompt(
    facility_name: str,
    city: str,
    state: str,
    facility_type: str,
    corporation_name: str,
    address: str = "",
    website: str = "",
) -> tuple[str, str, list]:
    """
    Build the OpenRouter prompt for researching a facility missing leadership.

    Returns (prompt_text, source_tier, domain_filter) where:
      source_tier "1" = specific association URL(s) injected from the guide
      source_tier "2" = login-required association noted; fallbacks used
      source_tier "3" = no public association contacts; fallbacks only
      domain_filter    = list of bare domains for search_domain_filter (Tier 1 only;
                         empty list for Tier 2/3 where open web search is needed)
    """
    type_desc = {
        "SNF":  "skilled nursing facility",
        "ALF":  "assisted living facility",
        "ILF":  "independent living facility",
        "CCRC": "continuing care retirement community",
    }.get((facility_type or "").upper(), "long-term care facility")

    location = f"{city}, {state}" if city and state else (state or "unknown location")

    tier, matching_sources, fallback_sources = _lookup_guide_sources(state, facility_type)
    source_text = _format_sources_for_prompt(
        tier, matching_sources, fallback_sources,
        state=state, facility_name=facility_name, city=city, website=website,
    )

    # Build facility identifier block — more fields = less chance of matching a
    # same-named facility in a different city or state.
    facility_lines = [f"Facility Name:   {facility_name}"]
    if address:
        facility_lines.append(f"Street Address:  {address}")
    facility_lines.append(f"Location:        {location}")
    facility_lines.append(f"Facility Type:   {type_desc}")
    facility_lines.append(f"Parent Company:  {corporation_name}")
    if website:
        facility_lines.append(f"Website:         {website}")
    facility_block = "\n".join(facility_lines)

    prompt = f"""I need you to find the current administrator or executive director
for the following healthcare facility:

{facility_block}

Search these sources in order, stopping when you find a verified name:
{source_text}

For each source you check, briefly note: source name → result (found / not found / inaccessible).

Return a JSON object with these fields:
- found_name:  full name of the administrator/executive director, or empty string if not found
- found_title: exact job title as listed at the source, or empty string
- found_email: publicly listed email address, or empty string
- found_phone: publicly listed phone number, or empty string
- confidence:  "high" (official association/CMS listing), "medium" (facility/operator site),
               "low" (LinkedIn/news/possibly outdated), or "not_found" (no verified name)
- source_url:  exact URL where the name was found, or empty string
- source_name: plain-English name of that source (e.g. "Colorado HCAP facility search", "CMS Care Compare listing")"""

    # Build domain filter for Tier 1 calls so Perplexity restricts its crawl to
    # the authoritative association site(s) + CMS Care Compare.  Tier 2/3 need
    # open web search because there is no publicly accessible directory URL.
    if tier == "1":
        tier_1_sources = [
            s for s in matching_sources
            if not s.get("login_required", False) and s.get("contacts_available")
        ]
        domain_filter: list = []
        for s in tier_1_sources:
            url = s.get("directory_url") or ""
            if url:
                bare = _extract_bare_domain(url)
                if bare and bare not in domain_filter:
                    domain_filter.append(bare)
        if "medicare.gov" not in domain_filter:
            domain_filter.append("medicare.gov")
        # If we know the facility's own website, add it so Perplexity can check
        # the facility's staff/leadership page without that domain being blocked
        # by the association-only filter.
        if website:
            bare_site = _extract_bare_domain(website)
            if bare_site and bare_site not in domain_filter:
                domain_filter.append(bare_site)
    else:
        domain_filter = []

    return prompt, tier, domain_filter


# ─── BigQuery data-loading helpers ───────────────────────────────────────────

def get_facility_ids_for_network(client: bigquery.Client, network_id: int) -> list[int]:
    """
    Return all HOSPITAL_IDs (child facilities) associated with a NETWORK_ID
    from both the SNF and ALF overview tables.
    """
    facility_ids: set[int] = set()

    _network_cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("network_id", "INT64", network_id),
    ])
    for key, table in [("SNF", DH_TABLES["snf_overview"]), ("ALF", DH_TABLES["alf_overview"])]:
        query = f"""
        SELECT DISTINCT CAST(HOSPITAL_ID AS INT64) AS hospital_id
        FROM `{table}`
        WHERE NETWORK_ID = @network_id
          AND HOSPITAL_ID IS NOT NULL
        """
        try:
            rows = client.query(query, job_config=_network_cfg).result()
            ids = {row.hospital_id for row in rows}
            facility_ids.update(ids)
            logger.info(f"{key} Overview: {len(ids)} child facilities")
        except Exception as e:
            logger.warning(f"{key} Overview query failed: {e}")

    return sorted(facility_ids)


def load_corporate_level_contacts(
    client: bigquery.Client,
    network_id: int,
) -> pd.DataFrame:
    """
    Load contacts from Definitive who are titled as corporate executives
    (CEO, COO, CFO, President, VP, etc.) and associated with the NETWORK_ID
    at the corporation level (HOSPITAL_ID = network_id).

    Returns a DataFrame of contacts suitable for research.
    """
    logger.info(f"Querying corporate-level contacts for NETWORK_ID={network_id}...")

    all_rows: list[dict] = []

    _net_cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("network_id", "INT64", network_id),
    ])

    # Titles containing these prefixes are former/retired employees — exclude entirely
    # so we don't burn Sonar Pro calls on contacts who no longer work here.
    _FORMER_FILTER = r"^(former|retired|ex-)\s"

    # Functional areas we are not interested in enriching — exclude regardless of seniority.
    _EXCLUDED_FUNCTIONS = r"marketing|development|\bcompliance\b|general counsel|plant operations|clinical services"

    # ── Source 1: Executives table (contacts listed directly under the network ID) ──
    exec_query = f"""
    SELECT
        EXECUTIVE_ID,
        CAST(HOSPITAL_ID AS INT64) AS hospital_id,
        EXECUTIVE_NAME,
        FIRST_NAME,
        LAST_NAME,
        LOWER(TRIM(TITLE)) AS title,
        EMAIL,
        DIRECT_PHONE,
        LOCATION_PHONE,
        LINKEDIN_PROFILE,
        HOSPITAL_NAME,
        LAST_UPDATE,
        'executives' AS source_table
    FROM `{DH_TABLES['executives']}`
    WHERE HOSPITAL_ID = @network_id
      AND REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{DH_W2_TITLE_PATTERN}')
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{_FORMER_FILTER}')
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{_EXCLUDED_FUNCTIONS}')
    """
    try:
        rows = [dict(r) for r in client.query(exec_query, job_config=_net_cfg).result()]
        all_rows.extend(rows)
        logger.info(f"Executives table (network level): {len(rows)} rows")
    except Exception as e:
        logger.warning(f"Executives table query failed: {e}")

    # ── Source 2: Corporate Executives table (HOSPITAL_ID = network_id) ──
    corp_query = f"""
    SELECT
        EXECUTIVE_ID,
        CAST(HOSPITAL_ID AS INT64) AS hospital_id,
        EXECUTIVE_NAME,
        FIRST_NAME,
        LAST_NAME,
        LOWER(TRIM(TITLE)) AS title,
        EMAIL,
        NULL AS DIRECT_PHONE,
        NULL AS LOCATION_PHONE,
        LINKEDIN_PROFILE,
        HOSPITAL_NAME,
        LAST_UPDATE,
        'corporate_executives' AS source_table
    FROM `{DH_TABLES['corporation']}`
    WHERE HOSPITAL_ID = @network_id
      AND REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{DH_W2_TITLE_PATTERN}')
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{_FORMER_FILTER}')
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{_EXCLUDED_FUNCTIONS}')
    """
    try:
        rows = [dict(r) for r in client.query(corp_query, job_config=_net_cfg).result()]
        all_rows.extend(rows)
        logger.info(f"Corporate Executives table (network level): {len(rows)} rows")
    except Exception as e:
        logger.warning(f"Corporate Executives table query failed: {e}")

    # ── Source 3: December Full Feed (contacts at network_id HOSPITAL_ID) ──
    feed_query = f"""
    SELECT
        EXECUTIVE_ID,
        CAST(HOSPITAL_ID AS INT64) AS hospital_id,
        EXECUTIVE_NAME,
        FIRST_NAME,
        LAST_NAME,
        LOWER(TRIM(TITLE)) AS title,
        EMAIL,
        NULL AS DIRECT_PHONE,
        NULL AS LOCATION_PHONE,
        LINKEDIN_PROFILE,
        HOSPITAL_NAME,
        LAST_UPDATE,
        'december_full_feed' AS source_table
    FROM `{DH_TABLES['full_feed']}`
    WHERE HOSPITAL_ID = @network_id
      AND REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{DH_W2_TITLE_PATTERN}')
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{_FORMER_FILTER}')
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{_EXCLUDED_FUNCTIONS}')
    """
    try:
        rows = [dict(r) for r in client.query(feed_query, job_config=_net_cfg).result()]
        all_rows.extend(rows)
        logger.info(f"December Full Feed (network level): {len(rows)} rows")
    except Exception as e:
        logger.warning(f"December Full Feed query failed: {e}")

    if not all_rows:
        logger.info("No corporate-level executive contacts found in Definitive Healthcare.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Deduplicate step 1: keep one row per EXECUTIVE_ID, preferring the richest source.
    # (same person can appear in multiple tables with the same EXECUTIVE_ID)
    priority = {"december_full_feed": 0, "corporate_executives": 1, "executives": 2}
    df["_priority"] = df["source_table"].map(priority).fillna(9)
    df = df.sort_values("_priority").drop_duplicates(subset=["EXECUTIVE_ID"], keep="first")
    df = df.drop(columns=["_priority"])

    # Deduplicate step 2: same person with DIFFERENT EXECUTIVE_IDs across tables.
    # Normalise full name (lower, strip) and keep the first occurrence (highest-priority
    # source already sorted above).
    df["_norm_name"] = (
        df["FIRST_NAME"].fillna("").str.strip().str.lower()
        + " "
        + df["LAST_NAME"].fillna("").str.strip().str.lower()
    ).str.strip()
    before = len(df)
    df = df.drop_duplicates(subset=["_norm_name"], keep="first")
    df = df.drop(columns=["_norm_name"])
    dupes_removed = before - len(df)
    if dupes_removed:
        logger.info(f"Removed {dupes_removed} cross-table name duplicate(s)")

    logger.info(f"{len(df)} corporate executive contacts ready for research")
    return df.reset_index(drop=True)


def load_hubspot_child_facilities(
    client: bigquery.Client,
    hubspot_corp_id: str,
) -> pd.DataFrame:
    """
    Return all active child facilities in HubSpot whose hs_parent_company_id
    matches the given corporation HubSpot ID.
    """
    logger.info(f"Querying HubSpot child facilities for parent ID={hubspot_corp_id}...")

    query = f"""
    SELECT
        CAST(properties_hs_object_id AS STRING)  AS hubspot_id,
        properties_name                           AS facility_name,
        properties_facility_type                  AS facility_type,
        properties_hierarchy_type                 AS hierarchy_type,
        properties_city                           AS city,
        properties_state                          AS state,
        properties_address                        AS address,
        properties_zip                            AS zip,
        properties_phone                          AS phone,
        properties_website                        AS website,
        CAST(properties_definitive_healthcare_id AS STRING) AS definitive_id
    FROM `{HUBSPOT_COMPANIES_TABLE}`
    WHERE (archived IS NOT TRUE OR archived IS NULL)
      AND CAST(properties_hs_parent_company_id AS STRING) = @corp_id
      AND properties_name IS NOT NULL
      AND TRIM(properties_name) != ''
    ORDER BY properties_name
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("corp_id", "STRING", hubspot_corp_id),
    ])

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        df = df.replace(["nan", "None", "<NA>"], "")
        logger.info(f"Found {len(df)} child facilities in HubSpot")
        return df
    except Exception as e:
        logger.error(f"Error loading child facilities: {e}")
        return pd.DataFrame()


def find_facilities_missing_leadership(
    client: bigquery.Client,
    child_facilities: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each child facility, check whether an Administrator or Executive Director
    contact is associated with it in HubSpot.

    Returns only the facilities that are *missing* that leadership contact.
    """
    if child_facilities.empty:
        return pd.DataFrame()

    facility_ids = child_facilities["hubspot_id"].dropna().unique().tolist()
    facility_ids = [fid for fid in facility_ids if fid]

    if not facility_ids:
        return pd.DataFrame()

    logger.info(f"Checking {len(facility_ids)} facilities for existing ED/Administrator contacts...")

    # Query HubSpot contacts associated with these facilities that hold target titles
    contact_query = f"""
    SELECT DISTINCT
        CAST(properties_associatedcompanyid AS STRING) AS associated_company_id,
        properties_firstname  AS first_name,
        properties_lastname   AS last_name,
        properties_email      AS email,
        LOWER(TRIM(properties_jobtitle)) AS job_title
    FROM `{HUBSPOT_CONTACTS_TABLE}`
    WHERE (archived IS NOT TRUE OR archived IS NULL)
      AND CAST(properties_associatedcompanyid AS STRING) IN UNNEST(@facility_ids)
      AND (
          LOWER(properties_jobtitle) LIKE '%executive director%'
          OR LOWER(properties_jobtitle) LIKE '%administrator%'
          OR REGEXP_CONTAINS(LOWER(TRIM(properties_jobtitle)), '^admin$|^admin[/,]|\\\\sadmin$|\\\\sadmin[/,]')
          OR LOWER(properties_jobtitle) LIKE '%director of nursing%'
          OR REGEXP_CONTAINS(LOWER(TRIM(properties_jobtitle)), '^don$|^don[/,]|\\\\sdon$|\\\\sdon[/,]')
      )
    """

    contact_job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("facility_ids", "STRING", facility_ids),
    ])
    try:
        contacts_df = client.query(contact_query, job_config=contact_job_config).to_dataframe()
        contacts_df = contacts_df.replace(["nan", "None", "<NA>"], "")
        logger.info(f"Found {len(contacts_df)} existing ED/Administrator contacts across facilities")
    except Exception as e:
        logger.warning(f"HubSpot contacts query failed: {e}")
        contacts_df = pd.DataFrame(columns=["associated_company_id"])

    # Facilities that already have at least one matching contact
    covered_ids = set(contacts_df["associated_company_id"].unique()) if not contacts_df.empty else set()

    missing = child_facilities[~child_facilities["hubspot_id"].isin(covered_ids)].copy()
    logger.info(f"{len(missing)} facilities missing an ED/Administrator contact")
    return missing.reset_index(drop=True)


# ─── State association BigQuery table lookup ──────────────────────────────────

# Priority-ordered column name candidates for each contact field.
# The table schema may vary; we try these in order and take the first non-empty value.
_SA_NAME_COLS  = [
    "administrator_name", "admin_name", "executive_director_name", "ed_name",
    "contact_name", "director_name", "full_name",
]
_SA_TITLE_COLS = [
    "administrator_title", "contact_title", "title", "job_title",
]
_SA_PHONE_COLS = [
    "administrator_phone", "admin_phone", "executive_director_phone", "ed_phone",
    "contact_phone", "phone", "telephone",
]
_SA_EMAIL_COLS = [
    "administrator_email", "admin_email", "executive_director_email", "ed_email",
    "contact_email", "email",
]
_SA_URL_COLS = [
    "source_url", "url", "directory_url", "facility_url", "profile_url",
]


def _map_state_assoc_row(row: dict) -> Optional[dict]:
    """
    Map a raw BigQuery row from the state association table to a standard contact dict.
    Uses priority-ordered column name lists to handle varying schemas.
    Returns None if no usable name is found.
    """
    row_lc = {k.lower(): v for k, v in row.items()}

    def _get(cols: list) -> str:
        for col in cols:
            val = row_lc.get(col)
            if val and str(val).strip() not in ("", "None", "nan"):
                return str(val).strip()
        return ""

    found_name  = _get(_SA_NAME_COLS)
    found_title = _get(_SA_TITLE_COLS)
    found_phone = _get(_SA_PHONE_COLS)
    found_email = _get(_SA_EMAIL_COLS)
    source_url  = _get(_SA_URL_COLS)

    if not found_name:
        return None

    return {
        "found_name":          found_name,
        "found_title":         found_title or "Administrator",
        "found_phone":         normalize_phone(found_phone) if found_phone else "",
        "found_email":         found_email,
        # Store source_url in research_findings so _extract_urls() picks it up
        # for the note's "Sources cited" / "How to Verify" section.
        "research_findings":   source_url,
        "research_confidence": "high",
        "source_tier":         "1",
        "source_path":         "state_association_bq_table",
    }


def query_state_association_bq_table(
    client: bigquery.Client,
    facility_name: str,
    city: str,
    state: str,
) -> Optional[dict]:
    """
    Look up contact data for a facility in the
    state_associations.state_association_snf_al_il_information BigQuery table.

    Tries an exact facility-name + state match first, then a LIKE match
    (table facility_name contains the search term) as a fallback.

    Returns a contact dict with keys:
        found_name, found_title, found_phone, found_email,
        research_confidence ("high"), source_tier ("1"),
        source_path ("state_association_bq_table")
    or None if no match is found or the query fails.
    """
    if not facility_name or not state:
        return None

    name_clean  = facility_name.strip()
    state_clean = state.strip().upper()

    strategies = [
        # 1. Exact facility_name + state
        (
            f"""
            SELECT *
            FROM `{STATE_ASSOC_TABLE}`
            WHERE LOWER(TRIM(facility_name)) = LOWER(@name)
              AND UPPER(TRIM(state)) = @state
            LIMIT 5
            """,
            [
                bigquery.ScalarQueryParameter("name",  "STRING", name_clean),
                bigquery.ScalarQueryParameter("state", "STRING", state_clean),
            ],
        ),
        # 2. Facility name contains the search term + state
        (
            f"""
            SELECT *
            FROM `{STATE_ASSOC_TABLE}`
            WHERE LOWER(facility_name) LIKE CONCAT('%', LOWER(@name), '%')
              AND UPPER(TRIM(state)) = @state
            LIMIT 5
            """,
            [
                bigquery.ScalarQueryParameter("name",  "STRING", name_clean),
                bigquery.ScalarQueryParameter("state", "STRING", state_clean),
            ],
        ),
    ]

    for sql, params in strategies:
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            rows = list(client.query(sql, job_config=job_config).result())
            if not rows:
                continue
            contact = _map_state_assoc_row(dict(rows[0]))
            if contact:
                return contact
        except Exception as exc:
            logger.warning("State association BQ table query failed for '%s' (%s): %s",
                           facility_name, state, exc)
            return None

    return None


def verify_contact_in_definitive(
    client: bigquery.Client,
    first_name: str,
    last_name: str,
    definitive_hospital_id: str,
) -> dict:
    """
    Check whether a contact is still listed in Definitive Healthcare for a
    specific facility (HOSPITAL_ID).

    Queries the Executives table first, then the Full Feed as a fallback.

    Returns a dict:
      confirmed (bool)        — True if found by name at this HOSPITAL_ID
      dh_name (str)           — Full name as stored in DH
      dh_title (str)          — Title in DH
      dh_email (str)          — Email in DH (may be empty)
      dh_last_update (str)    — LAST_UPDATE as ISO date string
      source_table (str)      — "executives" | "full_feed" | ""
      error (str)             — non-empty string if lookup was skipped or failed
    """
    _empty = {"confirmed": False, "dh_name": "", "dh_title": "", "dh_email": "",
              "dh_last_update": "", "source_table": ""}

    if not first_name or not last_name:
        return {**_empty, "error": "missing_name"}
    if not definitive_hospital_id or not str(definitive_hospital_id).strip().isdigit():
        return {**_empty, "error": "no_dh_id"}

    hospital_id_int = int(str(definitive_hospital_id).strip())

    for table_key in ("executives", "full_feed"):
        table = DH_TABLES[table_key]
        query = f"""
        SELECT
            COALESCE(NULLIF(TRIM(EXECUTIVE_NAME), ''),
                     CONCAT(TRIM(FIRST_NAME), ' ', TRIM(LAST_NAME))) AS dh_name,
            LOWER(TRIM(TITLE))              AS dh_title,
            TRIM(EMAIL)                     AS dh_email,
            CAST(LAST_UPDATE AS STRING)     AS dh_last_update
        FROM `{table}`
        WHERE CAST(HOSPITAL_ID AS INT64) = @hospital_id
          AND LOWER(TRIM(FIRST_NAME)) = LOWER(@first_name)
          AND LOWER(TRIM(LAST_NAME))  = LOWER(@last_name)
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("hospital_id", "INT64", hospital_id_int),
            bigquery.ScalarQueryParameter("first_name",  "STRING", first_name.strip()),
            bigquery.ScalarQueryParameter("last_name",   "STRING", last_name.strip()),
        ])
        try:
            rows = list(client.query(query, job_config=job_config).result())
            if rows:
                r = dict(rows[0])
                return {
                    "confirmed":      True,
                    "dh_name":        str(r.get("dh_name") or ""),
                    "dh_title":       str(r.get("dh_title") or ""),
                    "dh_email":       str(r.get("dh_email") or ""),
                    "dh_last_update": str(r.get("dh_last_update") or "")[:10],
                    "source_table":   table_key,
                    "error":          "",
                }
        except Exception as exc:
            logger.warning(
                "DH verification query (%s) failed for %s %s at HOSPITAL_ID=%s: %s",
                table_key, first_name, last_name, hospital_id_int, exc,
            )
            return {**_empty, "error": "query_failed"}

    return {**_empty, "error": ""}   # queried successfully, not found


# ─── Concurrent worker functions ─────────────────────────────────────────────

def _research_one_contact(
    row: dict,
    corporation_name: str,
    network_id: int,
    facility_type: str,
    corporate_domain: str = "",
) -> dict:
    """
    Worker for workflow_2.  Runs in a thread pool — one unassociated contact per call.

    Fast-track bypass (no Sonar Pro call):
      - C-suite titles (CEO, COO, CFO, CNO, CCO, CAO, President, Owner): DH is the
        authoritative source for these roles.  State association directories and CMS
        Care Compare never list corporate C-suite executives, so running Sonar Pro
        against those sources is wasteful and produces false negatives.
      - Any title whose DH LAST_UPDATE is within _DH_FRESHNESS_MONTHS months: the
        record is considered current and is written directly to HubSpot.

    Sonar Pro still runs for VP / Regional Director / Regional VP contacts whose DH
    record is stale (older than _DH_FRESHNESS_MONTHS months), because turnover is
    higher in those roles and a web search can surface a more current name.

    When *corporate_domain* is provided, stale VP/Regional records are researched
    using a domain-targeted prompt (_build_corporate_contact_research_prompt) that
    focuses on the corporation's own website and LinkedIn, rather than state
    association directories which will never list these roles.  The domain is also
    injected as ``search_domain_filter`` in the OpenRouter payload so Perplexity
    restricts its web crawl to the corporate domain.

    Acquires _W2_SEMAPHORE before calling the API to cap concurrency at
    _W2_MAX_WORKERS, then sleeps W2_RATE_LIMIT_SECONDS after releasing the semaphore
    so the next waiting thread can start immediately.
    """
    contact_name = (
        f"{row.get('FIRST_NAME', '')} {row.get('LAST_NAME', '')}".strip()
        or str(row.get("EXECUTIVE_NAME", "Unknown"))
    )
    title       = str(row.get("title", "")).title()
    state       = str(row.get("state", ""))
    email       = str(row.get("EMAIL") or "")
    # Use only DIRECT_PHONE as the individual's phone number.
    # LOCATION_PHONE is the facility's main switchboard — shared by everyone at
    # that location — so using it as a personal number causes multiple executives
    # to get the same phone written to their contact records.  FullEnrich is more
    # likely to surface an actual direct line for corporate contacts.
    phone       = str(row.get("DIRECT_PHONE") or "")
    linkedin    = str(row.get("LINKEDIN_PROFILE") or "")
    source      = str(row.get("source_table", ""))
    last_update = str(row.get("LAST_UPDATE") or "")

    result: dict = {
        "workflow":            "2 - Unassociated Contact",
        "executive_id":        row.get("EXECUTIVE_ID", ""),
        "contact_name":        contact_name,
        "title":               title,
        "email":               email,
        "phone":               phone,
        "linkedin":            linkedin,
        "found_name":          contact_name,
        "found_title":         title,
        "found_email":         email,
        "found_phone":         phone,
        "found_linkedin":      linkedin,
        "definitive_source":   source,
        "corporation_name":    corporation_name,
        "network_id":          network_id,
        "research_confidence": "high",   # sourced directly from Definitive Healthcare
        "source_path":         "definitive_healthcare",
        "dh_last_update":      last_update if last_update else "",
        "research_findings":   "",
        "research_error":      "",
        "researched_at":       datetime.now().isoformat(),
    }

    # ── Fast-track decision ───────────────────────────────────────────────────
    # C-suite titles are never found on state association directories or CMS Care
    # Compare, so Sonar Pro would always return not_found for them.  Skip it.
    # Any title with a fresh DH record is also trusted directly.
    is_csuite = _is_csuite_title(title)
    is_fresh  = _dh_record_is_fresh(last_update)

    if is_csuite:
        reason = "C-suite title — DH is authoritative; state associations do not list corporate executives"
        logger.info(f"{contact_name} ({title}): DH fast-track — {reason}")
        result["research_findings"] = json.dumps({
            "found_name":  contact_name,
            "found_title": title,
            "found_email": email,
            "found_phone": phone,
            "confidence":  "high",
            "source_url":  "",
            "source_name": "Definitive Healthcare",
            "reasoning":   f"Fast-tracked: {reason}",
        })
        result["research_source_name"] = "Definitive Healthcare"
        result["research_reasoning"] = (
            f"Contact sourced directly from Definitive Healthcare ({source} table). "
            f"{reason}. "
            + (f"DH record last updated: {last_update}." if last_update else "DH update date not available.")
        )
        return result

    if is_fresh:
        reason = f"DH record updated within {_DH_FRESHNESS_MONTHS} months ({last_update})"
        logger.info(f"{contact_name} ({title}): DH fast-track — {reason}")
        result["research_findings"] = json.dumps({
            "found_name":  contact_name,
            "found_title": title,
            "found_email": email,
            "found_phone": phone,
            "confidence":  "high",
            "source_url":  "",
            "source_name": "Definitive Healthcare",
            "reasoning":   f"Fast-tracked: {reason}",
        })
        result["research_source_name"] = "Definitive Healthcare"
        result["research_reasoning"] = (
            f"Contact sourced directly from Definitive Healthcare ({source} table). "
            f"{reason}. High confidence — record is current."
        )
        return result

    # ── Sonar Pro research (stale VP / Regional Director records only) ────────
    # These roles have higher turnover and the DH record is older than
    # _DH_FRESHNESS_MONTHS months, so a live web search is worthwhile.
    try:
        stale_label = f"stale DH record ({last_update})" if last_update else "no DH update date"

        # ── Prompt selection ──────────────────────────────────────────────────
        # When the corporation's website domain is known, use the domain-targeted
        # prompt that focuses on the corporate website and LinkedIn.  This is far
        # more likely to find a VP or Regional Director than state association
        # directories, which never list these roles.
        # Fall back to the state-association prompt only when no domain is available.
        if corporate_domain:
            bare_domain = _extract_bare_domain(corporate_domain)
            prompt = _build_corporate_contact_research_prompt(
                contact_name=contact_name,
                title=title,
                corporation_name=corporation_name,
                corporate_domain=bare_domain,
                facility_type=facility_type,
                linkedin_url=linkedin,
            )
            # Inject search_domain_filter so Perplexity restricts its web crawl
            # to the corporate domain.  When we also have a LinkedIn URL for the
            # specific person, add linkedin.com so the profile check isn't blocked
            # by the domain filter.
            domain_filter = [bare_domain]
            if linkedin:
                domain_filter.append("linkedin.com")
            extra_params: dict = {
                "reasoning_effort":     "high",
                "search_domain_filter": domain_filter,
                "search_recency_filter": "year",
            }
            prompt_label = f"domain-targeted ({bare_domain})" + (" + LinkedIn profile" if linkedin else "")
        else:
            # No corporate domain on record — state association directories never
            # list VPs or Regional Directors, so skip them entirely and go straight
            # to LinkedIn, which is the only viable public source in this case.
            prompt = _build_w2_linkedin_fallback_prompt(
                contact_name=contact_name,
                title=title,
                corporation_name=corporation_name,
                facility_type=facility_type,
                linkedin_url=linkedin,
            )
            extra_params = {"reasoning_effort": "high", "search_recency_filter": "year"}
            prompt_label = "LinkedIn-only fallback (no domain)"

        # Workflow 2 contacts are unassociated (no direct facility URL), so they
        # benefit from the deeper reasoning model.
        structured_format = {"type": "json_schema", "json_schema": _CONTACT_JSON_SCHEMA}

        logger.info(f"{contact_name} ({title}): querying Sonar Reasoning Pro [{stale_label}, {prompt_label}]...")
        with _W2_SEMAPHORE:
            research_result = call_openrouter(
                W2_VERIFY_SYSTEM_PROMPT,
                prompt,
                model=OPENROUTER_MODEL_TIER23,
                response_format=structured_format,
                extra_params=extra_params,
                timeout=W2_TIMEOUT,
            )
        # Sleep AFTER releasing the semaphore so the next waiting thread can
        # start its API call immediately rather than waiting out our rate-limit pause.
        time.sleep(W2_RATE_LIMIT_SECONDS)
        logger.info(f"{contact_name}: research complete ({len(research_result)} chars)")
        result["research_findings"] = research_result
        result["source_path"] = "sonar_pro"

        # Parse the structured response and promote fields to top-level result keys
        # so the downstream _process_found_contact pipeline sees them.
        found_name, found_title, confidence, found_email, found_phone, reasoning, source_name = (
            parse_found_name(research_result)
        )
        if found_name and confidence != "not_found":
            result.update({
                "found_name":          found_name,
                "found_title":         found_title,
                "found_email":         found_email or email,
                "found_phone":         found_phone or phone,
                "found_linkedin":      linkedin,
                "research_confidence": confidence,
                "research_reasoning":  reasoning,
                "research_source_name": source_name,
            })
        else:
            # Sonar found nothing — fall back to the DH record itself so we
            # don't silently discard a contact we know exists.
            result.update({
                "research_confidence": "low",
                "research_reasoning":  (
                    f"Sonar Pro could not verify {contact_name} on a live source "
                    f"(DH record is {stale_label}). Falling back to DH data — treat with caution."
                ),
                "research_source_name": "Definitive Healthcare (unverified — stale record)",
            })
    except RuntimeError as e:
        result["research_error"] = str(e)
        logger.error(f"{contact_name}: research failed — {e}")

    return result


def _research_one_facility(
    row: dict,
    hubspot_corp_id: str,
    corporation_name: str,
    client,  # bigquery.Client — thread-safe
) -> dict:
    """
    Worker for workflow_1.  Runs in a thread pool — one facility per call.

    Same rate-limiting pattern as _research_one_contact.
    """
    facility_name = str(row.get("facility_name", "Unknown Facility"))
    city          = str(row.get("city", ""))
    state         = str(row.get("state", ""))
    facility_type = str(row.get("facility_type", ""))
    hubspot_id    = str(row.get("hubspot_id", ""))
    definitive_id = str(row.get("definitive_id", ""))
    address       = str(row.get("address", "") or "")
    website       = str(row.get("website", "") or "")

    result: dict = {
        "workflow":              "1 - Facility Missing Contact",
        "hubspot_facility_id":   hubspot_id,
        "facility_name":         facility_name,
        "city":                  city,
        "state":                 state,
        "facility_type":         facility_type,
        "definitive_id":         definitive_id,
        "corporation_name":      corporation_name,
        "hubspot_corp_id":       hubspot_corp_id,
        "found_name":            "",
        "found_title":           "",
        "found_email":           "",
        "found_phone":           "",
        "research_confidence":   "not_found",
        "source_tier":           "3",
        "hubspot_contact_ids":   "",
        "hubspot_match_detail":  "",
        "hubspot_match_summary": "",
        "research_findings":     "",
        "research_error":        "",
        "researched_at":         datetime.now().isoformat(),
    }

    # ── State association BigQuery table check (cheapest source — run first) ───
    sa_contact = query_state_association_bq_table(client, facility_name, city, state)
    if sa_contact:
        logger.info(f"{facility_name}: found in state association BQ table — {sa_contact['found_name']}")
        result.update(sa_contact)
        matches, match_strategy = lookup_hubspot_contact_enhanced(
            client=client,
            full_name=sa_contact["found_name"],
            email=sa_contact.get("found_email", ""),
            phone=sa_contact.get("found_phone", ""),
            facility_hubspot_id=hubspot_id,
            parent_hubspot_id=hubspot_corp_id,
        )
        ids, detail, summary = _format_hubspot_matches_enhanced(
            matches, match_strategy=match_strategy, facility_name=facility_name
        )
        result.update({
            "hubspot_contact_ids":   ids,
            "hubspot_match_detail":  detail,
            "hubspot_match_summary": summary,
        })
        return result

    try:
        prompt, source_tier, domain_filter = _build_facility_research_prompt(
            facility_name=facility_name,
            city=city,
            state=state,
            facility_type=facility_type,
            corporation_name=corporation_name,
            address=address,
            website=website,
        )
        result["source_tier"] = source_tier

        # ── Tiered model selection ─────────────────────────────────────────────
        # Tier 1 → sonar-pro + search_domain_filter pinned to the association
        #           site(s) and CMS Care Compare.  Direct URL available; cheaper
        #           model sufficient.
        # Tier 2 → sonar-reasoning-pro + reasoning_effort=medium (login-required
        #           association; fallback sources needed, moderate complexity)
        # Tier 3 → sonar-reasoning-pro + reasoning_effort=high (no directory at
        #           all; needs deep open web search to locate the contact)
        if source_tier == "1":
            model = OPENROUTER_MODEL
            extra: dict = {"search_recency_filter": "year"}
            if domain_filter:
                extra["search_domain_filter"] = domain_filter
            model_label = "Sonar Pro"
        elif source_tier == "2":
            model       = OPENROUTER_MODEL_TIER23
            extra       = {"reasoning_effort": "medium", "search_recency_filter": "year"}
            model_label = "Sonar Reasoning Pro (medium effort)"
        else:
            model       = OPENROUTER_MODEL_TIER23
            extra       = {"reasoning_effort": "high", "search_recency_filter": "year"}
            model_label = "Sonar Reasoning Pro (high effort)"

        # ── Structured JSON output (Rec #1) ──────────────────────────────────────────
        structured_format = {"type": "json_schema", "json_schema": _CONTACT_JSON_SCHEMA}

        logger.info(f"{facility_name}: querying {model_label} (tier {source_tier})...")
        with _W1_SEMAPHORE:
            research_result = call_openrouter(
                RESEARCH_SYSTEM_PROMPT,
                prompt,
                model=model,
                response_format=structured_format,
                extra_params=extra,
            )
        # Sleep AFTER releasing the semaphore so the next waiting thread can
        # start its API call immediately rather than waiting out our rate-limit pause.
        time.sleep(W1_RATE_LIMIT_SECONDS)
        logger.info(f"{facility_name}: research complete ({len(research_result)} chars)")
        result["research_findings"] = (
            research_result[:2000] + " [TRUNCATED]"
            if len(research_result) > 2000
            else research_result
        )

        found_name, found_title, confidence, found_email, found_phone, reasoning, source_name = parse_found_name(
            research_result
        )
        result.update({
            "found_name":          found_name,
            "found_title":         found_title,
            "research_confidence": confidence,
            "found_email":         found_email,
            "found_phone":         found_phone,
            "research_reasoning":  reasoning,
            "research_source_name": source_name,
        })

        if found_name and confidence != "not_found":
            logger.info(f"{facility_name}: found {found_name} ({found_title}, confidence={confidence})")
            matches, match_strategy = lookup_hubspot_contact_enhanced(
                client=client,
                full_name=found_name,
                email=found_email,
                phone=found_phone,
                facility_hubspot_id=hubspot_id,
                parent_hubspot_id=hubspot_corp_id,
            )
            ids, detail, summary = _format_hubspot_matches_enhanced(
                matches, match_strategy=match_strategy, facility_name=facility_name
            )
            result.update({
                "hubspot_contact_ids":   ids,
                "hubspot_match_detail":  detail,
                "hubspot_match_summary": summary,
            })
        else:
            logger.info(f"{facility_name}: no contact found (confidence={confidence})")

    except RuntimeError as e:
        result["research_error"] = str(e)
        logger.error(f"{facility_name}: research failed — {e}")

    return result


def _build_w2_linkedin_fallback_prompt(
    contact_name: str,
    title: str,
    corporation_name: str,
    facility_type: str,
    linkedin_url: str = "",
) -> str:
    """
    Build a LinkedIn-only verification prompt for W2 contacts where no corporate
    domain is available.

    State association directories never list VPs or Regional Directors, so the
    old fallback (_build_contact_research_prompt) was wasting API calls on an
    impossible search.  This prompt restricts Sonar to LinkedIn only — the only
    viable public source when the corporate website domain is unknown.
    """
    type_desc = {
        "SNF":  "skilled nursing facility (SNF) operator",
        "ALF":  "assisted living facility (ALF) operator",
        "BOTH": "skilled nursing and assisted living operator",
    }.get((facility_type or "").upper(), "long-term care operator")

    if linkedin_url:
        step0 = (
            f"0. Go directly to this LinkedIn profile: {linkedin_url}\n"
            f"   Confirm whether their current employer is {corporation_name} and whether "
            f"their title matches or is consistent with \"{title}\".\n"
            f"   If confirmed, return high or medium confidence and stop here.\n"
        )
    else:
        step0 = ""

    return f"""I need you to verify whether {contact_name} is currently the {title} at \
{corporation_name}, a {type_desc}.

No corporate website domain is on record, so search LinkedIn only:
{step0}\
1. LinkedIn: search for "{contact_name}" "{corporation_name}" to find their current profile.
2. LinkedIn: search for "{corporation_name}" "{title}" to find whoever currently holds this role.

For each source you check, briefly note: source name → result (found / not found / inaccessible).

Return a JSON object with these fields:
- found_name:  full name as listed on the source, or empty string if not found
- found_title: exact job title as listed, or empty string
- found_email: publicly listed email address, or empty string
- found_phone: publicly listed phone number, or empty string
- confidence:  "medium" (LinkedIn showing a current role at this company),
               "low" (LinkedIn with unclear tenure or possibly outdated),
               or "not_found" (no verified current role located)
- source_url:  exact URL where the name was found, or empty string
- source_name: plain-English name (e.g. "LinkedIn profile — {corporation_name}")
- reasoning:   1–3 sentences explaining what you found and why you assigned that confidence"""


# ─── Workflow 2 corporate-website discovery ───────────────────────────────────

def _build_w2_discovery_prompt(
    corporation_name: str,
    corporate_domain: str,
    facility_type: str,
    titles: list,
) -> str:
    """
    Build the Sonar Pro prompt for a proactive corporate-website sweep.

    Unlike _build_corporate_contact_research_prompt (which verifies a specific
    named person from DH), this prompt asks Sonar to DISCOVER up to 10 current
    executives from the corporation's own website and LinkedIn, restricted to
    the W2_CORPORATE_TITLES cohort.
    """
    type_desc = {
        "SNF":  "skilled nursing facility (SNF) operator",
        "ALF":  "assisted living facility (ALF) operator",
        "BOTH": "skilled nursing and assisted living operator",
    }.get((facility_type or "").upper(), "long-term care operator")

    domain_display = corporate_domain or corporation_name
    title_list = "\n".join(f"  - {t}" for t in titles)

    return f"""Find up to 10 current corporate executives at {corporation_name}, \
a {type_desc}.

Only return contacts whose titles match one of these target roles — reject any other titles:
{title_list}

Search these sources in order, stopping when you have found up to 10 verified contacts:
1. {domain_display} — check pages titled 'About Us', 'Leadership', 'Our Team', \
'Management', 'Corporate Governance', or 'Investor Relations'.
   Common URL patterns to try: /about-us, /about/leadership, /leadership-team, \
/our-team, /management, /corporate-governance, /investor-relations/management.
2. LinkedIn — search for people currently employed at "{corporation_name}" in the \
target roles. Example: site:linkedin.com/in "{corporation_name}" "Chief Executive Officer".

Return a JSON object with a "contacts" array (up to 10 items).
If no verified contacts are found at all, return {{"contacts": []}}.
Do NOT include anyone whose current title is outside the target role list above."""


def search_facility_website_for_leadership(
    facility_name: str,
    domain: str,
    city: str,
    state: str,
    facility_type: str,
    corporation_name: str = "",
    titles: list[str] | None = None,
) -> list[dict]:
    """
    Sonar Reasoning Pro sweep of a facility's own website + LinkedIn to discover
    current leadership contacts (Administrator, Executive Director, Director of Nursing).

    Analogous to search_corporation_website_for_executives for W2.
    Uses _CONTACT_LIST_JSON_SCHEMA so up to 5 contacts can be returned per call.
    search_domain_filter pins the crawl to the facility domain + linkedin.com.

    Returns a list of result dicts compatible with the W1 pipeline format, with
    found_linkedin populated so FullEnrich Bulk Enrich can use it downstream.
    Returns [] when no domain is available or no verified contacts are found.
    Never raises.

    *titles* overrides the default FACILITY_LEADERSHIP_TITLES from config.py,
    enabling ICP-driven customisation without changing shared config.
    """
    from config import FACILITY_LEADERSHIP_TITLES

    if not domain:
        logger.info("W1 Sonar facility sweep: no domain on record — skipping")
        return []

    bare_domain = _extract_bare_domain(domain)
    if not bare_domain:
        logger.info("W1 Sonar facility sweep: could not derive bare domain — skipping")
        return []

    target_titles = titles if titles is not None else FACILITY_LEADERSHIP_TITLES
    title_list = "\n".join(f"  - {t}" for t in target_titles)
    location = f"{city}, {state}" if city and state else (state or "")

    prompt = (
        f"Find up to 5 current leadership staff at {facility_name}"
        + (f", {location}" if location else "")
        + f".\n\n"
        f"Only return contacts whose titles match one of these roles — reject any other titles:\n"
        f"{title_list}\n\n"
        f"Search these sources in order:\n"
        f"1. {bare_domain} — check pages titled 'About Us', 'Our Team', 'Staff', "
        f"'Leadership', or 'Contact Us'.\n"
        f"2. LinkedIn — search for people currently at \"{facility_name}\" in the target roles. "
        f"Example: site:linkedin.com/in \"{facility_name}\" administrator\n\n"
        f"If no verified contacts are found, return {{\"contacts\": []}}.\n"
        f"Do NOT include anyone whose current title is outside the target role list above."
    )

    # Title filter applied to Sonar results — accept only healthcare leadership roles.
    _FACILITY_TITLE_RE = re.compile(
        r"\b(administrator|executive director|director of nursing)\b", re.IGNORECASE
    )

    extra_params: dict = {
        "reasoning_effort":      "medium",
        "search_domain_filter":  [bare_domain, "linkedin.com"],
        "search_recency_filter": "year",
    }
    structured_format = {"type": "json_schema", "json_schema": _CONTACT_LIST_JSON_SCHEMA}

    logger.info(
        f"W1 Sonar facility sweep: querying Sonar Reasoning Pro for {facility_name} ({bare_domain})..."
    )
    try:
        with _W1_SEMAPHORE:
            raw = call_openrouter(
                W1_DISCOVERY_SYSTEM_PROMPT,
                prompt,
                model=OPENROUTER_MODEL_TIER23,
                response_format=structured_format,
                extra_params=extra_params,
                timeout=W2_TIMEOUT,
            )
        time.sleep(W1_RATE_LIMIT_SECONDS)
        data = json.loads(raw)
        contacts = data.get("contacts", [])
    except (RuntimeError, json.JSONDecodeError, AttributeError, TypeError) as exc:
        logger.warning(f"W1 Sonar facility sweep: API/parse error ({exc}) — skipping")
        return []

    results = []
    for c in contacts[:5]:
        full_name = (c.get("full_name") or "").strip()
        title     = (c.get("title") or "").strip()
        if not full_name or not title:
            continue
        if not _FACILITY_TITLE_RE.search(title):
            logger.info(
                f"W1 Sonar facility sweep: dropping '{full_name}' — title '{title}' not in target list"
            )
            continue
        results.append({
            "workflow":             "1 - Facility Missing Contact",
            "found_name":           full_name,
            "found_title":          title,
            "found_email":          c.get("email", ""),
            "found_phone":          c.get("phone", ""),
            "found_linkedin":       c.get("linkedin_url", ""),
            "research_confidence":  c.get("confidence", "low"),
            "research_reasoning":   c.get("reasoning", ""),
            "research_source_name": c.get("source_name", ""),
            "research_findings":    c.get("source_url", ""),
            "source_path":          "sonar_pro_facility_sweep",
            "source_tier":          "",
            "hubspot_contact_ids":  "",
            "research_error":       "",
        })

    logger.info(
        f"W1 Sonar facility sweep: {len(results)} contact(s) accepted at {facility_name}"
    )
    return results


def search_corporation_website_for_executives(
    corporation_name: str,
    corporate_domain: str,
    facility_type: str,
    known_names: set,
) -> list:
    """
    Call Sonar Reasoning Pro to discover corporate executives directly from the
    corporation's own website and LinkedIn.

    Runs for every Workflow 2 enrichment — additive to DH results.  Results are
    filtered against DH_W2_TITLE_PATTERN so only the target cohort is returned,
    and deduplicated against *known_names* (a set of lowercase names already
    found by the DH query).

    Returns a list of result dicts compatible with the W2 pipeline format.
    *known_names* is mutated in-place: each accepted name is added so downstream
    callers (FullEnrich supplement) can continue deduplication without re-reading
    the returned list.
    """
    from config import W2_CORPORATE_TITLES, DH_W2_TITLE_PATTERN

    if not corporate_domain:
        logger.info("W2 Sonar discovery: no corporate domain on record — skipping")
        return []

    bare_domain = _extract_bare_domain(corporate_domain)
    if not bare_domain:
        logger.info("W2 Sonar discovery: could not derive bare domain — skipping")
        return []

    title_re = re.compile(DH_W2_TITLE_PATTERN, re.IGNORECASE)

    prompt = _build_w2_discovery_prompt(
        corporation_name=corporation_name,
        corporate_domain=bare_domain,
        facility_type=facility_type,
        titles=W2_CORPORATE_TITLES,
    )
    extra_params: dict = {
        # Discovery is already well-targeted (domain-filtered, specific title list),
        # so medium effort is sufficient and faster/cheaper than high.
        "reasoning_effort":     "medium",
        "search_domain_filter": [bare_domain, "linkedin.com"],
        "search_recency_filter": "year",
    }
    structured_format = {"type": "json_schema", "json_schema": _CONTACT_LIST_JSON_SCHEMA}

    logger.info(f"W2 Sonar discovery: querying Sonar Reasoning Pro for executives at {bare_domain}...")
    try:
        with _W2_SEMAPHORE:
            raw = call_openrouter(
                W2_DISCOVERY_SYSTEM_PROMPT,
                prompt,
                model=OPENROUTER_MODEL_TIER23,
                response_format=structured_format,
                extra_params=extra_params,
                timeout=W2_TIMEOUT,
            )
        time.sleep(W2_RATE_LIMIT_SECONDS)
        data = json.loads(raw)
        contacts = data.get("contacts", [])
    except (RuntimeError, json.JSONDecodeError, AttributeError, TypeError) as exc:
        logger.warning(f"W2 Sonar discovery: API/parse error ({exc}) — skipping")
        return []

    results = []
    for c in contacts[:10]:  # hard cap at 10
        full_name = (c.get("full_name") or "").strip()
        title     = (c.get("title") or "").strip()

        if not full_name or not title:
            continue

        # Reject titles outside the target cohort
        if not title_re.search(title):
            logger.info(f"W2 Sonar discovery: dropping '{full_name}' — title '{title}' not in target list")
            continue

        # Deduplicate against names already found by DH (and previously accepted discoveries)
        name_key = full_name.lower()
        if name_key in known_names:
            continue

        confidence = c.get("confidence", "low")
        results.append({
            "workflow":             "2 - Unassociated Contact",
            "contact_name":         full_name,
            "title":                title,
            "email":                c.get("email", ""),
            "phone":                c.get("phone", ""),
            "linkedin":             c.get("linkedin_url", ""),
            "found_name":           full_name,
            "found_title":          title,
            "found_email":          c.get("email", ""),
            "found_phone":          c.get("phone", ""),
            "found_linkedin":       c.get("linkedin_url", ""),
            "research_findings":    c.get("source_url", ""),
            "research_confidence":  confidence,
            "research_reasoning":   c.get("reasoning", ""),
            "research_source_name": c.get("source_name", ""),
            "source_path":          "sonar_pro_discovery",
            "source_tier":          "",
            "hubspot_contact_ids":  "",
            "research_error":       "",
        })
        known_names.add(name_key)

    logger.info(f"W2 Sonar discovery: {len(results)} new contact(s) accepted after title filter")
    return results


# ─── Workflow runners ─────────────────────────────────────────────────────────

def workflow_2_research_contacts(
    client: bigquery.Client,
    network_id: int,
    corporation_name: str,
    facility_type: str,
    corporate_domain: str = "",
) -> list[dict]:
    """
    Workflow 2: Find corporate-level ED/Administrator contacts in Definitive that
    are not assigned to a specific facility, then research them on the web.

    *corporate_domain* (e.g. "brookdale.com") is passed to _research_one_contact
    so that stale VP / Regional Director records can be verified against the
    corporation's own website rather than state association directories.
    """
    logger.info("WORKFLOW 2 — Researching Unassociated Definitive Contacts")

    contacts_df = load_corporate_level_contacts(client, network_id)
    if contacts_df.empty:
        logger.info("No contacts to research for Workflow 2.")
        return []

    rows  = contacts_df.to_dict("records")
    total = len(rows)
    logger.info(f"Researching {total} contact(s) ({_W2_MAX_WORKERS} concurrent)...")

    ordered: list[dict] = [{}] * total
    with ThreadPoolExecutor(max_workers=_W2_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _research_one_contact, row, corporation_name, network_id,
                facility_type, corporate_domain,
            ): i
            for i, row in enumerate(rows)
        }
        done = 0
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
            done += 1
            logger.info(f"[{done}/{total}] contacts researched")

    return ordered


def workflow_1_research_facilities(
    client: bigquery.Client,
    hubspot_corp_id: str,
    corporation_name: str,
) -> list[dict]:
    """
    Workflow 1: Find HubSpot child facilities missing ED/Administrator contacts,
    research each on state association sites, then check if the found person
    already exists as a HubSpot contact (returning their record ID if so).
    """
    logger.info("WORKFLOW 1 — Researching Facilities Missing Leadership Contacts")

    child_facilities = load_hubspot_child_facilities(client, hubspot_corp_id)
    if child_facilities.empty:
        logger.info("No child facilities found in HubSpot for Workflow 1.")
        return []

    missing = find_facilities_missing_leadership(client, child_facilities)
    if missing.empty:
        logger.info("All child facilities already have an ED/Administrator contact in HubSpot.")
        return []

    rows  = missing.to_dict("records")
    total = len(rows)
    logger.info(f"Researching {total} facilit(y/ies) ({_W1_MAX_WORKERS} concurrent)...")

    ordered: list[dict] = [{}] * total
    with ThreadPoolExecutor(max_workers=_W1_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _research_one_facility, row, hubspot_corp_id, corporation_name, client
            ): i
            for i, row in enumerate(rows)
        }
        done = 0
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
            done += 1
            logger.info(f"[{done}/{total}] facilities researched")

    return ordered


def save_csv(rows: list[dict], filename: str) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        logger.info(f"No rows to save for {filename}")
        return

    fieldnames = list(rows[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved {len(rows)} row(s) → {filename}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def _prompt_user() -> tuple[str, int]:
    """Interactively prompt the user for HubSpot corp ID and Definitive NETWORK_ID."""
    print("\n" + "=" * 70)
    print("  STATE ASSOCIATION HUBSPOT DEFINITIVE MATCHING")
    print("=" * 70)
    print("\nThis tool researches executive directors and administrators for a")
    print("healthcare corporation using state association websites.\n")

    hubspot_corp_id = input("Enter the HubSpot Corporation ID: ").strip()
    if not hubspot_corp_id:
        print("❌ HubSpot Corporation ID cannot be empty.")
        sys.exit(1)

    raw_network_id = input("Enter the Definitive Healthcare NETWORK_ID: ").strip()
    try:
        network_id = int(raw_network_id)
    except ValueError:
        print(f"❌ Invalid NETWORK_ID '{raw_network_id}' — must be an integer.")
        sys.exit(1)

    return hubspot_corp_id, network_id


def _lookup_corporation_name(client: bigquery.Client, hubspot_corp_id: str) -> tuple[str, str]:
    """
    Return (corporation_name, facility_type) for the given HubSpot company ID.
    Falls back to ('Unknown Corporation', '') if not found.
    """
    query = f"""
    SELECT
        properties_name         AS company_name,
        properties_facility_type AS facility_type
    FROM `{HUBSPOT_COMPANIES_TABLE}`
    WHERE CAST(properties_hs_object_id AS STRING) = @corp_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("corp_id", "STRING", hubspot_corp_id),
    ])
    try:
        rows = list(client.query(query, job_config=job_config).result())
        if rows:
            return str(rows[0].company_name or "Unknown Corporation"), str(rows[0].facility_type or "")
    except Exception as e:
        logger.warning(f"Could not look up corporation name: {e}")
    return "Unknown Corporation", ""


def main() -> None:
    if not BIGQUERY_AVAILABLE:
        print("❌ BigQuery is not available. Exiting.")
        sys.exit(1)

    if not OPENROUTER_API_KEY:
        print("\n⚠️  WARNING: OPENROUTER_API_KEY is not set.")
        print("   Data will be gathered from BigQuery, but AI research calls will fail.")
        print("   Set OPENROUTER_API_KEY before running to enable state-association research.\n")

    # ── User input ────────────────────────────────────────────────────────────
    hubspot_corp_id, network_id = _prompt_user()

    # ── BigQuery client ───────────────────────────────────────────────────────
    print("\n🔌 Connecting to BigQuery...")
    try:
        client, auth_method = init_bigquery_client(
            project_id=BQ_PROJECT_ID,
            location=BQ_LOCATION,
        )
        print(f"  ✓ Connected ({auth_method})")
    except Exception as e:
        print(f"\n❌ BigQuery connection failed: {e}")
        sys.exit(1)

    # ── Look up corporation context ───────────────────────────────────────────
    print(f"\n🏢 Looking up HubSpot corporation ID={hubspot_corp_id}...")
    corporation_name, facility_type = _lookup_corporation_name(client, hubspot_corp_id)
    print(f"  Corporation: {corporation_name}")
    print(f"  Facility type: {facility_type or '(not set)'}")
    print(f"  Definitive NETWORK_ID: {network_id}")

    # ── Workflow 1: Facilities missing leadership (+ HubSpot contact lookup) ──
    w1_results = workflow_1_research_facilities(
        client=client,
        hubspot_corp_id=hubspot_corp_id,
        corporation_name=corporation_name,
    )
    save_csv(w1_results, OUTPUT_FACILITIES)

    # ── Workflow 2: Unassociated Definitive contacts ───────────────────────────
    w2_results = workflow_2_research_contacts(
        client=client,
        network_id=network_id,
        corporation_name=corporation_name,
        facility_type=facility_type,
    )
    save_csv(w2_results, OUTPUT_CONTACTS)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  ✅ COMPLETE")
    print("=" * 70)
    print(f"  Workflow 1 facilities researched : {len(w1_results)}")
    print(f"  Workflow 2 contacts researched   : {len(w2_results)}")
    if w1_results:
        print(f"  Facility results → {OUTPUT_FACILITIES}")
    if w2_results:
        print(f"  Contact results  → {OUTPUT_CONTACTS}")
    print()


if __name__ == "__main__":
    main()
