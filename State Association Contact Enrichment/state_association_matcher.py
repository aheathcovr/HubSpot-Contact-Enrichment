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

import os
import re
import sys
import time
import csv
from datetime import datetime
from typing import Optional

import requests
import pandas as pd

# ─── PyYAML (for state association guide parsing) ─────────────────────────────
try:
    import yaml
except ImportError:
    print("✗ ERROR: PyYAML not available.")
    print("   Install with: pip install pyyaml")
    sys.exit(1)

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
    print("✗ ERROR: BigQuery dependencies not available.")
    print("   Install with: pip install google-cloud-bigquery")
    sys.exit(1)

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

OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL    = os.getenv("OPENROUTER_MODEL", "perplexity/sonar-pro")

# Titles that identify facility-level leadership we care about
TARGET_TITLES = {"executive director", "administrator"}

RATE_LIMIT_SECONDS = 2.5  # Pause between OpenRouter calls to avoid rate-limiting

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


# ─── OpenRouter helpers ───────────────────────────────────────────────────────

def call_openrouter(prompt: str, model: str = OPENROUTER_MODEL) -> str:
    """
    Call the OpenRouter chat-completions endpoint and return the response text.

    Raises:
        RuntimeError: if OPENROUTER_API_KEY is not set or the request fails.
    """
    if not OPENROUTER_API_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. "
            "Export it as an environment variable before running this script."
        )

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type":  "application/json",
        "HTTP-Referer":  "https://github.com/HubSpot-Definitive-Matching",
        "X-Title":       "State Association Matcher",
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        response = requests.post(
            f"{OPENROUTER_BASE_URL}/chat/completions",
            headers=headers,
            json=payload,
            timeout=90,
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()
    except requests.HTTPError as e:
        raise RuntimeError(f"OpenRouter HTTP error: {e.response.status_code} — {e.response.text}") from e
    except Exception as e:
        raise RuntimeError(f"OpenRouter call failed: {e}") from e


def parse_found_name(research_text: str) -> tuple[str, str, str, str, str]:
    """
    Extract the structured JSON block from an OpenRouter facility research response.

    Returns (found_name, found_title, confidence, found_email, found_phone).
    Returns ("", "", "not_found", "", "") if no JSON block is present or parsing fails.
    """
    import re
    import json

    # Look for the last ```json ... ``` block in the response
    matches = re.findall(r"```json\s*(\{.*?\})\s*```", research_text, re.DOTALL)
    if not matches:
        # Try bare JSON object as fallback
        matches = re.findall(r'\{"found_name".*?\}', research_text, re.DOTALL)

    for raw in reversed(matches):  # use last match (most likely the structured block)
        try:
            data = json.loads(raw)
            name  = str(data.get("found_name",  "") or "").strip()
            title = str(data.get("found_title", "") or "").strip()
            conf  = str(data.get("confidence",  "not_found") or "not_found").strip().lower()
            email = str(data.get("found_email", "") or "").strip()
            phone = str(data.get("found_phone", "") or "").strip()
            return name, title, conf, email, phone
        except (json.JSONDecodeError, AttributeError):
            continue

    return "", "", "not_found", "", ""


def normalize_phone(phone: str) -> str:
    """
    Normalize a phone number for comparison by removing all non-digit characters.
    Returns empty string if no digits found.
    """
    if not phone:
        return ""
    digits = "".join(c for c in str(phone) if c.isdigit())
    return digits if len(digits) >= 10 else ""


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

    first_name = parts[0].replace("'", "\\'") if parts else ""
    last_name = " ".join(parts[1:]).replace("'", "\\'") if len(parts) > 1 else ""
    normalized_phone = normalize_phone(phone)

    all_matches: list[dict] = []
    strategy_used = "not_found"

    # ── Strategy 1: Email lookup (highest confidence) ───────────────────────────
    if email and "@" in email:
        email_clean = email.strip().lower().replace("'", "\\'")
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
          AND LOWER(TRIM(properties_email)) = LOWER('{email_clean}')
        LIMIT 5
        """
        try:
            rows = client.query(query).to_dataframe()
            rows = rows.replace(["nan", "None", "<NA>"], "")
            for row in rows.to_dict("records"):
                row["match_type"] = "email_match"
                all_matches.append(row)
            if all_matches:
                return all_matches, "email_exact"
        except Exception as e:
            print(f"    ⚠ HubSpot email lookup failed: {e}")

    # ── Strategy 2: Phone lookup ────────────────────────────────────────────────
    if normalized_phone and len(normalized_phone) >= 10:
        # Search both phone and mobile_phone fields
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
              REGEXP_REPLACE(properties_phone, r'[^0-9]', '') LIKE '%{normalized_phone[-10:]}%'
              OR REGEXP_REPLACE(properties_mobilephone, r'[^0-9]', '') LIKE '%{normalized_phone[-10:]}%'
          )
        LIMIT 10
        """
        try:
            rows = client.query(query).to_dataframe()
            rows = rows.replace(["nan", "None", "<NA>"], "")
            phone_matches = []
            for row in rows.to_dict("records"):
                row["match_type"] = "phone_match"
                phone_matches.append(row)
            if phone_matches:
                return phone_matches, "phone_exact"
        except Exception as e:
            print(f"    ⚠ HubSpot phone lookup failed: {e}")

    # ── Strategy 3-5: Name lookup with company association priority ─────────────
    if first_name and last_name:
        # Build base name query
        name_query = f"""
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
          AND LOWER(TRIM(properties_firstname)) = LOWER('{first_name}')
          AND LOWER(TRIM(properties_lastname))  = LOWER('{last_name}')
        """

        try:
            rows = client.query(name_query).to_dataframe()
            rows = rows.replace(["nan", "None", "<NA>"], "")
            name_matches = rows.to_dict("records")

            if not name_matches:
                return [], "name_not_found"

            # Priority 3: Match at the specific facility
            if facility_hubspot_id:
                facility_matches = [
                    m for m in name_matches
                    if m.get("associated_company_id") == facility_hubspot_id
                ]
                if facility_matches:
                    for m in facility_matches:
                        m["match_type"] = "name_facility_match"
                    return facility_matches, "name_facility"

            # Priority 4: Match at the parent company
            if parent_hubspot_id:
                parent_matches = [
                    m for m in name_matches
                    if m.get("associated_company_id") == parent_hubspot_id
                ]
                if parent_matches:
                    for m in parent_matches:
                        m["match_type"] = "name_parent_match"
                    return parent_matches, "name_parent"

            # Priority 5: Any name match (may be at other companies)
            for m in name_matches:
                m["match_type"] = "name_any_match"
            return name_matches, "name_any"

        except Exception as e:
            print(f"    ⚠ HubSpot name lookup failed for '{full_name}': {e}")
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
                print(f"  ⚠ Guide YAML parse warning (block skipped): {e}")

    except FileNotFoundError:
        print(f"  ⚠ State association guide not found at {guide_path} — prompts will use generic sources")
    except Exception as e:
        print(f"  ⚠ Failed to load state association guide: {e} — prompts will use generic sources")

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
) -> str:
    """
    Build the 'Please search the following sources:' bullet text for a research prompt.

    Tier 1: lead with specific verified association URLs + access notes.
    Tier 2: note login barrier, fall through to CMS + state DOH fallbacks.
    Tier 3: skip association search, lead directly with CMS + state DOH fallbacks.
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
        if facility_name:
            lines.append(
                f"- The facility's own website"
                f" — search \"{facility_name} {city} {state_label} administrator\""
            )
        lines.append(
            f"- LinkedIn — search \"{facility_name} administrator\" or \"{facility_name} executive director\""
            if facility_name else
            "- LinkedIn — search facility name + administrator"
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
        if facility_name:
            lines.append(
                f"- The facility's own website"
                f" — search \"{facility_name} {city} {state_label} administrator\""
            )
        lines.append(
            f"- LinkedIn — search \"{facility_name} administrator\" or \"{facility_name} executive director\""
            if facility_name else
            "- LinkedIn — search facility name + administrator"
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
        if facility_name:
            lines.append(
                f"- The facility's own website"
                f" — search \"{facility_name} {city} {state_label} administrator\""
            )
        lines.append(
            f"- LinkedIn — search \"{facility_name} administrator\" or \"{facility_name} executive director\""
            if facility_name else
            "- LinkedIn — search facility name + administrator"
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

    return f"""You are a healthcare market research assistant. I need you to search publicly
available state association websites and directories to find information about the following
person:

Contact Name: {contact_name}
Known Title:  {title}
Corporation:  {corporation_name}
Location:     {state_context}

Please search the following types of sources:
{assoc_block}
- CMS Care Compare / Nursing Home Compare listings for {corporation_name} facilities
  (https://www.medicare.gov/care-compare/)
- LinkedIn or professional directories for "{contact_name}" + "{title}"

Report back:
1. Whether you found this person listed on any state association or regulatory website
2. The specific facility or facilities they are listed at (name, address, city, state)
3. Their contact information if publicly listed (phone, email)
4. The source URL where you found this information
5. Any notes about their current role or facility affiliation

If you cannot find this person, say so explicitly and note which sources you checked."""


def _build_facility_research_prompt(
    facility_name: str,
    city: str,
    state: str,
    facility_type: str,
    corporation_name: str,
) -> tuple[str, str]:
    """
    Build the OpenRouter prompt for researching a facility missing leadership.

    Returns (prompt_text, source_tier) where source_tier is "1", "2", or "3":
      "1" = specific association URL(s) injected from the guide
      "2" = login-required association noted; fallbacks used
      "3" = no public association contacts; fallbacks only
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
        state=state, facility_name=facility_name, city=city,
    )

    prompt = f"""You are a healthcare market research assistant. I need you to find the current
administrator or executive director for the following healthcare facility:

Facility Name:   {facility_name}
Location:        {location}
Facility Type:   {type_desc}
Parent Company:  {corporation_name}

Please search the following sources:
{source_text}

Report back:
1. The name of the current administrator or executive director (if found)
2. Their job title exactly as listed
3. Their contact information if publicly available (phone, email)
4. The source URL where you found this information
5. Any notes (e.g., recently changed leadership, facility under new management)

If you cannot find leadership information, say so explicitly and list which sources you checked.

At the very end of your response, include this JSON block (fill in what you found):
```json
{{"found_name": "First Last", "found_title": "Administrator", "found_email": "email@example.com", "found_phone": "555-123-4567", "confidence": "high"}}
```
Valid confidence values: "high", "medium", "low", "not_found".
If no person was found, use:
```json
{{"found_name": "", "found_title": "", "found_email": "", "found_phone": "", "confidence": "not_found"}}
```"""

    return prompt, tier


# ─── BigQuery data-loading helpers ───────────────────────────────────────────

def get_facility_ids_for_network(client: bigquery.Client, network_id: int) -> list[int]:
    """
    Return all HOSPITAL_IDs (child facilities) associated with a NETWORK_ID
    from both the SNF and ALF overview tables.
    """
    facility_ids: set[int] = set()

    for key, table in [("SNF", DH_TABLES["snf_overview"]), ("ALF", DH_TABLES["alf_overview"])]:
        query = f"""
        SELECT DISTINCT CAST(HOSPITAL_ID AS INT64) AS hospital_id
        FROM `{table}`
        WHERE NETWORK_ID = {network_id}
          AND HOSPITAL_ID IS NOT NULL
        """
        try:
            rows = client.query(query).result()
            ids = {row.hospital_id for row in rows}
            facility_ids.update(ids)
            print(f"  ✓ {key} Overview: {len(ids)} child facilities")
        except Exception as e:
            print(f"  ⚠ {key} Overview query failed: {e}")

    return sorted(facility_ids)


def load_corporate_level_contacts(
    client: bigquery.Client,
    network_id: int,
    facility_ids: list[int],
) -> pd.DataFrame:
    """
    Load contacts from Definitive who are:
      - Titled "Executive Director" or "Administrator"
      - Associated with the NETWORK_ID at the corporation level
        (HOSPITAL_ID = network_id in the Executives or Corporate Executives table)
      - NOT also appearing at a specific child facility

    Returns a DataFrame of contacts suitable for state-association research.
    """
    print(f"\n  Querying corporate-level contacts for NETWORK_ID={network_id}...")

    facility_id_set: set[int] = set(facility_ids)

    all_rows: list[dict] = []

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
    WHERE HOSPITAL_ID = {network_id}
      AND LOWER(TRIM(TITLE)) IN ('executive director', 'administrator')
    """
    try:
        rows = [dict(r) for r in client.query(exec_query).result()]
        all_rows.extend(rows)
        print(f"    Executives table (network level): {len(rows)} rows")
    except Exception as e:
        print(f"    ⚠ Executives table query failed: {e}")

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
    WHERE HOSPITAL_ID = {network_id}
      AND LOWER(TRIM(TITLE)) IN ('executive director', 'administrator')
    """
    try:
        rows = [dict(r) for r in client.query(corp_query).result()]
        all_rows.extend(rows)
        print(f"    Corporate Executives table (network level): {len(rows)} rows")
    except Exception as e:
        print(f"    ⚠ Corporate Executives table query failed: {e}")

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
    WHERE HOSPITAL_ID = {network_id}
      AND LOWER(TRIM(TITLE)) IN ('executive director', 'administrator')
    """
    try:
        rows = [dict(r) for r in client.query(feed_query).result()]
        all_rows.extend(rows)
        print(f"    December Full Feed (network level): {len(rows)} rows")
    except Exception as e:
        print(f"    ⚠ December Full Feed query failed: {e}")

    if not all_rows:
        print("  ℹ No corporate-level ED/Administrator contacts found.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # ── Find which EXECUTIVE_IDs also appear at a child facility ──────────────
    exec_ids = df["EXECUTIVE_ID"].dropna().unique().tolist()

    # Build set of exec_ids that have a facility-level row in ANY Definitive source
    facility_exec_ids: set = set()
    if exec_ids and facility_ids:
        exec_ids_str = ",".join(str(int(e)) for e in exec_ids)
        fac_ids_str  = ",".join(str(f) for f in facility_ids)

        for table_key in ("executives", "full_feed"):
            fac_check_query = f"""
            SELECT DISTINCT EXECUTIVE_ID
            FROM `{DH_TABLES[table_key]}`
            WHERE EXECUTIVE_ID IN ({exec_ids_str})
              AND CAST(HOSPITAL_ID AS INT64) IN ({fac_ids_str})
            """
            try:
                rows = client.query(fac_check_query).result()
                ids  = {row.EXECUTIVE_ID for row in rows}
                facility_exec_ids.update(ids)
            except Exception as e:
                print(f"    ⚠ Facility cross-check ({table_key}) failed: {e}")

    # ── Exclude contacts that already have a facility-level assignment ─────────
    before = len(df)
    df = df[~df["EXECUTIVE_ID"].isin(facility_exec_ids)].copy()
    excluded = before - len(df)
    if excluded:
        print(f"  ✓ Excluded {excluded} contact(s) already assigned to a child facility")

    # Deduplicate: keep one row per EXECUTIVE_ID (prefer december_full_feed > corporate > executives)
    priority = {"december_full_feed": 0, "corporate_executives": 1, "executives": 2}
    df["_priority"] = df["source_table"].map(priority).fillna(9)
    df = df.sort_values("_priority").drop_duplicates(subset=["EXECUTIVE_ID"], keep="first")
    df = df.drop(columns=["_priority"])

    print(f"  ✓ {len(df)} corporate-only ED/Administrator contacts ready for research")
    return df.reset_index(drop=True)


def load_hubspot_child_facilities(
    client: bigquery.Client,
    hubspot_corp_id: str,
) -> pd.DataFrame:
    """
    Return all active child facilities in HubSpot whose hs_parent_company_id
    matches the given corporation HubSpot ID.
    """
    print(f"\n  Querying HubSpot child facilities for parent ID={hubspot_corp_id}...")

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
        CAST(properties_definitive_healthcare_id AS STRING) AS definitive_id
    FROM `{HUBSPOT_COMPANIES_TABLE}`
    WHERE (archived IS NOT TRUE OR archived IS NULL)
      AND CAST(properties_hs_parent_company_id AS STRING) = '{hubspot_corp_id}'
      AND properties_name IS NOT NULL
      AND TRIM(properties_name) != ''
    ORDER BY properties_name
    """

    try:
        df = client.query(query).to_dataframe()
        df = df.replace(["nan", "None", "<NA>"], "")
        print(f"  ✓ Found {len(df)} child facilities in HubSpot")
        return df
    except Exception as e:
        print(f"  ✗ Error loading child facilities: {e}")
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

    print(f"\n  Checking {len(facility_ids)} facilities for existing ED/Administrator contacts...")

    # Build a SQL list of facility IDs
    ids_sql = ",".join(f"'{fid}'" for fid in facility_ids)

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
      AND CAST(properties_associatedcompanyid AS STRING) IN ({ids_sql})
      AND (
          LOWER(properties_jobtitle) LIKE '%executive director%'
          OR LOWER(properties_jobtitle) LIKE '%administrator%'
      )
    """

    try:
        contacts_df = client.query(contact_query).to_dataframe()
        contacts_df = contacts_df.replace(["nan", "None", "<NA>"], "")
        print(f"  ✓ Found {len(contacts_df)} existing ED/Administrator contacts across facilities")
    except Exception as e:
        print(f"  ⚠ HubSpot contacts query failed: {e}")
        contacts_df = pd.DataFrame(columns=["associated_company_id"])

    # Facilities that already have at least one matching contact
    covered_ids = set(contacts_df["associated_company_id"].unique()) if not contacts_df.empty else set()

    missing = child_facilities[~child_facilities["hubspot_id"].isin(covered_ids)].copy()
    print(f"  ✓ {len(missing)} facilities missing an ED/Administrator contact")
    return missing.reset_index(drop=True)


# ─── Workflow runners ─────────────────────────────────────────────────────────

def workflow_2_research_contacts(
    client: bigquery.Client,
    network_id: int,
    corporation_name: str,
    facility_type: str,
) -> list[dict]:
    """
    Workflow 2: Find corporate-level ED/Administrator contacts in Definitive that
    are not assigned to a specific facility, then research them on the web.
    """
    print("\n" + "=" * 70)
    print("  WORKFLOW 2 — Researching Unassociated Definitive Contacts")
    print("=" * 70)

    facility_ids = get_facility_ids_for_network(client, network_id)
    print(f"  Total child facilities under NETWORK_ID {network_id}: {len(facility_ids)}")

    contacts_df = load_corporate_level_contacts(client, network_id, facility_ids)
    if contacts_df.empty:
        print("  ℹ No contacts to research for Workflow 2.")
        return []

    results: list[dict] = []
    total = len(contacts_df)

    print(f"\n  🔍 Researching {total} contact(s) on the web...")

    for idx, row in contacts_df.iterrows():
        contact_name = (
            f"{row.get('FIRST_NAME', '')} {row.get('LAST_NAME', '')}".strip()
            or str(row.get("EXECUTIVE_NAME", "Unknown"))
        )
        title = str(row.get("title", "")).title()
        state = str(row.get("state", ""))
        email = str(row.get("EMAIL", ""))
        phone = str(row.get("DIRECT_PHONE") or row.get("LOCATION_PHONE") or "")
        linkedin = str(row.get("LINKEDIN_PROFILE", ""))
        source = str(row.get("source_table", ""))

        print(f"\n  [{idx + 1}/{total}] Researching: {contact_name} ({title})")

        research_result = ""
        error_msg = ""
        try:
            prompt = _build_contact_research_prompt(
                contact_name=contact_name,
                title=title,
                corporation_name=corporation_name,
                state=state,
                facility_type=facility_type,
            )
            research_result = call_openrouter(prompt)
            print(f"    ✓ Research complete ({len(research_result)} chars)")
        except RuntimeError as e:
            error_msg = str(e)
            print(f"    ✗ Research failed: {error_msg}")

        results.append({
            "workflow":           "2 - Unassociated Contact",
            "executive_id":       row.get("EXECUTIVE_ID", ""),
            "contact_name":       contact_name,
            "title":              title,
            "email":              email,
            "phone":              phone,
            "linkedin":           linkedin,
            "definitive_source":  source,
            "corporation_name":   corporation_name,
            "network_id":         network_id,
            "research_findings":  research_result,
            "research_error":     error_msg,
            "researched_at":      datetime.now().isoformat(),
        })

        if idx < total - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    return results


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
    print("\n" + "=" * 70)
    print("  WORKFLOW 1 — Researching Facilities Missing Leadership Contacts")
    print("=" * 70)

    child_facilities = load_hubspot_child_facilities(client, hubspot_corp_id)
    if child_facilities.empty:
        print("  ℹ No child facilities found in HubSpot for Workflow 1.")
        return []

    missing = find_facilities_missing_leadership(client, child_facilities)
    if missing.empty:
        print("  ✅ All child facilities already have an ED/Administrator contact in HubSpot!")
        return []

    total = len(missing)
    results: list[dict] = []

    print(f"\n  🔍 Researching {total} facilit(y/ies) on state association sites...")

    for idx, row in missing.iterrows():
        facility_name = str(row.get("facility_name", "Unknown Facility"))
        city          = str(row.get("city", ""))
        state         = str(row.get("state", ""))
        facility_type = str(row.get("facility_type", ""))
        hubspot_id    = str(row.get("hubspot_id", ""))
        definitive_id = str(row.get("definitive_id", ""))

        print(f"\n  [{idx + 1}/{total}] Researching: {facility_name} ({city}, {state})")

        research_result = ""
        error_msg = ""
        found_name = ""
        found_title = ""
        found_email = ""
        found_phone = ""
        confidence = "not_found"
        source_tier = "3"
        hubspot_contact_ids = ""
        hubspot_match_detail = ""
        hubspot_match_summary = ""

        try:
            prompt, source_tier = _build_facility_research_prompt(
                facility_name=facility_name,
                city=city,
                state=state,
                facility_type=facility_type,
                corporation_name=corporation_name,
            )
            research_result = call_openrouter(prompt)
            print(f"    ✓ Research complete ({len(research_result)} chars)")

            # Extract the structured name from the JSON block in the response
            found_name, found_title, confidence, found_email, found_phone = parse_found_name(research_result)

            if found_name and confidence != "not_found":
                print(f"    🔎 Found: {found_name} ({found_title}, confidence={confidence})")
                if found_email:
                    print(f"    🔎 Email: {found_email}")
                if found_phone:
                    print(f"    🔎 Phone: {found_phone}")
                print(f"    🔎 Checking HubSpot for existing contact...")
                
                # Use enhanced lookup with email, phone, facility ID, and parent ID
                matches, match_strategy = lookup_hubspot_contact_enhanced(
                    client=client,
                    full_name=found_name,
                    email=found_email,
                    phone=found_phone,
                    facility_hubspot_id=hubspot_id,
                    parent_hubspot_id=hubspot_corp_id,
                )
                hubspot_contact_ids, hubspot_match_detail, hubspot_match_summary = _format_hubspot_matches_enhanced(
                    matches, match_strategy=match_strategy, facility_name=facility_name
                )
                
                if matches:
                    print(f"    ✓ HubSpot match: {hubspot_match_summary}")
                    print(f"       {hubspot_match_detail}")
                else:
                    print(f"    ℹ Not found in HubSpot contacts")
            else:
                print(f"    ℹ No administrator/ED identified (confidence={confidence})")

        except RuntimeError as e:
            error_msg = str(e)
            print(f"    ✗ Research failed: {error_msg}")

        results.append({
            "workflow":              "1 - Facility Missing Contact",
            "hubspot_facility_id":   hubspot_id,
            "facility_name":         facility_name,
            "city":                  city,
            "state":                 state,
            "facility_type":         facility_type,
            "definitive_id":         definitive_id,
            "corporation_name":      corporation_name,
            "hubspot_corp_id":       hubspot_corp_id,
            "found_name":            found_name,
            "found_title":           found_title,
            "found_email":           found_email,
            "found_phone":           found_phone,
            "research_confidence":   confidence,
            "source_tier":           source_tier,
            "hubspot_contact_ids":   hubspot_contact_ids,
            "hubspot_match_detail":  hubspot_match_detail,
            "hubspot_match_summary": hubspot_match_summary,
            "research_findings":     research_result[:2000] if research_result else "",
            "research_error":        error_msg,
            "researched_at":         datetime.now().isoformat(),
        })

        if idx < total - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    return results


def save_csv(rows: list[dict], filename: str) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        print(f"  ℹ No rows to save for {filename}")
        return

    fieldnames = list(rows[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n💾 Saved {len(rows)} row(s) → {filename}")


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
    WHERE CAST(properties_hs_object_id AS STRING) = '{hubspot_corp_id}'
    LIMIT 1
    """
    try:
        rows = list(client.query(query).result())
        if rows:
            return str(rows[0].company_name or "Unknown Corporation"), str(rows[0].facility_type or "")
    except Exception as e:
        print(f"  ⚠ Could not look up corporation name: {e}")
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
