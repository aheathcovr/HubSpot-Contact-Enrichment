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

import hashlib
import json
import logging
import os
import re
import sys
import time
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

# BigQuery RE2 pattern used to filter corporate executive titles in Workflow 2.
# Matches: CEO/Chief Executive, COO/Chief Operating, CFO/Chief Financial,
# Vice President (any), Regional Director (any), standalone President.
DH_W2_TITLE_PATTERN = (
    r"chief executive|chief operating|chief financial"
    r"|\bceo\b|\bcoo\b|\bcfo\b"
    r"|vice president"
    r"|regional director"
    r"|\bpresident\b"
)

OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL         = os.getenv("OPENROUTER_MODEL",          "perplexity/sonar-pro")
OPENROUTER_MODEL_TIER23  = os.getenv("OPENROUTER_MODEL_TIER23",   "perplexity/sonar-reasoning-pro")

# Structured JSON schema enforced on every OpenRouter response.
# Using response_format eliminates the need for regex-based JSON extraction
# and guarantees a parseable result on every call.
_CONTACT_JSON_SCHEMA = {
    "name": "contact_result",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "found_name":  {"type": "string"},
            "found_title": {"type": "string"},
            "found_email": {"type": "string"},
            "found_phone": {"type": "string"},
            "confidence":  {
                "type": "string",
                "enum": ["high", "medium", "low", "not_found"]
            },
            "source_url":  {"type": "string"},
            "reasoning":   {"type": "string"},
        },
        "required": ["found_name", "found_title", "found_email",
                     "found_phone", "confidence", "source_url", "reasoning"],
        "additionalProperties": False,
     },
}
GCS_CACHE_BUCKET    = os.getenv("GCS_CACHE_BUCKET", "")

# Titles that identify facility-level leadership we care about
TARGET_TITLES = {"executive director", "administrator"}

RATE_LIMIT_SECONDS = 2.5  # Pause between OpenRouter calls to avoid rate-limiting

# ─── LLM helpers ──────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

# Stable system prompt shared by both research workflows.
# Kept here so the user portions returned by the prompt builders stay variable-only,
# enabling safe response caching (temperature=0).
RESEARCH_SYSTEM_PROMPT = """\
You are a healthcare market research specialist finding verified contact information \
for facility leadership (Administrator, Executive Director, Director of Nursing) at \
US senior care facilities.

Search methodology — follow this order for every lookup:
1. Check the specific association directory or URL listed in the request
2. Search CMS Care Compare (medicare.gov/care-compare) for the facility or person
3. Check the facility or operator's own website for a staff/leadership page
4. Search LinkedIn as a last resort: "[Facility Name] administrator site:linkedin.com"

Accuracy rules (strictly enforced):
- Report ONLY names found directly on a verifiable source — do not infer or combine partial information
- confidence="high"      → name appears on an official association directory or CMS listing
- confidence="medium"    → name found on facility website or operator site only
- confidence="low"       → found on LinkedIn, a news mention, or a source that may be outdated
- confidence="not_found" → no verified name located; never guess or hallucinate a name

Always cite the exact URL where you found the name.
In the "reasoning" field, write 2–5 sentences explaining: which specific source confirmed this person's name and title, any date visible on that source (event date, publication date, page last-updated), why you are confident this is the current person and not a former employee, and any corroborating sources. If multiple sources agree, name them. If the source is recent (within 12 months), say so explicitly.
Your response MUST be a single valid JSON object matching the required schema — no extra text.\
"""

# Disk cache for LLM responses — used as fallback when GCS_CACHE_BUCKET is not set.
# Safe because all calls use temperature=0. Delete to force fresh lookups.
_LLM_CACHE_DIR = Path(__file__).parent / ".llm_cache"

# GCS cache client — lazily initialized on first use, shared across all threads.
_gcs_client_lock     = threading.Lock()
_gcs_client_instance = None   # google.cloud.storage.Client or False (unavailable)


def _get_gcs_client():
    """Return a shared GCS client, or False if GCS is unavailable/unconfigured."""
    global _gcs_client_instance
    if _gcs_client_instance is None:
        with _gcs_client_lock:
            if _gcs_client_instance is None:
                try:
                    from google.cloud import storage as _gcs
                    _gcs_client_instance = _gcs.Client()
                except Exception as exc:
                    logger.warning("GCS client unavailable (%s) — using disk cache", exc)
                    _gcs_client_instance = False
    return _gcs_client_instance


def _gcs_cache_get(key: str) -> Optional[str]:
    """Return cached LLM response from GCS, or None on miss/error."""
    client = _get_gcs_client()
    if not client:
        return None
    try:
        blob = client.bucket(GCS_CACHE_BUCKET).blob(f"llm_cache/{key}.json")
        if blob.exists():
            logger.info("GCS LLM cache hit (key=%s)", key)
            return json.loads(blob.download_as_text())["response"]
    except Exception as exc:
        logger.warning("GCS cache get failed (key=%s): %s", key, exc)
    return None


def _gcs_cache_set(key: str, response: str) -> None:
    """Write LLM response to GCS cache. Fails silently."""
    client = _get_gcs_client()
    if not client:
        return
    try:
        blob = client.bucket(GCS_CACHE_BUCKET).blob(f"llm_cache/{key}.json")
        blob.upload_from_string(
            json.dumps({"response": response}),
            content_type="application/json",
        )
    except Exception as exc:
        logger.warning("GCS cache set failed (key=%s): %s", key, exc)


# Persistent HTTP session for OpenRouter — reuses TCP connections across calls.
def _build_openrouter_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type":  "application/json",
        "HTTP-Referer":  "https://github.com/HubSpot-Definitive-Matching",
        "X-Title":       "State Association Matcher",
    })
    return session


_OPENROUTER_SESSION = _build_openrouter_session()

# Concurrency settings for workflow loops.
# _MAX_WORKERS threads run simultaneously; the semaphore limits live API calls
# to the same number so rate limiting stays predictable.
_MAX_WORKERS        = 3
_OPENROUTER_SEMAPHORE = threading.Semaphore(_MAX_WORKERS)

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

def call_openrouter(
    system_prompt: str,
    user_prompt: str,
    model: str = OPENROUTER_MODEL,
    response_format: Optional[dict] = None,
    extra_params: Optional[dict] = None,
) -> str:
    """
    Call the OpenRouter chat-completions endpoint and return the response text.

    Uses temperature=0 for deterministic structured output and a shared persistent
    HTTP session (TCP connection reuse).  Responses are cached:
      - In GCS (cross-instance, persistent) when GCS_CACHE_BUCKET env var is set.
      - On disk (.llm_cache/) as a fallback for local development.

    Args:
        system_prompt:   The system-role message.
        user_prompt:     The user-role message.
        model:           OpenRouter model slug.
        response_format: Optional dict passed as ``response_format`` in the payload.
                         Pass ``{"type": "json_schema", "json_schema": _CONTACT_JSON_SCHEMA}``
                         to enforce structured JSON output and skip regex parsing.
        extra_params:    Optional dict of additional top-level payload keys, e.g.
                         ``{"reasoning_effort": "high"}`` for sonar-reasoning-pro.

    Raises:
        RuntimeError: if OPENROUTER_API_KEY is not set or the request fails.
    """
    if not OPENROUTER_API_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. "
            "Export it as an environment variable before running this script."
        )

    # ── Cache key (shared between GCS and disk backends) ─────────────────────
    # Include model + extra_params in the key so different parameter sets
    # never collide in the cache.
    cache_seed = system_prompt + user_prompt + model
    if response_format:
        cache_seed += json.dumps(response_format, sort_keys=True)
    if extra_params:
        cache_seed += json.dumps(extra_params, sort_keys=True)
    cache_key = hashlib.sha256(cache_seed.encode()).hexdigest()[:20]

    # ── GCS cache lookup (production) ───────────────────────────────────────
    if GCS_CACHE_BUCKET:
        cached = _gcs_cache_get(cache_key)
        if cached is not None:
            return cached
    else:
        # ── Disk cache fallback (local dev) ─────────────────────────────────
        cache_file = _LLM_CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            logger.info("Disk LLM cache hit (key=%s)", cache_key)
            return json.loads(cache_file.read_text())["response"]

    # ── Build payload ─────────────────────────────────────────────────────────────────
    payload: dict = {
        "model":       model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
    }
    if response_format:
        payload["response_format"] = response_format
    if extra_params:
        payload.update(extra_params)

    # ── Call OpenRouter via shared session ────────────────────────────────────
    try:
        response = _OPENROUTER_SESSION.post(
            f"{OPENROUTER_BASE_URL}/chat/completions",
            json=payload,
            timeout=120,  # increased for reasoning-pro which can take longer
        )
        response.raise_for_status()
        data = response.json()
        result = data["choices"][0]["message"]["content"].strip()
    except requests.HTTPError as e:
        raise RuntimeError(f"OpenRouter HTTP error: {e.response.status_code} — {e.response.text}") from e
    except Exception as e:
        raise RuntimeError(f"OpenRouter call failed: {e}") from e

    # ── Cache write ──────────────────────────────────────────────────────────────────
    if GCS_CACHE_BUCKET:
        _gcs_cache_set(cache_key, result)
    else:
        _LLM_CACHE_DIR.mkdir(exist_ok=True)
        ((_LLM_CACHE_DIR / f"{cache_key}.json")).write_text(json.dumps({"response": result}))

    return result


def parse_found_name(research_text: str) -> tuple[str, str, str, str, str, str]:
    """
    Extract structured contact fields from an OpenRouter research response.

    Handles two response formats:
      1. Structured output (response_format=json_schema): the entire response IS
         a valid JSON object — parsed directly with no regex.
      2. Legacy free-text output: the JSON block is embedded inside a ```json
         fenced code block or as a bare object at the end of the response.

    Returns (found_name, found_title, confidence, found_email, found_phone, reasoning).
    Returns ("", "", "not_found", "", "", "") if parsing fails entirely.
    """
    import re
    import json

    def _extract_fields(data: dict) -> tuple[str, str, str, str, str, str]:
        name      = str(data.get("found_name",  "") or "").strip()
        title     = str(data.get("found_title", "") or "").strip()
        conf      = str(data.get("confidence",  "not_found") or "not_found").strip().lower()
        email     = str(data.get("found_email", "") or "").strip()
        phone     = str(data.get("found_phone", "") or "").strip()
        reasoning = str(data.get("reasoning",   "") or "").strip()
        if conf not in ("high", "medium", "low", "not_found"):
            logger.warning(
                "parse_found_name: unexpected confidence value %r — treating as not_found", conf
            )
            conf = "not_found"
        return name, title, conf, email, phone, reasoning

    # ── Path 1: structured output — the whole response is a JSON object ──────────
    stripped = research_text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            data = json.loads(stripped)
            if "found_name" in data or "confidence" in data:
                return _extract_fields(data)
        except (json.JSONDecodeError, AttributeError):
            pass  # fall through to legacy path

    # ── Path 2: legacy free-text — JSON embedded in a fenced code block ────────
    matches = re.findall(r"```json\s*(\{.*?\})\s*```", research_text, re.DOTALL)
    if not matches:
        # Try bare JSON object as fallback
        matches = re.findall(r'\{"found_name".*?\}', research_text, re.DOTALL)

    for raw in reversed(matches):  # use last match (most likely the structured block)
        try:
            data = json.loads(raw)
            return _extract_fields(data)
        except (json.JSONDecodeError, AttributeError):
            continue

    logger.warning(
        "parse_found_name: no valid JSON block in response (response_len=%d)",
        len(research_text),
    )
    return "", "", "not_found", "", "", ""


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
            print(f"    ⚠ HubSpot email lookup failed: {e}")

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
            print(f"    ⚠ HubSpot phone lookup failed: {e}")

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
- source_url:  exact URL where the name was found, or empty string"""


def _build_facility_research_prompt(
    facility_name: str,
    city: str,
    state: str,
    facility_type: str,
    corporation_name: str,
    address: str = "",
    website: str = "",
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
- source_url:  exact URL where the name was found, or empty string"""

    return prompt, tier


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

    _net_cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("network_id", "INT64", network_id),
    ])

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
    """
    try:
        rows = [dict(r) for r in client.query(exec_query, job_config=_net_cfg).result()]
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
    WHERE HOSPITAL_ID = @network_id
      AND REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{DH_W2_TITLE_PATTERN}')
    """
    try:
        rows = [dict(r) for r in client.query(corp_query, job_config=_net_cfg).result()]
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
    WHERE HOSPITAL_ID = @network_id
      AND REGEXP_CONTAINS(LOWER(TRIM(TITLE)), r'{DH_W2_TITLE_PATTERN}')
    """
    try:
        rows = [dict(r) for r in client.query(feed_query, job_config=_net_cfg).result()]
        all_rows.extend(rows)
        print(f"    December Full Feed (network level): {len(rows)} rows")
    except Exception as e:
        print(f"    ⚠ December Full Feed query failed: {e}")

    if not all_rows:
        print("  ℹ No corporate-level executive contacts found in Definitive Healthcare.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # ── Find which EXECUTIVE_IDs also appear at a child facility ──────────────
    exec_ids = df["EXECUTIVE_ID"].dropna().unique().tolist()

    # Build set of exec_ids that have a facility-level row in ANY Definitive source
    facility_exec_ids: set = set()
    if exec_ids and facility_ids:
        exec_ids_list = [int(e) for e in exec_ids]
        fac_ids_list  = [int(f) for f in facility_ids]

        for table_key in ("executives", "full_feed"):
            fac_check_query = f"""
            SELECT DISTINCT EXECUTIVE_ID
            FROM `{DH_TABLES[table_key]}`
            WHERE EXECUTIVE_ID IN UNNEST(@exec_ids)
              AND CAST(HOSPITAL_ID AS INT64) IN UNNEST(@fac_ids)
            """
            fac_check_cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("exec_ids", "INT64", exec_ids_list),
                bigquery.ArrayQueryParameter("fac_ids",  "INT64", fac_ids_list),
            ])
            try:
                rows = client.query(fac_check_query, job_config=fac_check_cfg).result()
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
        print(f"  ✓ Found {len(contacts_df)} existing ED/Administrator contacts across facilities")
    except Exception as e:
        print(f"  ⚠ HubSpot contacts query failed: {e}")
        contacts_df = pd.DataFrame(columns=["associated_company_id"])

    # Facilities that already have at least one matching contact
    covered_ids = set(contacts_df["associated_company_id"].unique()) if not contacts_df.empty else set()

    missing = child_facilities[~child_facilities["hubspot_id"].isin(covered_ids)].copy()
    print(f"  ✓ {len(missing)} facilities missing an ED/Administrator contact")
    return missing.reset_index(drop=True)


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
            return {**_empty, "error": f"query_failed: {exc}"}

    return {**_empty, "error": ""}   # queried successfully, not found


# ─── Concurrent worker functions ─────────────────────────────────────────────

def _research_one_contact(
    row: dict,
    corporation_name: str,
    network_id: int,
    facility_type: str,
) -> dict:
    """
    Worker for workflow_2.  Runs in a thread pool — one unassociated contact per call.

    Acquires _OPENROUTER_SEMAPHORE before calling the API to cap concurrency at
    _MAX_WORKERS, then sleeps RATE_LIMIT_SECONDS inside the semaphore so each slot
    turns over at a controlled rate before being released.
    """
    contact_name = (
        f"{row.get('FIRST_NAME', '')} {row.get('LAST_NAME', '')}".strip()
        or str(row.get("EXECUTIVE_NAME", "Unknown"))
    )
    title    = str(row.get("title", "")).title()
    state    = str(row.get("state", ""))
    email    = str(row.get("EMAIL", ""))
    phone    = str(row.get("DIRECT_PHONE") or row.get("LOCATION_PHONE") or "")
    linkedin = str(row.get("LINKEDIN_PROFILE", ""))
    source   = str(row.get("source_table", ""))

    result: dict = {
        "workflow":            "2 - Unassociated Contact",
        "executive_id":        row.get("EXECUTIVE_ID", ""),
        "contact_name":        contact_name,
        "title":               title,
        "email":               email,
        "phone":               phone,
        "linkedin":            linkedin,
        "definitive_source":   source,
        "corporation_name":    corporation_name,
        "network_id":          network_id,
        "research_confidence": "high",   # sourced directly from Definitive Healthcare
        "research_findings":   "",
        "research_error":      "",
        "researched_at":       datetime.now().isoformat(),
    }

    try:
        prompt = _build_contact_research_prompt(
            contact_name=contact_name,
            title=title,
            corporation_name=corporation_name,
            state=state,
            facility_type=facility_type,
        )

        # Workflow 2 contacts are unassociated (no direct facility URL), so they
        # always benefit from the deeper reasoning model.
        structured_format = {"type": "json_schema", "json_schema": _CONTACT_JSON_SCHEMA}

        print(f"    → {contact_name}: querying Sonar Reasoning Pro (high effort)...")
        with _OPENROUTER_SEMAPHORE:
            research_result = call_openrouter(
                RESEARCH_SYSTEM_PROMPT,
                prompt,
                model=OPENROUTER_MODEL_TIER23,
                response_format=structured_format,
                extra_params={"reasoning_effort": "high"},
            )
            time.sleep(RATE_LIMIT_SECONDS)
        print(f"    ✓ {contact_name}: complete ({len(research_result)} chars)")
        result["research_findings"] = research_result
    except RuntimeError as e:
        result["research_error"] = str(e)
        print(f"    ✗ {contact_name}: failed — {e}")

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

    try:
        prompt, source_tier = _build_facility_research_prompt(
            facility_name=facility_name,
            city=city,
            state=state,
            facility_type=facility_type,
            corporation_name=corporation_name,
        )
        result["source_tier"] = source_tier

        # ── Tiered model selection (Rec #2) ────────────────────────────────────────────
        # Tier 1 → sonar-pro (direct association URL, cheaper, fast)
        # Tier 2/3 → sonar-reasoning-pro + reasoning_effort=high (no direct URL;
        #             deeper multi-step reasoning needed to locate the contact)
        if source_tier == "1":
            model       = OPENROUTER_MODEL
            extra       = None
            model_label = "Sonar Pro"
        else:
            model       = OPENROUTER_MODEL_TIER23
            extra       = {"reasoning_effort": "high"}
            model_label = "Sonar Reasoning Pro (high effort)"

        # ── Structured JSON output (Rec #1) ──────────────────────────────────────────
        structured_format = {"type": "json_schema", "json_schema": _CONTACT_JSON_SCHEMA}

        print(f"    → {facility_name}: querying {model_label} (tier {source_tier})...")
        with _OPENROUTER_SEMAPHORE:
            research_result = call_openrouter(
                RESEARCH_SYSTEM_PROMPT,
                prompt,
                model=model,
                response_format=structured_format,
                extra_params=extra,
            )
            time.sleep(RATE_LIMIT_SECONDS)
        print(f"    ✓ {facility_name}: complete ({len(research_result)} chars)")
        result["research_findings"] = research_result[:2000]

        found_name, found_title, confidence, found_email, found_phone, reasoning = parse_found_name(
            research_result
        )
        result.update({
            "found_name":          found_name,
            "found_title":         found_title,
            "research_confidence": confidence,
            "found_email":         found_email,
            "found_phone":         found_phone,
            "research_reasoning":  reasoning,
        })

        if found_name and confidence != "not_found":
            print(f"    🔎 {facility_name}: found {found_name} ({found_title}, {confidence})")
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
            print(f"    ℹ {facility_name}: no contact found (confidence={confidence})")

    except RuntimeError as e:
        result["research_error"] = str(e)
        print(f"    ✗ {facility_name}: failed — {e}")

    return result


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

    rows  = contacts_df.to_dict("records")
    total = len(rows)
    print(f"\n  🔍 Researching {total} contact(s) ({_MAX_WORKERS} concurrent)...")

    ordered: list[dict] = [{}] * total
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _research_one_contact, row, corporation_name, network_id, facility_type
            ): i
            for i, row in enumerate(rows)
        }
        done = 0
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
            done += 1
            print(f"  [{done}/{total}] contacts researched")

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

    rows  = missing.to_dict("records")
    total = len(rows)
    print(f"\n  🔍 Researching {total} facilit(y/ies) ({_MAX_WORKERS} concurrent)...")

    ordered: list[dict] = [{}] * total
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
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
            print(f"  [{done}/{total}] facilities researched")

    return ordered


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
