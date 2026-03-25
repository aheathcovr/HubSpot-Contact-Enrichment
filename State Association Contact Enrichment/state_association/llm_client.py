"""
OpenRouter / Perplexity Sonar Pro client.

Provides call_openrouter() with:
- Two-tier LLM response caching (GCS + disk) via cache.py
- Persistent HTTP session with TCP connection reuse
- Rate-limit semaphore to cap concurrent API calls
- Structured JSON schema enforcement for reliable output parsing
"""

import hashlib
import json
import logging
import os
import threading
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from state_association.cache import cache_get, cache_set

logger = logging.getLogger(__name__)

# ── OpenRouter configuration ───────────────────────────────────────────────────

OPENROUTER_API_KEY   = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL  = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL         = os.getenv("OPENROUTER_MODEL",        "perplexity/sonar-pro")
OPENROUTER_MODEL_TIER23  = os.getenv("OPENROUTER_MODEL_TIER23", "perplexity/sonar-reasoning-pro")

RATE_LIMIT_SECONDS: float = 2.5  # Pause between calls to avoid rate-limiting

# Maximum concurrent OpenRouter calls; semaphore keeps live calls predictable.
_MAX_WORKERS: int = 3
_OPENROUTER_SEMAPHORE = threading.Semaphore(_MAX_WORKERS)

# Structured JSON schema enforced on every OpenRouter response.
# Using response_format eliminates the need for regex-based JSON extraction
# and guarantees a parseable result on every call.
# Schema for multi-contact discovery calls (Workflow 2 website sweep).
# Returns an object with a "contacts" array so a single Sonar call can surface
# up to 10 corporate executives rather than requiring one call per title.
_CONTACT_LIST_JSON_SCHEMA: dict = {
    "name": "contact_list_result",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "contacts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "full_name":   {"type": "string"},
                        "title":       {"type": "string"},
                        "email":       {"type": "string"},
                        "phone":       {"type": "string"},
                        "source_url":  {"type": "string"},
                        "source_name": {"type": "string"},
                        "confidence":  {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                        },
                        "reasoning":   {"type": "string"},
                    },
                    "required": [
                        "full_name", "title", "email", "phone",
                        "source_url", "source_name", "confidence", "reasoning",
                    ],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["contacts"],
        "additionalProperties": False,
    },
}

# System prompt for the per-contact single-object schema.
_CONTACT_JSON_SCHEMA: dict = {
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
                "enum": ["high", "medium", "low", "not_found"],
            },
            "source_url":  {"type": "string"},
            "source_name": {"type": "string"},
            "reasoning":   {"type": "string"},
        },
        "required": [
            "found_name", "found_title", "found_email",
            "found_phone", "confidence", "source_url", "source_name", "reasoning",
        ],
        "additionalProperties": False,
    },
}

# Stable system prompt shared by both research workflows.
RESEARCH_SYSTEM_PROMPT: str = """\
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
In the "source_name" field, write the human-readable name of the specific website or publication where you found this person — be specific about the organization and page type (examples: "Washington State Assisted Living Association member directory", "Colorado HCAP facility search", "Brookdale Living corporate news blog", "CMS Care Compare facility listing", "facility website leadership page", "LinkedIn profile"). Do not write a URL here — write a plain-English name a salesperson would recognize.
In the "reasoning" field, write 2–5 sentences explaining: which specific source confirmed this person's name and title, any date visible on that source (event date, publication date, page last-updated), why you are confident this is the current person and not a former employee, and any corroborating sources. If multiple sources agree, name them. If the source is recent (within 12 months), say so explicitly.
Your response MUST be a single valid JSON object matching the required schema — no extra text.\
"""

# System prompt for Workflow 2 corporate-website discovery calls.
# Used with _CONTACT_LIST_JSON_SCHEMA — returns an array of up to 10 executives.
W2_DISCOVERY_SYSTEM_PROMPT: str = """\
You are a healthcare industry research specialist finding current corporate executive \
contacts for long-term care and senior housing operators.

Your task is to find up to 10 currently-employed corporate executives at the named company, \
restricted strictly to these seniority levels: C-suite officers (CEO, COO, CFO, CNO, CCO, CAO), \
President, Owner, Vice President (any variant), Senior Vice President (any variant), \
Regional Vice President, Regional Director of Operations.

Search methodology — follow this order:
1. Check the company's own website — look specifically for pages titled 'About Us', \
'Leadership', 'Our Team', 'Management', 'Corporate Governance', or 'Investor Relations'. \
Common URL patterns: /about-us, /about/leadership, /leadership-team, /our-team, \
/management, /corporate-governance, /investor-relations/management.
2. Search LinkedIn for people with a current position at this company in the target roles. \
Use searches like: "{company name}" "CEO" site:linkedin.com/in

Accuracy rules (strictly enforced):
- Return ONLY people with a verified CURRENT role at this company — not former employees
- confidence="high"   → name found on the official corporate website
- confidence="medium" → name found on LinkedIn showing a current role at this company
- confidence="low"    → name found in a press release, news article, or indirect source
- Never fabricate names, titles, emails, or phone numbers
- Return an empty contacts array if no verified executives are found

In the "source_name" field write the human-readable name of the page or profile \
(e.g. "Acme Health leadership page", "LinkedIn profile — Acme Health"). Do not write a URL here.
In the "reasoning" field write 1–3 sentences: which source confirmed the name and title, \
the recency of that source, and why you believe this is the current person in the role.
Your response MUST be a single valid JSON object with a "contacts" array — no extra text.\
"""

# ── HTTP session ───────────────────────────────────────────────────────────────

def _build_openrouter_session() -> requests.Session:
    """Build a persistent session with retry logic and shared auth headers."""
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


# Shared session — reuses TCP connections across all calls in the process.
_OPENROUTER_SESSION: requests.Session = _build_openrouter_session()


# ── Public API ─────────────────────────────────────────────────────────────────

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
    HTTP session (TCP connection reuse). Responses are cached via cache.py:
      - GCS (cross-instance, persistent) when GCS_CACHE_BUCKET env var is set.
      - Disk (.llm_cache/) as a fallback for local development.

    Args:
        system_prompt:   The system-role message.
        user_prompt:     The user-role message.
        model:           OpenRouter model slug.
        response_format: Optional dict passed as ``response_format`` in the payload.
        extra_params:    Optional dict of additional top-level payload keys.

    Raises:
        RuntimeError: if OPENROUTER_API_KEY is not set or the request fails.
    """
    if not OPENROUTER_API_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. "
            "Export it as an environment variable before running this script."
        )

    # ── Cache key ─────────────────────────────────────────────────────────────
    cache_seed = system_prompt + user_prompt + model
    if response_format:
        cache_seed += json.dumps(response_format, sort_keys=True)
    if extra_params:
        cache_seed += json.dumps(extra_params, sort_keys=True)
    cache_key = hashlib.sha256(cache_seed.encode()).hexdigest()[:20]

    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    # ── Build payload ─────────────────────────────────────────────────────────
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

    # ── Call OpenRouter ───────────────────────────────────────────────────────
    try:
        response = _OPENROUTER_SESSION.post(
            f"{OPENROUTER_BASE_URL}/chat/completions",
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
        result = data["choices"][0]["message"]["content"].strip()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"OpenRouter HTTP error: {e.response.status_code} — {e.response.text}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"OpenRouter call failed: {e}") from e

    cache_set(cache_key, result)
    return result


def parse_found_name(research_text: str) -> tuple[str, str, str, str, str, str, str]:
    """
    Extract structured contact fields from an OpenRouter research response.

    Handles two response formats:
      1. Structured output (response_format=json_schema): the entire response IS
         a valid JSON object — parsed directly with no regex.
      2. Legacy free-text output: the JSON block is embedded inside a ```json
         fenced code block or as a bare object at the end of the response.

    Returns (found_name, found_title, confidence, found_email, found_phone, reasoning, source_name).
    Returns ("", "", "not_found", "", "", "", "") if parsing fails entirely.
    """
    def _extract_fields(data: dict) -> tuple[str, str, str, str, str, str, str]:
        name        = str(data.get("found_name",  "") or "").strip()
        title       = str(data.get("found_title", "") or "").strip()
        conf        = str(data.get("confidence",  "not_found") or "not_found").strip().lower()
        email       = str(data.get("found_email", "") or "").strip()
        phone       = str(data.get("found_phone", "") or "").strip()
        reasoning   = str(data.get("reasoning",   "") or "").strip()
        source_name = str(data.get("source_name", "") or "").strip()
        if conf not in ("high", "medium", "low", "not_found"):
            logger.warning(
                "parse_found_name: unexpected confidence value %r — treating as not_found", conf
            )
            conf = "not_found"
        return name, title, conf, email, phone, reasoning, source_name

    import re

    # ── Path 1: structured output ─────────────────────────────────────────────
    stripped = research_text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            data = json.loads(stripped)
            if "found_name" in data or "confidence" in data:
                return _extract_fields(data)
        except (json.JSONDecodeError, AttributeError):
            pass

    # ── Path 2: legacy free-text with embedded JSON ───────────────────────────
    matches = re.findall(r"```json\s*(\{.*?\})\s*```", research_text, re.DOTALL)
    if not matches:
        matches = re.findall(r'\{"found_name".*?\}', research_text, re.DOTALL)

    for raw in reversed(matches):
        try:
            data = json.loads(raw)
            return _extract_fields(data)
        except (json.JSONDecodeError, AttributeError):
            continue

    logger.warning(
        "parse_found_name: no valid JSON block in response (response_len=%d)",
        len(research_text),
    )
    return "", "", "not_found", "", "", "", ""


def normalize_phone(phone: str) -> str:
    """
    Normalize a phone number for comparison by removing all non-digit characters.
    Returns empty string if fewer than 10 digits found.
    """
    if not phone:
        return ""
    digits = "".join(c for c in str(phone) if c.isdigit())
    return digits if len(digits) >= 10 else ""
