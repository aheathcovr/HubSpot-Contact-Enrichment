"""
FullEnrich People Search + Contact Enrich client.

Public functions:
  search_facility_contacts(company_name, city, state, titles, domain) -> list[dict]
  enrich_contact_info(firstname, lastname, company_name, domain, linkedin_url) -> dict

Used as a last-resort fallback in Workflow 1 when Sonar Pro + Definitive Healthcare
both fail to find an Administrator, Executive Director, or Director of Nursing.

API: POST https://app.fullenrich.com/api/v2/people/search
Auth: Authorization: Bearer <FULLENRICH_API_KEY>

StringFilters format (required by FullEnrich):
    [{"value": "...", "exact_match": false}]   ← array of filter objects

Company identifier priority:
    1. domain (e.g. "valleyviewcarecenter.org") — most precise; used when available.
       Falls back to company_name if domain search returns empty (domain not indexed).
    2. company_name — used when no domain is set on the HubSpot record.

Location disambiguation:
    Many SNF/ALF/ILF facilities share identical names across states. When searching
    by company name (not domain), we fetch up to 5 candidates per title and rank
    them by how closely their person location (city, region) matches the target
    facility's city and state. The best-matching candidate is returned; if no
    location data is present, the first result is accepted.

Returned list of dicts (one per unique person found):
    {
        "full_name":    str,
        "title":        str,   # from employment.current.title
        "linkedin_url": str,   # from social_profiles.linkedin.url, may be ""
    }

Key behaviours:
    - FULLENRICH_API_KEY not set → return [] immediately, no HTTP call
    - Network/HTTP error → return [], log warning — never raises
    - Empty or missing results → return []
    - requests timeout = 15s
"""

import logging
import os
import re
import time

import requests

logger = logging.getLogger(__name__)

_SEARCH_URL  = "https://app.fullenrich.com/api/v2/people/search"
_ENRICH_URL  = "https://app.fullenrich.com/api/v2/contact/enrich/bulk"
_TIMEOUT     = 15
_POLL_INTERVAL = 5   # seconds between polling attempts
_POLL_MAX      = 18  # max polls → 18 × 5s = 90s total wait

# US state name → abbreviation map for normalising FullEnrich region strings
_STATE_ABBR: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}


def _strip_domain(website: str) -> str:
    """
    Extract a bare domain from a website URL or return the value as-is.

    Examples:
        "https://www.valleyviewcarecenter.org/about" → "valleyviewcarecenter.org"
        "valleyviewcarecenter.org"                   → "valleyviewcarecenter.org"
        ""                                           → ""
    """
    if not website:
        return ""
    domain = re.sub(r"^https?://", "", website.strip())
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.split("/")[0].split("?")[0].strip()
    return domain.lower()


def _normalise_state(value: str) -> str:
    """Return the 2-letter state abbreviation for *value* (name or abbr), upper-cased."""
    v = (value or "").strip()
    if len(v) == 2:
        return v.upper()
    return _STATE_ABBR.get(v.lower(), v.upper())


def _location_score(person: dict, target_city: str, target_state: str) -> int:
    """
    Score how well a FullEnrich person result matches the target facility location.

    Returns:
        2 — city AND state match
        1 — state match only
        0 — no location data or no match

    Used to rank candidates when multiple same-named facilities exist in different states.
    """
    loc = person.get("location") or {}
    region = str(loc.get("region") or "").strip()
    city = str(loc.get("city") or "").strip()

    if not region and not city:
        return 0

    state_match = (
        _normalise_state(region) == _normalise_state(target_state)
    ) if target_state and region else False

    city_match = (
        city.lower() == (target_city or "").strip().lower()
    ) if target_city and city else False

    if state_match and city_match:
        return 2
    if state_match:
        return 1
    return 0


def _search_one_title(
    api_key: str,
    title: str,
    company_name: str = "",
    domain: str = "",
    limit: int = 1,
) -> list[dict]:
    """
    Call the FullEnrich People Search API for a single title filter.

    Uses domain (current_company_domains) when available, falls back to
    company name (current_company_names). Returns raw person dicts or [].
    """
    payload: dict = {
        "current_position_titles": [{"value": title, "exact_match": False}],
        "limit": limit,
    }

    if domain:
        payload["current_company_domains"] = [{"value": domain, "exact_match": True}]
        identifier = domain
    else:
        payload["current_company_names"] = [{"value": company_name, "exact_match": False}]
        identifier = company_name

    try:
        resp = requests.post(
            _SEARCH_URL,
            json=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning(
            "FullEnrich search failed (identifier=%r, title=%r): %s",
            identifier, title, exc,
        )
        return []

    people = data.get("people") or []
    if not isinstance(people, list):
        logger.debug("FullEnrich unexpected response shape: %s", list(data.keys()))
        return []
    return people


def _extract_contact(person: dict) -> dict:
    """Map a raw FullEnrich person dict to our internal contact dict."""
    full_name = str(person.get("full_name") or "").strip()
    if not full_name:
        first = str(person.get("first_name") or "").strip()
        last = str(person.get("last_name") or "").strip()
        full_name = f"{first} {last}".strip()

    # Job title lives under employment.current.title
    employment = person.get("employment") or {}
    current_job = employment.get("current") or {}
    title = str(current_job.get("title") or "").strip()

    # LinkedIn URL lives under social_profiles.linkedin.url
    social = person.get("social_profiles") or {}
    linkedin_data = social.get("linkedin") or {}
    linkedin_url = str(linkedin_data.get("url") or "").strip()

    return {
        "full_name":    full_name,
        "title":        title,
        "linkedin_url": linkedin_url,
    }


# Keywords that indicate an IT/tech role rather than a healthcare facility role.
# Used to filter out false positives like "Database Administrator" when searching
# for facility Administrators.
_TECH_TITLE_KEYWORDS = re.compile(
    r"\b("
    r"database|data\s*base|dba"
    r"|network|networks"
    r"|systems?\s+admin|sysadmin"
    r"|software"
    r"|information\s+technology|i\.?t\."
    r"|cloud|infrastructure|devops|dev\s*ops"
    r"|security\s+admin|cybersecurity"
    r")\b",
    re.IGNORECASE,
)


def _is_tech_role(title: str) -> bool:
    """Return True if the title is clearly an IT/tech role, not a healthcare facility role."""
    return bool(_TECH_TITLE_KEYWORDS.search(title or ""))


def _best_candidate(
    people: list[dict],
    city: str,
    state: str,
    identifier: str,
    title_filter: str,
) -> dict | None:
    """
    From a list of FullEnrich person dicts, return the best location-matched contact.

    Scoring: 2 = city+state match, 1 = state match, 0 = no data.
    Falls back to the first result when no location data is available.
    Logs the score so operators can judge match quality.

    Skips candidates whose actual title is a tech/IT role (e.g. "Database Administrator")
    so that searches for healthcare "Administrator" don't return IT staff.
    """
    if not people:
        return None

    # Filter out tech/IT roles before scoring
    eligible = []
    for p in people:
        contact = _extract_contact(p)
        if _is_tech_role(contact.get("title", "")):
            logger.debug(
                "FullEnrich skipping tech role %r at %r (title_filter=%r)",
                contact["title"], identifier, title_filter,
            )
        else:
            eligible.append(p)

    if not eligible:
        logger.info(
            "FullEnrich: all candidates for %r/title=%r were tech roles — skipping",
            identifier, title_filter,
        )
        return None

    scored = [(p, _location_score(p, city, state)) for p in eligible]
    best_person, best_score = max(scored, key=lambda x: x[1])

    contact = _extract_contact(best_person)
    if not contact["full_name"]:
        return None

    score_label = {2: "city+state", 1: "state-only", 0: "no-location-data"}[best_score]
    logger.info(
        "FullEnrich found: %r (%r) at %r via title=%r [location match: %s]",
        contact["full_name"], contact["title"], identifier, title_filter, score_label,
    )
    if best_score == 0 and len(people) > 1:
        logger.warning(
            "FullEnrich name search for %r matched %d candidate(s) but none had "
            "location data — returning first result; verify this is the correct facility",
            identifier, len(people),
        )

    return contact


def search_facility_contacts(
    company_name: str,
    city: str,
    state: str,
    titles: list[str],
    domain: str = "",
) -> list[dict]:
    """
    Search FullEnrich for people at the facility matching any of *titles*.

    When *domain* is provided (from the HubSpot `website` property), it is used as
    the company identifier via `current_company_domains` (exact match) — immune to
    name collision. Falls back to *company_name* if domain search returns nothing.

    When searching by company name, fetches up to 5 candidates per title and picks
    the one whose person location best matches *city* / *state*, reducing the risk of
    returning a contact from a same-named facility in a different state.

    Makes one primary API call per title (plus one fallback call when domain returns
    empty). Deduplicates results by full name across all titles.

    Returns a list of dicts: [{full_name, title, linkedin_url}, ...]
    Returns [] if the API key is not set, no results are found, or any error occurs.
    Never raises.
    """
    api_key = os.getenv("FULLENRICH_API_KEY", "").strip()
    if not api_key:
        logger.debug("FULLENRICH_API_KEY not set — skipping FullEnrich search")
        return []

    if not titles or (not company_name and not domain):
        return []

    bare_domain = _strip_domain(domain)
    identifier = bare_domain or company_name

    contacts: list[dict] = []
    seen_names: set[str] = set()

    for title_filter in titles:
        # Domain search: exact match, no ambiguity → limit=1 is fine
        if bare_domain:
            raw_people = _search_one_title(
                api_key=api_key, title=title_filter, domain=bare_domain, limit=1,
            )
            # Fall back to name search if domain not indexed in FullEnrich
            if not raw_people and company_name:
                logger.debug(
                    "FullEnrich domain search empty for %r/%r — retrying by company name",
                    bare_domain, title_filter,
                )
                raw_people = _search_one_title(
                    api_key=api_key, title=title_filter,
                    company_name=company_name, limit=5,
                )
        else:
            # Name-only search: fetch more candidates so we can pick by location
            raw_people = _search_one_title(
                api_key=api_key, title=title_filter,
                company_name=company_name, limit=5,
            )

        contact = _best_candidate(raw_people, city, state, identifier, title_filter)
        if contact and contact["full_name"] not in seen_names:
            seen_names.add(contact["full_name"])
            contacts.append(contact)

    return contacts


def _parse_enrich_response(data: dict) -> dict:
    """
    Extract email, phone, and supporting metadata from a completed FullEnrich v2 response.

    v2 response shape:
        data[0].contact_info.most_probable_work_email.{email, status}
        data[0].contact_info.most_probable_phone.{number, region}
        data[0].contact_info.work_emails[].{email, status}
        data[0].contact_info.phones[].{number, region}

    Returned dict keys:
        email           str   — most probable work email (or "")
        email_status    str   — FullEnrich deliverability: "DELIVERABLE" | "NOT_DELIVERABLE" | ""
        phone           str   — most probable phone number (or "")
        phone_region    str   — ISO country code of most probable phone, e.g. "US"
        all_emails      list  — [{"email": str, "status": str}, ...]
        all_phones      list  — [{"number": str, "region": str}, ...]
    """
    _empty: dict = {
        "email": "", "email_status": "", "phone": "",
        "phone_region": "", "all_emails": [], "all_phones": [],
    }
    records = data.get("data") or []
    if not records or not isinstance(records, list):
        return _empty
    contact_info = (records[0] or {}).get("contact_info") or {}

    email_obj = contact_info.get("most_probable_work_email") or {}
    email = str(email_obj.get("email") or "").strip()
    email_status = str(email_obj.get("status") or "").strip().upper()

    phone_obj = contact_info.get("most_probable_phone") or {}
    phone = str(phone_obj.get("number") or "").strip()
    phone_region = str(phone_obj.get("region") or "").strip().upper()

    all_emails = [
        {"email": str(e.get("email") or "").strip(), "status": str(e.get("status") or "").strip().upper()}
        for e in (contact_info.get("work_emails") or [])
        if e.get("email")
    ]
    all_phones = [
        {"number": str(p.get("number") or "").strip(), "region": str(p.get("region") or "").strip().upper()}
        for p in (contact_info.get("phones") or [])
        if p.get("number")
    ]

    return {
        "email":        email,
        "email_status": email_status,
        "phone":        phone,
        "phone_region": phone_region,
        "all_emails":   all_emails,
        "all_phones":   all_phones,
    }


def enrich_contact_info(
    firstname: str,
    lastname: str,
    company_name: str = "",
    domain: str = "",
    linkedin_url: str = "",
) -> dict:
    """
    Enrich a single contact via the FullEnrich v2 Bulk Enrich API to find email / phone.

    API: POST https://app.fullenrich.com/api/v2/contact/enrich/bulk
    Auth: Authorization: Bearer <FULLENRICH_API_KEY>

    Request body:
        {
            "name": "<job-name>",
            "data": [{
                "first_name": str,
                "last_name":  str,
                "domain":     str,   # OR "company_name" if no domain
                "linkedin_url": str, # optional, improves hit rate
                "enrich_fields": ["contact.emails", "contact.phones"]
            }]
        }

    Submits one contact, polls GET /api/v2/contact/enrich/bulk/{id} until
    status == "FINISHED" (up to 90 s), then extracts email and phone.

    Priority for company identifier:
        1. domain (bare hostname, e.g. "edenhc.com") — most precise
        2. company_name — fallback when no domain available

    Returns:
        {"email": str, "email_status": str, "phone": str}
    All values are "" on error, timeout, or missing data. Never raises.
    """
    _empty: dict = {"email": "", "email_status": "", "phone": ""}

    api_key = os.getenv("FULLENRICH_API_KEY", "").strip()
    if not api_key:
        logger.debug("FULLENRICH_API_KEY not set — skipping FullEnrich enrich")
        return _empty

    if not firstname or not lastname:
        return _empty

    bare_domain = _strip_domain(domain)
    identifier = bare_domain or company_name or f"{firstname} {lastname}"

    contact_record: dict = {
        "first_name":    firstname,
        "last_name":     lastname,
        "enrich_fields": ["contact.emails", "contact.phones"],
    }
    if bare_domain:
        contact_record["domain"] = bare_domain
    elif company_name:
        contact_record["company_name"] = company_name
    if linkedin_url:
        contact_record["linkedin_url"] = linkedin_url

    # Job name must be unique enough to avoid collisions on concurrent runs
    job_name = f"revops-{firstname.lower()}-{lastname.lower()}-{int(time.time())}"

    try:
        resp = requests.post(
            _ENRICH_URL,
            json={"name": job_name, "data": [contact_record]},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning(
            "FullEnrich enrich POST failed (%r at %r): %s",
            f"{firstname} {lastname}", identifier, exc,
        )
        return _empty

    enrichment_id = data.get("enrichment_id") or data.get("id")
    if not enrichment_id:
        logger.warning(
            "FullEnrich enrich: no id in response keys: %s", list(data.keys())
        )
        return _empty

    poll_url = f"{_ENRICH_URL}/{enrichment_id}"
    for attempt in range(1, _POLL_MAX + 1):
        time.sleep(_POLL_INTERVAL)
        try:
            poll_resp = requests.get(
                poll_url,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=_TIMEOUT,
            )
            poll_resp.raise_for_status()
            poll_data = poll_resp.json()
        except Exception as exc:
            logger.warning(
                "FullEnrich enrich poll failed (attempt %d/%d, id=%r): %s",
                attempt, _POLL_MAX, enrichment_id, exc,
            )
            continue

        status = (poll_data.get("status") or "").upper()
        logger.debug(
            "FullEnrich enrich job %r: status=%r (attempt %d/%d)",
            enrichment_id, status, attempt, _POLL_MAX,
        )
        if status == "FINISHED":
            result = _parse_enrich_response(poll_data)
            logger.info(
                "FullEnrich enrich: %r at %r → email=%r (%s) phone=%r",
                f"{firstname} {lastname}", identifier,
                result["email"], result["email_status"], result["phone"],
            )
            return result
        if status in ("ERROR", "FAILED", "CANCELLED"):
            logger.warning(
                "FullEnrich enrich job %r ended with status=%r", enrichment_id, status
            )
            return _empty

    logger.warning(
        "FullEnrich enrich job %r timed out after %ds",
        enrichment_id, _POLL_MAX * _POLL_INTERVAL,
    )
    return _empty
