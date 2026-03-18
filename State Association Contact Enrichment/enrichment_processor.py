"""
Enrichment orchestrator.

Invoked as a background task by webhook_server.py after returning 200 to HubSpot.

Routing by hierarchy_type:
  "Individual Facility / Child"  → Workflow 1: research THIS facility for a missing
                                   Administrator or Executive Director via state
                                   association sites.
  "Corporation / Operator"       → Workflow 2: research Definitive Healthcare corporate-
                                   level contacts that aren't assigned to a facility.

Flow:
  1. Load company properties from HubSpot
  2. 30-day guard — skip if enriched recently
  3. Init BigQuery client
  4. Route: child facility → W1 single-facility research
            parent corp    → W2 Definitive corporate contacts
  5. For each found contact: create in HubSpot or add association
  6. Write summary note on company record
  7. Update revops_enrichment_date + reset request_to_enrich
"""

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pandas as pd

from email_verifier import verify_email_millionverifier
from fullenrich_client import enrich_contact_info, search_facility_contacts
from hubspot_client import HubSpotClient, HubSpotError
from state_association_matcher import (
    _build_facility_research_prompt,
    call_openrouter,
    find_facilities_missing_leadership,
    lookup_hubspot_contact_enhanced,
    parse_found_name,
    verify_contact_in_definitive,
    workflow_2_research_contacts,
    RATE_LIMIT_SECONDS,
    RESEARCH_SYSTEM_PROMPT,
)
from utils.bigquery_client import init_bigquery_client

logger = logging.getLogger(__name__)

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "gen-lang-client-0844868008")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Titles that indicate a corporate-level executive → associate with parent corp
CORPORATE_TITLE_KEYWORDS = {
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
}

CHILD_HIERARCHY_TYPE = "Individual Facility / Child"
CORP_HIERARCHY_TYPE = "Corporation / Operator"

# Titles searched by FullEnrich when Sonar Pro + DH both find nothing (Workflow 1)
LEADERSHIP_TITLES = ["Administrator", "Executive Director", "Director of Nursing"]

# Corporate titles searched by FullEnrich as a supplement in Workflow 2.
# Must mirror the intent of DH_W2_TITLE_PATTERN in state_association_matcher.py
# so the fallback searches for the same cohort as the primary DH query.
W2_CORPORATE_TITLES = [
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


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class ContactAction:
    """Records what was done for a single found contact."""

    action: str          # "created_new" | "associated_existing" | "no_contact_found" | "error"
    contact_id: str = ""
    contact_name: str = ""
    contact_title: str = ""
    facility_name: str = ""
    target_company_id: str = ""
    detail: str = ""
    source_urls: list[str] = field(default_factory=list)
    # Research metadata — used to build the salesperson-readable note
    confidence: str = ""        # "high" | "medium" | "low" | "not_found"
    source_tier: str = ""       # "1" | "2" | "3" — state association tier
    found_email: str = ""
    found_phone: str = ""
    found_linkedin: str = ""
    email_validation_status: str = ""   # VERIFIED | INVALID | UNKNOWN | RISKY | UNVERIFIED | ERROR
    email_validation_quality: str = ""  # good | ok | bad | ""
    fe_email_found: bool = False        # True when FullEnrich Bulk Enrich contributed the email
    research_reasoning: str = ""        # Sonar Pro explanation of why this contact was identified
    research_source_name: str = ""      # Human-readable name of the source (e.g. "WA HCAP directory")
    source_path: str = ""               # "existing_contact" | "sonar_pro" | "fullenrich_people_search"
    fe_people_search_ran: bool = False  # True when FE People Search ran but found nothing
    dh_verified: bool = False           # True when DH confirms this person at this facility
    dh_verification_detail: str = ""    # Human-readable DH verification outcome
    dh_last_update: str = ""            # Raw DH LAST_UPDATE date string (YYYY-MM-DD)
    fe_updated_at: str = ""             # FullEnrich profile last-updated date (ISO string)


# ── Title routing ─────────────────────────────────────────────────────────────


def _is_corporate_title(title: str) -> bool:
    """Return True if the title is a corporate-level role."""
    t = (title or "").lower()
    return any(kw in t for kw in CORPORATE_TITLE_KEYWORDS)


def _association_target(
    title: str,
    child_company_id: str,
    parent_company_id: str,
) -> str:
    """
    Route found contacts to the right company record based on their title.

    Corporate titles (COO, CEO, CFO, VP, Regional Director, …) → parent corp.
    Facility titles (Administrator, Executive Director, DON, …) → child facility.
    Falls back to parent if child ID is missing.
    """
    if _is_corporate_title(title):
        return parent_company_id
    return child_company_id or parent_company_id


# ── Helpers ───────────────────────────────────────────────────────────────────


def _extract_urls(text: str) -> list[str]:
    """Extract unique http(s) URLs from a block of text."""
    if not text:
        return []
    urls = re.findall(r"https?://[^\s\)\]>\"']+", text)
    seen: set[str] = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' into (first, last). Returns ('', '') on failure."""
    parts = (full_name or "").strip().split()
    if not parts:
        return "", ""
    first = parts[0]
    last = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first, last


def _names_match(found_name: str, hs_firstname: str, hs_lastname: str) -> bool:
    """Case-insensitive first+last name comparison."""
    first, last = _split_name(found_name)
    return (
        first.lower() == (hs_firstname or "").strip().lower()
        and last.lower() == (hs_lastname or "").strip().lower()
    )


# ── Source recency helpers ────────────────────────────────────────────────────

_MONTH_MAP: dict[str, int] = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}

_MONTH_PATTERN = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december"
    r"|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)[.,\s]+(\d{4})\b",
    re.IGNORECASE,
)
_ISO_DATE_PATTERN = re.compile(r"\b(20\d{2})-(0[1-9]|1[0-2])-\d{2}\b")


def _extract_most_recent_date(text: str) -> datetime | None:
    """Return the most recent month+year date mentioned in *text*, or None."""
    if not text:
        return None
    dates: list[datetime] = []
    for m in _MONTH_PATTERN.finditer(text):
        month_num = _MONTH_MAP.get(m.group(1).lower())
        year = int(m.group(2))
        if month_num and 2015 <= year <= 2035:
            dates.append(datetime(year, month_num, 1))
    for m in _ISO_DATE_PATTERN.finditer(text):
        dates.append(datetime(int(m.group(1)), int(m.group(2)), 1))
    return max(dates) if dates else None


def _is_source_stale(reasoning: str) -> tuple[bool, str]:
    """
    Return (stale, label) where stale=True when the most recent date cited
    in *reasoning* is more than 12 months ago.

    label is a human-readable string like "January 2024".
    """
    d = _extract_most_recent_date(reasoning)
    if d is None:
        return False, ""
    cutoff = datetime.now() - timedelta(days=365)
    if d < cutoff:
        label = d.strftime("%B %Y")
        return True, label
    return False, ""


# ── Data freshness property helper ───────────────────────────────────────────


def _compute_data_freshness(
    source_path: str,
    research_source_name: str,
    research_reasoning: str,
    dh_verified: bool,
    dh_last_update: str,
    fe_updated_at: str,
) -> str:
    """
    Build the value for the HubSpot contact property
    'data_freshness_according_to_definitive'.

    Encodes which system identified the contact and how recently
    that system verified them at this facility.
    """
    if source_path == "definitive_healthcare":
        # Fast-tracked directly from DH without Sonar Pro verification
        if dh_last_update:
            return f"Definitive Healthcare — last updated {dh_last_update}"
        return "Definitive Healthcare — update date not available"

    if source_path == "sonar_pro":
        d = _extract_most_recent_date(research_reasoning)
        source_label = research_source_name if research_source_name else "web search"
        if d:
            return f"{source_label} ({d.strftime('%B %Y')})"
        return f"{source_label} — source date not available"

    if source_path == "fullenrich_people_search":
        if fe_updated_at:
            # ISO date like "2024-08-15" → "August 2024"
            try:
                parts = fe_updated_at[:10].split("-")
                if len(parts) == 3:
                    d = datetime(int(parts[0]), int(parts[1]), 1)
                    date_label = d.strftime("%B %Y")
                else:
                    date_label = fe_updated_at[:10]
            except (ValueError, IndexError):
                date_label = fe_updated_at[:10]
            return f"FullEnrich People Search / LinkedIn (profile updated {date_label})"
        return "FullEnrich People Search / LinkedIn — profile date not available"

    if source_path == "existing_contact":
        if dh_verified and dh_last_update:
            return f"Definitive Healthcare — last updated {dh_last_update}"
        if dh_verified:
            return "Definitive Healthcare — confirmed (update date not available)"
        if dh_last_update:
            # In DH but update date doesn't help if not confirmed at this facility
            return "Definitive Healthcare — not confirmed at this facility"
        return "HubSpot existing record — not confirmed in Definitive Healthcare"

    return ""


# ── Note helpers — all output HTML (HubSpot renders note body as HTML) ────────


def _email_verification_line(email: str, ev: dict, source: str = "existing", fe_email_status: str = "") -> str:
    """HTML block for an email verification result."""
    mv_status = ev.get("status", "")
    mv_text = {
        "VERIFIED":   "✓ Verified — inbox confirmed deliverable",
        "INVALID":    "✗ Invalid — address does not exist or will bounce",
        "RISKY":      "~ Risky — catch-all domain; deliverability uncertain",
        "UNKNOWN":    "? Unknown — mail server blocked SMTP probe",
        "UNVERIFIED": "Not checked (API key not configured)",
        "ERROR":      "Error — validation attempt failed",
    }.get(mv_status, mv_status)

    src = "Existing HubSpot record" if source == "existing" else "Research finding"
    h = (
        f"<b>Email:</b> {email}<br>"
        f"<b>Source:</b> {src}<br>"
        f"<b>MillionVerifier:</b> {mv_text}<br>"
    )
    if mv_status == "UNKNOWN":
        h += "<em>Microsoft 365 / Exchange Online blocks SMTP verification probes as anti-spam protection — Unknown does not mean the email is invalid.</em><br>"
    if fe_email_status:
        fe_text = {"DELIVERABLE": "✓ Deliverable", "NOT_DELIVERABLE": "✗ Not deliverable"}.get(fe_email_status, fe_email_status)
        h += f"<b>FullEnrich:</b> {fe_text}<br>"
    if ev.get("role"):
        h += "<em>⚠ Role-based address (e.g. admin@, info@) — lower deliverability confidence</em><br>"
    if ev.get("free"):
        h += "<em>⚠ Free/disposable email provider</em><br>"
    if ev.get("didyoumean"):
        h += f"<em>⚠ Possible typo — did you mean: {ev['didyoumean']}?</em><br>"
    return h


def _email_added_line(email: str, fe_info: dict, ev: dict) -> str:
    """HTML block for an email discovered by FullEnrich and added to HubSpot."""
    fe_status = fe_info.get("email_status", "")
    fe_text = {"DELIVERABLE": "✓ Deliverable", "NOT_DELIVERABLE": "✗ Not deliverable"}.get(fe_status, fe_status)
    mv_status = ev.get("status", "")
    mv_text = {
        "VERIFIED": "✓ Verified", "INVALID": "✗ Invalid", "RISKY": "~ Risky",
        "UNKNOWN": "? Unknown (M365 blocks SMTP probe — likely valid)",
        "UNVERIFIED": "Not checked", "ERROR": "Error",
    }.get(mv_status, mv_status)
    return (
        f"<b>✚ Email added:</b> {email}<br>"
        f"<b>Source:</b> FullEnrich waterfall email enrichment<br>"
        f"<b>FullEnrich:</b> {fe_text}<br>"
        f"<b>MillionVerifier:</b> {mv_text}<br>"
    )


def _collect_additional_emails(fe_info: dict, primary_email: str) -> str:
    """
    Return a semicolon-delimited string of DELIVERABLE alternate emails from
    FullEnrich's all_emails list, excluding the primary email already written
    to the contact's email property.

    HubSpot stores these in the hs_additional_emails property and uses them
    for deduplication and display alongside the primary address.
    """
    primary = (primary_email or "").strip().lower()
    extras = [
        e["email"]
        for e in (fe_info.get("all_emails") or [])
        if e.get("email")
        and e.get("status") == "DELIVERABLE"
        and e["email"].strip().lower() != primary
    ]
    return ";".join(extras)


def _phone_added_line(fe_info: dict) -> str:
    """HTML block for a phone number discovered by FullEnrich and added to HubSpot."""
    phone = fe_info.get("phone", "")
    region = fe_info.get("phone_region", "")
    display = f"{phone} ({region})" if region else phone
    h = (
        f"<b>✚ Phone added:</b> {display}<br>"
        f"<b>Source:</b> FullEnrich professional phone data<br>"
    )
    others = [p for p in (fe_info.get("all_phones") or []) if p.get("number") and p["number"] != phone]
    if others:
        alt_list = ", ".join(
            f"{p['number']} ({p['region']})" if p.get("region") else p["number"]
            for p in others[:4]
        )
        h += f"<b>Additional numbers found:</b> {alt_list}<br>"
    return h


def _build_contact_enrichment_note(
    contact_name: str,
    contact_title: str,
    facility_name: str,
    note_lines: list[str],
    fe_info: dict,
    research_reasoning: str = "",
) -> str:
    """HTML enrichment note written directly on a contact record."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    has_changes = any("✚" in line or "✗" in line for line in note_lines)
    section = "Email Check &amp; Changes Made" if has_changes else "Email Deliverability Check"

    h = (
        f"<b>RevOps Enrichment &middot; {contact_name}</b><br>"
        f"<b>Run date:</b> {now_str} &nbsp;&middot;&nbsp; "
        f"<b>Title:</b> {contact_title} &nbsp;&middot;&nbsp; "
        f"<b>Facility:</b> {facility_name}<br><br>"
    )
    if research_reasoning:
        h += f"<b>Why This Contact</b><br>{research_reasoning}<br><br>"
    h += f"<b>{section}</b><br>"
    for entry in note_lines:
        h += entry  # each helper already returns HTML

    h += (
        "<br><b>About These Results</b><br>"
        "MillionVerifier verifies email by connecting to the mail server via SMTP. "
        "<b>Microsoft 365 and Exchange Online</b> block this probe as anti-spam protection, "
        "returning an Unknown result. This is normal for corporate email — it does <b>not</b> "
        "mean the address is bad.<br>"
    )
    if fe_info.get("email"):
        h += (
            "FullEnrich uses a multi-provider waterfall (LinkedIn data, partner databases, warm SMTP pools) "
            "to independently verify deliverability. <b>Deliverable from FullEnrich is a strong positive "
            "signal</b> even when MillionVerifier returns Unknown.<br>"
        )

    extras = [
        p for p in (fe_info.get("all_phones") or [])
        if p.get("number") and p["number"] != fe_info.get("phone", "")
    ]
    if extras:
        h += "<br><b>Additional Phone Numbers (FullEnrich)</b><br>Use as fallback if the primary doesn't connect:<ul>"
        for p in extras[:5]:
            region = f" ({p['region']})" if p.get("region") else ""
            h += f"<li>{p['number']}{region}</li>"
        h += "</ul>"

    return h


# ── Contact processing ────────────────────────────────────────────────────────


def _process_found_contact(
    hs: HubSpotClient,
    found_name: str,
    found_title: str,
    found_email: str,
    found_phone: str,
    confidence: str,
    pre_populated_contact_ids: str,
    target_company_id: str,
    facility_name: str,
    research_findings: str,
    source_tier: str = "",
    found_linkedin: str = "",
    facility_domain: str = "",
    research_reasoning: str = "",
    source_path: str = "",
    fe_people_search_ran: bool = False,
    dh_verified: bool = False,
    dh_verification_detail: str = "",
    dh_last_update: str = "",
    fe_updated_at: str = "",
    research_source_name: str = "",
) -> ContactAction:
    """
    Decide whether to create a new HubSpot contact or associate an existing one.
    """
    source_urls = _extract_urls(research_findings)[:20]

    # ── Email validation (MillionVerifier) ────────────────────────────────────
    # email_to_use is cleared when the email is definitively INVALID so it is
    # never written to HubSpot.  found_email is preserved in _base for the note.
    email_validation: dict = {}
    email_to_use = found_email

    if found_email:
        email_validation = verify_email_millionverifier(found_email)
        if email_validation.get("status") == "INVALID":
            logger.info(
                "Email %r failed MillionVerifier (quality=%r) — suppressing from HubSpot",
                found_email,
                email_validation.get("quality"),
            )
            email_to_use = ""

    _base = dict(
        confidence=confidence,
        source_tier=source_tier,
        found_email=found_email,
        found_phone=found_phone,
        found_linkedin=found_linkedin,
        source_urls=source_urls,
        facility_name=facility_name,
        target_company_id=target_company_id,
        email_validation_status=email_validation.get("status", ""),
        email_validation_quality=email_validation.get("quality", ""),
        research_reasoning=research_reasoning,
        source_path=source_path,
        fe_people_search_ran=fe_people_search_ran,
        dh_verified=dh_verified,
        dh_verification_detail=dh_verification_detail,
        dh_last_update=dh_last_update,
        fe_updated_at=fe_updated_at,
        research_source_name=research_source_name,
    )

    # Compute data freshness value once — written to HubSpot on both create + update paths
    data_freshness_value = _compute_data_freshness(
        source_path=source_path,
        research_source_name=research_source_name,
        research_reasoning=research_reasoning,
        dh_verified=dh_verified,
        dh_last_update=dh_last_update,
        fe_updated_at=fe_updated_at,
    )

    if not found_name or confidence == "not_found":
        return ContactAction(action="no_contact_found", detail="No contact identified by research", **_base)

    first, last = _split_name(found_name)
    if not first:
        return ContactAction(
            action="no_contact_found",
            detail=f"Could not parse name: '{found_name}'",
            **_base,
        )

    # ── Pre-populated IDs from BigQuery HubSpot lookup (W1) ──────────────────
    matched_contact_id: str = ""
    matched_detail: str = ""

    existing_ids = [x for x in (pre_populated_contact_ids or "").split("|") if x]
    for cid in existing_ids:
        try:
            hs.associate_contact_to_company(cid, target_company_id)
            matched_contact_id = cid
            matched_detail = f"Added association to existing contact #{cid}"
            break
        except HubSpotError as exc:
            logger.warning(f"Association failed for contact {cid}: {exc}")

    # ── Live email search (W2 and fallback for W1) ────────────────────────────
    if not matched_contact_id and email_to_use:
        existing = hs.search_contacts_by_email(email_to_use)
        for contact in existing:
            if _names_match(found_name, contact["firstname"], contact["lastname"]):
                try:
                    hs.associate_contact_to_company(contact["id"], target_company_id)
                    matched_contact_id = contact["id"]
                    matched_detail = f"Email match → added association to existing #{contact['id']}"
                    break
                except HubSpotError as exc:
                    logger.warning(f"Email-match association failed: {exc}")

    # ── Post-association: verify/enrich email + phone on existing contact ─────
    if matched_contact_id:
        enriched_email = found_email
        enriched_email_status = email_validation.get("status", "")
        enriched_email_quality = email_validation.get("quality", "")
        enriched_phone = found_phone

        # Track what actually changed so we can write a meaningful note
        note_lines: list[str] = []
        fe_info: dict = {}   # FullEnrich result (populated if called)

        try:
            existing_props = hs.get_contact_properties(matched_contact_id)
            existing_email = existing_props.get("email", "")
            existing_phone = existing_props.get("phone", "")

            # ── 30-day guard: skip MV + FullEnrich if enriched recently ───────
            last_enriched_raw = existing_props.get("most_recent_enrichment_date", "")
            if last_enriched_raw:
                try:
                    raw = str(last_enriched_raw).strip()
                    if raw.isdigit():
                        last_dt = datetime.utcfromtimestamp(int(raw) / 1000)
                    else:
                        last_dt = datetime.strptime(raw[:10], "%Y-%m-%d")
                    days_since = (datetime.utcnow() - last_dt).days
                    if days_since < 30:
                        logger.info(
                            f"Skipping MV/FullEnrich for contact {matched_contact_id}: "
                            f"enriched {days_since} day(s) ago (< 30 day threshold)"
                        )
                        return ContactAction(
                            action="associated_existing",
                            contact_id=matched_contact_id,
                            contact_name=found_name,
                            contact_title=found_title,
                            detail=matched_detail + " (skipped re-enrichment — within 30-day window)",
                            **_base,
                        )
                except (ValueError, TypeError) as exc:
                    logger.warning(
                        f"Could not parse most_recent_enrichment_date={last_enriched_raw!r} "
                        f"for contact {matched_contact_id}: {exc}"
                    )

            # ── Always document email verification status ─────────────────────
            # MV ran at the top of this function when found_email was supplied
            # (e.g. from _enrich_existing_leadership). Surface that result in
            # the contact note regardless of whether anything else changes.
            if enriched_email and email_validation:
                note_lines.append(
                    _email_verification_line(enriched_email, email_validation, source="existing")
                )
                if email_validation.get("status") == "INVALID":
                    logger.info(
                        "Existing contact %s email %r failed MillionVerifier — keeping but flagging",
                        matched_contact_id, enriched_email,
                    )

            # ── Verify email that exists only on the HubSpot record ───────────
            # (found_email was empty, so MV wasn't run at the top of the function)
            elif not enriched_email and existing_email:
                ev = verify_email_millionverifier(existing_email)
                enriched_email = existing_email
                enriched_email_status = ev.get("status", "")
                enriched_email_quality = ev.get("quality", "")
                note_lines.append(_email_verification_line(existing_email, ev, source="existing"))
                if ev.get("status") == "INVALID":
                    logger.info(
                        "Existing contact %s email %r failed MillionVerifier — keeping but flagging",
                        matched_contact_id, existing_email,
                    )

            if not enriched_phone and existing_phone:
                enriched_phone = existing_phone

            existing_linkedin = existing_props.get("hs_linkedin_url", "")

            # ── Write data freshness property ─────────────────────────────────
            if data_freshness_value:
                try:
                    hs.update_contact(
                        matched_contact_id,
                        {"data_freshness_according_to_definitive": data_freshness_value},
                    )
                except HubSpotError as exc:
                    logger.warning(
                        "Could not write data_freshness_according_to_definitive on contact %s: %s",
                        matched_contact_id, exc,
                    )

            # ── FullEnrich: find missing email and/or phone ───────────────────
            if not existing_email or not existing_phone:
                fe_first, fe_last = _split_name(found_name)
                fe_info = enrich_contact_info(
                    firstname=fe_first,
                    lastname=fe_last,
                    company_name=facility_name,
                    domain=facility_domain,
                    linkedin_url=found_linkedin,
                )
                hs_updates: dict = {}

                if not existing_email and fe_info.get("email"):
                    new_email = fe_info["email"]
                    ev = verify_email_millionverifier(new_email)
                    enriched_email_status = ev.get("status", "")
                    enriched_email_quality = ev.get("quality", "")
                    if ev.get("status") != "INVALID":
                        hs_updates["email"] = new_email
                        enriched_email = new_email
                        note_lines.append(
                            _email_added_line(new_email, fe_info, ev)
                        )
                    else:
                        logger.info(
                            "FullEnrich email %r for contact %s failed MillionVerifier — suppressed",
                            new_email, matched_contact_id,
                        )
                        note_lines.append(
                            f"<b>✗ Email suppressed:</b> {new_email}<br>"
                            f"<b>Reason:</b> FullEnrich found this address but MillionVerifier flagged it as INVALID<br>"
                        )

                if not existing_phone and fe_info.get("phone"):
                    hs_updates["phone"] = fe_info["phone"]
                    enriched_phone = fe_info["phone"]
                    note_lines.append(_phone_added_line(fe_info))

                # Additional DELIVERABLE emails → hs_additional_emails
                primary_for_extras = hs_updates.get("email") or existing_email
                alt_emails = _collect_additional_emails(fe_info, primary_for_extras)
                if alt_emails:
                    hs_updates["hs_additional_emails"] = alt_emails

                if hs_updates:
                    hs.update_contact(matched_contact_id, hs_updates)

            # ── Write LinkedIn URL if we have it and the contact doesn't ──────
            linkedin_to_write = found_linkedin or fe_info.get("linkedin_url", "")
            if linkedin_to_write and not existing_linkedin:
                try:
                    hs.update_contact(matched_contact_id, {"hs_linkedin_url": linkedin_to_write})
                    source = "FullEnrich People Search" if found_linkedin else "FullEnrich Bulk Enrich"
                    note_lines.append(
                        f"<b>✚ LinkedIn added:</b> <a href=\"{linkedin_to_write}\">{linkedin_to_write}</a><br>"
                        f"<b>Source:</b> {source}<br>"
                    )
                except HubSpotError as exc:
                    logger.warning(
                        "Could not write hs_linkedin_url for contact %s: %s",
                        matched_contact_id, exc,
                    )

            # ── Stamp most_recent_enrichment_date ────────────────────────────
            try:
                hs.update_contact(
                    matched_contact_id,
                    {"most_recent_enrichment_date": str(HubSpotClient.epoch_ms_today_utc())},
                )
            except HubSpotError as exc:
                logger.warning(
                    "Could not set most_recent_enrichment_date on contact %s: %s",
                    matched_contact_id, exc,
                )

            # ── Write enrichment note on the contact record ───────────────────
            # Always write when we have anything to report (MV check alone counts)
            if note_lines or enriched_email:
                try:
                    contact_note = _build_contact_enrichment_note(
                        contact_name=found_name,
                        contact_title=found_title,
                        facility_name=facility_name,
                        note_lines=note_lines,
                        fe_info=fe_info,
                        research_reasoning=research_reasoning,
                    )
                    hs.create_note_on_contact(matched_contact_id, contact_note)
                except HubSpotError as exc:
                    logger.warning(
                        "Could not write enrichment note on contact %s: %s",
                        matched_contact_id, exc,
                    )

        except HubSpotError as exc:
            logger.warning(
                "Post-association enrichment failed for contact %s: %s",
                matched_contact_id, exc,
            )

        _base.update(
            found_email=enriched_email,
            found_phone=enriched_phone,
            email_validation_status=enriched_email_status,
            email_validation_quality=enriched_email_quality,
        )
        return ContactAction(
            action="associated_existing",
            contact_id=matched_contact_id,
            contact_name=found_name,
            contact_title=found_title,
            detail=matched_detail,
            **_base,
        )

    # ── Create new contact ────────────────────────────────────────────────────
    contact_props: dict = {"firstname": first, "lastname": last, "jobtitle": found_title or ""}
    if email_to_use:
        contact_props["email"] = email_to_use.strip()
    if found_phone:
        contact_props["phone"] = found_phone.strip()
    if found_linkedin:
        contact_props["hs_linkedin_url"] = found_linkedin.strip()

    try:
        new_id = hs.create_contact(contact_props)
        hs.associate_contact_to_company(new_id, target_company_id)
    except HubSpotError as exc:
        logger.error(
            f"Failed to create contact for '{found_name}': {exc} | "
            f"props={contact_props} | response={exc.response_text}"
        )
        return ContactAction(
            action="error",
            contact_name=found_name,
            contact_title=found_title,
            detail=f"HubSpot API error: {exc}",
            **_base,
        )

    # ── FullEnrich bulk enrich on the new contact (fill missing email/phone) ─
    # Runs whenever the contact was created without a complete email + phone.
    new_note_lines: list[str] = []
    new_fe_info: dict = {}
    if not email_to_use or not found_phone:
        new_fe_info = enrich_contact_info(
            firstname=first,
            lastname=last,
            company_name=facility_name,
            domain=facility_domain,
            linkedin_url=found_linkedin,
        )
        new_updates: dict = {}

        if not email_to_use and new_fe_info.get("email"):
            new_email = new_fe_info["email"]
            ev = verify_email_millionverifier(new_email)
            if ev.get("status") != "INVALID":
                new_updates["email"] = new_email
                _base["found_email"] = new_email
                _base["email_validation_status"] = ev.get("status", "")
                _base["email_validation_quality"] = ev.get("quality", "")
                _base["fe_email_found"] = True
                new_note_lines.append(_email_added_line(new_email, new_fe_info, ev))
            else:
                new_note_lines.append(
                    f"<b>✗ Email suppressed:</b> {new_email}<br>"
                    f"<b>Reason:</b> FullEnrich found this address but MillionVerifier flagged it as INVALID<br>"
                )

        if not found_phone and new_fe_info.get("phone"):
            new_updates["phone"] = new_fe_info["phone"]
            _base["found_phone"] = new_fe_info["phone"]
            new_note_lines.append(_phone_added_line(new_fe_info))

        # Additional DELIVERABLE emails → hs_additional_emails
        primary_for_extras = new_updates.get("email") or email_to_use
        alt_emails = _collect_additional_emails(new_fe_info, primary_for_extras)
        if alt_emails:
            new_updates["hs_additional_emails"] = alt_emails

        # LinkedIn from bulk enrich — only needed when People Search didn't supply one
        if not found_linkedin and new_fe_info.get("linkedin_url"):
            new_updates["hs_linkedin_url"] = new_fe_info["linkedin_url"]
            new_note_lines.append(
                f"<b>✚ LinkedIn added:</b> <a href=\"{new_fe_info['linkedin_url']}\">{new_fe_info['linkedin_url']}</a><br>"
                f"<b>Source:</b> FullEnrich Bulk Enrich<br>"
            )

        if data_freshness_value:
            new_updates["data_freshness_according_to_definitive"] = data_freshness_value

        if new_updates:
            try:
                hs.update_contact(new_id, new_updates)
            except HubSpotError as exc:
                logger.warning(f"Post-creation FullEnrich update failed for {new_id}: {exc}")

    elif data_freshness_value:
        # No FullEnrich run (contact was created with full email+phone already) —
        # still write freshness on its own
        try:
            hs.update_contact(new_id, {"data_freshness_according_to_definitive": data_freshness_value})
        except HubSpotError as exc:
            logger.warning(f"Could not write data_freshness_according_to_definitive on new contact {new_id}: {exc}")

    # ── Stamp most_recent_enrichment_date on the new contact ─────────────────
    try:
        hs.update_contact(
            new_id,
            {"most_recent_enrichment_date": str(HubSpotClient.epoch_ms_today_utc())},
        )
    except HubSpotError as exc:
        logger.warning(f"Could not set most_recent_enrichment_date on new contact {new_id}: {exc}")

    if new_note_lines:
        try:
            contact_note = _build_contact_enrichment_note(
                contact_name=found_name,
                contact_title=found_title,
                facility_name=facility_name,
                note_lines=new_note_lines,
                fe_info=new_fe_info,
                research_reasoning=research_reasoning,
            )
            hs.create_note_on_contact(new_id, contact_note)
        except HubSpotError as exc:
            logger.warning(f"Could not write enrichment note on new contact {new_id}: {exc}")

    return ContactAction(
        action="created_new",
        contact_id=new_id,
        contact_name=found_name,
        contact_title=found_title,
        detail=f"Created new contact #{new_id} and associated with company {target_company_id}",
        **_base,
    )


# ── Leadership title matching ──────────────────────────────────────────────────

# Regex patterns that identify admin/ED/DON titles (mirrors the BigQuery filter)
_LEADERSHIP_TITLE_RE = re.compile(
    r"executive director"
    r"|administrator"
    r"|director of nursing"
    r"|^admin$|^admin[/,]|\sadmin$|\sadmin[/,]"
    r"|^don$|^don[/,]|\sdon$|\sdon[/,]",
    re.IGNORECASE,
)


def _is_leadership_title(title: str) -> bool:
    """Return True if *title* matches admin / Executive Director / DON patterns."""
    return bool(_LEADERSHIP_TITLE_RE.search((title or "").strip()))


# ── Existing leadership enrichment ────────────────────────────────────────────


def _enrich_existing_leadership(
    hs: HubSpotClient,
    company_id: str,
    props: dict,
    parent_corp_name: str,
    bq_client=None,
) -> list[dict]:
    """
    Called when the facility already has an Admin/ED in HubSpot.

    Fetches associated contacts, filters for leadership titles, and returns
    result dicts for each one so they flow through _process_found_contact →
    associated_existing → email/phone enrichment.

    Returns [] if no leadership contacts can be fetched from HubSpot.
    """
    facility_name = props.get("name", "")
    city          = props.get("city", "") or ""
    state         = props.get("state", "") or ""
    facility_type = props.get("facility_type", "") or ""
    definitive_id = props.get("definitive_healthcare_id", "") or ""
    parent_id     = props.get("hs_parent_company_id", "") or ""

    try:
        all_contacts = hs.get_associated_contacts(company_id)
    except Exception as exc:
        logger.warning(f"Could not fetch contacts for {facility_name}: {exc}")
        return []

    leadership = [c for c in all_contacts if _is_leadership_title(c["jobtitle"])]
    if not leadership:
        logger.info(f"  No leadership contacts returned by HubSpot for {facility_name}")
        return []

    logger.info(
        f"  Fetched {len(leadership)} existing leadership contact(s) for {facility_name} — "
        "will check/enrich email and phone"
    )

    results: list[dict] = []
    for c in leadership:
        full_name = f"{c['firstname']} {c['lastname']}".strip()

        # ── Definitive Healthcare verification ────────────────────────────────
        dh_verified = False
        dh_verification_detail = ""
        dh_last_update_val = ""
        if bq_client and definitive_id:
            dh = verify_contact_in_definitive(
                client=bq_client,
                first_name=c["firstname"],
                last_name=c["lastname"],
                definitive_hospital_id=definitive_id,
            )
            if dh.get("error") == "query_failed":
                dh_verification_detail = "DH lookup unavailable (query error)"
            elif dh["confirmed"]:
                dh_verified = True
                dh_last_update_val = dh.get("dh_last_update", "")
                table_label = {"executives": "Executives table", "full_feed": "Full Feed"}.get(
                    dh["source_table"], dh["source_table"]
                )
                update_str = f", updated {dh_last_update_val}" if dh_last_update_val else ""
                dh_verification_detail = f"✓ Confirmed in DH {table_label}{update_str}"
                logger.info(
                    "DH confirmed %s at %s (%s)",
                    full_name, facility_name, dh["source_table"],
                )
            else:
                dh_verification_detail = "✗ Not found in Definitive Healthcare — verify this contact is still current"
                logger.info(
                    "DH: %s not found at HOSPITAL_ID=%s (%s)",
                    full_name, definitive_id, facility_name,
                )
        elif not definitive_id:
            dh_verification_detail = "Facility has no Definitive Healthcare ID — cannot verify"
        else:
            dh_verification_detail = "DH check skipped (BigQuery unavailable)"

        results.append({
            "workflow":               "1 - Facility Missing Contact",
            "hubspot_facility_id":    company_id,
            "facility_name":          facility_name,
            "city":                   city,
            "state":                  state,
            "facility_type":          facility_type,
            "definitive_id":          definitive_id,
            "corporation_name":       parent_corp_name,
            "hubspot_corp_id":        parent_id,
            "found_name":             full_name,
            "found_title":            c["jobtitle"],
            "found_email":            c["email"],
            "found_phone":            c["phone"],
            "research_confidence":    "high",
            "source_tier":            "",
            "source_path":            "existing_contact",
            "dh_verified":            dh_verified,
            "dh_verification_detail": dh_verification_detail,
            "dh_last_update":         dh_last_update_val,
            "hubspot_contact_ids":    c["id"],
            "hubspot_match_detail":   "already associated",
            "hubspot_match_summary":  "existing",
            "research_findings":      "",
            "research_error":         "",
            "researched_at":          datetime.now().isoformat(),
        })
    return results


# ── Workflow 1 — single facility ──────────────────────────────────────────────


def _run_workflow1_single_facility(
    bq_client,
    hs: HubSpotClient,
    company_id: str,
    props: dict,
    parent_corp_name: str,
    missing: "pd.DataFrame | None" = None,
) -> list[dict]:
    """
    Run Workflow 1 research on a single child facility.

    Checks whether the facility already has an Administrator or Executive Director
    in HubSpot (unless `missing` is pre-supplied by the caller). If not found,
    calls OpenRouter to find one via state association sites.

    Returns a list of result dicts matching the schema of
    workflow_1_research_facilities() so the rest of the pipeline is identical.
    """
    facility_name = props.get("name", "Unknown Facility")
    city = props.get("city", "") or ""
    state = props.get("state", "") or ""
    address = props.get("address", "") or ""
    facility_type = props.get("facility_type", "") or ""
    definitive_id = props.get("definitive_healthcare_id", "") or ""
    website = props.get("website", "") or ""

    logger.info(f"W1 single-facility: {facility_name} ({city}, {state})")

    # Build a single-row DataFrame matching the schema load_hubspot_child_facilities returns
    facility_df = pd.DataFrame([{
        "hubspot_id": company_id,
        "facility_name": facility_name,
        "facility_type": facility_type,
        "hierarchy_type": props.get("hierarchy_type", ""),
        "city": city,
        "state": state,
        "address": "",
        "zip": "",
        "phone": "",
        "definitive_id": definitive_id,
    }])

    # Check whether this facility already has an Admin/ED in HubSpot
    # (use pre-computed result if caller ran this concurrently)
    if missing is None:
        missing = find_facilities_missing_leadership(bq_client, facility_df)
    if missing.empty:
        logger.info(f"  ✅ {facility_name} already has an Admin/ED — verifying against DH + checking email/phone")
        return _enrich_existing_leadership(hs, company_id, props, parent_corp_name, bq_client=bq_client)

    # Research the facility
    result: dict = {
        "workflow": "1 - Facility Missing Contact",
        "hubspot_facility_id": company_id,
        "facility_name": facility_name,
        "city": city,
        "state": state,
        "facility_type": facility_type,
        "definitive_id": definitive_id,
        "corporation_name": parent_corp_name,
        "hubspot_corp_id": props.get("hs_parent_company_id", ""),
        "found_name": "",
        "found_title": "",
        "found_email": "",
        "found_phone": "",
        "research_confidence": "not_found",
        "source_tier": "3",
        "source_path": "sonar_pro",   # Sonar Pro always runs on this path
        "hubspot_contact_ids": "",
        "hubspot_match_detail": "",
        "hubspot_match_summary": "",
        "research_findings": "",
        "research_error": "",
        "researched_at": datetime.now().isoformat(),
    }

    try:
        prompt, source_tier = _build_facility_research_prompt(
            facility_name=facility_name,
            city=city,
            state=state,
            facility_type=facility_type,
            corporation_name=parent_corp_name,
            address=address,
            website=website,
        )
        result["source_tier"] = source_tier

        logger.info(f"  Calling OpenRouter for {facility_name}...")
        research_result = call_openrouter(RESEARCH_SYSTEM_PROMPT, prompt)
        result["research_findings"] = research_result[:2000]
        logger.info(f"  Research complete ({len(research_result)} chars)")

        found_name, found_title, confidence, found_email, found_phone, reasoning, source_name = parse_found_name(
            research_result
        )
        result.update({
            "found_name":             found_name,
            "found_title":            found_title,
            "research_confidence":    confidence,
            "found_email":            found_email,
            "found_phone":            found_phone,
            "research_reasoning":     reasoning,
            "research_source_name":   source_name,
        })

        if found_name and confidence != "not_found":
            logger.info(f"  Found: {found_name} ({found_title}, confidence={confidence})")
            matches, match_strategy = lookup_hubspot_contact_enhanced(
                client=bq_client,
                full_name=found_name,
                email=found_email,
                phone=found_phone,
                facility_hubspot_id=company_id,
                parent_hubspot_id=props.get("hs_parent_company_id", ""),
            )
            from state_association_matcher import _format_hubspot_matches_enhanced
            ids, detail, summary = _format_hubspot_matches_enhanced(
                matches, match_strategy=match_strategy, facility_name=facility_name
            )
            result.update({
                "hubspot_contact_ids": ids,
                "hubspot_match_detail": detail,
                "hubspot_match_summary": summary,
            })
        else:
            logger.info(f"  No Admin/ED found (confidence={confidence})")

    except RuntimeError as exc:
        result["research_error"] = str(exc)
        logger.error(f"  Research failed: {exc}")

    # ── FullEnrich fallback ────────────────────────────────────────────────────
    # If Sonar Pro + DH found nothing, try FullEnrich People Search as a last resort.
    if not result.get("found_name"):
        logger.info(f"  W1 found nothing — trying FullEnrich for {facility_name}")
        fe_contacts = search_facility_contacts(
            company_name=facility_name,
            city=city,
            state=state,
            titles=LEADERSHIP_TITLES,
            domain=website,
        )
        if fe_contacts:
            logger.info(f"  FullEnrich returned {len(fe_contacts)} contact(s) for {facility_name}")
            fe_results = []
            for fe in fe_contacts:
                r = dict(result)
                r.update({
                    "found_name":          fe["full_name"],
                    "found_title":         fe["title"],
                    "research_confidence": "low_fullenrich",
                    "source_tier":         "",   # not found via state directory
                    "source_path":         "fullenrich_people_search",
                    "found_email":         "",
                    "found_phone":         "",
                    "found_linkedin":      fe.get("linkedin_url", ""),
                    "fe_updated_at":       fe.get("updated_at", "") or fe.get("current_start", ""),
                    "research_findings":   "",   # LinkedIn URL shown via found_linkedin, not as a web-search source
                    "research_error":      "",
                    "hubspot_contact_ids": "",
                })
                fe_results.append(r)
            return fe_results
        else:
            logger.info(f"  FullEnrich: no contacts found for {facility_name}")
            result["fe_people_search_ran"] = True

    return [result]


# ── Note builder ──────────────────────────────────────────────────────────────


_CONFIDENCE_LABELS = {
    "high":           "HIGH  — found on official association directory or CMS listing",
    "medium":         "MED   — found on facility/operator website",
    "low":            "LOW   — found on LinkedIn or potentially outdated source",
    "low_fullenrich": "LOW   — found via FullEnrich contact search (LinkedIn-sourced data)",
    "not_found":      "—     — not found",
}

_TIER_LABELS = {
    "1": "Tier 1 — public directory (no login required)",
    "2": "Tier 2 — login-required or members-only directory",
    "3": "Tier 3 — no state directory available; web search only",
}

_ACTION_LABELS = {
    "created_new":       "✚ Created new HubSpot contact",
    "associated_existing": "🔗 Linked to existing HubSpot contact",
    "no_contact_found":  "○ No contact identified",
    "error":             "✗ Error",
}


def _build_note(
    company_name: str,
    workflow_label: str,
    results: list[dict],
    actions: list[ContactAction],
    errors: list[str],
    portal_id: int = 0,
) -> str:
    """
    Build the salesperson-readable HTML summary note for the company record.

    HubSpot renders the note body as HTML, so we use semantic tags for clean
    display in both the collapsed activity-feed preview and the expanded view.
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    n_found = sum(1 for a in actions if a.action not in ("no_contact_found", "error"))
    n_created = sum(1 for a in actions if a.action == "created_new")
    n_associated = sum(1 for a in actions if a.action == "associated_existing")
    n_missed = sum(1 for a in actions if a.action == "no_contact_found")

    # Pull tier info from first result that has it
    tier_str = ""
    for r in results:
        t = str(r.get("source_tier", ""))
        if t:
            tier_str = t
            break

    # ── Header ───────────────────────────────────────────────────────────────
    h = (
        f"<b>RevOps Contact Enrichment &middot; {company_name}</b><br>"
        f"<b>Run date:</b> {now_str} &nbsp;&middot;&nbsp; "
        f"<b>Workflow:</b> {workflow_label}<br><br>"
        f"<b>Summary:</b> {len(results)} researched &nbsp;&middot;&nbsp; "
        f"{n_found} found &nbsp;&middot;&nbsp; "
        f"{n_created} created &nbsp;&middot;&nbsp; "
        f"{n_associated} linked &nbsp;&middot;&nbsp; "
        f"{n_missed} not found<br><br>"
    )

    # ── Research Process (dynamic step outcomes) ──────────────────────────────
    existing_contact_actions = [a for a in actions if a.source_path == "existing_contact"]
    dh_fasttrack_actions     = [a for a in actions if a.source_path == "definitive_healthcare"]
    sonar_pro_actions        = [a for a in actions if a.source_path == "sonar_pro"]
    fe_ps_actions            = [a for a in actions if a.source_path == "fullenrich_people_search"]
    fe_ps_ran_not_found      = any(a.fe_people_search_ran for a in actions)
    fe_people_search         = bool(fe_ps_actions)   # used later for How to Verify
    fe_bulk_enrich_actions   = [a for a in actions if a.fe_email_found]

    h += "<b>Research Process</b><br><ol>"

    if existing_contact_actions:
        # Fast path: facility already had an Admin/ED in HubSpot
        names = ", ".join(
            f"{a.contact_name} ({a.contact_title})"
            for a in existing_contact_actions if a.contact_name
        )
        h += f"<li><b>HubSpot existing contact check</b> &mdash; ✓ Found on file: {names}</li>"

        # DH verification summary across all existing contacts
        confirmed  = [a for a in existing_contact_actions if a.dh_verified]
        unconfirmed = [a for a in existing_contact_actions if not a.dh_verified and a.dh_verification_detail]
        if confirmed and not unconfirmed:
            detail = confirmed[0].dh_verification_detail  # all same outcome — show once
            h += f"<li><b>Definitive Healthcare verification</b> &mdash; {detail}</li>"
        elif unconfirmed and not confirmed:
            detail = unconfirmed[0].dh_verification_detail
            h += f"<li><b>Definitive Healthcare verification</b> &mdash; {detail}</li>"
        elif confirmed or unconfirmed:
            # Mixed — summarise per contact
            parts = "; ".join(
                f"{a.contact_name}: {a.dh_verification_detail}"
                for a in existing_contact_actions if a.dh_verification_detail
            )
            h += f"<li><b>Definitive Healthcare verification</b> &mdash; {parts}</li>"
    elif dh_fasttrack_actions:
        # Workflow 2 fast-track: contacts sourced directly from DH, no Sonar Pro
        names = ", ".join(
            f"{a.contact_name} ({a.contact_title})"
            for a in dh_fasttrack_actions if a.contact_name
        )
        h += (
            f"<li><b>Definitive Healthcare data</b> &mdash; ⚡ Fast-tracked: "
            f"{len(dh_fasttrack_actions)} corporate executive(s) sourced directly from DH "
            f"(C-suite or fresh record — state association search skipped): {names}</li>"
        )
    else:
        # Full research path
        h += "<li><b>Definitive Healthcare data</b> &mdash; checked BigQuery for executives on file</li>"
        if tier_str:
            tier_label = _TIER_LABELS.get(tier_str, f"Tier {tier_str}")
            h += f"<li><b>State association directory</b> &mdash; {tier_label}</li>"
        else:
            h += "<li><b>State association directory</b> &mdash; no state directory available; web search used</li>"

        # Sonar Pro outcome
        sonar_found = [a for a in sonar_pro_actions if a.action in ("created_new", "associated_existing")]
        sonar_missed = (
            any(a.source_path == "sonar_pro" and a.action == "no_contact_found" for a in actions)
            or fe_ps_actions          # FE PS only runs after Sonar Pro finds nothing
            or fe_ps_ran_not_found
        )
        if sonar_found:
            a = sonar_found[0]
            conf_short = {"high": "HIGH", "medium": "MED", "low": "LOW"}.get(a.confidence, a.confidence.upper())
            h += f"<li><b>Sonar Pro web search</b> &mdash; ✓ Found {a.contact_name}, {a.contact_title} ({conf_short} confidence)</li>"
        elif sonar_missed:
            h += "<li><b>Sonar Pro web search</b> &mdash; ✗ No contact identified</li>"
        else:
            h += "<li><b>Sonar Pro web search</b> &mdash; Perplexity live web search</li>"

        # FullEnrich People Search outcome
        if fe_ps_actions:
            fe_names = ", ".join(
                f"{a.contact_name} ({a.contact_title})" for a in fe_ps_actions if a.contact_name
            )
            h += f"<li><b>FullEnrich People Search</b> &mdash; ✓ Found {len(fe_ps_actions)} contact(s): {fe_names}</li>"
        elif fe_ps_ran_not_found:
            h += "<li><b>FullEnrich People Search</b> &mdash; ✗ No contacts found</li>"

    # MillionVerifier outcome (runs when any email is present)
    emails_checked = [a for a in actions if a.email_validation_status and a.action != "error"]
    if emails_checked:
        mv_labels = {
            "VERIFIED":   "✓ Verified — inbox confirmed deliverable",
            "INVALID":    "✗ Invalid — address does not exist",
            "RISKY":      "~ Risky — catch-all domain; deliverability uncertain",
            "UNKNOWN":    "? Unverifiable (M365 blocks SMTP probe — likely valid)",
            "UNVERIFIED": "not checked (API key not configured)",
            "ERROR":      "validation error",
        }
        if len(emails_checked) == 1:
            a = emails_checked[0]
            mv_text = mv_labels.get(a.email_validation_status, a.email_validation_status)
            h += f"<li><b>MillionVerifier</b> &mdash; {mv_text}</li>"
        else:
            parts = "; ".join(
                f"{a.contact_name}: {mv_labels.get(a.email_validation_status, a.email_validation_status)}"
                for a in emails_checked
            )
            h += f"<li><b>MillionVerifier</b> &mdash; {parts}</li>"

    # FullEnrich Bulk Enrich outcome (email enrichment for a found contact)
    if fe_bulk_enrich_actions:
        fe_emails = ", ".join(a.found_email for a in fe_bulk_enrich_actions if a.found_email)
        h += f"<li><b>FullEnrich Bulk Enrich</b> &mdash; ✓ Email found: {fe_emails}</li>"

    h += "</ol><br>"

    # ── Per-contact results ───────────────────────────────────────────────────
    h += "<b>Results</b><br>"
    for action in actions:
        label = action.facility_name or action.contact_name or "Unknown"
        h += f"<b>&bull; {label}</b><br>"

        if action.action in ("created_new", "associated_existing"):
            conf_label = _CONFIDENCE_LABELS.get(action.confidence, action.confidence)
            h += f"<b>Contact:</b> {action.contact_name}, {action.contact_title}<br>"
            h += f"<b>Confidence:</b> {conf_label}<br>"

            # Data completeness summary
            email_check   = "✓" if action.found_email else "✗"
            phone_check   = "✓" if action.found_phone else "✗"
            linkedin_check = "✓" if action.found_linkedin else "✗"
            h += f"<b>Data:</b> Email {email_check} &nbsp;|&nbsp; Phone {phone_check} &nbsp;|&nbsp; LinkedIn {linkedin_check}<br>"

            if action.dh_verification_detail:
                h += f"<b>DH Status:</b> {action.dh_verification_detail}<br>"
            if action.research_reasoning:
                h += f"<b>Why:</b> {action.research_reasoning}<br>"
                stale, stale_label = _is_source_stale(action.research_reasoning)
                if stale:
                    h += f"<em>&#9888; Source may be outdated &mdash; most recent date cited: {stale_label}</em><br>"

            if action.found_email:
                vstatus  = action.email_validation_status
                vquality = action.email_validation_quality
                if vstatus == "VERIFIED":
                    v_suffix = " <em>&#10003; verified</em>"
                elif vstatus == "INVALID":
                    v_suffix = " <em>&#10007; INVALID &mdash; not written to HubSpot</em>"
                elif vstatus == "RISKY":
                    v_suffix = " <em>~ risky/catchall" + (f" &mdash; quality: {vquality}" if vquality else "") + "</em>"
                elif vstatus == "UNKNOWN":
                    v_suffix = " <em>? unverifiable (M365 blocks SMTP probe &mdash; likely valid)</em>"
                elif vstatus == "UNVERIFIED":
                    v_suffix = " <em>(not validated &mdash; API key not configured)</em>"
                elif vstatus == "ERROR":
                    v_suffix = " <em>(validation error &mdash; treat as unconfirmed)</em>"
                else:
                    v_suffix = ""
                h += f"<b>Email:</b> {action.found_email}{v_suffix}<br>"
            if action.found_phone:
                h += f"<b>Phone:</b> {action.found_phone}<br>"
            if action.found_linkedin:
                h += f"<b>LinkedIn:</b> <a href=\"{action.found_linkedin}\">{action.found_linkedin}</a><br>"

            act_label = _ACTION_LABELS.get(action.action, action.action)
            if action.contact_id and portal_id:
                contact_url = f"https://app.hubspot.com/contacts/{portal_id}/contact/{action.contact_id}"
                contact_ref = f" &mdash; <a href=\"{contact_url}\">{action.contact_name} #{action.contact_id}</a>"
            elif action.contact_id:
                contact_ref = f" #{action.contact_id}"
            else:
                contact_ref = ""
            h += f"<b>HubSpot:</b> {act_label}{contact_ref}<br>"

        elif action.action == "no_contact_found":
            h += "<b>Result:</b> No administrator or Executive Director identified<br>"
            if action.detail and action.detail != "No contact identified by research":
                h += f"<b>Detail:</b> {action.detail}<br>"

        else:  # error
            h += f"<b>Result:</b> Error &mdash; {action.detail}<br>"

        h += "<br>"

    # ── How to Verify ─────────────────────────────────────────────────────────
    all_urls: list[str] = []
    seen_urls: set[str] = set()
    for action in actions:
        for url in action.source_urls:
            if url not in seen_urls:
                seen_urls.add(url)
                all_urls.append(url)

    h += "<b>How to Verify</b><br>"
    if all_urls:
        h += "Sources cited for the contacts above:<ul>"
        for u in all_urls[:15]:
            display = u[:80] + ("..." if len(u) > 80 else "")
            h += f"<li><a href=\"{u}\">{display}</a></li>"
        h += "</ul>"
    elif fe_people_search:
        h += "Contacts were found via FullEnrich People Search — verify using the LinkedIn profile links in Results above.<br>"
    else:
        h += "No source URLs were returned by the web search agent for this run.<br>"

    # ── Errors ────────────────────────────────────────────────────────────────
    if errors:
        h += "<br><b>Errors / Warnings</b><ul>"
        for err in errors:
            h += f"<li>{err}</li>"
        h += "</ul>"

    return h


# ── Main entry point ──────────────────────────────────────────────────────────


def run_enrichment(company_id: str) -> None:
    """
    Full enrichment run for a single HubSpot company record.

    Routes to Workflow 1 (child facility) or Workflow 2 (parent corporation)
    based on the record's hierarchy_type property.
    """
    logger.info(f"Starting enrichment for company_id={company_id}")
    hs = HubSpotClient()
    errors: list[str] = []

    portal_id = 0
    try:
        portal_id = hs.get_portal_id()
    except Exception as exc:
        logger.warning(f"Could not fetch HubSpot portal ID: {exc}")

    # ── Step 1: Load company properties ──────────────────────────────────────
    try:
        props = hs.get_company_properties(company_id)
    except HubSpotError as exc:
        logger.error(f"Cannot load company {company_id}: {exc}")
        return

    company_name = props.get("name") or "Unknown Company"
    hierarchy_type = (props.get("hierarchy_type") or "").strip()
    definitive_id_raw = props.get("definitive_healthcare_id") or ""
    parent_company_id = props.get("hs_parent_company_id") or ""

    logger.info(
        f"Company: {company_name} | hierarchy_type={hierarchy_type} | "
        f"definitive_healthcare_id={definitive_id_raw}"
    )

    # ── Step 2: 30-day guard ──────────────────────────────────────────────────
    # HubSpot date-picker properties are stored/returned as "YYYY-MM-DD" strings
    # even though they are submitted as epoch-ms integers.  Handle both formats.
    last_enriched_raw = props.get("revops_enrichment_date")
    if last_enriched_raw:
        try:
            raw = str(last_enriched_raw).strip()
            if raw.isdigit():
                last_dt = datetime.utcfromtimestamp(int(raw) / 1000)
            else:
                last_dt = datetime.strptime(raw[:10], "%Y-%m-%d")
            days_since = (datetime.utcnow() - last_dt).days
            if days_since < 30:
                logger.info(
                    f"Skipping {company_id}: enriched {days_since} day(s) ago "
                    f"(< 30 day threshold)"
                )
                hs.update_company(company_id, {"request_to_enrich": "false"})
                return
        except (ValueError, TypeError) as exc:
            logger.warning(f"Could not parse revops_enrichment_date={last_enriched_ms}: {exc}")

    # ── Step 3: BigQuery client ───────────────────────────────────────────────
    bq_client = None
    try:
        bq_client, auth_method = init_bigquery_client(BQ_PROJECT_ID, BQ_LOCATION)
        logger.info(f"BigQuery connected via {auth_method}")
    except RuntimeError as exc:
        logger.error(f"BigQuery connection failed: {exc}")
        errors.append(f"BigQuery unavailable: {exc}")

    # ── Step 4: Route by hierarchy type ──────────────────────────────────────
    results: list[dict] = []
    workflow_label = ""

    if hierarchy_type == CHILD_HIERARCHY_TYPE:
        # ── Workflow 1: research this specific child facility ─────────────────
        workflow_label = "Workflow 1 — Facility Leadership Research"
        if bq_client:
            # Build the facility DataFrame here (no I/O) so both concurrent
            # tasks below can use it immediately.
            facility_df = pd.DataFrame([{
                "hubspot_id": company_id,
                "facility_name": props.get("name", "Unknown Facility"),
                "facility_type": props.get("facility_type", "") or "",
                "hierarchy_type": props.get("hierarchy_type", ""),
                "city": props.get("city", "") or "",
                "state": props.get("state", "") or "",
                "address": "",
                "zip": "",
                "phone": "",
                "definitive_id": props.get("definitive_healthcare_id", "") or "",
            }])

            def _fetch_parent_name() -> str:
                if not parent_company_id:
                    return company_name
                try:
                    parent_props = hs.get_company_properties(parent_company_id)
                    return parent_props.get("name") or company_name
                except HubSpotError:
                    return company_name

            # Run parent name fetch (HubSpot API) and BQ leadership check
            # concurrently — both are I/O-bound and independent of each other.
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_parent  = executor.submit(_fetch_parent_name)
                future_missing = executor.submit(
                    find_facilities_missing_leadership, bq_client, facility_df
                )
            parent_corp_name = future_parent.result()
            missing_df       = future_missing.result()

            try:
                results = _run_workflow1_single_facility(
                    bq_client=bq_client,
                    hs=hs,
                    company_id=company_id,
                    props=props,
                    parent_corp_name=parent_corp_name,
                    missing=missing_df,
                )
            except Exception as exc:
                logger.exception(f"Workflow 1 failed: {exc}")
                errors.append(f"Workflow 1 error: {exc}")

    elif hierarchy_type == CORP_HIERARCHY_TYPE:
        # ── Workflow 2: research Definitive corporate-level contacts ──────────
        workflow_label = "Workflow 2 — Definitive Corporate Contact Research"
        if bq_client and definitive_id_raw:
            try:
                network_id = int(definitive_id_raw)
                logger.info(
                    f"Running Workflow 2: Definitive corporate contacts "
                    f"(NETWORK_ID={network_id})"
                )
                results = workflow_2_research_contacts(
                    client=bq_client,
                    network_id=network_id,
                    corporation_name=company_name,
                    facility_type=props.get("facility_type", ""),
                )
                logger.info(f"Workflow 2 complete: {len(results)} contacts researched")
            except ValueError:
                msg = f"Invalid definitive_healthcare_id='{definitive_id_raw}' — not an integer"
                logger.warning(msg)
                errors.append(msg)
            except Exception as exc:
                logger.exception(f"Workflow 2 failed: {exc}")
                errors.append(f"Workflow 2 error: {exc}")
        elif not definitive_id_raw:
            msg = "Workflow 2 skipped: no definitive_healthcare_id on corporation record"
            logger.info(msg)
            errors.append(msg)

        # ── FullEnrich supplement: find corporate executives not in DH ────────
        # Runs regardless of whether DH returned results, so FullEnrich fills
        # gaps when DH has no record or definitive_id is missing.
        dh_names = {
            (r.get("contact_name") or "").lower().strip()
            for r in results
            if r.get("contact_name")
        }
        logger.info(
            f"W2 FullEnrich supplement: searching for corporate executives at '{company_name}'"
        )
        fe_corp_contacts = search_facility_contacts(
            company_name=company_name,
            city="",
            state=props.get("state", "") or "",
            titles=W2_CORPORATE_TITLES,
            domain=props.get("website", "") or "",
        )
        fe_added = 0
        for fe in fe_corp_contacts:
            if (fe["full_name"] or "").lower().strip() not in dh_names:
                results.append({
                    "workflow":            "2 - Unassociated Contact",
                    "contact_name":        fe["full_name"],
                    "title":               fe["title"],
                    "email":               "",
                    "phone":               "",
                    "linkedin":            fe.get("linkedin_url", ""),
                    "found_name":          fe["full_name"],
                    "found_title":         fe["title"],
                    "found_email":         "",
                    "found_phone":         "",
                    "found_linkedin":      fe.get("linkedin_url", ""),
                    # linkedin_url in research_findings so _extract_urls picks it up for the note
                    "research_findings":   fe.get("linkedin_url", ""),
                    "research_confidence": "low",
                    "source_tier":         "",
                    "hubspot_contact_ids": "",
                    "research_error":      "",
                })
                dh_names.add((fe["full_name"] or "").lower().strip())
                fe_added += 1
        if fe_added:
            logger.info(f"W2 FullEnrich supplement added {fe_added} new contact(s)")
        else:
            logger.info("W2 FullEnrich supplement: no additional contacts found")

    else:
        msg = (
            f"Unknown hierarchy_type='{hierarchy_type}'. "
            f"Expected '{CHILD_HIERARCHY_TYPE}' or '{CORP_HIERARCHY_TYPE}'."
        )
        logger.warning(msg)
        errors.append(msg)
        workflow_label = f"Unknown hierarchy type: {hierarchy_type}"

    # ── Step 5: Process results → create/associate contacts in HubSpot ───────
    actions: list[ContactAction] = []

    for result in results:
        found_name = result.get("found_name", "") or result.get("contact_name", "")
        found_title = result.get("found_title", "") or result.get("title", "")
        confidence = result.get("research_confidence", "not_found")
        child_id = result.get("hubspot_facility_id", "") or company_id

        # For W1: route by title; for W2: always parent corp
        if hierarchy_type == CHILD_HIERARCHY_TYPE:
            target_id = _association_target(found_title, child_id, parent_company_id or company_id)
        else:
            target_id = company_id

        action = _process_found_contact(
            hs=hs,
            found_name=found_name,
            found_title=found_title,
            found_email=result.get("found_email", "") or result.get("email", ""),
            found_phone=result.get("found_phone", "") or result.get("phone", ""),
            confidence=confidence if confidence else ("high" if found_name else "not_found"),
            pre_populated_contact_ids=result.get("hubspot_contact_ids", ""),
            target_company_id=target_id,
            facility_name=result.get("facility_name", "") or result.get("contact_name", ""),
            research_findings=result.get("research_findings", ""),
            source_tier=str(result.get("source_tier", "")),
            found_linkedin=result.get("found_linkedin", "") or result.get("linkedin", ""),
            facility_domain=props.get("website", "") or "",
            research_reasoning=result.get("research_reasoning", ""),
            source_path=result.get("source_path", ""),
            fe_people_search_ran=result.get("fe_people_search_ran", False),
            dh_verified=result.get("dh_verified", False),
            dh_verification_detail=result.get("dh_verification_detail", ""),
            dh_last_update=result.get("dh_last_update", ""),
            fe_updated_at=result.get("fe_updated_at", ""),
            research_source_name=result.get("research_source_name", ""),
        )
        actions.append(action)
        if action.action == "error":
            errors.append(f"Contact error ({found_name}): {action.detail}")

    # ── Step 6: Write summary note ────────────────────────────────────────────
    try:
        note_body = _build_note(
            company_name=company_name,
            workflow_label=workflow_label,
            results=results,
            actions=actions,
            errors=errors,
            portal_id=portal_id,
        )
        hs.create_note_on_company(company_id, note_body)
        logger.info(f"Note written to company {company_id}")
    except HubSpotError as exc:
        logger.error(f"Failed to create note on company {company_id}: {exc}")

    # ── Step 7: Update company properties ─────────────────────────────────────
    update_props = {
        "revops_enrichment_date": str(HubSpotClient.epoch_ms_today_utc()),
        "request_to_enrich": "false",
    }
    for attempt in range(2):
        try:
            hs.update_company(company_id, update_props)
            logger.info(
                f"Updated company {company_id}: "
                f"revops_enrichment_date set, request_to_enrich reset"
            )
            break
        except HubSpotError as exc:
            logger.error(
                f"Failed to update company {company_id} (attempt {attempt + 1}): {exc}"
            )

    logger.info(f"Enrichment complete for company_id={company_id}")
