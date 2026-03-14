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
from datetime import datetime, timezone

import pandas as pd

from email_verifier import verify_email_millionverifier
from hubspot_client import HubSpotClient, HubSpotError
from state_association_matcher import (
    _build_facility_research_prompt,
    call_openrouter,
    find_facilities_missing_leadership,
    lookup_hubspot_contact_enhanced,
    parse_found_name,
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
    email_validation_status: str = ""   # VERIFIED | INVALID | UNKNOWN | RISKY | UNVERIFIED | ERROR
    email_validation_quality: str = ""  # good | ok | bad | ""


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
        source_urls=source_urls,
        facility_name=facility_name,
        target_company_id=target_company_id,
        email_validation_status=email_validation.get("status", ""),
        email_validation_quality=email_validation.get("quality", ""),
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
    existing_ids = [x for x in (pre_populated_contact_ids or "").split("|") if x]
    if existing_ids:
        for contact_id in existing_ids:
            try:
                hs.associate_contact_to_company(contact_id, target_company_id)
                return ContactAction(
                    action="associated_existing",
                    contact_id=contact_id,
                    contact_name=found_name,
                    contact_title=found_title,
                    detail=f"Added association to existing contact #{contact_id}",
                    **_base,
                )
            except HubSpotError as exc:
                logger.warning(f"Association failed for contact {contact_id}: {exc}")

    # ── Live email search (W2 and fallback for W1) ────────────────────────────
    if email_to_use:
        existing = hs.search_contacts_by_email(email_to_use)
        for contact in existing:
            if _names_match(found_name, contact["firstname"], contact["lastname"]):
                try:
                    hs.associate_contact_to_company(contact["id"], target_company_id)
                    return ContactAction(
                        action="associated_existing",
                        contact_id=contact["id"],
                        contact_name=found_name,
                        contact_title=found_title,
                        detail=f"Email match → added association to existing #{contact['id']}",
                        **_base,
                    )
                except HubSpotError as exc:
                    logger.warning(f"Email-match association failed: {exc}")

    # ── Create new contact ────────────────────────────────────────────────────
    contact_props: dict = {"firstname": first, "lastname": last, "jobtitle": found_title or ""}
    if email_to_use:
        contact_props["email"] = email_to_use.strip()
    if found_phone:
        contact_props["phone"] = found_phone.strip()

    try:
        new_id = hs.create_contact(contact_props)
        hs.associate_contact_to_company(new_id, target_company_id)
        return ContactAction(
            action="created_new",
            contact_id=new_id,
            contact_name=found_name,
            contact_title=found_title,
            detail=f"Created new contact #{new_id} and associated with company {target_company_id}",
            **_base,
        )
    except HubSpotError as exc:
        logger.error(f"Failed to create contact for '{found_name}': {exc}")
        return ContactAction(
            action="error",
            contact_name=found_name,
            contact_title=found_title,
            detail=f"HubSpot API error: {exc}",
            **_base,
        )


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
    facility_type = props.get("facility_type", "") or ""
    definitive_id = props.get("definitive_healthcare_id", "") or ""

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
        logger.info(f"  ✅ {facility_name} already has an Admin/ED in HubSpot — skipping")
        return []

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
        )
        result["source_tier"] = source_tier

        logger.info(f"  Calling OpenRouter for {facility_name}...")
        research_result = call_openrouter(RESEARCH_SYSTEM_PROMPT, prompt)
        result["research_findings"] = research_result[:2000]
        logger.info(f"  Research complete ({len(research_result)} chars)")

        found_name, found_title, confidence, found_email, found_phone = parse_found_name(
            research_result
        )
        result.update({
            "found_name": found_name,
            "found_title": found_title,
            "research_confidence": confidence,
            "found_email": found_email,
            "found_phone": found_phone,
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

    return [result]


# ── Note builder ──────────────────────────────────────────────────────────────


_CONFIDENCE_LABELS = {
    "high":      "HIGH  — found on official association directory or CMS listing",
    "medium":    "MED   — found on facility/operator website",
    "low":       "LOW   — found on LinkedIn or potentially outdated source",
    "not_found": "—     — not found",
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
) -> str:
    """
    Build the salesperson-readable summary note for the company record.

    Designed to answer three questions at a glance:
      1. What did the automation look for and where?
      2. Who did it find (and how reliable is that information)?
      3. How can I quickly verify or disprove the finding?
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    divider = "─" * 66

    n_found = sum(1 for a in actions if a.action not in ("no_contact_found", "error"))
    n_created = sum(1 for a in actions if a.action == "created_new")
    n_associated = sum(1 for a in actions if a.action == "associated_existing")
    n_missed = sum(1 for a in actions if a.action == "no_contact_found")

    lines: list[str] = [
        f"RevOps Contact Enrichment  ·  {company_name}",
        f"Run date:  {now_str}",
        f"Workflow:  {workflow_label}",
        "",
        f"Summary:  {len(results)} researched  ·  {n_found} found  ·  "
        f"{n_created} created  ·  {n_associated} linked  ·  {n_missed} not found",
        "",
        divider,
        "WHAT WAS SEARCHED",
        divider,
    ]

    # Describe data sources consulted
    lines.append("  Data sources (checked in this order):")
    lines.append("    1. Definitive Healthcare Executives table (BigQuery)")
    lines.append("    2. Definitive Healthcare Contact Full Feed (BigQuery)")

    # Pull tier info from first result that has it
    tier_str = ""
    for r in results:
        t = str(r.get("source_tier", ""))
        if t:
            tier_str = t
            break
    if tier_str:
        tier_label = _TIER_LABELS.get(tier_str, f"Tier {tier_str}")
        lines.append(f"    3. State association directory — {tier_label}")
    else:
        lines.append("    3. State association directory")
    lines.append("    4. Web search via Perplexity Sonar Pro (live internet search)")

    # Show MillionVerifier as source #5 when at least one email was found
    if any(a.found_email for a in actions):
        mv_active = bool(os.getenv("MILLIONVERIFIER_API_KEY", "").strip())
        if mv_active:
            lines.append("    5. MillionVerifier API — email deliverability validation")
        else:
            lines.append("    5. MillionVerifier API — skipped (MILLIONVERIFIER_API_KEY not configured)")
    lines.append("")

    # ── Per-contact results ─────────────────────────────────────────────────
    lines += [divider, "RESULTS", divider]

    for action in actions:
        label = action.facility_name or action.contact_name or "Unknown"
        lines.append(f"  ● {label}")

        if action.action in ("created_new", "associated_existing"):
            conf_label = _CONFIDENCE_LABELS.get(action.confidence, action.confidence)
            lines.append(f"    Contact:    {action.contact_name},  {action.contact_title}")
            lines.append(f"    Confidence: {conf_label}")
            if action.found_email:
                vstatus  = action.email_validation_status
                vquality = action.email_validation_quality
                if vstatus == "VERIFIED":
                    v_suffix = " ✓ verified"
                elif vstatus == "INVALID":
                    v_suffix = " ✗ INVALID — not written to HubSpot"
                elif vstatus == "RISKY":
                    v_suffix = " ~ risky/catchall" + (f" — quality: {vquality}" if vquality else "")
                elif vstatus == "UNKNOWN":
                    v_suffix = " ? unverifiable"
                elif vstatus == "UNVERIFIED":
                    v_suffix = " (not validated — API key not configured)"
                elif vstatus == "ERROR":
                    v_suffix = " (validation error — treat as unconfirmed)"
                else:
                    v_suffix = ""
                lines.append(f"    Email:      {action.found_email}{v_suffix}")
            if action.found_phone:
                lines.append(f"    Phone:      {action.found_phone}")
            act_label = _ACTION_LABELS.get(action.action, action.action)
            contact_ref = f" #{action.contact_id}" if action.contact_id else ""
            lines.append(f"    HubSpot:    {act_label}{contact_ref}")

        elif action.action == "no_contact_found":
            lines.append(f"    Result:     No administrator or Executive Director identified")
            if action.detail and action.detail != "No contact identified by research":
                lines.append(f"    Detail:     {action.detail}")

        else:  # error
            lines.append(f"    Result:     Error — {action.detail}")

        lines.append("")

    # ── How to verify ────────────────────────────────────────────────────────
    # Collect all source URLs across all actions
    all_urls: list[str] = []
    seen_urls: set[str] = set()
    for action in actions:
        for url in action.source_urls:
            if url not in seen_urls:
                seen_urls.add(url)
                all_urls.append(url)

    lines += [divider, "HOW TO VERIFY", divider]
    lines.append("  Quick checks to confirm or disprove these findings:")
    lines.append("")
    lines.append("  • CMS Care Compare (search by facility name for current leadership):")
    lines.append("    https://www.medicare.gov/care-compare/")
    lines.append("")

    if all_urls:
        lines.append("  • Sources cited by the web search agent:")
        for url in all_urls[:15]:
            lines.append(f"    {url}")
        lines.append("")

    if tier_str == "1":
        lines.append("  • State association directory was public — check the URL in sources above")
    elif tier_str == "2":
        lines.append("  • State association directory requires login — verify through direct outreach")
    else:
        lines.append("  • No state directory available for this state — web search was primary source")

    # ── Errors ───────────────────────────────────────────────────────────────
    if errors:
        lines += ["", divider, "ERRORS / WARNINGS", divider]
        for err in errors:
            lines.append(f"  {err}")

    return "\n".join(lines)


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
    last_enriched_ms = props.get("revops_enrichment_date")
    if last_enriched_ms:
        try:
            last_dt = datetime.utcfromtimestamp(int(last_enriched_ms) / 1000)
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
