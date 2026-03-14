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
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from hubspot_client import HubSpotClient, HubSpotError
from state_association_matcher import (
    _build_facility_research_prompt,
    call_openrouter,
    find_facilities_missing_leadership,
    lookup_hubspot_contact_enhanced,
    parse_found_name,
    workflow_2_research_contacts,
    RATE_LIMIT_SECONDS,
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
) -> ContactAction:
    """
    Decide whether to create a new HubSpot contact or associate an existing one.
    """
    source_urls = _extract_urls(research_findings)[:20]

    if not found_name or confidence == "not_found":
        return ContactAction(
            action="no_contact_found",
            facility_name=facility_name,
            target_company_id=target_company_id,
            detail="No contact identified by research",
            source_urls=source_urls,
        )

    first, last = _split_name(found_name)
    if not first:
        return ContactAction(
            action="no_contact_found",
            facility_name=facility_name,
            target_company_id=target_company_id,
            detail=f"Could not parse name: '{found_name}'",
            source_urls=source_urls,
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
                    facility_name=facility_name,
                    target_company_id=target_company_id,
                    detail=f"Added association to existing contact #{contact_id}",
                    source_urls=source_urls,
                )
            except HubSpotError as exc:
                logger.warning(f"Association failed for contact {contact_id}: {exc}")

    # ── Live email search (W2 and fallback for W1) ────────────────────────────
    if found_email:
        existing = hs.search_contacts_by_email(found_email)
        for contact in existing:
            if _names_match(found_name, contact["firstname"], contact["lastname"]):
                try:
                    hs.associate_contact_to_company(contact["id"], target_company_id)
                    return ContactAction(
                        action="associated_existing",
                        contact_id=contact["id"],
                        contact_name=found_name,
                        contact_title=found_title,
                        facility_name=facility_name,
                        target_company_id=target_company_id,
                        detail=f"Email match → added association to existing #{contact['id']}",
                        source_urls=source_urls,
                    )
                except HubSpotError as exc:
                    logger.warning(f"Email-match association failed: {exc}")

    # ── Create new contact ────────────────────────────────────────────────────
    contact_props: dict = {"firstname": first, "lastname": last, "jobtitle": found_title or ""}
    if found_email:
        contact_props["email"] = found_email.strip()
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
            facility_name=facility_name,
            target_company_id=target_company_id,
            detail=f"Created new contact #{new_id} and associated with company {target_company_id}",
            source_urls=source_urls,
        )
    except HubSpotError as exc:
        logger.error(f"Failed to create contact for '{found_name}': {exc}")
        return ContactAction(
            action="error",
            contact_name=found_name,
            contact_title=found_title,
            facility_name=facility_name,
            target_company_id=target_company_id,
            detail=f"HubSpot API error: {exc}",
            source_urls=source_urls,
        )


# ── Workflow 1 — single facility ──────────────────────────────────────────────


def _run_workflow1_single_facility(
    bq_client,
    hs: HubSpotClient,
    company_id: str,
    props: dict,
    parent_corp_name: str,
) -> list[dict]:
    """
    Run Workflow 1 research on a single child facility.

    Checks whether the facility already has an Administrator or Executive Director
    in HubSpot. If not, calls OpenRouter to find one via state association sites.

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
        research_result = call_openrouter(prompt)
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


def _build_note(
    company_name: str,
    workflow_label: str,
    results: list[dict],
    actions: list[ContactAction],
    errors: list[str],
) -> str:
    """Build the summary note body for the company record."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d UTC")

    n_found = sum(1 for a in actions if a.action != "no_contact_found")
    n_created = sum(1 for a in actions if a.action == "created_new")
    n_associated = sum(1 for a in actions if a.action == "associated_existing")

    lines: list[str] = [
        f"RevOps Enrichment — {company_name}",
        f"Date: {now_str}",
        f"Workflow: {workflow_label}",
        "",
        f"Records researched: {len(results)} | "
        f"Contacts found: {n_found} | "
        f"Created: {n_created} | "
        f"Associated: {n_associated}",
        "",
    ]

    for action in actions:
        if action.action == "no_contact_found":
            label = action.facility_name or action.contact_name or "Unknown"
            lines.append(f"  • {label} — No administrator/ED identified")
        else:
            label = action.facility_name or action.contact_name or "Unknown"
            if action.action == "created_new":
                status = f"Created new contact #{action.contact_id}"
            elif action.action == "associated_existing":
                status = f"Added association to existing contact #{action.contact_id}"
            else:
                status = f"Error: {action.detail}"
            lines.append(f"  • {label} — {action.contact_name}, {action.contact_title}")
            lines.append(f"    Action: {status}")

    # Source URLs
    all_urls: list[str] = []
    seen: set[str] = set()
    for action in actions:
        for url in action.source_urls:
            if url not in seen:
                seen.add(url)
                all_urls.append(url)

    if all_urls:
        lines += ["", "── SOURCES ───────────────────────────────────────────────────────────"]
        for url in all_urls[:20]:
            lines.append(f"  {url}")

    if errors:
        lines += ["", "── ERRORS ────────────────────────────────────────────────────────────"]
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
            # Get parent corp name for the research prompt
            parent_corp_name = company_name  # fallback
            if parent_company_id:
                try:
                    parent_props = hs.get_company_properties(parent_company_id)
                    parent_corp_name = parent_props.get("name") or company_name
                except HubSpotError:
                    pass

            try:
                results = _run_workflow1_single_facility(
                    bq_client=bq_client,
                    hs=hs,
                    company_id=company_id,
                    props=props,
                    parent_corp_name=parent_corp_name,
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
