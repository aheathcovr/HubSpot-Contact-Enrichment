#!/usr/bin/env python3
"""
Diagnostic test for the enrichment pipeline — DRY RUN (no HubSpot writes).

Tests three specific Brookdale child facilities:
  44396264017 — Brookdale Park Place       (Spokane Valley, WA)
  44500085603 — Brookdale Federal Way      (Federal Way, WA)
  44533717539 — Brookdale Bristol          (Bristol, VA)

For each facility this script reports:
  1. BigQuery Definitive Healthcare data available (Executives + Full Feed tables)
  2. OpenRouter / Sonar Pro web research result
  3. Parse outcome (name, title, confidence, email, phone)
  4. Whether a HubSpot contact match was found in BQ

Nothing is written back to HubSpot.
"""

import os
import sys
import json
from datetime import datetime
from textwrap import indent

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from state_association_matcher import (
    init_bigquery_client,
    call_openrouter,
    _build_facility_research_prompt,
    parse_found_name,
    lookup_hubspot_contact_enhanced,
    _format_hubspot_matches_enhanced,
    OPENROUTER_MODEL,
    OPENROUTER_MODEL_TIER23,
    RESEARCH_SYSTEM_PROMPT,
    _CONTACT_JSON_SCHEMA,
    _OPENROUTER_SEMAPHORE,
    RATE_LIMIT_SECONDS,
    BQ_PROJECT_ID,
    BQ_LOCATION,
    DH_TABLES,
    HUBSPOT_COMPANIES_TABLE,
    HUBSPOT_CONTACTS_TABLE,
)

# ── Test fixtures ──────────────────────────────────────────────────────────────

FACILITIES = [
    {
        "hubspot_id": "44396264017",
        "facility_name": "Brookdale Park Place",
        "city": "Spokane Valley",
        "state": "WA",
        "facility_type": "AL/IL/MC",
        "definitive_id": "65293",
        "corporation_name": "Brookdale Senior Living",
        "hubspot_corp_id": "50791311268",
    },
    {
        "hubspot_id": "44500085603",
        "facility_name": "Brookdale Federal Way",
        "city": "Federal Way",
        "state": "WA",
        "facility_type": "AL/IL/MC",
        "definitive_id": "65207",
        "corporation_name": "Brookdale Senior Living",
        "hubspot_corp_id": "50791311268",
    },
    {
        "hubspot_id": "44533717539",
        "facility_name": "Brookdale Bristol",
        "city": "Bristol",
        "state": "VA",
        "facility_type": "AL/IL/MC",
        "definitive_id": "66784",
        "corporation_name": "Brookdale Senior Living",
        "hubspot_corp_id": "50791311268",
    },
]

# ── BigQuery diagnostic helpers ────────────────────────────────────────────────

def bq_dh_executives(client, definitive_id: str) -> list[dict]:
    """
    Pull executive records from the Definitive Healthcare Executives table
    for a given HOSPITAL_ID (facility-level DH ID).

    Actual schema: FIRST_NAME, LAST_NAME, TITLE, EMAIL, DIRECT_PHONE, LOCATION_PHONE
    """
    query = f"""
    SELECT
        EXECUTIVE_ID,
        FIRST_NAME,
        LAST_NAME,
        TITLE,
        EMAIL,
        DIRECT_PHONE,
        LOCATION_PHONE,
        STANDARDIZED_TITLE
    FROM `{DH_TABLES["executives"]}`
    WHERE CAST(HOSPITAL_ID AS STRING) = '{definitive_id}'
    ORDER BY TITLE
    LIMIT 20
    """
    try:
        rows = list(client.query(query).result())
        return [dict(r) for r in rows]
    except Exception as exc:
        return [{"error": str(exc)}]


def bq_dh_full_feed(client, definitive_id: str) -> list[dict]:
    """
    Pull contact records from the December Full Feed table for a given HOSPITAL_ID.

    Actual schema: FIRST_NAME, LAST_NAME, TITLE, EMAIL, PHONE, LOCATION_PHONE
    """
    query = f"""
    SELECT
        FIRST_NAME,
        LAST_NAME,
        TITLE,
        EMAIL,
        PHONE,
        LOCATION_PHONE,
        HOSPITAL_NAME
    FROM `{DH_TABLES["full_feed"]}`
    WHERE CAST(HOSPITAL_ID AS STRING) = '{definitive_id}'
    ORDER BY TITLE
    LIMIT 20
    """
    try:
        rows = list(client.query(query).result())
        return [dict(r) for r in rows]
    except Exception as exc:
        return [{"error": str(exc)}]


def bq_alf_overview(client, definitive_id: str) -> list[dict]:
    """
    Pull facility overview from ALF Overview table.

    Actual schema: HQ_CITY, HQ_STATE, NUMBER_BEDS — no embedded exec director fields.
    """
    query = f"""
    SELECT
        HOSPITAL_NAME,
        HQ_CITY,
        HQ_STATE,
        NUMBER_BEDS,
        HQ_PHONE,
        WEBSITE
    FROM `{DH_TABLES["alf_overview"]}`
    WHERE CAST(HOSPITAL_ID AS STRING) = '{definitive_id}'
    LIMIT 1
    """
    try:
        rows = list(client.query(query).result())
        return [dict(r) for r in rows]
    except Exception as exc:
        return [{"error": str(exc)}]


def bq_hubspot_existing_contacts(client, hubspot_id: str) -> list[dict]:
    """
    Check if any HubSpot contacts with Admin/ED titles are already
    associated with this company via `properties_associatedcompanyid`.

    NOTE: The HubSpot_Airbyte sync does not include a separate association table.
    We use the denormalized `properties_associatedcompanyid` field on each contact,
    which stores the contact's primary company association only.
    """
    query = f"""
    SELECT
        properties_firstname AS firstname,
        properties_lastname  AS lastname,
        properties_jobtitle  AS title,
        properties_email     AS email,
        CAST(properties_associatedcompanyid AS STRING) AS associated_company_id
    FROM `{HUBSPOT_CONTACTS_TABLE}`
    WHERE (archived IS NOT TRUE OR archived IS NULL)
      AND CAST(properties_associatedcompanyid AS STRING) = '{hubspot_id}'
      AND (
          LOWER(properties_jobtitle) LIKE '%administrator%'
          OR LOWER(properties_jobtitle) LIKE '%executive director%'
      )
    LIMIT 10
    """
    try:
        rows = list(client.query(query).result())
        return [dict(r) for r in rows]
    except Exception as exc:
        return [{"error": str(exc)}]


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _sep(char="─", width=72):
    return char * width


def _print_section(title: str):
    print(f"\n{_sep()}")
    print(f"  {title}")
    print(_sep())


def _print_table(rows: list[dict], label: str):
    if not rows:
        print(f"    (no rows)")
        return
    if "error" in rows[0]:
        print(f"    ✗ Query error: {rows[0]['error']}")
        return
    # Print header
    keys = list(rows[0].keys())
    print(f"    {' | '.join(keys)}")
    print(f"    {'-' * 60}")
    for row in rows:
        vals = [str(row.get(k, "")) or "" for k in keys]
        print(f"    {' | '.join(vals)}")


# ── Main diagnostic ────────────────────────────────────────────────────────────

def run_diagnostic():
    print("=" * 72)
    print("  ENRICHMENT PIPELINE DIAGNOSTIC — DRY RUN")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)
    print(f"\n  Facilities under test: {len(FACILITIES)}")
    print(f"  Parent corporation:    Brookdale Senior Living (50791311268)")

    # Connect to BigQuery
    print(f"\n🔌 Connecting to BigQuery...")
    client, auth_method = init_bigquery_client(BQ_PROJECT_ID, BQ_LOCATION)
    print(f"   ✓ Connected ({auth_method})")

    all_results = []

    for i, fac in enumerate(FACILITIES, 1):
        hid        = fac["hubspot_id"]
        fname      = fac["facility_name"]
        city       = fac["city"]
        state      = fac["state"]
        ftype      = fac["facility_type"]
        def_id     = fac["definitive_id"]
        corp_name  = fac["corporation_name"]
        corp_id    = fac["hubspot_corp_id"]

        print(f"\n{'=' * 72}")
        print(f"  [{i}/3]  {fname}  ({city}, {state})")
        print(f"          HubSpot ID: {hid}  |  Definitive ID: {def_id}")
        print(f"{'=' * 72}")

        # ── 1. BigQuery: existing HubSpot contacts ──────────────────────────
        print(f"\n  ① BigQuery — existing HubSpot Admin/ED contacts for this facility:")
        existing = bq_hubspot_existing_contacts(client, hid)
        if existing and "error" not in existing[0]:
            for c in existing:
                print(f"     ✓ {c.get('firstname','')} {c.get('lastname','')} "
                      f"| {c.get('title','')} | {c.get('email','')}")
        else:
            error = existing[0].get("error") if existing else None
            if error:
                print(f"     ✗ Query error: {error}")
            else:
                print(f"     ○ No Admin/ED contacts currently in HubSpot for this facility")

        # ── 2. BigQuery: Definitive Healthcare Executives table ─────────────
        print(f"\n  ② BigQuery — Definitive Healthcare Executives table (HOSPITAL_ID={def_id}):")
        dh_execs = bq_dh_executives(client, def_id)
        if dh_execs and "error" not in dh_execs[0]:
            for e in dh_execs:
                name = e.get('EXECUTIVE_NAME') or f"{e.get('FIRST_NAME','')} {e.get('LAST_NAME','')}".strip()
                phone = e.get('DIRECT_PHONE') or e.get('LOCATION_PHONE') or ""
                print(f"     • {name} | {e.get('TITLE','')} | {e.get('EMAIL','') or '(no email)'} | {phone or '(no phone)'}")
        else:
            error = dh_execs[0].get("error") if dh_execs else None
            if error:
                print(f"     ✗ Query error: {error}")
            else:
                print(f"     ○ No rows in Executives table for HOSPITAL_ID={def_id}")

        # ── 3. BigQuery: Definitive Healthcare Full Feed ──────────────────
        print(f"\n  ③ BigQuery — Definitive Healthcare Full Feed (HOSPITAL_ID={def_id}):")
        dh_feed = bq_dh_full_feed(client, def_id)
        if dh_feed and "error" not in dh_feed[0]:
            for e in dh_feed:
                name = e.get('EXECUTIVE_NAME') or f"{e.get('FIRST_NAME','')} {e.get('LAST_NAME','')}".strip()
                phone = e.get('PHONE') or e.get('LOCATION_PHONE') or ""
                print(f"     • {name} | {e.get('TITLE','')} | {e.get('EMAIL','') or '(no email)'} | {phone or '(no phone)'}")
        else:
            error = dh_feed[0].get("error") if dh_feed else None
            if error:
                print(f"     ✗ Query error: {error}")
            else:
                print(f"     ○ No rows in Full Feed for HOSPITAL_ID={def_id}")

        # ── 4. BigQuery: ALF Overview (exec director fields) ──────────────
        print(f"\n  ④ BigQuery — ALF Overview (HOSPITAL_ID={def_id}):")
        alf = bq_alf_overview(client, def_id)
        if alf and "error" not in alf[0]:
            row = alf[0]
            print(f"     Facility: {row.get('HOSPITAL_NAME','')}  ({row.get('HQ_CITY','')}, {row.get('HQ_STATE','')})")
            print(f"     Beds:     {row.get('NUMBER_BEDS','')}  |  Phone: {row.get('HQ_PHONE','')}  |  Site: {row.get('WEBSITE','')}")
            print(f"     NOTE: ALF Overview has no embedded exec director fields — use Executives table.")
        else:
            error = alf[0].get("error") if alf else None
            if error:
                print(f"     ✗ Query error: {error}")
            else:
                print(f"     ○ Not found in ALF Overview for HOSPITAL_ID={def_id}")

        # ── 5. OpenRouter web research ─────────────────────────────────────
        print(f"\n  ⑤ OpenRouter (Sonar Pro) web research:")
        prompt, source_tier, domain_filter = _build_facility_research_prompt(
            facility_name=fname,
            city=city,
            state=state,
            facility_type=ftype,
            corporation_name=corp_name,
        )
        if source_tier == "1":
            _model = OPENROUTER_MODEL
            _extra = {"search_recency_filter": "year"}
            if domain_filter:
                _extra["search_domain_filter"] = domain_filter
        elif source_tier == "2":
            _model = OPENROUTER_MODEL_TIER23
            _extra = {"reasoning_effort": "medium", "search_recency_filter": "year"}
        else:
            _model = OPENROUTER_MODEL_TIER23
            _extra = {"reasoning_effort": "high", "search_recency_filter": "year"}
        _structured_format = {"type": "json_schema", "json_schema": _CONTACT_JSON_SCHEMA}
        print(f"     Source tier: {source_tier}")
        print(f"     Calling Sonar (tier {source_tier})...")

        research_result = ""
        found_name = found_title = found_email = found_phone = ""
        confidence = "not_found"
        parse_error = ""

        try:
            with _OPENROUTER_SEMAPHORE:
                research_result = call_openrouter(
                    RESEARCH_SYSTEM_PROMPT, prompt,
                    model=_model,
                    response_format=_structured_format,
                    extra_params=_extra,
                )
            import time as _time; _time.sleep(RATE_LIMIT_SECONDS)
            cached_flag = " (cached)" if len(research_result) > 0 else ""
            print(f"     ✓ Response received ({len(research_result)} chars{cached_flag})")

            found_name, found_title, confidence, found_email, found_phone, _reasoning, _src_name = parse_found_name(
                research_result
            )

            print(f"\n     ── Parse result:")
            print(f"        Name:       {found_name or '(none)'}")
            print(f"        Title:      {found_title or '(none)'}")
            print(f"        Confidence: {confidence}")
            print(f"        Email:      {found_email or '(none)'}")
            print(f"        Phone:      {found_phone or '(none)'}")

            # Show the full LLM response for manual review
            print(f"\n     ── Raw LLM response (first 1500 chars):")
            preview = research_result[:1500]
            for line in preview.splitlines():
                print(f"        {line}")
            if len(research_result) > 1500:
                print(f"        ... [{len(research_result) - 1500} more chars truncated]")

        except RuntimeError as exc:
            parse_error = str(exc)
            print(f"     ✗ Error: {parse_error}")

        # ── 6. HubSpot BQ match check ─────────────────────────────────────
        hubspot_match_summary = ""
        if found_name and confidence != "not_found":
            print(f"\n  ⑥ HubSpot BQ contact match check:")
            try:
                matches, match_strategy = lookup_hubspot_contact_enhanced(
                    client=client,
                    full_name=found_name,
                    email=found_email,
                    phone=found_phone,
                    facility_hubspot_id=hid,
                    parent_hubspot_id=corp_id,
                )
                ids, detail, summary = _format_hubspot_matches_enhanced(
                    matches, match_strategy=match_strategy, facility_name=fname
                )
                hubspot_match_summary = summary
                if matches:
                    print(f"     ✓ Match found: {summary}")
                    print(f"       {detail}")
                else:
                    print(f"     ○ {found_name} not found in HubSpot contacts")
            except Exception as exc:
                print(f"     ✗ Lookup error: {exc}")
        else:
            print(f"\n  ⑥ HubSpot BQ match check: skipped (no name found)")

        all_results.append({
            "facility_name": fname,
            "city": city,
            "state": state,
            "definitive_id": def_id,
            "dh_executives_count": len([r for r in dh_execs if "error" not in r]),
            "dh_feed_count": len([r for r in dh_feed if "error" not in r]),
            "alf_exec_name": "",  # ALF Overview has no embedded exec director fields
            "source_tier": source_tier,
            "found_name": found_name,
            "found_title": found_title,
            "confidence": confidence,
            "found_email": found_email,
            "found_phone": found_phone,
            "hubspot_match": hubspot_match_summary,
            "error": parse_error,
        })

    # ── Summary report ─────────────────────────────────────────────────────
    print(f"\n\n{'=' * 72}")
    print("  DIAGNOSTIC SUMMARY")
    print(f"{'=' * 72}")
    print(f"\n  {'Facility':<30} {'DH Execs':>8} {'DH Feed':>7} {'Tier':>4}  {'Found Name':<25} {'Conf':<12} {'Email?'}")
    print(f"  {'-'*30} {'-'*8} {'-'*7} {'-'*4}  {'-'*25} {'-'*12} {'-'*6}")

    for r in all_results:
        has_email = "✓" if r["found_email"] else "✗"
        found = r["found_name"][:24] if r["found_name"] else "(not found)"
        print(
            f"  {r['facility_name']:<30} {r['dh_executives_count']:>8} "
            f"{r['dh_feed_count']:>7} {r['source_tier']:>4}  "
            f"{found:<25} {r['confidence']:<12} {has_email}"
        )

    print(f"\n  ── BigQuery Definitive Healthcare assessment:")
    total_dh_execs = sum(r["dh_executives_count"] for r in all_results)
    total_dh_feed  = sum(r["dh_feed_count"] for r in all_results)
    alf_names      = [r["alf_exec_name"] for r in all_results if r["alf_exec_name"]]
    print(f"     Exec rows found (Executives table): {total_dh_execs}")
    print(f"     Exec rows found (Full Feed table):  {total_dh_feed}")
    print(f"     ALF Overview exec director names:   {alf_names or '(none)'}")

    found_count = sum(1 for r in all_results if r["found_name"] and r["confidence"] != "not_found")
    high_conf   = sum(1 for r in all_results if r["confidence"] == "high")
    med_conf    = sum(1 for r in all_results if r["confidence"] == "medium")
    low_conf    = sum(1 for r in all_results if r["confidence"] == "low")

    print(f"\n  ── OpenRouter / Sonar Pro web research assessment:")
    print(f"     Contacts found:  {found_count}/{len(all_results)}")
    print(f"     High confidence: {high_conf}")
    print(f"     Med confidence:  {med_conf}")
    print(f"     Low confidence:  {low_conf}")
    print(f"     With email:      {sum(1 for r in all_results if r['found_email'])}")
    print(f"     With phone:      {sum(1 for r in all_results if r['found_phone'])}")

    print(f"\n{'=' * 72}\n")


if __name__ == "__main__":
    run_diagnostic()
