#!/usr/bin/env python3
"""
Test script for state_association_matcher.py - Workflow 1
Tests with 1 corporation and limits to 5 child facilities.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

# Import functions from the main script
from state_association_matcher import (
    init_bigquery_client,
    load_hubspot_child_facilities,
    find_facilities_missing_leadership,
    workflow_1_research_facilities,
    save_csv,
    BQ_PROJECT_ID,
    BQ_LOCATION,
    HUBSPOT_COMPANIES_TABLE,
)

# Test with Brookdale Senior Living (611 facilities)
TEST_HUBSPOT_CORP_ID = "50791311268"
TEST_NETWORK_ID = None  # Not needed for Workflow 1 test
MAX_FACILITIES = 5
TEST_STATE_FILTER = "WA"  # Filter for Washington state facilities

def main():
    print("=" * 70)
    print("  TEST: State Association Matcher - Workflow 1")
    print("=" * 70)
    print(f"\n  Test Corporation: {TEST_HUBSPOT_CORP_ID}")
    print(f"  Max Facilities: {MAX_FACILITIES}")
    
    # Connect to BigQuery
    print("\n🔌 Connecting to BigQuery...")
    client, auth_method = init_bigquery_client(
        project_id=BQ_PROJECT_ID,
        location=BQ_LOCATION,
    )
    print(f"  ✓ Connected ({auth_method})")
    
    # Get corporation name
    print(f"\n🏢 Looking up corporation...")
    query = f"""
    SELECT properties_name AS company_name, properties_facility_type AS facility_type
    FROM `{HUBSPOT_COMPANIES_TABLE}`
    WHERE CAST(properties_hs_object_id AS STRING) = '{TEST_HUBSPOT_CORP_ID}'
    LIMIT 1
    """
    rows = list(client.query(query).result())
    corp_name = rows[0].company_name if rows else "Unknown Corporation"
    facility_type = rows[0].facility_type if rows else ""
    print(f"  Corporation: {corp_name}")
    print(f"  Facility type: {facility_type or '(not set)'}")
    
    # Load child facilities
    print(f"\n📋 Loading child facilities...")
    child_facilities = load_hubspot_child_facilities(client, TEST_HUBSPOT_CORP_ID)
    
    if child_facilities.empty:
        print("  ✗ No child facilities found!")
        return
    
    # Find facilities missing leadership
    print(f"\n🔍 Finding facilities missing leadership contacts...")
    missing = find_facilities_missing_leadership(client, child_facilities)
    
    if missing.empty:
        print("  ✅ All facilities have ED/Administrator contacts!")
        return
    
    # Filter for Washington state if specified
    if TEST_STATE_FILTER:
        missing = missing[missing['state'].str.upper() == TEST_STATE_FILTER.upper()].copy()
        print(f"  ✓ Filtered to {len(missing)} facilities in {TEST_STATE_FILTER}")
        
        if missing.empty:
            print(f"  ✗ No facilities found in {TEST_STATE_FILTER}!")
            return
    
    # Limit to MAX_FACILITIES
    test_facilities = missing.head(MAX_FACILITIES).copy()
    print(f"\n📊 Test sample: {len(test_facilities)} facilities")
    for idx, row in test_facilities.iterrows():
        print(f"    - {row['facility_name']} ({row['city']}, {row['state']})")
    
    # Run Workflow 1 on the test sample
    print("\n" + "=" * 70)
    print("  Running Workflow 1 research...")
    print("=" * 70)
    
    # We need to modify the workflow to use our test sample
    # Instead of calling workflow_1_research_facilities, we'll do it inline
    
    import time
    from datetime import datetime
    from state_association_matcher import (
        call_openrouter,
        _build_facility_research_prompt,
        parse_found_name,
        lookup_hubspot_contact_enhanced,
        _format_hubspot_matches_enhanced,
        RATE_LIMIT_SECONDS,
        RESEARCH_SYSTEM_PROMPT,
    )
    
    results = []
    total = len(test_facilities)
    
    for idx, row in test_facilities.iterrows():
        facility_name = str(row.get("facility_name", "Unknown Facility"))
        city = str(row.get("city", ""))
        state = str(row.get("state", ""))
        fac_type = str(row.get("facility_type", ""))
        hubspot_id = str(row.get("hubspot_id", ""))
        definitive_id = str(row.get("definitive_id", ""))
        
        print(f"\n[{idx + 1}/{total}] {facility_name} ({city}, {state})")
        
        research_result = ""
        error_msg = ""
        found_name = ""
        found_title = ""
        found_email = ""
        found_phone = ""
        confidence = "not_found"
        hubspot_contact_ids = ""
        hubspot_match_detail = ""
        hubspot_match_summary = ""
        
        try:
            prompt, source_tier = _build_facility_research_prompt(
                facility_name=facility_name,
                city=city,
                state=state,
                facility_type=fac_type,
                corporation_name=corp_name,
            )
            print(f"  → Calling Perplexity Sonar Pro...")
            research_result = call_openrouter(RESEARCH_SYSTEM_PROMPT, prompt)
            print(f"  ✓ Response received ({len(research_result)} chars)")
            
            # Parse the found name (now includes email and phone)
            found_name, found_title, confidence, found_email, found_phone = parse_found_name(research_result)
            
            if found_name and confidence != "not_found":
                print(f"  🔎 Found: {found_name} ({found_title}, confidence={confidence})")
                if found_email:
                    print(f"  🔎 Email: {found_email}")
                if found_phone:
                    print(f"  🔎 Phone: {found_phone}")
                print(f"  → Checking HubSpot for existing contact...")
                
                # Use enhanced lookup with email, phone, facility ID, and parent ID
                matches, match_strategy = lookup_hubspot_contact_enhanced(
                    client=client,
                    full_name=found_name,
                    email=found_email,
                    phone=found_phone,
                    facility_hubspot_id=hubspot_id,
                    parent_hubspot_id=TEST_HUBSPOT_CORP_ID,
                )
                hubspot_contact_ids, hubspot_match_detail, hubspot_match_summary = _format_hubspot_matches_enhanced(
                    matches, match_strategy=match_strategy, facility_name=facility_name
                )
                
                if matches:
                    print(f"  ✓ HubSpot match: {hubspot_match_summary}")
                    print(f"     {hubspot_match_detail}")
                else:
                    print(f"  ℹ Not found in HubSpot")
            else:
                print(f"  ℹ No administrator/ED identified (confidence={confidence})")
                
        except RuntimeError as e:
            error_msg = str(e)
            print(f"  ✗ Error: {error_msg}")
        
        results.append({
            "workflow": "1 - Facility Missing Contact",
            "hubspot_facility_id": hubspot_id,
            "facility_name": facility_name,
            "city": city,
            "state": state,
            "facility_type": fac_type,
            "definitive_id": definitive_id,
            "corporation_name": corp_name,
            "hubspot_corp_id": TEST_HUBSPOT_CORP_ID,
            "found_name": found_name,
            "found_title": found_title,
            "found_email": found_email,
            "found_phone": found_phone,
            "research_confidence": confidence,
            "hubspot_contact_ids": hubspot_contact_ids,
            "hubspot_match_detail": hubspot_match_detail,
            "hubspot_match_summary": hubspot_match_summary,
            "research_findings": research_result[:2000] if research_result else "",
            "research_error": error_msg,
            "researched_at": datetime.now().isoformat(),
        })
        
        # Rate limiting between calls
        if idx < total - 1:
            time.sleep(RATE_LIMIT_SECONDS)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"test_workflow1_results_{timestamp}.csv"
    save_csv(results, output_file)
    
    # Summary
    print("\n" + "=" * 70)
    print("  TEST COMPLETE")
    print("=" * 70)
    print(f"  Facilities tested: {len(results)}")
    print(f"  Results saved to: {output_file}")
    
    # Quick summary of findings
    found_count = sum(1 for r in results if r["found_name"] and r["research_confidence"] != "not_found")
    print(f"  Administrators found: {found_count}/{len(results)}")
    
    for r in results:
        if r["found_name"]:
            print(f"    - {r['facility_name']}: {r['found_name']} ({r['found_title']})")

if __name__ == "__main__":
    main()