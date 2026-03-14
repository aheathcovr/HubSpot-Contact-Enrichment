#!/usr/bin/env python3
"""
Run Workflow 1 for all Brookdale Senior Living facilities.
This processes all facilities missing ED/Administrator contacts.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from state_association_matcher import (
    init_bigquery_client,
    workflow_1_research_facilities,
    save_csv,
    BQ_PROJECT_ID,
    BQ_LOCATION,
    HUBSPOT_COMPANIES_TABLE,
)

# Brookdale Senior Living HubSpot Corporation ID
BROOKDALE_CORP_ID = "50791311268"
BROOKDALE_NETWORK_ID = None  # Not needed for Workflow 1

def main():
    print("=" * 70)
    print("  BROOKDALE SENIOR LIVING - WORKFLOW 1")
    print("  Finding Administrators for Facilities Missing Leadership")
    print("=" * 70)
    
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
    SELECT properties_name AS company_name
    FROM `{HUBSPOT_COMPANIES_TABLE}`
    WHERE CAST(properties_hs_object_id AS STRING) = '{BROOKDALE_CORP_ID}'
    LIMIT 1
    """
    rows = list(client.query(query).result())
    corp_name = rows[0].company_name if rows else "Unknown Corporation"
    print(f"  Corporation: {corp_name}")
    print(f"  HubSpot ID: {BROOKDALE_CORP_ID}")
    
    # Run Workflow 1
    print("\n" + "=" * 70)
    print("  Starting Workflow 1 research...")
    print("=" * 70)
    print("\n  NOTE: This will process ALL facilities missing leadership contacts.")
    print("  Rate limiting: 2.5 seconds between each API call.")
    print("  Estimated time: ~18 minutes for ~419 facilities")
    print("")
    
    results = workflow_1_research_facilities(
        client=client,
        hubspot_corp_id=BROOKDALE_CORP_ID,
        corporation_name=corp_name,
    )
    
    # Save results
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"brookdale_workflow1_{timestamp}.csv"
    save_csv(results, output_file)
    
    # Summary
    print("\n" + "=" * 70)
    print("  COMPLETE")
    print("=" * 70)
    print(f"  Total facilities processed: {len(results)}")
    
    # Count successes
    found = sum(1 for r in results if r.get("found_name") and r.get("research_confidence") != "not_found")
    print(f"  Administrators found: {found}/{len(results)}")
    
    # Count HubSpot matches
    matched = sum(1 for r in results if r.get("hubspot_contact_ids"))
    print(f"  Already in HubSpot: {matched}")
    
    print(f"\n  Results saved to: {output_file}")

if __name__ == "__main__":
    main()