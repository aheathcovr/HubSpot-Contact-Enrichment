// Shared test data. Every name and id here is made up.
import type { Props } from './hubspot-fake.ts';

// Monday 26 Oct 2026, 16:00 UTC.
export const NOW = Date.UTC(2026, 9, 26, 16, 0, 0);
export const COMPANIES = '0-2';
export const COMPANY_ID = '5001';

export const FACILITY = 'Individual Facility / Child';
export const CORPORATION = 'Corporation / Operator';

export const company = (over: Props = {}): { id: string; properties: Props; contacts?: string[] } => ({
  id: COMPANY_ID,
  properties: {
    name: 'Elm Court Care Center',
    hierarchy_type: FACILITY,
    request_to_enrich: 'false',
    revops_enrichment_date: null,
    ...over,
  },
});

export const contact = (id: string, over: Props = {}): { id: string; properties: Props } => ({
  id,
  properties: {
    firstname: 'Pat',
    lastname: 'Lee',
    jobtitle: 'Administrator',
    email: 'pat.lee@example.org',
    email_verification_result: null,
    email_verification_date: null,
    most_recent_enrichment_date: null,
    ...over,
  },
});

// What the card sends HubSpot to run the function: the action, the record, and the property HubSpot adds itself.
export const ctx = (action: string, over: { objectId?: string; objectTypeId?: string; sent?: Record<string, string>; extra?: Record<string, unknown> } = {}) => ({
  parameters: { action, objectId: over.objectId ?? COMPANY_ID, objectTypeId: over.objectTypeId ?? COMPANIES, ...(over.extra ?? {}) },
  propertiesToSend: over.sent ?? { hs_object_id: over.objectId ?? COMPANY_ID },
});

// A date at UTC midnight, as epoch milliseconds, the way the service writes revops_enrichment_date.
export const ms = (y: number, m: number, d: number) => String(Date.UTC(y, m - 1, d));
