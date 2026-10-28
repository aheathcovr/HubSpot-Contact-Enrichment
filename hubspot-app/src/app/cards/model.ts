// Enrichment card: what it asks the app function for, and how it puts the answer into words. No HubSpot in this
// file, so the tests run it without HubSpot.

// The uid of the app function (functions/enrichment-hsmeta.json), which is the name the card calls it by.
export const FUNCTION_NAME = 'enrichment';
// HubSpot adds these properties of the record the card is on to the call.
export const SENT_PROPERTIES = ['hs_object_id'];
// HubSpot's object type id for companies.
export const COMPANY_TYPE_ID = '0-2';

export type Reason = 'already_requested' | 'recently_enriched' | 'no_company_type' | null;
export type TagVariant = 'default' | 'warning' | 'success' | 'error' | 'info';

export interface CompanySummary {
  id: string;
  name: string;
  kind: 'facility' | 'corporation' | 'other';
  requested: boolean;
  // Dates are YYYY-MM-DD, in UTC, as the service stamps them.
  lastEnrichedOn: string | null;
  daysSince: number | null;
  nextEligibleOn: string | null;
}

export interface ContactRow {
  id: string;
  name: string;
  title: string | null;
  email: string | null;
  // MillionVerifier's own word for the email: ok, catch_all, unknown, invalid, ...
  emailCheck: string | null;
  emailCheckedOn: string | null;
  enrichedOn: string | null;
}

export interface StatusResult {
  ok: true;
  company: CompanySummary;
  canRequest: boolean;
  reason: Reason;
  message: string | null;
  contacts: ContactRow[];
  moreContacts: boolean;
  contactsError?: string;
}

export interface RequestResult {
  ok: true;
  requested: true;
  company: CompanySummary;
  canRequest: false;
  reason: Reason;
  message: string | null;
}

// The function answers { ok: true, ... } or { ok: false, error }. A failure becomes an error with its message.
export function readResult<T>(res: unknown): T {
  if (!res || typeof res !== 'object' || Array.isArray(res) || typeof (res as { ok?: unknown }).ok !== 'boolean') {
    throw new Error('The enrichment app sent back something the card could not read.');
  }
  const r = res as { ok: boolean; error?: unknown };
  if (!r.ok) throw new Error(typeof r.error === 'string' && r.error ? r.error : 'The enrichment app did not give a reason.');
  return res as T;
}

export function messageOf(err: unknown): string {
  if (err instanceof Error && err.message) return err.message;
  if (typeof err === 'string' && err) return err;
  return 'Something went wrong. Try again.';
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

// 2026-10-21 -> "Oct 21, 2026". Nothing for no date; a text that is not a date comes back as it is.
export function formatDay(day: string | null): string {
  if (!day) return '';
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
  if (!m) return day;
  return `${MONTHS[Number(m[2]) - 1]} ${Number(m[3])}, ${m[1]}`;
}

export interface TagLook {
  label: string;
  variant: TagVariant;
}

// The service leaves a company alone for 30 days after a run; a run that old or older is shown as old, not as recent.
const RECENT_DAYS = 30;

// Company Type (hierarchy_type): the property a company has to have set to a facility or a corporation before the
// service can research it. The card shows HubSpot's own editor for it when it is not.
export const COMPANY_TYPE_PROPERTY = 'hierarchy_type';
export const TYPE_NEEDED_TAG: TagLook = { label: 'Company Type needed', variant: 'warning' };

export function statusTag(company: CompanySummary): TagLook {
  if (company.requested) return { label: 'Run requested', variant: 'warning' };
  if (company.daysSince === null || company.lastEnrichedOn === null) return { label: 'Not enriched yet', variant: 'default' };
  const days = company.daysSince;
  if (days < 0) return { label: `Enriched ${formatDay(company.lastEnrichedOn)}`, variant: 'success' };
  if (days >= RECENT_DAYS) return { label: `Last enriched ${days} days ago`, variant: 'default' };
  if (days === 0) return { label: 'Enriched today', variant: 'success' };
  return { label: days === 1 ? 'Enriched 1 day ago' : `Enriched ${days} days ago`, variant: 'success' };
}

const CHECKS: Record<string, TagLook> = {
  ok: { label: 'Good', variant: 'success' },
  catch_all: { label: 'Risky', variant: 'warning' },
  unknown: { label: 'Unknown', variant: 'warning' },
  invalid: { label: 'Invalid', variant: 'error' },
  disposable: { label: 'Disposable', variant: 'error' },
  skipped: { label: 'Not checked', variant: 'default' },
  error: { label: 'Check failed', variant: 'default' },
};

// MillionVerifier's word for an email, as a tag. A word this does not know is shown as it is.
export function emailCheckTag(result: string | null): TagLook | null {
  const word = (result ?? '').trim().toLowerCase();
  if (!word) return null;
  if (CHECKS[word]) return CHECKS[word];
  const text = word.replace(/_/g, ' ');
  return { label: text.charAt(0).toUpperCase() + text.slice(1), variant: 'default' };
}

// The contacts the service has enriched, then the others. The function sends them already in order.
export function groupContacts(contacts: ContactRow[]): { enriched: ContactRow[]; others: ContactRow[] } {
  return { enriched: contacts.filter((c) => c.enrichedOn), others: contacts.filter((c) => !c.enrichedOn) };
}
