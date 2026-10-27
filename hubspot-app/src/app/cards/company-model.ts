// Company card, picking contacts: what it asks the enrichment service and how it words what comes back. No HubSpot in
// this file, so the tests run it without HubSpot. The shapes mirror what the service sends (webhook_server.py,
// /app/company/status and /app/company/run).
import type { Quota, RunView } from './contact-model.ts';

export type CompanyKind = 'facility' | 'corporation' | 'other';

export interface BatchObjective {
  id: 'find_email' | 'find_mobile' | 'verify_email' | 'enrich';
  label: string;
  spends: string;
}

export interface CompanyInfo {
  id: string;
  name: string;
  kind: CompanyKind;
}

export interface CompanyStatus {
  ok: true;
  company: CompanyInfo;
  objectives: BatchObjective[];
  // The titles the company runs look for on this kind of company, as quick picks for a title search.
  titlePresets: string[];
  limits: { maxContacts: number; maxTitles: number };
  quota: Quota;
  runs: RunView[];
}

export type ItemStatus = 'started' | 'reused' | 'skipped' | 'failed';

// One run a request meant, or the reason a contact got none. Enrich can mean two runs for one contact.
export interface BatchItem {
  contactId: string;
  objective: string;
  status: ItemStatus;
  reason: string | null;
  run: RunView | null;
}

export interface BatchResult {
  ok: true;
  batchId: string;
  items: BatchItem[];
  started: number;
  quota: Quota;
}

export const statusPath = (companyId: string): string => `/company/status?companyId=${companyId}`;

export function startButtonText(picked: number): string {
  return picked === 0 ? 'Choose contacts' : `Start for ${picked} ${picked === 1 ? 'contact' : 'contacts'}`;
}

// Enrich is a plan: for each contact it can be one run that looks or checks the email and one that looks for a mobile.
export const runsUpTo = (objective: string, picked: number): number => (objective === 'enrich' ? picked * 2 : picked);

export function quotaWarning(objective: string, picked: number, remaining: number): string | null {
  const most = runsUpTo(objective, picked);
  if (picked === 0 || most <= remaining) return null;
  return `This could use up to ${most} ${most === 1 ? 'run' : 'runs'} and you have ${remaining} left today.`;
}

export interface BatchAlert {
  type: 'success' | 'info';
  title: string;
  message: string;
}

// What a batch did, in words: how many runs started, which were the earlier runs, and who was skipped and why.
export function batchAlert(result: BatchResult, names: Record<string, string>): BatchAlert {
  const nameOf = (id: string) => names[id] ?? `Contact ${id}`;
  const started = result.items.filter((i) => i.status === 'started').length;
  const reused = result.items.filter((i) => i.status === 'reused').length;
  const reusedText = reused === 1 ? '1 was asked in the last day, so the earlier run is shown.' : `${reused} were asked in the last day, so the earlier runs are shown.`;

  if (started === 0 && reused > 0) {
    return { type: 'info', title: 'Already asked', message: `${reusedText} Nothing more was spent.` };
  }
  const parts: string[] = [];
  if (started > 0) parts.push(started === 1 ? '1 run started. This card updates when it finishes.' : `${started} runs started. This card updates when they finish.`);
  if (reused > 0) parts.push(reusedText);
  for (const i of result.items.filter((x) => x.status === 'skipped')) parts.push(`Skipped ${nameOf(i.contactId)}: ${i.reason ?? 'Nothing to do.'}`);
  for (const i of result.items.filter((x) => x.status === 'failed')) parts.push(`Could not start for ${nameOf(i.contactId)}: ${i.reason ?? 'Try again.'}`);
  return { type: 'success', title: started > 0 ? 'Runs started' : 'Nothing started', message: parts.join(' ') };
}

// ── Finding people by job title ───────────────────────────────────────────────

// A person a title search found. Names are kept by the service for two days, not in the run record.
export interface Candidate {
  candidateId: string;
  name: string;
  title: string | null;
  linkedinUrl: string | null;
  updatedAt: string | null;
  // The contact that is already this person on this company, when there is one.
  linkedContactId: string | null;
  // How adding this person is going, from the newest add asked for.
  added: RunView | null;
}

export interface CandidateList {
  ok: true;
  ready: boolean;
  run: RunView;
  candidates: Candidate[];
}

export interface FindResult {
  ok: true;
  reused: boolean;
  run: RunView;
  quota: Quota;
}

// One person an add was asked for, or the reason they got none.
export interface AddItem {
  candidateId: string;
  objective: string;
  status: ItemStatus;
  reason: string | null;
  run: RunView | null;
}

export interface AddResult {
  ok: true;
  batchId: string;
  items: AddItem[];
  started: number;
  quota: Quota;
}

export const candidatesPath = (companyId: string, runId: string): string => `/company/candidates?companyId=${companyId}&runId=${runId}`;

// What a rep typed into the "other titles" box: one per comma, semicolon or line.
export const parseTitles = (text: string): string[] => text.split(/[,;\n]/).map((t) => t.trim()).filter(Boolean);

// The quick picks first, then what was typed, with no title twice (in any case).
export function combineTitles(picks: string[], typed: string): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const title of [...picks, ...parseTitles(typed)]) {
    const key = title.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      out.push(title);
    }
  }
  return out;
}

export const searchButtonText = (titles: number): string => (titles === 0 ? 'Choose job titles' : `Search for ${titles} ${titles === 1 ? 'title' : 'titles'}`);

export const addButtonText = (people: number): string => (people === 0 ? 'Choose people' : `Add ${people} ${people === 1 ? 'person' : 'people'}`);

export function findAlert(result: FindResult): BatchAlert {
  return result.reused
    ? { type: 'info', title: 'Already searched', message: 'The same titles were searched in the last day, so the earlier results are shown. Nothing more was spent.' }
    : { type: 'success', title: 'Search started', message: 'The service is looking. This card shows the people it finds.' };
}

// What an add did, in words: how many people are being added, and who was skipped and why.
export function addAlert(result: AddResult, names: Record<string, string>): BatchAlert {
  const nameOf = (id: string) => names[id] ?? `Person ${id}`;
  const started = result.items.filter((i) => i.status === 'started').length;
  const reused = result.items.filter((i) => i.status === 'reused').length;
  const reusedText = reused === 1 ? '1 was already asked for, so the earlier run is shown.' : `${reused} were already asked for, so the earlier runs are shown.`;

  if (started === 0 && reused > 0) return { type: 'info', title: 'Already added', message: `${reusedText} Nothing more was spent.` };
  const parts: string[] = [];
  if (started > 0) parts.push(started === 1 ? '1 person is being added. This card updates when it finishes.' : `${started} people are being added. This card updates when they finish.`);
  if (reused > 0) parts.push(reusedText);
  for (const i of result.items.filter((x) => x.status === 'skipped')) parts.push(`Skipped ${nameOf(i.candidateId)}: ${i.reason ?? 'Nothing to do.'}`);
  for (const i of result.items.filter((x) => x.status === 'failed')) parts.push(`Could not start for ${nameOf(i.candidateId)}: ${i.reason ?? 'Try again.'}`);
  return { type: 'success', title: started > 0 ? 'Adding people' : 'Nothing started', message: parts.join(' ') };
}
