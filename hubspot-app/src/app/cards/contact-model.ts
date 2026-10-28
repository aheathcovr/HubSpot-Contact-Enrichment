// Contact card: how it talks to the enrichment service and how it puts what comes back into words. No HubSpot in this
// file, so the tests run it without HubSpot. The shapes mirror what the service sends (webhook_server.py,
// app_run_store.run_view, contact_objectives).
import type { ServerlessRunnerParams } from '@hubspot/ui-extensions';
import type { TagVariant } from './model.ts';

// The app function the cards call to reach the service (the uid in functions/service-hsmeta.json; a test checks). A
// card cannot call the service itself: a static-auth app has no client secret for HubSpot to sign a fetch with, so
// the function makes the call, says who is asking from HubSpot's own context, and signs it.
export const SERVICE_FUNCTION = 'service';
// HubSpot's object type id for contacts.
export const CONTACT_TYPE_ID = '0-1';

export type ObjectiveId = 'find_email' | 'find_mobile' | 'verify_email';

export interface ObjectiveView {
  id: ObjectiveId;
  label: string;
  available: boolean;
  reason: string | null;
  spends: string;
}

export interface ContactInfo {
  id: string;
  name: string;
  title: string | null;
  email: string | null;
  mobile: string | null;
  emailCheck: string | null;
  emailCheckedOn: string | null;
}

export interface Quota {
  cap: number;
  used: number;
  remaining: number;
}

export interface RunView {
  runId: string;
  contactId: string;
  objective: string;
  status: 'queued' | 'running' | 'done' | 'failed';
  outcome: string | null;
  message: string | null;
  wrote: string[];
  requestedBy: string;
  createdAt: string;
  finishedAt: string | null;
  // Set for a run started from a company card; the contact card's own runs have neither.
  companyId: string | null;
  batchId: string | null;
}

export interface ContactStatus {
  ok: true;
  contact: ContactInfo;
  objectives: ObjectiveView[];
  quota: Quota;
  runs: RunView[];
}

export interface StartResult {
  ok: true;
  reused: boolean;
  run: RunView;
  quota: Quota;
}

export interface ServiceReply {
  status: number;
  json: unknown;
}

export type Service = (method: 'GET' | 'POST', path: string, body?: Record<string, unknown>) => Promise<ServiceReply>;

// hubspot.serverless, as far as the cards use it: a function's name and its parameters, and what it returns.
export type FunctionRunner = (name: string, options: Omit<ServerlessRunnerParams, 'name'>) => Promise<unknown>;

// What the service refused, with its HTTP status and, for the daily cap, the rep's quota.
export class ServiceError extends Error {
  status: number;
  quota?: Quota;

  constructor(message: string, status: number, quota?: Quota) {
    super(message);
    this.status = status;
    this.quota = quota;
  }
}

const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === 'object' && !Array.isArray(v);

const isQuota = (v: unknown): v is Quota =>
  isObject(v) && typeof v.cap === 'number' && typeof v.used === 'number' && typeof v.remaining === 'number';

function statusText(status: number): string {
  if (status === 401 || status === 403) return "HubSpot's request was not accepted by the enrichment service.";
  if (status === 503) return 'The enrichment service is not set up for the app yet.';
  if (status === 404) return 'The enrichment service could not find that.';
  return `The enrichment service answered ${status}.`;
}

// The service answers { ok: true, ... } with a 2xx, or { ok: false, error } with a refusal. A refusal becomes an error.
export function readReply<T>(reply: ServiceReply): T {
  const body = reply.json;
  if (reply.status < 200 || reply.status >= 300) {
    const refusal = isObject(body) ? body : {};
    const message = typeof refusal.error === 'string' && refusal.error ? refusal.error : statusText(reply.status);
    throw new ServiceError(message, reply.status, isQuota(refusal.quota) ? refusal.quota : undefined);
  }
  if (!isObject(body)) throw new ServiceError('The enrichment service sent back something the card could not read.', reply.status);
  return body as T;
}

// What the service function returns: the service's own answer, or its own refusal in the same shape, as { status, json }.
function readFunctionAnswer(answer: unknown): ServiceReply {
  if (!isObject(answer) || typeof answer.status !== 'number' || !Number.isInteger(answer.status)) {
    throw new ServiceError('The app sent back something the card could not read.', 0);
  }
  return { status: answer.status, json: answer.json ?? null };
}

export function makeService(run: FunctionRunner): Service {
  return async (method, path, body) => {
    const parameters = body === undefined ? { method, path } : { method, path, body };
    return readFunctionAnswer(await run(SERVICE_FUNCTION, { parameters: parameters as ServerlessRunnerParams['parameters'] }));
  };
}

export const runIsOpen = (run: RunView): boolean => run.status === 'queued' || run.status === 'running';

const OBJECTIVE_LABELS: Record<string, string> = { find_email: 'Find email', find_mobile: 'Find mobile', verify_email: 'Verify email' };
// The two runs the company card makes that start without a contact: a search by job title, and adding one person it found.
const COMPANY_RUN_LABELS: Record<string, string> = { find_by_title: 'Find by job title', add_candidate: 'Add person' };

export const objectiveLabel = (id: string): string => OBJECTIVE_LABELS[id] ?? COMPANY_RUN_LABELS[id] ?? id;

export interface RunLook {
  label: string;
  variant: TagVariant;
}

// What a run is called on the card. "Needs a look" is a run that found or held something the rep should read in the
// note on the contact; the service's own message says which.
export function runTag(run: RunView): RunLook {
  if (run.status === 'queued') return { label: 'Waiting', variant: 'default' };
  if (run.status === 'running') return { label: 'Running', variant: 'warning' };
  if (run.status === 'failed') return { label: 'Failed', variant: 'error' };
  switch (run.outcome) {
    case 'found':
    case 'verified':
      return { label: 'Done', variant: 'success' };
    case 'held':
    case 'not_written':
    case 'moved':
      return { label: 'Needs a look', variant: 'warning' };
    case 'not_found':
      return { label: 'Nothing found', variant: 'default' };
    case 'skipped':
      return { label: 'Skipped', variant: 'default' };
    default:
      return { label: 'Done', variant: 'default' };
  }
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

// "just now", "5 min ago", "3 h ago", and the date (UTC) once it is a day old or more.
export function agoText(iso: string | null, nowMs: number): string {
  const at = iso ? Date.parse(iso) : NaN;
  if (Number.isNaN(at)) return '';
  const diff = nowMs - at;
  const date = new Date(at);
  const day = `${MONTHS[date.getUTCMonth()]} ${date.getUTCDate()}`;
  if (diff < 0) return day;
  if (diff < 60_000) return 'just now';
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)} min ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)} h ago`;
  return day;
}
