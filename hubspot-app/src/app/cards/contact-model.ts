// Contact card: how it talks to the enrichment service and how it puts what comes back into words. No HubSpot in this
// file, so the tests run it without HubSpot. The shapes mirror what the service sends (webhook_server.py,
// app_run_store.run_view, contact_objectives).
import type { TagVariant } from './model.ts';

// Where hubspot.fetch calls. It must sit under an entry of permittedUrls.fetch in app-hsmeta.json (a test checks).
export const SERVICE_URL = 'https://hubspot-enrichment-kpqza7tiga-uc.a.run.app/app';
// HubSpot's object type id for contacts.
export const CONTACT_TYPE_ID = '0-1';
// A fetch waits this long. HubSpot's default is 15 seconds; the service answers a start or a status at once.
const TIMEOUT_MS = 20000;

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

export type FetchLike = (
  url: string,
  options: { method: 'GET' | 'POST'; timeout: number; body?: Record<string, unknown> },
) => Promise<{ status: number; json(): Promise<unknown> }>;

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

export function makeService(doFetch: FetchLike, base: string = SERVICE_URL): Service {
  return async (method, path, body) => {
    const response = await doFetch(`${base}${path}`, body === undefined ? { method, timeout: TIMEOUT_MS } : { method, timeout: TIMEOUT_MS, body });
    let json: unknown = null;
    try {
      json = await response.json();
    } catch {
      json = null;
    }
    return { status: response.status, json };
  };
}

export const runIsOpen = (run: RunView): boolean => run.status === 'queued' || run.status === 'running';

const OBJECTIVE_LABELS: Record<string, string> = { find_email: 'Find email', find_mobile: 'Find mobile', verify_email: 'Verify email' };

export const objectiveLabel = (id: string): string => OBJECTIVE_LABELS[id] ?? id;

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
