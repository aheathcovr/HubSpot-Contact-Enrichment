// Shared fakes for the tests of the company card's picking and title search: made-up data, a fake app function (the
// company's contacts) and a fake enrichment service (the runs, the quota and the people a search found).
import { useState } from 'react';
import { Button, Checkbox, Tag, Text } from '@hubspot/ui-extensions';
import { createRenderer } from '@hubspot/ui-extensions/testing';
import { EnrichmentView, type Runner } from '../src/app/cards/EnrichmentView.tsx';
import type { CompanySummary, ContactRow } from '../src/app/cards/model.ts';
import type { AddItem, BatchItem, BatchObjective, Candidate, CompanyStatus } from '../src/app/cards/company-model.ts';
import type { Quota, RunView, Service, ServiceReply } from '../src/app/cards/contact-model.ts';
import { COMPANIES, COMPANY_ID } from './fixtures.ts';

export const NOW = Date.parse('2026-10-26T16:10:00Z');
export { COMPANY_ID };

export const company: CompanySummary = { id: COMPANY_ID, name: 'Elm Court Care Center', kind: 'facility', requested: false, lastEnrichedOn: null, daysSince: null, nextEligibleOn: null };

export const row = (id: string, name: string, over: Partial<ContactRow> = {}): ContactRow => ({
  id, name, title: null, email: null, emailCheck: null, emailCheckedOn: null, enrichedOn: null, ...over,
});

// Contact 1 was enriched by the service, so it is in the first list; 2 and 3 are in "Other contacts".
export const CONTACTS = [row('1', 'Pat Lee', { title: 'Administrator', enrichedOn: '2026-10-14' }), row('2', 'Sam Ortiz'), row('3', 'Kim Day')];

export const functionStatus = (contacts = CONTACTS) => ({ ok: true, company, canRequest: true, reason: null, message: null, contacts, moreContacts: false });

export const OBJECTIVES: BatchObjective[] = [
  { id: 'find_email', label: 'Find email', spends: 'up to 2 lookups and 1 check' },
  { id: 'find_mobile', label: 'Find mobile', spends: '1 lookup' },
  { id: 'verify_email', label: 'Verify email', spends: '1 check' },
  { id: 'enrich', label: 'Enrich', spends: 'whatever is missing: up to 3 lookups and 1 check' },
];

export const PRESETS = ['Administrator', 'Executive Director', 'Director of Nursing'];

export const quota = (remaining = 22, cap = 25): Quota => ({ cap, used: cap - remaining, remaining });

export const run = (over: Partial<RunView> = {}): RunView => ({
  runId: 'a'.repeat(32), contactId: '1', objective: 'find_email', status: 'queued', outcome: null, message: null, wrote: [],
  requestedBy: 'rep@example.org', createdAt: '2026-10-26T16:09:00Z', finishedAt: null, companyId: COMPANY_ID, batchId: 'b'.repeat(32), ...over,
});

export const serviceStatus = (over: Partial<CompanyStatus> = {}): CompanyStatus => ({
  ok: true, company: { id: COMPANY_ID, name: 'Elm Court Care Center', kind: 'facility' }, objectives: OBJECTIVES, titlePresets: PRESETS,
  limits: { maxContacts: 10, maxTitles: 5 }, quota: quota(), runs: [], ...over,
});

export const reply = (json: unknown, code = 200): ServiceReply => ({ status: code, json });

export const item = (contactId: string, objective: string, status: BatchItem['status'], over: Partial<BatchItem> = {}): BatchItem => ({
  contactId, objective, status, reason: null, run: null, ...over,
});

export const started = (items: BatchItem[], remaining = 20) =>
  reply({ ok: true, batchId: 'b'.repeat(32), items, started: items.filter((i) => i.status === 'started').length, quota: quota(remaining) }, 202);

// ── The title search ──────────────────────────────────────────────────────────

export const FIND_ID = 'f'.repeat(32);

export const findRun = (over: Partial<RunView> = {}): RunView =>
  run({ runId: FIND_ID, contactId: '', objective: 'find_by_title', status: 'queued', batchId: null, ...over });

export const candidate = (id: string, name: string, over: Partial<Candidate> = {}): Candidate => ({
  candidateId: id, name, title: 'Medical Director', linkedinUrl: null, updatedAt: null, linkedContactId: null, added: null, ...over,
});

export const aitem = (candidateId: string, status: AddItem['status'], over: Partial<AddItem> = {}): AddItem => ({
  candidateId, objective: 'add_candidate', status, reason: null, run: null, ...over,
});

export const addStarted = (items: AddItem[], remaining = 20) =>
  reply({ ok: true, batchId: 'b'.repeat(32), items, started: items.filter((i) => i.status === 'started').length, quota: quota(remaining) }, 202);

export const found = (candidates: Candidate[], over: Partial<RunView> = {}) =>
  reply({ ok: true, ready: true, run: findRun({ status: 'done', outcome: 'found', message: `Found ${candidates.length} people. Choose who to add.`, ...over }), candidates });

// ── Fakes ─────────────────────────────────────────────────────────────────────

export interface Call {
  method: string;
  path: string;
  body?: Record<string, unknown>;
}

export interface FakeOptions {
  status?: (n: number) => ServiceReply | Error;
  start?: (body: Record<string, unknown>) => ServiceReply | Error;
  find?: (body: Record<string, unknown>) => ServiceReply | Error;
  candidates?: (n: number) => ServiceReply | Error;
  add?: (body: Record<string, unknown>) => ServiceReply | Error;
  // A start, find or add waits until released, so a test can look at the card while it is in flight.
  hold?: boolean;
}

// A fake enrichment service. Each GET answers in turn (the function is given the call count for its route).
export function fakeService(opts: FakeOptions = {}) {
  const calls: Call[] = [];
  const pending: (() => void)[] = [];
  const counts = { status: 0, candidates: 0 };
  const service: Service = async (method, path, body) => {
    calls.push({ method, path, body });
    let answer: ServiceReply | Error;
    if (method === 'GET' && path.startsWith('/company/candidates')) {
      counts.candidates += 1;
      answer = (opts.candidates ?? (() => found([])))(counts.candidates);
    } else if (method === 'GET') {
      counts.status += 1;
      answer = (opts.status ?? (() => reply(serviceStatus())))(counts.status);
    } else {
      if (opts.hold) await new Promise<void>((resolve) => pending.push(resolve));
      if (path === '/company/find') answer = (opts.find ?? (() => reply({ ok: true, reused: false, run: findRun(), quota: quota(21) }, 202)))(body ?? {});
      else if (path === '/company/add') answer = (opts.add ?? (() => addStarted([])))(body ?? {});
      else answer = (opts.start ?? (() => started([item('1', 'find_email', 'started', { run: run() })])))(body ?? {});
    }
    if (answer instanceof Error) throw answer;
    return answer;
  };
  return {
    service,
    calls,
    release: () => pending.splice(0).forEach((r) => r()),
    kinds: () => calls.map((c) => c.method),
    gets: (prefix: string) => calls.filter((c) => c.method === 'GET' && c.path.startsWith(prefix)).length,
    posts: (path: string) => calls.filter((c) => c.method === 'POST' && c.path === path),
  };
}

export type Fake = ReturnType<typeof fakeService>;

export const functionRunner = (contacts = CONTACTS): Runner => async () => functionStatus(contacts);

export function setup(service: Service | undefined, opts: { contacts?: ContactRow[]; runPollEveryMs?: number; runMaxPolls?: number } = {}) {
  const r = createRenderer('crm.record.sidebar');
  r.mocks.context.crm.objectId = Number(COMPANY_ID);
  r.mocks.context.crm.objectTypeId = COMPANIES;
  r.render(
    <EnrichmentView
      context={r.mocks.context}
      actions={r.mocks.actions}
      run={functionRunner(opts.contacts)}
      service={service}
      pollEveryMs={0}
      runPollEveryMs={opts.runPollEveryMs ?? 0}
      runMaxPolls={opts.runMaxPolls}
      now={() => NOW}
    />,
  );
  return r;
}

// The same card on one instance that is moved from company to company, as HubSpot may do when a rep opens another record.
export function setupSwitching(service: Service, opts: { runPollEveryMs?: number; runMaxPolls?: number } = {}) {
  let switchTo: (id: number) => void = () => {};
  const r = createRenderer('crm.record.sidebar');
  r.mocks.context.crm.objectId = Number(COMPANY_ID);
  r.mocks.context.crm.objectTypeId = COMPANIES;
  function Harness() {
    const [id, setId] = useState(Number(COMPANY_ID));
    switchTo = setId;
    const context = { ...r.mocks.context, crm: { ...r.mocks.context.crm, objectId: id } };
    return (
      <EnrichmentView context={context} actions={r.mocks.actions} run={functionRunner()} service={service} pollEveryMs={0}
        runPollEveryMs={opts.runPollEveryMs ?? 0} runMaxPolls={opts.runMaxPolls} now={() => NOW} />
    );
  }
  r.render(<Harness />);
  return { r, moveTo: (id: number) => switchTo(id) };
}

export type Rendered = ReturnType<typeof setup>;
export const texts = (r: Rendered) => r.findAll(Text).map((t) => t.text ?? '');
export const tags = (r: Rendered) => r.findAll(Tag).map((t) => [t.text, t.props.variant]);
export const button = (r: Rendered, label: string) => r.find(Button, (b) => b.text === label);
export const box = (r: Rendered, name: string) => r.find(Checkbox, (c) => c.props.name === name);
export const settle = (ms = 80) => new Promise((resolve) => setTimeout(resolve, ms));
export const alerts = (r: Rendered) => r.mocks.actions.addAlert.calls.map((c: unknown[]) => c[0]);
