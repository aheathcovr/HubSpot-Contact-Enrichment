// Renders the Enrichment card with HubSpot's test renderer and a fake app function.
import { describe, expect, it } from 'vitest';
import { Accordion, Button, EmptyState, ErrorState, LoadingSpinner, Tag, Text } from '@hubspot/ui-extensions';
import { createRenderer } from '@hubspot/ui-extensions/testing';
import { EnrichmentView, type Runner } from '../src/app/cards/EnrichmentView.tsx';
import type { CompanySummary, ContactRow } from '../src/app/cards/model.ts';
import { COMPANIES, COMPANY_ID } from './fixtures.ts';

const company = (over: Partial<CompanySummary> = {}): CompanySummary => ({
  id: COMPANY_ID,
  name: 'Elm Court Care Center',
  kind: 'facility',
  requested: false,
  lastEnrichedOn: null,
  daysSince: null,
  nextEligibleOn: null,
  ...over,
});

const row = (id: string, over: Partial<ContactRow> = {}): ContactRow => ({
  id,
  name: `Contact ${id}`,
  title: null,
  email: null,
  emailCheck: null,
  emailCheckedOn: null,
  enrichedOn: null,
  ...over,
});

const statusResult = (over: Record<string, unknown> = {}) => ({
  ok: true,
  company: company(),
  canRequest: true,
  reason: null,
  message: null,
  contacts: [] as ContactRow[],
  moreContacts: false,
  ...over,
});

const RECENT = {
  company: company({ lastEnrichedOn: '2026-10-21', daysSince: 5, nextEligibleOn: '2026-11-20' }),
  canRequest: false,
  reason: 'recently_enriched',
  message: 'Enriched 5 days ago. The service skips a company enriched in the last 30 days, so a new run can be requested from 2026-11-20.',
};
const REQUESTED = {
  company: company({ requested: true }),
  canRequest: false,
  reason: 'already_requested',
  message: 'A run is already requested for this company. The service starts it within moments and clears the request when it is done.',
};

interface Call {
  name: string;
  options: { propertiesToSend?: string[]; parameters?: Record<string, unknown> };
}

// A fake hubspot.serverless. `status` answers each status call in turn (the last answer repeats); `request` holds
// until released, so a test can look at the card while a request is in flight.
function fakeRun(opts: { status?: (n: number) => unknown; request?: () => unknown; hold?: boolean } = {}) {
  const calls: Call[] = [];
  const pending: (() => void)[] = [];
  let statuses = 0;
  const run: Runner = async (name, options) => {
    calls.push({ name, options: options as Call['options'] });
    const action = (options?.parameters as Record<string, unknown>)?.action;
    if (action === 'status') {
      statuses += 1;
      const answer = (opts.status ?? (() => statusResult()))(statuses);
      if (answer instanceof Error) throw answer;
      return answer;
    }
    if (action === 'request') {
      if (opts.hold) await new Promise<void>((resolve) => pending.push(resolve));
      const answer = (opts.request ?? (() => ({ ok: true, requested: true, ...REQUESTED })))();
      if (answer instanceof Error) throw answer;
      return answer;
    }
    return { ok: false, error: 'unknown' };
  };
  const release = () => pending.splice(0).forEach((r) => r());
  return { run, calls, release, actions: () => calls.map((c) => c.options.parameters?.action) };
}

function setup(run: Runner, opts: { objectTypeId?: string; pollEveryMs?: number; maxPolls?: number } = {}) {
  const r = createRenderer('crm.record.sidebar');
  r.mocks.context.crm.objectId = Number(COMPANY_ID);
  r.mocks.context.crm.objectTypeId = opts.objectTypeId ?? COMPANIES;
  r.render(<EnrichmentView context={r.mocks.context} actions={r.mocks.actions} run={run} pollEveryMs={opts.pollEveryMs ?? 0} maxPolls={opts.maxPolls} />);
  return r;
}

type Rendered = ReturnType<typeof setup>;
const texts = (r: Rendered) => r.findAll(Text).map((t) => t.text ?? '');
const tags = (r: Rendered) => r.findAll(Tag).map((t) => [t.text, t.props.variant]);
const button = (r: Rendered, label: string) => r.find(Button, (b) => b.text === label);
const settle = (ms = 80) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Enrichment card', () => {
  it('loads the status, then shows a company that was never enriched with the request button ready', async () => {
    const f = fakeRun();
    const r = setup(f.run);

    expect(r.maybeFind(LoadingSpinner)).not.toBeNull();
    await r.waitFor(() => expect(tags(r)).toEqual([['Not enriched yet', 'default']]));
    expect(button(r, 'Request enrichment').props.disabled).toBe(false);
    expect(texts(r)).toContain('No enrichment run on record for this company.');
    expect(f.calls[0]).toEqual({
      name: 'enrichment',
      options: { propertiesToSend: ['hs_object_id'], parameters: { objectId: COMPANY_ID, objectTypeId: COMPANIES, action: 'status' } },
    });
  });

  it('shows a recent run, why a new request is not allowed, and a disabled button', async () => {
    const r = setup(fakeRun({ status: () => statusResult(RECENT) }).run);

    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched 5 days ago', 'success']]));
    expect(button(r, 'Request enrichment').props.disabled).toBe(true);
    expect(texts(r)).toContain(RECENT.message);
  });

  it('allows a request for a company last enriched long ago', async () => {
    const old = statusResult({ company: company({ lastEnrichedOn: '2026-09-11', daysSince: 45, nextEligibleOn: '2026-10-11' }) });
    const r = setup(fakeRun({ status: () => old }).run);

    await r.waitFor(() => expect(tags(r)).toEqual([['Last enriched 45 days ago', 'default']]));
    expect(button(r, 'Request enrichment').props.disabled).toBe(false);
    expect(texts(r)).toContain('Last enriched Sep 11, 2026.');
  });

  it('asks for a run, shows it waiting, and says so with an alert', async () => {
    const f = fakeRun({ hold: true });
    const r = setup(f.run);
    await r.waitFor(() => expect(button(r, 'Request enrichment').props.disabled).toBe(false));

    button(r, 'Request enrichment').trigger('onClick');

    await r.waitFor(() => expect(button(r, 'Requesting').props.disabled).toBe(true));
    expect(f.calls[1].options.parameters).toEqual({ objectId: COMPANY_ID, objectTypeId: COMPANIES, action: 'request' });
    f.release();
    await r.waitFor(() => expect(tags(r)).toEqual([['Run requested', 'warning']]));
    expect(button(r, 'Request enrichment').props.disabled).toBe(true);
    expect(texts(r)).toContain(REQUESTED.message);
    expect(r.mocks.actions.addAlert.calls).toEqual([
      [{ type: 'success', title: 'Enrichment requested', message: 'The service starts the run within moments. This card updates when it finishes.' }],
    ]);
  });

  it('sends one request however many times the button is pressed', async () => {
    const f = fakeRun({ hold: true });
    const r = setup(f.run);
    await r.waitFor(() => expect(button(r, 'Request enrichment').props.disabled).toBe(false));

    button(r, 'Request enrichment').trigger('onClick');
    await r.waitFor(() => expect(button(r, 'Requesting').props.disabled).toBe(true));
    button(r, 'Requesting').trigger('onClick');
    f.release();
    await r.waitFor(() => expect(tags(r)).toEqual([['Run requested', 'warning']]));

    expect(f.actions().filter((a) => a === 'request')).toHaveLength(1);
  });

  it('says why a request was refused and reloads the status', async () => {
    let n = 0;
    const f = fakeRun({
      status: () => (++n === 1 ? statusResult() : statusResult(REQUESTED)),
      request: () => ({ ok: false, error: REQUESTED.message, reason: 'already_requested' }),
    });
    const r = setup(f.run);
    await r.waitFor(() => expect(button(r, 'Request enrichment').props.disabled).toBe(false));

    button(r, 'Request enrichment').trigger('onClick');

    await r.waitFor(() => expect(r.mocks.actions.addAlert.calls).toHaveLength(1));
    expect(r.mocks.actions.addAlert.calls[0]).toEqual([{ type: 'danger', title: 'Not requested', message: REQUESTED.message }]);
    await r.waitFor(() => expect(tags(r)).toEqual([['Run requested', 'warning']]));
    expect(f.actions()).toEqual(['status', 'request', 'status']);
  });

  it('says so when the request itself fails, and lets the rep try again', async () => {
    const f = fakeRun({ request: () => new Error('The app is missing a permission for setting Request to enrich (403).') });
    const r = setup(f.run);
    await r.waitFor(() => expect(button(r, 'Request enrichment').props.disabled).toBe(false));

    button(r, 'Request enrichment').trigger('onClick');

    await r.waitFor(() => expect(r.mocks.actions.addAlert.calls).toHaveLength(1));
    expect(r.mocks.actions.addAlert.calls[0][0]).toMatchObject({ type: 'danger', title: 'Not requested', message: 'The app is missing a permission for setting Request to enrich (403).' });
    await r.waitFor(() => expect(button(r, 'Request enrichment').props.disabled).toBe(false));
  });

  it('checks again while a run is requested and shows the result when it finishes', async () => {
    const done = statusResult({
      company: company({ lastEnrichedOn: '2026-10-26', daysSince: 0, nextEligibleOn: '2026-11-25' }),
      canRequest: false,
      reason: 'recently_enriched',
      message: 'Enriched today.',
      contacts: [row('7', { name: 'Pat Lee', enrichedOn: '2026-10-26' })],
    });
    const f = fakeRun({ status: (n) => (n < 3 ? statusResult(REQUESTED) : done) });
    const r = setup(f.run, { pollEveryMs: 15 });

    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched today', 'success']]));
    expect(texts(r)).toContain('Pat Lee');
    const polls = f.actions().length;
    expect(polls).toBe(3);
    await settle();
    expect(f.actions()).toHaveLength(polls);
  });

  it('skips a check that fails while waiting, says nothing, and carries on', async () => {
    const done = statusResult({ company: company({ lastEnrichedOn: '2026-10-26', daysSince: 0, nextEligibleOn: '2026-11-25' }), canRequest: false, reason: 'recently_enriched', message: 'Enriched today.' });
    const f = fakeRun({
      status: (n) => (n === 2 ? new Error('HubSpot is rate limiting the app. Try again in a minute.') : n < 4 ? statusResult(REQUESTED) : done),
    });
    const r = setup(f.run, { pollEveryMs: 15 });

    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched today', 'success']]));

    expect(f.actions()).toHaveLength(4);
    expect(r.mocks.actions.addAlert.calls).toEqual([]);
    expect(r.maybeFind(ErrorState)).toBeNull();
  });

  it('starts checking again after Refresh once it had given up', async () => {
    let n = 0;
    const done = statusResult({ company: company({ lastEnrichedOn: '2026-10-26', daysSince: 0, nextEligibleOn: '2026-11-25' }), canRequest: false, reason: 'recently_enriched', message: 'Enriched today.' });
    const f = fakeRun({ status: () => (++n <= 4 ? statusResult(REQUESTED) : done) });
    const r = setup(f.run, { pollEveryMs: 10, maxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched today', 'success']]));
    expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(false);
  });

  it('does not check again when nothing is requested', async () => {
    const f = fakeRun();
    const r = setup(f.run, { pollEveryMs: 15 });
    await r.waitFor(() => expect(tags(r)).toEqual([['Not enriched yet', 'default']]));

    await settle();

    expect(f.actions()).toEqual(['status']);
  });

  it('stops checking after a while and says it is still waiting', async () => {
    const f = fakeRun({ status: () => statusResult(REQUESTED) });
    const r = setup(f.run, { pollEveryMs: 10, maxPolls: 2 });

    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const calls = f.actions().length;
    await settle();

    expect(calls).toBe(3);
    expect(f.actions()).toHaveLength(calls);
  });

  it('refreshes when asked', async () => {
    let n = 0;
    const f = fakeRun({ status: () => (++n === 1 ? statusResult() : statusResult(RECENT)) });
    const r = setup(f.run);
    await r.waitFor(() => expect(tags(r)).toEqual([['Not enriched yet', 'default']]));

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched 5 days ago', 'success']]));
    expect(f.actions()).toEqual(['status', 'status']);
  });

  it('keeps what it shows and says so when a refresh fails', async () => {
    let n = 0;
    const f = fakeRun({ status: () => (++n === 1 ? statusResult(RECENT) : new Error('HubSpot is rate limiting the app. Try again in a minute.')) });
    const r = setup(f.run);
    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched 5 days ago', 'success']]));

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(r.mocks.actions.addAlert.calls).toHaveLength(1));
    expect(r.mocks.actions.addAlert.calls[0]).toEqual([{ type: 'danger', title: 'Status not refreshed', message: 'HubSpot is rate limiting the app. Try again in a minute.' }]);
    expect(tags(r)).toEqual([['Enriched 5 days ago', 'success']]);
  });

  it('shows an error when the status does not load, and tries again on request', async () => {
    let n = 0;
    const f = fakeRun({ status: () => (++n === 1 ? new Error('The app has no access token. Reinstall the app.') : statusResult()) });
    const r = setup(f.run);

    await r.waitFor(() => expect(r.maybeFind(ErrorState)).not.toBeNull());
    expect(r.find(ErrorState).props.title).toBe('The enrichment status did not load');
    expect(texts(r)).toContain('The app has no access token. Reinstall the app.');
    button(r, 'Try again').trigger('onClick');

    await r.waitFor(() => expect(tags(r)).toEqual([['Not enriched yet', 'default']]));
    expect(r.maybeFind(ErrorState)).toBeNull();
  });

  it('does not work on a record that is not a company', async () => {
    const f = fakeRun();
    const r = setup(f.run, { objectTypeId: '0-1' });

    expect(r.find(EmptyState).props.title).toBe('Not available here');
    expect(texts(r)).toContain('The Enrichment card works on company records.');
    await settle();
    expect(f.calls).toEqual([]);
  });
});

describe('Enrichment card: contacts', () => {
  const contacts = [
    row('1', { name: 'Pat Lee', title: 'Administrator', email: 'pat.lee@example.org', emailCheck: 'ok', emailCheckedOn: '2026-10-21', enrichedOn: '2026-10-21' }),
    row('2', { name: 'Sam Roe', title: 'Director of Nursing', email: 'sam.roe@example.org', emailCheck: 'catch_all', enrichedOn: '2026-09-01' }),
    row('3', { name: 'Kim Day', email: null }),
    row('4', { name: 'Al Fox', title: 'Receptionist', email: 'al@example.org', emailCheck: 'invalid' }),
  ];

  it('lists the contacts the service enriched, with title, email, check and date', async () => {
    const r = setup(fakeRun({ status: () => statusResult({ contacts }) }).run);

    await r.waitFor(() => expect(texts(r)).toContain('Contacts the service enriched (2)'));
    expect(texts(r)).toEqual(expect.arrayContaining(['Pat Lee', 'Administrator', 'pat.lee@example.org', 'Enriched Oct 21, 2026', 'Sam Roe', 'Director of Nursing', 'sam.roe@example.org', 'Enriched Sep 1, 2026']));
    expect(tags(r)).toEqual(expect.arrayContaining([['Good', 'success'], ['Risky', 'warning']]));
  });

  it('puts the other contacts in a section of their own', async () => {
    const r = setup(fakeRun({ status: () => statusResult({ contacts }) }).run);

    await r.waitFor(() => expect(r.findAll(Accordion)).toHaveLength(1));
    expect(r.find(Accordion).props.title).toBe('Other contacts (2)');
    expect(texts(r)).toEqual(expect.arrayContaining(['Kim Day', 'Al Fox', 'Receptionist']));
    expect(tags(r)).toEqual(expect.arrayContaining([['Invalid', 'error']]));
  });

  it('shows no section for the contacts it did not enrich when there are none', async () => {
    const r = setup(fakeRun({ status: () => statusResult({ contacts: contacts.slice(0, 2) }) }).run);

    await r.waitFor(() => expect(texts(r)).toContain('Contacts the service enriched (2)'));
    expect(r.findAll(Accordion)).toHaveLength(0);
  });

  it('says there are no enriched contacts when only others are linked', async () => {
    const r = setup(fakeRun({ status: () => statusResult({ contacts: contacts.slice(2) }) }).run);

    await r.waitFor(() => expect(texts(r)).toContain('The service has not enriched any contact here yet.'));
    expect(r.find(Accordion).props.title).toBe('Other contacts (2)');
  });

  it('says so when no contact is linked', async () => {
    const r = setup(fakeRun().run);

    await r.waitFor(() => expect(texts(r)).toContain('No contacts are linked to this company yet.'));
    expect(r.findAll(Accordion)).toHaveLength(0);
  });

  it('says when there are more contacts than it shows', async () => {
    const r = setup(fakeRun({ status: () => statusResult({ contacts, moreContacts: true }) }).run);

    await r.waitFor(() => expect(texts(r)).toContain('Showing the first 100 contacts. This company has more.'));
  });

  it('says when the contacts could not be read, and still shows the company', async () => {
    const r = setup(fakeRun({ status: () => statusResult({ ...RECENT, contacts: [], contactsError: 'The app is missing a permission for reading the contacts (403).' }) }).run);

    await r.waitFor(() => expect(tags(r)).toEqual([['Enriched 5 days ago', 'success']]));
    expect(texts(r)).toContain('Contacts could not be read: The app is missing a permission for reading the contacts (403).');
  });
});
