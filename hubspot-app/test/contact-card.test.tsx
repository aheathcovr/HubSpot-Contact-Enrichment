// Renders the Contact check card with HubSpot's test renderer and a fake service.
import { useState } from 'react';
import { describe, expect, it } from 'vitest';
import { Button, EmptyState, ErrorState, LoadingSpinner, Tag, Text } from '@hubspot/ui-extensions';
import { createRenderer } from '@hubspot/ui-extensions/testing';
import { ContactCheckView } from '../src/app/cards/ContactCheckView.tsx';
import type { ContactInfo, ContactStatus, ObjectiveView, Quota, RunView, Service, ServiceReply } from '../src/app/cards/contact-model.ts';

const NOW = Date.parse('2026-10-26T16:10:00Z');
const CONTACT_ID = '42';

const OPEN: ObjectiveView[] = [
  { id: 'find_email', label: 'Find email', available: true, reason: null, spends: 'up to 2 lookups and 1 check' },
  { id: 'find_mobile', label: 'Find mobile', available: true, reason: null, spends: '1 lookup' },
  { id: 'verify_email', label: 'Verify email', available: false, reason: 'No email to check.', spends: '1 check' },
];

const contact = (over: Partial<ContactInfo> = {}): ContactInfo => ({
  id: CONTACT_ID, name: 'Pat Lee', title: 'Administrator', email: null, mobile: null, emailCheck: null, emailCheckedOn: null, ...over,
});

const run = (over: Partial<RunView> = {}): RunView => ({
  runId: 'a'.repeat(32), contactId: CONTACT_ID, objective: 'find_email', status: 'queued', outcome: null, message: null, wrote: [],
  requestedBy: 'rep@example.org', createdAt: '2026-10-26T16:09:00Z', finishedAt: null, ...over,
});

const quota = (remaining = 22, cap = 25): Quota => ({ cap, used: cap - remaining, remaining });

const status = (over: Partial<ContactStatus> = {}): ContactStatus => ({
  ok: true, contact: contact(), objectives: OPEN, quota: quota(), runs: [], ...over,
});

const reply = (json: unknown, code = 200): ServiceReply => ({ status: code, json });

interface Call {
  method: string;
  path: string;
  body?: Record<string, unknown>;
}

// A fake service. `status` answers each status call in turn (it is given the call count); `start` answers a start.
// With `hold`, a start waits until released so a test can look at the card while it is in flight.
function fakeService(opts: { status?: (n: number) => ServiceReply | Error; start?: (body: Record<string, unknown>) => ServiceReply | Error; hold?: boolean } = {}) {
  const calls: Call[] = [];
  const pending: (() => void)[] = [];
  let statuses = 0;
  const service: Service = async (method, path, body) => {
    calls.push({ method, path, body });
    let answer: ServiceReply | Error;
    if (method === 'GET') {
      statuses += 1;
      answer = (opts.status ?? (() => reply(status())))(statuses);
    } else {
      if (opts.hold) await new Promise<void>((resolve) => pending.push(resolve));
      answer = (opts.start ?? (() => reply({ ok: true, reused: false, run: run(), quota: quota(21) }, 202)))(body ?? {});
    }
    if (answer instanceof Error) throw answer;
    return answer;
  };
  return { service, calls, release: () => pending.splice(0).forEach((r) => r()), kinds: () => calls.map((c) => c.method) };
}

function setup(service: Service, opts: { objectTypeId?: string; pollEveryMs?: number; maxPolls?: number } = {}) {
  const r = createRenderer('crm.record.sidebar');
  r.mocks.context.crm.objectId = Number(CONTACT_ID);
  r.mocks.context.crm.objectTypeId = opts.objectTypeId ?? '0-1';
  r.render(<ContactCheckView context={r.mocks.context} actions={r.mocks.actions} service={service} pollEveryMs={opts.pollEveryMs ?? 0} maxPolls={opts.maxPolls} now={() => NOW} />);
  return r;
}

type Rendered = ReturnType<typeof setup>;
const texts = (r: Rendered) => r.findAll(Text).map((t) => t.text ?? '');
const tags = (r: Rendered) => r.findAll(Tag).map((t) => [t.text, t.props.variant]);
const button = (r: Rendered, label: string) => r.find(Button, (b) => b.text === label);
const settle = (ms = 80) => new Promise((resolve) => setTimeout(resolve, ms));
const alerts = (r: Rendered) => r.mocks.actions.addAlert.calls.map((c: unknown[]) => c[0]);

describe('Contact check card', () => {
  it('loads the contact\'s status, then shows who it is and the three objectives', async () => {
    const f = fakeService();
    const r = setup(f.service);

    expect(r.maybeFind(LoadingSpinner)).not.toBeNull();
    await r.waitFor(() => expect(texts(r)).toContain('Pat Lee'));
    expect(texts(r)).toEqual(expect.arrayContaining(['Administrator', 'No email on record.', 'No mobile on record.']));
    expect(['Find email', 'Find mobile', 'Verify email'].map((l) => button(r, l).props.disabled)).toEqual([false, false, true]);
    expect(f.calls[0]).toEqual({ method: 'GET', path: '/contact/status?contactId=42', body: undefined });
  });

  it('says how many runs the rep has left today', async () => {
    const r = setup(fakeService({ status: () => reply(status({ quota: quota(22) })) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('22 of 25 runs left today.'));
  });

  it('says what each open objective spends, and why a closed one is closed', async () => {
    const r = setup(fakeService().service);

    await r.waitFor(() => expect(texts(r)).toContain('Uses up to 2 lookups and 1 check.'));
    expect(texts(r)).toEqual(expect.arrayContaining(['Uses 1 lookup.', 'No email to check.']));
    expect(texts(r)).not.toContain('Uses 1 check.');
  });

  it('shows the email, how it checked out, and the mobile', async () => {
    const body = status({ contact: contact({ email: 'pat.lee@example.org', mobile: '405-555-0101', emailCheck: 'ok', emailCheckedOn: '2026-10-21' }) });
    const r = setup(fakeService({ status: () => reply(body) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('pat.lee@example.org'));
    expect(texts(r)).toEqual(expect.arrayContaining(['Mobile 405-555-0101', 'Checked Oct 21, 2026']));
    expect(tags(r)).toEqual([['Good', 'success']]);
  });

  it('shows a risky email in amber', async () => {
    const body = status({ contact: contact({ email: 'pat.lee@example.org', emailCheck: 'catch_all' }) });
    const r = setup(fakeService({ status: () => reply(body) }).service);

    await r.waitFor(() => expect(tags(r)).toEqual([['Risky', 'warning']]));
  });

  it('does not offer any objective once the day\'s runs are used up', async () => {
    const r = setup(fakeService({ status: () => reply(status({ quota: quota(0, 2) })) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('You have used all 2 runs for today.'));
    expect(['Find email', 'Find mobile', 'Verify email'].map((l) => button(r, l).props.disabled)).toEqual([true, true, true]);
  });
});

describe('Contact check card: asking for a run', () => {
  it('starts the run the rep chose and shows it waiting', async () => {
    const f = fakeService({ hold: true });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(button(r, 'Starting').props.disabled).toBe(true));
    expect(f.calls[1]).toEqual({ method: 'POST', path: '/contact/run', body: { contactId: '42', objective: 'find_email' } });
    expect(['Find mobile', 'Verify email'].map((l) => button(r, l).props.disabled)).toEqual([true, true]);
    f.release();
    await r.waitFor(() => expect(tags(r)).toEqual([['Waiting', 'default']]));
    expect(texts(r)).toContain('21 of 25 runs left today.');
    expect(alerts(r)).toEqual([{ type: 'success', title: 'Run started', message: 'The service is working on it. This card updates when it finishes.' }]);
  });

  it('sends one start however many times the button is pressed', async () => {
    const f = fakeService({ hold: true });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');
    await r.waitFor(() => expect(button(r, 'Starting').props.disabled).toBe(true));
    button(r, 'Starting').trigger('onClick');
    f.release();
    await r.waitFor(() => expect(tags(r)).toEqual([['Waiting', 'default']]));

    expect(f.kinds().filter((k) => k === 'POST')).toHaveLength(1);
  });

  it('says so when the same thing was already asked in the last day', async () => {
    const f = fakeService({ start: () => reply({ ok: true, reused: true, run: run({ requestedBy: 'other@example.org' }), quota: quota(22) }, 202) });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'info', title: 'Already asked', message: 'This was asked of this contact in the last day, so the earlier run is shown. Nothing more was spent.' });
  });

  it('checks again while a run is going and shows what it did when it finishes', async () => {
    const done = status({
      contact: contact({ email: 'pat.lee@example.org', emailCheck: 'ok' }),
      objectives: [{ ...OPEN[0], available: false, reason: 'Already has an email. Use Verify email.' }, OPEN[1], { ...OPEN[2], available: true, reason: null }],
      runs: [run({ status: 'done', outcome: 'found', message: 'Email added. MillionVerifier says it is good.', wrote: ['email'], finishedAt: '2026-10-26T16:09:30Z' })],
    });
    const f = fakeService({
      status: (n) => reply(n === 1 ? status() : n < 4 ? status({ runs: [run({ status: n === 2 ? 'queued' : 'running' })] }) : done),
      start: () => reply({ ok: true, reused: false, run: run(), quota: quota(21) }, 202),
    });
    const r = setup(f.service, { pollEveryMs: 15 });
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Email added. MillionVerifier says it is good.'));
    expect(tags(r)).toEqual(expect.arrayContaining([['Done', 'success'], ['Good', 'success']]));
    expect(texts(r)).toContain('pat.lee@example.org');
    expect(button(r, 'Verify email').props.disabled).toBe(false);
    const gets = f.kinds().filter((k) => k === 'GET').length;
    await settle();
    expect(f.kinds().filter((k) => k === 'GET')).toHaveLength(gets);
  });

  it('shows a run that is going as in progress on its own objective', async () => {
    const r = setup(fakeService({ status: () => reply(status({ runs: [run({ status: 'running' })] })) }).service);

    await r.waitFor(() => expect(tags(r)).toEqual([['Running', 'warning']]));
    expect(button(r, 'Find email').props.disabled).toBe(true);
    expect(texts(r)).toContain('A run is in progress.');
    expect(button(r, 'Find mobile').props.disabled).toBe(false);
  });

  it('says why a start was refused, then looks again', async () => {
    let n = 0;
    const f = fakeService({
      status: () => reply(++n === 1 ? status() : status({ objectives: [{ ...OPEN[0], available: false, reason: 'Already has an email. Use Verify email.' }, OPEN[1], OPEN[2]] })),
      start: () => reply({ ok: false, error: 'Already has an email. Use Verify email.' }, 409),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'Already has an email. Use Verify email.' });
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(true));
    expect(f.kinds()).toEqual(['GET', 'POST', 'GET']);
  });

  it('takes the new quota from a refusal for the daily cap, even when the look after it fails', async () => {
    const f = fakeService({
      status: (n) => (n === 1 ? reply(status({ quota: quota(1, 2) })) : new Error('Network down')),
      start: () => reply({ ok: false, error: 'You have used all 2 runs for today.', quota: quota(0, 2) }, 429),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('You have used all 2 runs for today.'));
    expect(alerts(r)[0]).toMatchObject({ type: 'danger', title: 'Not started' });
  });

  it('lets the rep ask for another thing once the first has been started', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));
    button(r, 'Find email').trigger('onClick');
    await r.waitFor(() => expect(tags(r)).toEqual([['Waiting', 'default']]));

    button(r, 'Find mobile').trigger('onClick');

    await r.waitFor(() => expect(f.kinds().filter((k) => k === 'POST')).toHaveLength(2));
    expect(f.calls.filter((c) => c.method === 'POST').map((c) => c.body)).toEqual([
      { contactId: '42', objective: 'find_email' },
      { contactId: '42', objective: 'find_mobile' },
    ]);
  });

  it('shows an earlier run once when the service hands that same run back', async () => {
    const earlier = run({ status: 'done', outcome: 'not_found', message: 'No email found.', finishedAt: '2026-10-26T16:09:30Z' });
    const f = fakeService({
      status: () => reply(status({ runs: [earlier] })),
      start: () => reply({ ok: true, reused: true, run: earlier, quota: quota(22) }, 202),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(tags(r)).toEqual([['Nothing found', 'default']]);
  });

  it('shows the five newest runs and drops the oldest when a sixth is started', async () => {
    const older = [1, 2, 3, 4, 5].map((n) => run({ runId: String(n).repeat(32), objective: 'find_mobile', status: 'done', outcome: 'not_found', message: `Older run ${n}.` }));
    const f = fakeService({
      status: () => reply(status({ runs: older })),
      start: () => reply({ ok: true, reused: false, run: run({ runId: 'f'.repeat(32), status: 'queued' }), quota: quota(21) }, 202),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(tags(r)[0]).toEqual(['Waiting', 'default']));
    expect(tags(r)).toHaveLength(5);
    expect(texts(r)).not.toContain('Older run 5.');
  });

  it('says so when the service cannot be reached, and lets the rep try again', async () => {
    const f = fakeService({ start: () => new Error('Network down') });
    const r = setup(f.service);
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));

    button(r, 'Find email').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'Network down' });
    await r.waitFor(() => expect(button(r, 'Find email').props.disabled).toBe(false));
  });
});

describe('Contact check card: waiting and failing', () => {
  it('skips a check that fails while waiting, says nothing, and carries on', async () => {
    const done = status({ runs: [run({ status: 'done', outcome: 'not_found', message: 'No email found.', finishedAt: '2026-10-26T16:09:30Z' })] });
    const f = fakeService({
      status: (n) => (n === 2 ? new Error('Network down') : n < 4 ? reply(status({ runs: [run({ status: 'running' })] })) : reply(done)),
    });
    const r = setup(f.service, { pollEveryMs: 15 });

    await r.waitFor(() => expect(tags(r)).toEqual([['Nothing found', 'default']]));

    expect(alerts(r)).toEqual([]);
    expect(f.kinds()).toHaveLength(4);
  });

  it('stops checking after a while, says it is still waiting, and starts again on Refresh', async () => {
    let n = 0;
    const done = status({ runs: [run({ status: 'done', outcome: 'found', message: 'Email added.' })] });
    const f = fakeService({ status: () => reply(++n <= 4 ? status({ runs: [run({ status: 'running' })] }) : done) });
    const r = setup(f.service, { pollEveryMs: 10, maxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const calls = f.kinds().length;
    await settle();
    expect(f.kinds()).toHaveLength(calls);

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(tags(r)).toEqual([['Done', 'success']]));
    expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(false);
  });

  it('checks again for a run it has just started, after giving up on an earlier one', async () => {
    const f = fakeService({
      status: () => reply(status({ runs: [run({ status: 'running' })] })),
      start: () => reply({ ok: true, reused: false, run: run({ runId: 'b'.repeat(32), objective: 'find_mobile', status: 'queued' }), quota: quota(21) }, 202),
    });
    const r = setup(f.service, { pollEveryMs: 10, maxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const gets = () => f.kinds().filter((k) => k === 'GET').length;
    const before = gets();

    button(r, 'Find mobile').trigger('onClick');

    await r.waitFor(() => expect(gets()).toBe(before + 2));
    await settle();
    expect(gets()).toBe(before + 2);
  });

  it('does not say it is still waiting once the look after a refused start shows the run finished', async () => {
    let n = 0;
    const f = fakeService({
      status: () => reply(++n <= 2 ? status({ runs: [run({ status: 'running' })] }) : status({ runs: [run({ status: 'done', outcome: 'found', message: 'Email added.' })] })),
      start: () => reply({ ok: false, error: 'Something went wrong. Try again.' }, 500),
    });
    const r = setup(f.service, { pollEveryMs: 10, maxPolls: 1 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));

    button(r, 'Find mobile').trigger('onClick');

    await r.waitFor(() => expect(tags(r)).toEqual([['Done', 'success']]));
    expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(false);
  });

  it('gives a contact it is moved to the same patience as the first, even after giving up on the first', async () => {
    const f = fakeService({ status: () => reply(status({ runs: [run({ status: 'running' })] })) });
    let switchTo: (id: number) => void = () => {};
    const r = createRenderer('crm.record.sidebar');
    r.mocks.context.crm.objectId = 42;
    r.mocks.context.crm.objectTypeId = '0-1';
    function Harness() {
      const [id, setId] = useState(42);
      switchTo = setId;
      const context = { ...r.mocks.context, crm: { ...r.mocks.context.crm, objectId: id } };
      return <ContactCheckView context={context} actions={r.mocks.actions} service={f.service} pollEveryMs={10} maxPolls={2} now={() => NOW} />;
    }
    r.render(<Harness />);
    await r.waitFor(() => expect(texts(r as never).some((t) => /still waiting/i.test(t))).toBe(true));
    const gets = () => f.kinds().filter((k) => k === 'GET').length;
    const before = gets();

    switchTo(43);

    await r.waitFor(() => expect(gets()).toBe(before + 3));
    expect(f.calls.at(-1)?.path).toBe('/contact/status?contactId=43');
    await settle();
    expect(gets()).toBe(before + 3);
  });

  it('does not check again at all when checking again is turned off', async () => {
    const f = fakeService({ status: () => reply(status({ runs: [run({ status: 'running' })] })) });
    const r = setup(f.service, { pollEveryMs: 0 });
    await r.waitFor(() => expect(tags(r)).toEqual([['Running', 'warning']]));

    await settle();

    expect(f.kinds()).toEqual(['GET']);
  });

  it('does not check again when nothing is going', async () => {
    const f = fakeService();
    const r = setup(f.service, { pollEveryMs: 15 });
    await r.waitFor(() => expect(texts(r)).toContain('Pat Lee'));

    await settle();

    expect(f.kinds()).toEqual(['GET']);
  });

  it('shows who asked, what happened and how long ago, newest first', async () => {
    const runs = [
      run({ runId: 'b'.repeat(32), objective: 'verify_email', status: 'done', outcome: 'verified', message: 'Checked: the email is good.', requestedBy: 'rep@example.org', createdAt: '2026-10-26T16:05:00Z' }),
      run({ runId: 'a'.repeat(32), objective: 'find_mobile', status: 'failed', outcome: 'error', message: 'Something went wrong. Nothing was changed. Try again.', requestedBy: 'other@example.org', createdAt: '2026-10-26T13:10:00Z' }),
    ];
    const r = setup(fakeService({ status: () => reply(status({ runs })) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('Recent runs'));
    expect(texts(r)).toEqual(expect.arrayContaining([
      'Verify email', 'Checked: the email is good.', 'Asked by rep@example.org, 5 min ago',
      'Find mobile', 'Something went wrong. Nothing was changed. Try again.', 'Asked by other@example.org, 3 h ago',
    ]));
    expect(tags(r)).toEqual([['Done', 'success'], ['Failed', 'error']]);
  });

  it('says so when there are no runs yet', async () => {
    const r = setup(fakeService().service);

    await r.waitFor(() => expect(texts(r)).toContain('No runs on this contact yet.'));
  });

  it('refreshes when asked', async () => {
    let n = 0;
    const f = fakeService({ status: () => reply(++n === 1 ? status() : status({ contact: contact({ name: 'Patricia Lee' }) })) });
    const r = setup(f.service);
    await r.waitFor(() => expect(texts(r)).toContain('Pat Lee'));

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Patricia Lee'));
  });

  it('turns Refresh off while it is refreshing', async () => {
    let release = () => {};
    let gets = 0;
    const service: Service = async () => {
      gets += 1;
      if (gets === 2) await new Promise<void>((resolve) => { release = resolve; });
      return reply(status());
    };
    const r = setup(service);
    await r.waitFor(() => expect(texts(r)).toContain('Pat Lee'));

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(button(r, 'Refreshing').props.disabled).toBe(true));
    release();
    await r.waitFor(() => expect(button(r, 'Refresh').props.disabled).toBe(false));
  });

  it('keeps what it shows and says so when a refresh fails', async () => {
    let n = 0;
    const f = fakeService({ status: () => (++n === 1 ? reply(status()) : new Error('Network down')) });
    const r = setup(f.service);
    await r.waitFor(() => expect(texts(r)).toContain('Pat Lee'));

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Status not refreshed', message: 'Network down' });
    expect(texts(r)).toContain('Pat Lee');
  });

  it('shows an error when the status does not load, with the service\'s reason, and tries again on request', async () => {
    let n = 0;
    const f = fakeService({ status: () => (++n === 1 ? reply({ ok: false, error: 'The service is not set up for the HubSpot app.' }, 503) : reply(status())) });
    const r = setup(f.service);

    await r.waitFor(() => expect(r.maybeFind(ErrorState)).not.toBeNull());
    expect(r.find(ErrorState).props.title).toBe('The contact check did not load');
    expect(texts(r)).toContain('The service is not set up for the HubSpot app.');
    button(r, 'Try again').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Pat Lee'));
    expect(r.maybeFind(ErrorState)).toBeNull();
  });

  it('does not work on a record that is not a contact', async () => {
    const f = fakeService();
    const r = setup(f.service, { objectTypeId: '0-2' });

    expect(r.find(EmptyState).props.title).toBe('Not available here');
    expect(texts(r)).toContain('The Contact check works on contact records.');
    await settle();
    expect(f.calls).toEqual([]);
  });
});
