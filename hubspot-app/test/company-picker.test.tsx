// Renders the Enrichment card's "pick contacts" part with HubSpot's test renderer, a fake app function (the company's
// contacts) and a fake service (the runs). The fakes are in company-fakes.tsx.
import { describe, expect, it } from 'vitest';
import { Checkbox, Select } from '@hubspot/ui-extensions';
import { alerts, box, button, fakeService, item, quota, reply, run, serviceStatus, settle, setup, setupSwitching, started, tags, texts, type Fake, type Rendered } from './company-fakes.tsx';
import { COMPANY_ID } from './company-fakes.tsx';

const pickBox = (r: Rendered, id: string) => box(r, `pick-${id}`);
// The test renderer types an event's argument as the handler itself; at run time it passes the value through.
const tick = (r: Rendered, id: string, checked: boolean) => pickBox(r, id).trigger('onChange', checked as never);
const pick = (r: Rendered, ...ids: string[]) => ids.forEach((id) => tick(r, id, true));
const ready = (r: Rendered) => r.waitFor(() => expect(r.findAll(Checkbox)).toHaveLength(3));
const posts = (f: Fake) => f.posts('/company/run');

describe('Enrichment card: picking contacts', () => {
  it('shows a checkbox for every contact, in both lists, and none when there is no service to ask', async () => {
    const r = setup(fakeService().service);
    await ready(r);

    expect(r.findAll(Checkbox).map((c) => c.props.name)).toEqual(['pick-1', 'pick-2', 'pick-3']);
    expect(r.findAll(Checkbox).map((c) => c.text)).toEqual(['Pat Lee', 'Sam Ortiz', 'Kim Day']);

    const plain = setup(undefined);
    await plain.waitFor(() => expect(texts(plain)).toContain('No enrichment run on record for this company.'));
    expect(plain.findAll(Checkbox)).toHaveLength(0);
  });

  it('asks the service for this company\'s status, and offers what to do with the default on Enrich', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await ready(r);

    expect(f.calls[0]).toEqual({ method: 'GET', path: `/company/status?companyId=${COMPANY_ID}`, body: undefined });
    expect(r.find(Select).props.options).toEqual([
      { label: 'Find email', value: 'find_email' }, { label: 'Find mobile', value: 'find_mobile' },
      { label: 'Verify email', value: 'verify_email' }, { label: 'Enrich', value: 'enrich' },
    ]);
    expect(r.find(Select).props.value).toBe('enrich');
    expect(texts(r)).toContain('Uses whatever is missing: up to 3 lookups and 1 check.');
    expect(texts(r)).toContain('22 of 25 runs left today.');
  });

  it('keeps the start button off until a contact is ticked, and says how many it will start for', async () => {
    const r = setup(fakeService().service);
    await ready(r);
    expect(button(r, 'Choose contacts').props.disabled).toBe(true);

    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact').props.disabled).toBe(false));
    pick(r, '2');

    await r.waitFor(() => expect(button(r, 'Start for 2 contacts').props.disabled).toBe(false));
  });

  it('lets a tick be taken back', async () => {
    const r = setup(fakeService().service);
    await ready(r);
    pick(r, '1', '2');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    tick(r, '1', false);

    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());
  });

  it('stops more being ticked once the limit is reached, and lets them be taken back', async () => {
    const f = fakeService({ status: () => reply(serviceStatus({ limits: { maxContacts: 2, maxTitles: 5 } })) });
    const r = setup(f.service);
    await ready(r);

    pick(r, '1', '2');

    await r.waitFor(() => expect(pickBox(r, '3').props.readOnly).toBe(true));
    expect(pickBox(r, '1').props.readOnly).toBeFalsy();
    tick(r, '1', false);
    await r.waitFor(() => expect(pickBox(r, '3').props.readOnly).toBeFalsy());
  });

  it('starts runs for the contacts ticked, with the objective chosen, and says what started', async () => {
    const f = fakeService({
      start: () => started([item('1', 'find_mobile', 'started', { run: run({ objective: 'find_mobile' }) }), item('2', 'find_mobile', 'started', { run: run({ runId: 'c'.repeat(32), contactId: '2', objective: 'find_mobile' }) })]),
    });
    const r = setup(f.service);
    await ready(r);
    r.find(Select).trigger('onChange', 'find_mobile' as never);
    pick(r, '1', '2');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    button(r, 'Start for 2 contacts').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(posts(f)[0]).toEqual({ method: 'POST', path: '/company/run', body: { companyId: COMPANY_ID, contactIds: ['1', '2'], objective: 'find_mobile' } });
    expect(alerts(r)[0]).toEqual({ type: 'success', title: 'Runs started', message: '2 runs started. This card updates when they finish.' });
    expect(texts(r)).toContain('20 of 25 runs left today.');
  });

  it('sends the contacts in the order they are listed, whatever order they were ticked in', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await ready(r);
    pick(r, '3', '1');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    button(r, 'Start for 2 contacts').trigger('onClick');

    await r.waitFor(() => expect(posts(f)).toHaveLength(1));
    expect(posts(f)[0].body?.contactIds).toEqual(['1', '3']);
  });

  it('clears the ticks once the runs are started, and shows the new runs with the contact\'s name', async () => {
    const f = fakeService({ start: () => started([item('1', 'find_email', 'started', { run: run() })]) });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());

    button(r, 'Start for 1 contact').trigger('onClick');

    await r.waitFor(() => expect(tags(r)).toEqual(expect.arrayContaining([['Waiting', 'default']])));
    expect(pickBox(r, '1').props.checked).toBe(false);
    expect(button(r, 'Choose contacts').props.disabled).toBe(true);
    expect(texts(r)).toEqual(expect.arrayContaining(['Pat Lee · Find email', 'Asked by rep@example.org, 1 min ago']));
  });

  it('sends one start however many times the button is pressed', async () => {
    const f = fakeService({ hold: true });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());

    button(r, 'Start for 1 contact').trigger('onClick');
    await r.waitFor(() => expect(button(r, 'Starting').props.disabled).toBe(true));
    button(r, 'Starting').trigger('onClick');
    f.release();
    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));

    expect(posts(f)).toHaveLength(1);
  });

  it('says who was skipped and why, in the same note', async () => {
    const f = fakeService({
      start: () => started([item('1', 'find_email', 'started', { run: run() }), item('2', 'find_email', 'skipped', { reason: 'Already has an email. Use Verify email.' })]),
    });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1', '2');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    button(r, 'Start for 2 contacts').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toMatchObject({ type: 'success', message: '1 run started. This card updates when it finishes. Skipped Sam Ortiz: Already has an email. Use Verify email.' });
  });

  it('says so, and keeps the ticks, when the service refuses, and takes the quota it sends', async () => {
    const f = fakeService({
      status: (n) => reply(serviceStatus({ quota: quota(n === 1 ? 1 : 0) })),
      start: () => reply({ ok: false, error: 'This needs 2 runs and you have 1 left today.', quota: quota(1) }, 429),
    });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1', '2');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    button(r, 'Start for 2 contacts').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'This needs 2 runs and you have 1 left today.' });
    expect(pickBox(r, '1').props.checked).toBe(true);
    expect(pickBox(r, '2').props.checked).toBe(true);
  });

  it('says so when the service cannot be reached, and lets the rep try again', async () => {
    const f = fakeService({ start: () => new Error('Network down') });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());

    button(r, 'Start for 1 contact').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'Network down' });
    await r.waitFor(() => expect(button(r, 'Start for 1 contact').props.disabled).toBe(false));
  });

  it('warns when what a choice could use is more than the rep has left, and still lets the service decide', async () => {
    const f = fakeService({ status: () => reply(serviceStatus({ quota: quota(4) })) });
    const r = setup(f.service);
    await ready(r);

    pick(r, '1', '2', '3');

    await r.waitFor(() => expect(texts(r)).toContain('This could use up to 6 runs and you have 4 left today.'));
    expect(button(r, 'Start for 3 contacts').props.disabled).toBe(false);
  });

  it('does not offer to start anything once the day\'s runs are used up', async () => {
    const r = setup(fakeService({ status: () => reply(serviceStatus({ quota: quota(0, 2) })) }).service);
    await ready(r);

    pick(r, '1');

    await r.waitFor(() => expect(texts(r)).toContain('You have used all 2 runs for today.'));
    expect(button(r, 'Start for 1 contact').props.disabled).toBe(true);
  });

  it('carries on without picking, and says why, when the service is not set up', async () => {
    const f = fakeService({ status: () => reply({ ok: false, error: 'The service is not set up for the HubSpot app.' }, 503) });
    const r = setup(f.service);

    await r.waitFor(() => expect(texts(r)).toContain('Picking contacts is not available: The service is not set up for the HubSpot app.'));
    expect(r.findAll(Checkbox)).toHaveLength(0);
    expect(button(r, 'Request enrichment')).toBeTruthy();
    expect(texts(r)).toContain('Pat Lee');
  });
});

describe('Enrichment card: the runs on this company', () => {
  const earlier = [
    run({ runId: '2'.repeat(32), contactId: '2', objective: 'find_mobile', status: 'done', outcome: 'not_found', message: 'Seamless had no number for this person.', createdAt: '2026-10-26T13:10:00Z', requestedBy: 'other@example.org' }),
    run({ runId: '1'.repeat(32), contactId: '1', objective: 'verify_email', status: 'done', outcome: 'verified', message: 'Checked: the email is good.', createdAt: '2026-10-26T16:05:00Z' }),
  ];

  it('shows who asked, what happened and how long ago, with the contact\'s name', async () => {
    const r = setup(fakeService({ status: () => reply(serviceStatus({ runs: earlier })) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('Recent runs'));
    expect(texts(r)).toEqual(expect.arrayContaining([
      'Pat Lee · Verify email', 'Checked: the email is good.', 'Asked by rep@example.org, 5 min ago',
      'Sam Ortiz · Find mobile', 'Seamless had no number for this person.', 'Asked by other@example.org, 3 h ago',
    ]));
    expect(tags(r)).toEqual(expect.arrayContaining([['Done', 'success'], ['Nothing found', 'default']]));
  });

  it('calls a contact it has no name for by its id, and shows no more than five runs', async () => {
    const many = [1, 2, 3, 4, 5, 6].map((n) => run({ runId: String(n).repeat(32), contactId: n === 1 ? '999' : '2', status: 'done', outcome: 'found', message: `Run ${n}.` }));
    const r = setup(fakeService({ status: () => reply(serviceStatus({ runs: many })) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('Recent runs'));

    expect(texts(r)).toContain('Contact 999 · Find email');
    expect(texts(r).filter((t) => /^Run \d\.$/.test(t))).toHaveLength(5);
  });

  it('shows a search or an add without a made-up contact name, and names the contact once an add has made one', async () => {
    const runs = [
      run({ runId: '3'.repeat(32), contactId: '', objective: 'find_by_title', status: 'done', outcome: 'found', message: 'Found 2 people. Choose who to add.', createdAt: '2026-10-26T16:08:00Z' }),
      run({ runId: '2'.repeat(32), contactId: '', objective: 'add_candidate', status: 'running', createdAt: '2026-10-26T16:07:00Z' }),
      run({ runId: '1'.repeat(32), contactId: '2', objective: 'add_candidate', status: 'done', outcome: 'found', message: 'Created a new contact and linked it to Elm Court Care Center.', createdAt: '2026-10-26T16:06:00Z' }),
    ];
    const r = setup(fakeService({ status: () => reply(serviceStatus({ runs })) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('Recent runs'));

    expect(texts(r)).toEqual(expect.arrayContaining(['Find by job title', 'Add person', 'Sam Ortiz · Add person']));
    expect(texts(r).some((t) => /^Contact\s*·/.test(t) || t.includes('Contact  ·'))).toBe(false);
  });

  it('says nothing about runs when there are none', async () => {
    const r = setup(fakeService().service);
    await ready(r);

    expect(texts(r)).not.toContain('Recent runs');
  });

  it('checks again while a run is going and stops when it has finished', async () => {
    const done = serviceStatus({ runs: [run({ status: 'done', outcome: 'found', message: 'Email added.' })] });
    const f = fakeService({ status: (n) => reply(n === 1 ? serviceStatus({ runs: [run({ status: 'queued' })] }) : n === 2 ? serviceStatus({ runs: [run({ status: 'running' })] }) : done) });
    const r = setup(f.service, { runPollEveryMs: 15 });

    await r.waitFor(() => expect(texts(r)).toContain('Email added.'));
    const gets = f.kinds().filter((k) => k === 'GET').length;
    await settle();

    expect(f.kinds().filter((k) => k === 'GET')).toHaveLength(gets);
    expect(gets).toBe(3);
  });

  it('stops checking after a while, says it is still waiting, and starts again on Refresh', async () => {
    let n = 0;
    const f = fakeService({ status: () => reply(++n <= 3 ? serviceStatus({ runs: [run({ status: 'running' })] }) : serviceStatus({ runs: [run({ status: 'done', outcome: 'found', message: 'Email added.' })] })) });
    const r = setup(f.service, { runPollEveryMs: 10, runMaxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const calls = f.kinds().length;
    await settle();
    expect(f.kinds()).toHaveLength(calls);

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Email added.'));
    expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(false);
  });

  it('skips a check that fails while waiting, says nothing, and carries on', async () => {
    const f = fakeService({ status: (n) => (n === 2 ? new Error('Network down') : n < 4 ? reply(serviceStatus({ runs: [run({ status: 'running' })] })) : reply(serviceStatus({ runs: [run({ status: 'done', outcome: 'found', message: 'Email added.' })] }))) });
    const r = setup(f.service, { runPollEveryMs: 15 });

    await r.waitFor(() => expect(texts(r)).toContain('Email added.'));

    expect(alerts(r)).toEqual([]);
  });

  it('checks the runs again, after a start, even when the first check has already given up', async () => {
    const f = fakeService({
      status: () => reply(serviceStatus({ runs: [run({ status: 'running' })] })),
      start: () => started([item('2', 'find_email', 'started', { run: run({ runId: 'c'.repeat(32), contactId: '2', status: 'queued' }) })]),
    });
    const r = setup(f.service, { runPollEveryMs: 10, runMaxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const gets = () => f.kinds().filter((k) => k === 'GET').length;
    const before = gets();
    pick(r, '2');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());

    button(r, 'Start for 1 contact').trigger('onClick');

    await r.waitFor(() => expect(gets()).toBe(before + 2));
  });
});

describe('Enrichment card: picking contacts, the edges', () => {
  it('does not tick more than the limit even when asked to', async () => {
    const r = setup(fakeService({ status: () => reply(serviceStatus({ limits: { maxContacts: 2, maxTitles: 5 } })) }).service);
    await ready(r);

    pick(r, '1', '2');
    tick(r, '3', true);

    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());
    await settle();
    expect(pickBox(r, '3').props.checked).toBe(false);
    expect(button(r, 'Start for 2 contacts')).toBeTruthy();
  });

  it('does nothing when the start button is pressed with nobody ticked', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await ready(r);

    button(r, 'Choose contacts').trigger('onClick');
    await settle();

    expect(posts(f)).toHaveLength(0);
  });

  it('lets the rep start again once the first start is done', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await ready(r);
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());
    button(r, 'Start for 1 contact').trigger('onClick');
    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));

    pick(r, '2');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());
    button(r, 'Start for 1 contact').trigger('onClick');

    await r.waitFor(() => expect(posts(f)).toHaveLength(2));
    expect(posts(f)[1].body?.contactIds).toEqual(['2']);
  });

  it('shows a run once when the service hands back one that is already shown', async () => {
    const earlier = run({ status: 'done', outcome: 'not_found', message: 'Seamless had no number for this person.' });
    const f = fakeService({
      status: () => reply(serviceStatus({ runs: [earlier] })),
      start: () => started([item('1', 'find_email', 'reused', { run: earlier })]),
    });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());

    button(r, 'Start for 1 contact').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(tags(r).filter(([label]) => label === 'Nothing found')).toHaveLength(1);
  });

  it('keeps the runs newest first when new and earlier runs come back together', async () => {
    const existing = run({ runId: 'b'.repeat(32), contactId: '2', objective: 'find_mobile', status: 'done', outcome: 'not_found', createdAt: '2026-10-26T16:05:00Z' });
    const reused = run({ runId: 'd'.repeat(32), contactId: '3', objective: 'verify_email', status: 'done', outcome: 'verified', createdAt: '2026-10-26T13:10:00Z' });
    const fresh = run({ runId: 'c'.repeat(32), contactId: '1', objective: 'find_email', status: 'queued', createdAt: '2026-10-26T16:09:00Z' });
    const f = fakeService({
      status: () => reply(serviceStatus({ runs: [existing] })),
      start: () => started([item('3', 'verify_email', 'reused', { run: reused }), item('1', 'find_email', 'started', { run: fresh })]),
    });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1', '3');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    button(r, 'Start for 2 contacts').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(texts(r).filter((t) => t.includes(' · '))).toEqual(['Pat Lee · Find email', 'Sam Ortiz · Find mobile', 'Kim Day · Verify email']);
  });

  it('takes the quota a refusal sends even when the look after it fails', async () => {
    const f = fakeService({
      status: (n) => (n === 1 ? reply(serviceStatus({ quota: quota(5) })) : new Error('Network down')),
      start: () => reply({ ok: false, error: 'This needs 2 runs and you have 1 left today.', quota: quota(1) }, 429),
    });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1', '2');
    await r.waitFor(() => expect(button(r, 'Start for 2 contacts')).toBeTruthy());

    button(r, 'Start for 2 contacts').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('1 of 25 runs left today.'));
  });

  it('looks again after a refusal, because the contacts may have changed', async () => {
    const f = fakeService({
      status: (n) => reply(serviceStatus({ quota: quota(n === 1 ? 5 : 3) })),
      start: () => reply({ ok: false, error: 'Something went wrong. Try again.' }, 500),
    });
    const r = setup(f.service);
    await ready(r);
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());

    button(r, 'Start for 1 contact').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('3 of 25 runs left today.'));
    expect(f.gets('/company/status')).toBe(2);
  });

  it('says so, and keeps what it shows, when a refresh fails', async () => {
    const f = fakeService({ status: (n) => (n === 1 ? reply(serviceStatus()) : new Error('Network down')) });
    const r = setup(f.service);
    await ready(r);

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toContainEqual({ type: 'danger', title: 'Status not refreshed', message: 'Network down' }));
    expect(r.findAll(Checkbox)).toHaveLength(3);
  });

  it('does not check again at all when checking again is turned off', async () => {
    const f = fakeService({ status: () => reply(serviceStatus({ runs: [run({ status: 'running' })] })) });
    const r = setup(f.service, { runPollEveryMs: 0 });
    await r.waitFor(() => expect(tags(r)).toEqual(expect.arrayContaining([['Running', 'warning']])));

    await settle();

    expect(f.gets('/company/status')).toBe(1);
  });

  it('checks again as often after Refresh as it did at first', async () => {
    const f = fakeService({ status: () => reply(serviceStatus({ runs: [run({ status: 'running' })] })) });
    const r = setup(f.service, { runPollEveryMs: 10, runMaxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const before = f.gets('/company/status');

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(f.gets('/company/status')).toBe(before + 3));
  });

  it('starts a company it is moved to with all of its patience, and with nobody ticked', async () => {
    const f = fakeService({ status: () => reply(serviceStatus({ runs: [run({ status: 'running' })] })) });
    const { r, moveTo } = setupSwitching(f.service, { runPollEveryMs: 10, runMaxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    pick(r, '1');
    await r.waitFor(() => expect(button(r, 'Start for 1 contact')).toBeTruthy());
    const before = f.gets('/company/status');

    moveTo(6002);

    await r.waitFor(() => expect(f.gets('/company/status')).toBe(before + 3));
    expect(button(r, 'Choose contacts')).toBeTruthy();
    expect(f.calls.at(-1)?.path).toBe('/company/status?companyId=6002');
  });
});
