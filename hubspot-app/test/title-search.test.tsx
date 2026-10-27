// Renders the Enrichment card's "find people by job title" part with HubSpot's test renderer, a fake app function and a
// fake service. The fakes are in company-fakes.tsx. The card never writes anything itself: it asks the service to
// search, shows the people found, and asks it to add the ones chosen.
import { describe, expect, it } from 'vitest';
import { Checkbox, Input, MultiSelect } from '@hubspot/ui-extensions';
import {
  FIND_ID, PRESETS, aitem, addStarted, alerts, box, button, candidate, fakeService, findRun, found, quota, reply, run, serviceStatus, settle, setup, tags, texts,
  type Fake, type Rendered,
} from './company-fakes.tsx';
import { COMPANY_ID } from './company-fakes.tsx';

// The test renderer types an event's argument as the handler itself; at run time it passes the value through.
const choose = (r: Rendered, titles: string[]) => r.find(MultiSelect).trigger('onChange', titles as never);
const type = (r: Rendered, text: string) => r.find(Input).trigger('onInput', text as never);
const person = (r: Rendered, id: string) => box(r, `cand-${id}`);
const tickPerson = (r: Rendered, id: string, checked = true) => person(r, id).trigger('onChange', checked as never);
const people = (r: Rendered) => r.findAll(Checkbox).filter((c) => String(c.props.name).startsWith('cand-')).map((c) => c.text);
const ready = (r: Rendered) => r.waitFor(() => expect(r.maybeFind(MultiSelect)).not.toBeNull());

const PAT = candidate('aaaaaaaaaaaa', 'Pat Lee', { title: 'Medical Director' });
const SAM = candidate('bbbbbbbbbbbb', 'Sam Ortiz', { title: 'Dietitian' });
const KIM = candidate('cccccccccccc', 'Kim Day', { title: 'Social Worker' });

// A finished search the status already lists, so the card brings its people back without a new search.
const withSearch = (over: Record<string, unknown> = {}) => serviceStatus({ runs: [findRun({ status: 'done', outcome: 'found', message: 'Found 2 people. Choose who to add.' })], ...over });
const gets = (f: Fake) => f.gets('/company/candidates');

describe('Enrichment card: asking for a search', () => {
  it('offers the titles the company runs look for, an other-titles box, and keeps the button off until a title is chosen', async () => {
    const r = setup(fakeService().service);
    await ready(r);

    expect(r.find(MultiSelect).props.options).toEqual(PRESETS.map((t) => ({ label: t, value: t })));
    expect(r.maybeFind(Input)).not.toBeNull();
    expect(button(r, 'Choose job titles').props.disabled).toBe(true);
    expect(texts(r)).toContain('Find people by job title');
  });

  it('counts the quick picks and what was typed together, without repeats, and says how many it will search for', async () => {
    const r = setup(fakeService().service);
    await ready(r);

    choose(r, ['Administrator', 'Director of Nursing']);
    type(r, 'administrator, Medical Director');

    await r.waitFor(() => expect(button(r, 'Search for 3 titles').props.disabled).toBe(false));
  });

  it('searches for the quick picks first and then what was typed, and says the search started', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Director of Nursing']);
    type(r, 'Medical Director; Dietitian');
    await r.waitFor(() => expect(button(r, 'Search for 3 titles')).toBeTruthy());

    button(r, 'Search for 3 titles').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(f.posts('/company/find')[0]).toEqual({ method: 'POST', path: '/company/find', body: { companyId: COMPANY_ID, titles: ['Director of Nursing', 'Medical Director', 'Dietitian'] } });
    expect(alerts(r)[0]).toEqual({ type: 'success', title: 'Search started', message: 'The service is looking. This card shows the people it finds.' });
    expect(texts(r)).toContain('Searching for people. This card updates when it finishes.');
  });

  it('says so when the same titles were searched in the last day', async () => {
    const f = fakeService({ find: () => reply({ ok: true, reused: true, run: findRun(), quota: quota(22) }, 202) });
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toMatchObject({ type: 'info', title: 'Already searched' });
  });

  it('refuses more titles than the service allows, and says so', async () => {
    const r = setup(fakeService({ status: () => reply(serviceStatus({ limits: { maxContacts: 10, maxTitles: 2 } })) }).service);
    await ready(r);

    choose(r, ['Administrator', 'Executive Director']);
    type(r, 'Medical Director');

    await r.waitFor(() => expect(texts(r)).toContain('Choose at most 2 titles.'));
    expect(button(r, 'Search for 3 titles').props.disabled).toBe(true);
  });

  it('does not offer a search once the day\'s runs are used up', async () => {
    const r = setup(fakeService({ status: () => reply(serviceStatus({ quota: quota(0, 2) })) }).service);
    await ready(r);

    choose(r, ['Administrator']);

    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());
    expect(button(r, 'Search for 1 title').props.disabled).toBe(true);
  });

  it('says why a search was refused, keeps the titles, and takes the quota it sends', async () => {
    const f = fakeService({ find: () => reply({ ok: false, error: 'You have used all 25 runs for today.', quota: quota(0) }, 429) });
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'You have used all 25 runs for today.' });
    expect(r.find(MultiSelect).props.value).toEqual(['Administrator']);
  });

  it('sends one search however many times the button is pressed', async () => {
    const f = fakeService({ hold: true });
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');
    await r.waitFor(() => expect(button(r, 'Searching').props.disabled).toBe(true));
    button(r, 'Searching').trigger('onClick');
    f.release();
    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));

    expect(f.posts('/company/find')).toHaveLength(1);
  });

  it('says so when the service cannot be reached', async () => {
    const r = setup(fakeService({ find: () => new Error('Network down') }).service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'Network down' });
  });
});

describe('Enrichment card: the people a search found', () => {
  it('checks while the search runs, then lists the people with their titles', async () => {
    const f = fakeService({
      find: () => reply({ ok: true, reused: false, run: findRun(), quota: quota(21) }, 202),
      candidates: (n) => (n === 1 ? reply({ ok: true, ready: false, run: findRun({ status: 'running' }), candidates: [] }) : found([PAT, SAM])),
    });
    const r = setup(f.service, { runPollEveryMs: 15 });
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(people(r)).toEqual(['Pat Lee', 'Sam Ortiz']));
    expect(f.calls.find((c) => c.path.startsWith('/company/candidates'))?.path).toBe(`/company/candidates?companyId=${COMPANY_ID}&runId=${FIND_ID}`);
    expect(texts(r)).toEqual(expect.arrayContaining(['Found 2 people', 'Medical Director', 'Dietitian']));
    const settled = gets(f);
    await settle();
    expect(gets(f)).toBe(settled);
  });

  it('brings back the people of a finished search when the card is opened again', async () => {
    const f = fakeService({ status: () => reply(withSearch()), candidates: () => found([PAT, SAM]) });
    const r = setup(f.service);

    await r.waitFor(() => expect(people(r)).toEqual(['Pat Lee', 'Sam Ortiz']));
    expect(f.posts('/company/find')).toEqual([]);
  });

  it('does not bring back a search that failed, or one older than a day', async () => {
    const old = findRun({ status: 'done', outcome: 'found', createdAt: '2026-10-25T10:00:00Z' });
    const failed = findRun({ runId: 'e'.repeat(32), status: 'failed', outcome: 'error', message: 'FullEnrich has no credits left, so people cannot be searched for.' });
    const f = fakeService({ status: () => reply(serviceStatus({ runs: [failed, old] })), candidates: () => found([PAT]) });
    const r = setup(f.service);
    await ready(r);
    await settle();

    expect(gets(f)).toBe(0);
    expect(people(r)).toEqual([]);
  });

  it('says when nobody was found, and when the search could not run', async () => {
    const none = setup(fakeService({ status: () => reply(withSearch()), candidates: () => found([], { outcome: 'not_found', message: 'No one found for these titles at this company.' }) }).service);
    await none.waitFor(() => expect(texts(none)).toContain('No one found for these titles at this company.'));
    expect(none.findAll(Checkbox).filter((c) => String(c.props.name).startsWith('cand-'))).toHaveLength(0);

    const failed = setup(fakeService({
      status: () => reply(withSearch()),
      candidates: () => reply({ ok: true, ready: false, run: findRun({ status: 'failed', outcome: 'error', message: 'FullEnrich has no credits left, so people cannot be searched for.' }), candidates: [] }),
    }).service);
    await failed.waitFor(() => expect(texts(failed)).toContain('FullEnrich has no credits left, so people cannot be searched for.'));
  });

  it('marks a person who is already a contact of this company', async () => {
    const r = setup(fakeService({ status: () => reply(withSearch()), candidates: () => found([PAT, candidate('bbbbbbbbbbbb', 'Sam Ortiz', { linkedContactId: '901' })]) }).service);

    await r.waitFor(() => expect(texts(r).filter((t) => t === 'Already a contact here.')).toHaveLength(1));
  });

  it('says the results are gone, rather than showing nothing, when they have expired', async () => {
    const f = fakeService({ status: () => reply(withSearch()), candidates: () => reply({ ok: false, error: 'Those search results are no longer available. Search again.' }, 410) });
    const r = setup(f.service);

    await r.waitFor(() => expect(texts(r)).toContain('Those search results are no longer available. Search again.'));
    expect(people(r)).toEqual([]);
  });
});

describe('Enrichment card: adding the people chosen', () => {
  const list = () => ({ status: () => reply(withSearch()), candidates: () => found([PAT, SAM, KIM]) });
  const open = async (opts = {}) => {
    const f = fakeService({ ...list(), ...opts });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));
    return { f, r };
  };

  it('keeps the add button off until someone is chosen, and says how many it will add', async () => {
    const { r } = await open();
    expect(button(r, 'Choose people').props.disabled).toBe(true);

    tickPerson(r, 'aaaaaaaaaaaa');
    await r.waitFor(() => expect(button(r, 'Add 1 person').props.disabled).toBe(false));
    tickPerson(r, 'bbbbbbbbbbbb');

    await r.waitFor(() => expect(button(r, 'Add 2 people').props.disabled).toBe(false));
  });

  it('adds the people chosen, in the order they are listed, and says what started', async () => {
    const f = fakeService({
      ...list(),
      add: () => addStarted([aitem('aaaaaaaaaaaa', 'started', { run: run({ runId: '1'.repeat(32), contactId: '', objective: 'add_candidate' }) }), aitem('cccccccccccc', 'started', { run: run({ runId: '2'.repeat(32), contactId: '', objective: 'add_candidate' }) })]),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));
    tickPerson(r, 'cccccccccccc');
    tickPerson(r, 'aaaaaaaaaaaa');
    await r.waitFor(() => expect(button(r, 'Add 2 people')).toBeTruthy());

    button(r, 'Add 2 people').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(f.posts('/company/add')[0].body).toEqual({ companyId: COMPANY_ID, runId: FIND_ID, candidateIds: ['aaaaaaaaaaaa', 'cccccccccccc'] });
    expect(alerts(r)[0]).toEqual({ type: 'success', title: 'Adding people', message: '2 people are being added. This card updates when they finish.' });
    expect(person(r, 'aaaaaaaaaaaa').props.checked).toBe(false);
    expect(button(r, 'Choose people').props.disabled).toBe(true);
  });

  it('shows how adding each person is going, and checks again until it is done', async () => {
    const adding = (status: 'queued' | 'running' | 'done') => run({ runId: '1'.repeat(32), contactId: status === 'done' ? '901' : '', objective: 'add_candidate', status, outcome: status === 'done' ? 'found' : null, message: status === 'done' ? 'Created a new contact and linked it to Elm Court Care Center.' : null });
    let n = 0;
    const f = fakeService({
      status: () => reply(withSearch()),
      candidates: () => found([candidate('aaaaaaaaaaaa', 'Pat Lee', { added: adding(++n === 1 ? 'running' : 'done') }), SAM]),
    });
    const r = setup(f.service, { runPollEveryMs: 15 });

    await r.waitFor(() => expect(texts(r)).toContain('Created a new contact and linked it to Elm Court Care Center.'));
    expect(tags(r)).toEqual(expect.arrayContaining([['Done', 'success']]));
    const settled = gets(f);
    await settle();
    expect(gets(f)).toBe(settled);
  });

  it('does not let a person already being added or added be chosen again, but lets a failed add be tried again', async () => {
    const added = (status: 'running' | 'failed') => run({ runId: '1'.repeat(32), contactId: '', objective: 'add_candidate', status, outcome: status === 'failed' ? 'error' : null });
    const f = fakeService({
      status: () => reply(withSearch()),
      candidates: () => found([candidate('aaaaaaaaaaaa', 'Pat Lee', { added: added('running') }), candidate('bbbbbbbbbbbb', 'Sam Ortiz', { added: added('failed') }), KIM]),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));

    expect(person(r, 'aaaaaaaaaaaa').props.readOnly).toBe(true);
    expect(person(r, 'bbbbbbbbbbbb').props.readOnly).toBeFalsy();
    expect(person(r, 'cccccccccccc').props.readOnly).toBeFalsy();
  });

  it('stops more being chosen once the limit is reached', async () => {
    const f = fakeService({ ...list(), status: () => reply(withSearch({ limits: { maxContacts: 2, maxTitles: 5 } })) });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));

    tickPerson(r, 'aaaaaaaaaaaa');
    tickPerson(r, 'bbbbbbbbbbbb');

    await r.waitFor(() => expect(person(r, 'cccccccccccc').props.readOnly).toBe(true));
  });

  it('warns when there are more people chosen than runs left, and keeps the button off when none are left', async () => {
    const warn = setup(fakeService({ ...list(), status: () => reply(withSearch({ quota: quota(2) })) }).service);
    await warn.waitFor(() => expect(people(warn)).toHaveLength(3));
    tickPerson(warn, 'aaaaaaaaaaaa');
    tickPerson(warn, 'bbbbbbbbbbbb');
    tickPerson(warn, 'cccccccccccc');
    await warn.waitFor(() => expect(texts(warn)).toContain('This could use up to 3 runs and you have 2 left today.'));

    const none = setup(fakeService({ ...list(), status: () => reply(withSearch({ quota: quota(0) })) }).service);
    await none.waitFor(() => expect(people(none)).toHaveLength(3));
    tickPerson(none, 'aaaaaaaaaaaa');
    await none.waitFor(() => expect(button(none, 'Add 1 person')).toBeTruthy());
    expect(button(none, 'Add 1 person').props.disabled).toBe(true);
  });

  it('says why an add was refused, and keeps the people chosen', async () => {
    const { r } = await open({ add: () => reply({ ok: false, error: 'This needs 2 runs and you have 1 left today.', quota: quota(1) }, 429) });
    tickPerson(r, 'aaaaaaaaaaaa');
    tickPerson(r, 'bbbbbbbbbbbb');
    await r.waitFor(() => expect(button(r, 'Add 2 people')).toBeTruthy());

    button(r, 'Add 2 people').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toEqual({ type: 'danger', title: 'Not started', message: 'This needs 2 runs and you have 1 left today.' });
    expect(person(r, 'aaaaaaaaaaaa').props.checked).toBe(true);
  });

  it('names who was skipped and why', async () => {
    const { r } = await open({ add: () => addStarted([aitem('aaaaaaaaaaaa', 'started', { run: run({ runId: '1'.repeat(32), contactId: '', objective: 'add_candidate' }) }), aitem('bbbbbbbbbbbb', 'skipped', { reason: 'Not in these search results.' })]) });
    tickPerson(r, 'aaaaaaaaaaaa');
    tickPerson(r, 'bbbbbbbbbbbb');
    await r.waitFor(() => expect(button(r, 'Add 2 people')).toBeTruthy());

    button(r, 'Add 2 people').trigger('onClick');

    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));
    expect(alerts(r)[0]).toMatchObject({ message: '1 person is being added. This card updates when it finishes. Skipped Sam Ortiz: Not in these search results.' });
  });

  it('sends one add however many times the button is pressed', async () => {
    const { f, r } = await open({ hold: true });
    tickPerson(r, 'aaaaaaaaaaaa');
    await r.waitFor(() => expect(button(r, 'Add 1 person')).toBeTruthy());

    button(r, 'Add 1 person').trigger('onClick');
    await r.waitFor(() => expect(button(r, 'Adding').props.disabled).toBe(true));
    button(r, 'Adding').trigger('onClick');
    f.release();
    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));

    expect(f.posts('/company/add')).toHaveLength(1);
  });

  it('stops checking after a while, says it is still waiting, and starts again on Refresh', async () => {
    const adding = (status: 'running' | 'done') => run({ runId: '1'.repeat(32), contactId: '', objective: 'add_candidate', status, outcome: status === 'done' ? 'found' : null, message: status === 'done' ? 'Linked an existing contact to Elm Court Care Center.' : null });
    let n = 0;
    const f = fakeService({ status: () => reply(withSearch()), candidates: () => found([candidate('aaaaaaaaaaaa', 'Pat Lee', { added: adding(++n <= 3 ? 'running' : 'done') })]) });
    const r = setup(f.service, { runPollEveryMs: 10, runMaxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    const calls = gets(f);
    await settle();
    expect(gets(f)).toBe(calls);

    button(r, 'Refresh').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Linked an existing contact to Elm Court Care Center.'));
  });
});

describe('Enrichment card: finding by title, the edges', () => {
  const adding = (id: string, status: 'queued' | 'running' = 'queued') => run({ runId: id.repeat(32).slice(0, 32), contactId: '', objective: 'add_candidate', status });
  const list = () => ({ status: () => reply(withSearch()), candidates: () => found([PAT, SAM, KIM]) });
  const open = async (opts = {}) => {
    const f = fakeService({ ...list(), ...opts });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));
    return { f, r };
  };

  it('does not choose more people than the limit even when asked to', async () => {
    const f = fakeService({ ...list(), status: () => reply(withSearch({ limits: { maxContacts: 2, maxTitles: 5 } })) });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));

    tickPerson(r, 'aaaaaaaaaaaa');
    tickPerson(r, 'bbbbbbbbbbbb');
    tickPerson(r, 'cccccccccccc');

    await r.waitFor(() => expect(button(r, 'Add 2 people')).toBeTruthy());
    await settle();
    expect(person(r, 'cccccccccccc').props.checked).toBe(false);
  });

  it('reads the people of a search made earlier at once, rather than wait for a check', async () => {
    const f = fakeService({
      find: () => reply({ ok: true, reused: true, run: findRun({ status: 'done', outcome: 'found' }), quota: quota(22) }, 202),
      candidates: () => found([PAT, SAM]),
    });
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(people(r)).toEqual(['Pat Lee', 'Sam Ortiz']));
  });

  it('lets the rep search again once the first search is started', async () => {
    const f = fakeService();
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());
    button(r, 'Search for 1 title').trigger('onClick');
    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(f.posts('/company/find')).toHaveLength(2));
  });

  it('lets the rep add more people once the first add is started', async () => {
    const { f, r } = await open({ add: () => addStarted([aitem('aaaaaaaaaaaa', 'started', { run: adding('1') })]) });
    tickPerson(r, 'aaaaaaaaaaaa');
    await r.waitFor(() => expect(button(r, 'Add 1 person')).toBeTruthy());
    button(r, 'Add 1 person').trigger('onClick');
    await r.waitFor(() => expect(alerts(r)).toHaveLength(1));

    tickPerson(r, 'cccccccccccc');
    await r.waitFor(() => expect(button(r, 'Add 1 person')).toBeTruthy());
    button(r, 'Add 1 person').trigger('onClick');

    await r.waitFor(() => expect(f.posts('/company/add')).toHaveLength(2));
    expect(f.posts('/company/add')[1].body?.candidateIds).toEqual(['cccccccccccc']);
  });

  it('shows at once that the people just added are being added, and does not let them be chosen again', async () => {
    const { r } = await open({
      add: () => addStarted([aitem('aaaaaaaaaaaa', 'started', { run: adding('1') }), aitem('bbbbbbbbbbbb', 'started', { run: adding('2') })]),
    });
    tickPerson(r, 'aaaaaaaaaaaa');
    tickPerson(r, 'bbbbbbbbbbbb');
    await r.waitFor(() => expect(button(r, 'Add 2 people')).toBeTruthy());

    button(r, 'Add 2 people').trigger('onClick');

    await r.waitFor(() => expect(tags(r).filter(([label]) => label === 'Waiting')).toHaveLength(2));
    expect(person(r, 'aaaaaaaaaaaa').props.readOnly).toBe(true);
    expect(person(r, 'cccccccccccc').props.readOnly).toBeFalsy();
  });

  it('takes the quota a refused search sends', async () => {
    const f = fakeService({ find: () => reply({ ok: false, error: 'You have used all 25 runs for today.', quota: quota(0) }, 429) });
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('You have used all 25 runs for today.'));
  });

  it('takes the quota a refused add sends', async () => {
    const { r } = await open({ add: () => reply({ ok: false, error: 'This needs 2 runs and you have 1 left today.', quota: quota(1) }, 429) });
    tickPerson(r, 'aaaaaaaaaaaa');
    tickPerson(r, 'bbbbbbbbbbbb');
    await r.waitFor(() => expect(button(r, 'Add 2 people')).toBeTruthy());

    button(r, 'Add 2 people').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('1 of 25 runs left today.'));
  });

  it('looks at the runs again after a search is started, so the search shows among them', async () => {
    const f = fakeService({ status: (n) => reply(serviceStatus({ runs: n === 1 ? [] : [findRun({ status: 'running' })] })) });
    const r = setup(f.service);
    await ready(r);
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Find by job title'));
  });

  it('looks at the runs again after people are added, so the adds show among them', async () => {
    const f = fakeService({
      ...list(),
      status: (n) => reply(withSearch({ runs: n === 1 ? [findRun({ status: 'done', outcome: 'found' })] : [adding('1', 'running'), findRun({ status: 'done', outcome: 'found' })] })),
      add: () => addStarted([aitem('aaaaaaaaaaaa', 'started', { run: adding('1') })]),
    });
    const r = setup(f.service);
    await r.waitFor(() => expect(people(r)).toHaveLength(3));
    tickPerson(r, 'aaaaaaaaaaaa');
    await r.waitFor(() => expect(button(r, 'Add 1 person')).toBeTruthy());

    button(r, 'Add 1 person').trigger('onClick');

    await r.waitFor(() => expect(texts(r)).toContain('Add person'));
  });

  it('gives a new search all the patience, after giving up on an earlier one', async () => {
    const f = fakeService({
      status: () => reply(withSearch()),
      candidates: () => reply({ ok: true, ready: false, run: findRun({ status: 'running' }), candidates: [] }),
      find: () => reply({ ok: true, reused: false, run: findRun({ runId: 'e'.repeat(32) }), quota: quota(21) }, 202),
    });
    const r = setup(f.service, { runPollEveryMs: 10, runMaxPolls: 2 });
    await r.waitFor(() => expect(texts(r).some((t) => /still waiting/i.test(t))).toBe(true));
    choose(r, ['Administrator']);
    await r.waitFor(() => expect(button(r, 'Search for 1 title')).toBeTruthy());
    const before = gets(f);

    button(r, 'Search for 1 title').trigger('onClick');

    await r.waitFor(() => expect(gets(f)).toBe(before + 2));
  });

  it('says one person, not one people', async () => {
    const r = setup(fakeService({ status: () => reply(withSearch()), candidates: () => found([PAT]) }).service);

    await r.waitFor(() => expect(texts(r)).toContain('Found 1 person'));
  });
});
