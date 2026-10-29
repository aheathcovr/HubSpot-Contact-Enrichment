// The Enrichment card shows what each run is doing and did, as the contact card does: a spinner with the newest step while a
// run is going, and every step under a heading. The title search matters most: "no one found" has to come with the searches
// that were made and why each person who came back was not used, or a failed search looks the same as an empty one.
import { describe, expect, it } from 'vitest';
import { Accordion, LoadingSpinner } from '@hubspot/ui-extensions';
import { createRenderer } from '@hubspot/ui-extensions/testing';
import { EnrichmentView } from '../src/app/cards/EnrichmentView.tsx';
import type { RunStep } from '../src/app/cards/contact-model.ts';
import { COMPANIES } from './fixtures.ts';
import {
  COMPANY_ID, NOW, candidate, fakeService, findRun, found, functionRunner, reply, run, serviceStatus, settle, setup, tags, texts, type Rendered,
} from './company-fakes.tsx';

const step = (over: Partial<RunStep> = {}): RunStep => ({
  at: '2026-10-26T16:09:02.000Z', vendor: 'fullenrich', result: 'ok', text: 'Searched FullEnrich for "Administrator" at elmcourt.example.org: 3 people.', ...over,
});
const spinners = (r: Rendered) => r.findAll(LoadingSpinner).map((s) => s.props.label);
const headings = (r: Rendered) => r.findAll(Accordion).map((a) => [a.props.title, a.props.defaultOpen]).filter(([title]) => String(title).startsWith('What'));

describe('Enrichment card: what a run did', () => {
  it('shows what a run on a contact is doing, and has done, under the recent runs', async () => {
    const going = run({
      status: 'running',
      steps: [step({ text: 'Asked FullEnrich for a work email at elmcourt.example.org.' }), step({ at: '2026-10-26T16:09:03.000Z', text: 'Waiting for FullEnrich to finish the lookup (usually under a minute).' })],
    });
    const r = setup(fakeService({ status: () => reply(serviceStatus({ runs: [going] })) }).service);

    await r.waitFor(() => expect(spinners(r)).toContain('Waiting for FullEnrich to finish the lookup (usually under a minute) · 1 min 0 s'));
    expect(headings(r)).toEqual([['What is happening (2 steps so far)', true]]);
    expect(texts(r)).toContain('Asked FullEnrich for a work email at elmcourt.example.org.');
  });

  it('keeps the steps of a finished run closed, with how many there were and how long it took', async () => {
    const done = run({
      status: 'done', outcome: 'not_found', message: 'No email found.', finishedAt: '2026-10-26T16:09:09Z',
      steps: [step(), step({ result: 'empty', text: 'FullEnrich finished and found no work email.' })],
    });
    const r = setup(fakeService({ status: () => reply(serviceStatus({ runs: [done] })) }).service);

    await r.waitFor(() => expect(headings(r)).toEqual([['What happened (2 steps, 9 s)', false]]));
    expect(spinners(r)).toEqual([]);
  });
});

describe('Enrichment card: what a title search did', () => {
  const withSearch = (search: ReturnType<typeof findRun>, candidates: ReturnType<typeof candidate>[] = []) => ({
    status: () => reply(serviceStatus({ runs: [search] })),
    candidates: () => (search.status === 'done' ? found(candidates, { ...search }) : reply({ ok: true, ready: false, run: search, candidates: [] })),
  });

  it('says what a search that is going is doing right now, beside a spinner', async () => {
    const going = findRun({ status: 'running', steps: [step({ text: 'Searching FullEnrich for 1 title at Elm Court Care Center: Administrator.', vendor: 'service' })] });
    const r = setup(fakeService(withSearch(going)).service);

    await r.waitFor(() => expect(spinners(r)).toContain('Searching FullEnrich for 1 title at Elm Court Care Center: Administrator · 1 min 0 s'));
    expect(texts(r)).toContain('Searching for people. This card updates when it finishes.');
  });

  it('shows why no one was found: what was searched, and why each person who came back was not used', async () => {
    const empty = findRun({
      status: 'done', outcome: 'not_found', message: 'No one found for these titles at this company.', finishedAt: '2026-10-26T16:09:02Z',
      steps: [
        step({ vendor: 'service', result: 'info', text: 'Searching FullEnrich for 1 title at Elm Court Care Center: Administrator.' }),
        step({ text: 'Searched FullEnrich for "Administrator" at elmcourt.example.org: 3 people.' }),
        step({ result: 'empty', text: 'The best match for "Administrator" has no location on their profile, so they cannot be tied to Tulsa, OK; not used.' }),
      ],
    });
    const r = setup(fakeService(withSearch(empty)).service);

    await r.waitFor(() => expect(texts(r)).toContain('No one found for these titles at this company.'));
    expect(headings(r)).toEqual([['What happened (3 steps, 2 s)', false]]);
    expect(texts(r)).toContain('The best match for "Administrator" has no location on their profile, so they cannot be tied to Tulsa, OK; not used.');
  });

  it('shows a search\'s steps once, not again among the recent runs', async () => {
    const going = findRun({ status: 'running', steps: [step({ vendor: 'service', result: 'info', text: 'Searching FullEnrich for 1 title at Elm Court Care Center: Administrator.' })] });
    const r = setup(fakeService(withSearch(going)).service);

    await r.waitFor(() => expect(headings(r)).toEqual([['What is happening (1 step so far)', true]]));
    expect(spinners(r)).toHaveLength(1);
    expect(texts(r).filter((t) => t === 'Searching FullEnrich for 1 title at Elm Court Care Center: Administrator.')).toHaveLength(1);
  });

  it('keeps the steps of an earlier search among the recent runs, since only the newest is shown with the people', async () => {
    const newer = findRun({ status: 'done', outcome: 'found', message: 'Found 1 person. Choose who to add.', finishedAt: '2026-10-26T16:09:50Z', createdAt: '2026-10-26T16:09:40Z' });
    const older = findRun({
      runId: 'e'.repeat(32), status: 'done', outcome: 'not_found', message: 'No one found for these titles at this company.', createdAt: '2026-10-26T16:05:00Z', finishedAt: '2026-10-26T16:05:03Z',
      steps: [step({ at: '2026-10-26T16:05:02.000Z', result: 'empty', text: 'Searched FullEnrich for "Dietitian" at elmcourt.example.org: no one.' })],
    });
    const r = setup(fakeService({ status: () => reply(serviceStatus({ runs: [newer, older] })), candidates: () => found([candidate('aaaaaaaaaaaa', 'Pat Lee')], { ...newer }) }).service);

    await r.waitFor(() => expect(headings(r)).toEqual([['What happened (1 step, 3 s)', false]]));
    expect(texts(r)).toContain('Searched FullEnrich for "Dietitian" at elmcourt.example.org: no one.');
  });

  it('shows a search that could not be done as failed, with the vendor that failed in red, not as an empty result', async () => {
    const failed = findRun({
      status: 'failed', outcome: 'error', finishedAt: '2026-10-26T16:09:02Z',
      message: 'FullEnrich could not be searched, so this is not a finding that no one is there: see What happened. Try again later.',
      steps: [step({ result: 'failed', text: 'Search for "Administrator" at elmcourt.example.org failed: HTTP 503 (the vendor had a problem).' })],
    });
    const r = setup(fakeService(withSearch(failed)).service);

    await r.waitFor(() => expect(texts(r)).toContain(failed.message));
    expect(tags(r)).toEqual(expect.arrayContaining([['FullEnrich', 'error']]));
    expect(texts(r)).not.toContain('No one found for these titles at this company.');
  });

  it('shows the progress of the add of a person inside that person\'s row', async () => {
    const adding = run({
      runId: '9'.repeat(32), contactId: '', objective: 'add_candidate', status: 'running',
      steps: [step({ vendor: 'seamless', result: 'info', text: 'Asked Seamless to research the person at elmcourt.example.org.' })],
    });
    const done = findRun({ status: 'done', outcome: 'found', message: 'Found 1 person. Choose who to add.', finishedAt: '2026-10-26T16:09:02Z' });
    const r = setup(fakeService({
      status: () => reply(serviceStatus({ runs: [done] })),
      candidates: () => found([candidate('aaaaaaaaaaaa', 'Pat Lee', { added: adding })], { ...done }),
    }).service);

    await r.waitFor(() => expect(spinners(r)).toContain('Asked Seamless to research the person at elmcourt.example.org · 1 min 0 s'));
  });
});

describe('Enrichment card: a run is shown once', () => {
  it('shows the steps of an add inside the person\'s row, and not again among the recent runs', async () => {
    const adding = run({
      runId: '9'.repeat(32), contactId: '', objective: 'add_candidate', status: 'running',
      steps: [step({ vendor: 'seamless', result: 'info', text: 'Asked Seamless to research the person at elmcourt.example.org.' })],
    });
    const done = findRun({ status: 'done', outcome: 'found', message: 'Found 1 person. Choose who to add.', finishedAt: '2026-10-26T16:09:02Z' });
    const r = setup(fakeService({
      status: () => reply(serviceStatus({ runs: [adding, done] })),
      candidates: () => found([candidate('aaaaaaaaaaaa', 'Pat Lee', { added: adding })], { ...done }),
    }).service);

    await r.waitFor(() => expect(spinners(r)).toContain('Asked Seamless to research the person at elmcourt.example.org · 1 min 0 s'));
    expect(spinners(r).filter((label) => String(label).startsWith('Asked Seamless'))).toHaveLength(1);
    expect(texts(r).filter((t) => t === 'Asked Seamless to research the person at elmcourt.example.org.')).toHaveLength(1);
  });
});

describe('Enrichment card: how often it asks', () => {
  it('asks again every two seconds by default while a run is going', async () => {
    const f = fakeService({ status: () => reply(serviceStatus({ runs: [run({ status: 'running' })] })) });
    const r = createRenderer('crm.record.sidebar');
    r.mocks.context.crm.objectId = Number(COMPANY_ID);
    r.mocks.context.crm.objectTypeId = COMPANIES;
    r.render(<EnrichmentView context={r.mocks.context} actions={r.mocks.actions} run={functionRunner()} service={f.service} pollEveryMs={0} now={() => NOW} />);

    await r.waitFor(() => expect(f.gets('/company/status')).toBe(1));
    await settle(1000);
    expect(f.gets('/company/status')).toBe(1);
    await r.waitFor(() => expect(f.gets('/company/status')).toBeGreaterThanOrEqual(2), { timeoutInMs: 2500 });
  });
});
