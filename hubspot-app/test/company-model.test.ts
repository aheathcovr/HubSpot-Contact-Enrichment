// The company card's picking logic without HubSpot: what it asks the service, and how it words what comes back.
import { describe, expect, it } from 'vitest';
import {
  addAlert,
  addButtonText,
  batchAlert,
  candidatesPath,
  combineTitles,
  findAlert,
  parseTitles,
  quotaWarning,
  runsUpTo,
  searchButtonText,
  startButtonText,
  statusPath,
  type AddItem,
  type AddResult,
  type BatchItem,
  type BatchResult,
  type FindResult,
} from '../src/app/cards/company-model.ts';

const item = (contactId: string, status: BatchItem['status'], over: Partial<BatchItem> = {}): BatchItem => ({
  contactId,
  objective: 'find_email',
  status,
  reason: null,
  run: null,
  ...over,
});

const result = (items: BatchItem[]): BatchResult => ({
  ok: true,
  batchId: 'b'.repeat(32),
  items,
  started: items.filter((i) => i.status === 'started').length,
  quota: { cap: 25, used: 3, remaining: 22 },
});

const NAMES = { '1': 'Pat Lee', '2': 'Sam Ortiz' };

describe('what the card asks the service', () => {
  it('asks for the company status by company id', () => {
    expect(statusPath('500')).toBe('/company/status?companyId=500');
  });
});

describe('the start button', () => {
  it.each([
    [0, 'Choose contacts'],
    [1, 'Start for 1 contact'],
    [2, 'Start for 2 contacts'],
    [10, 'Start for 10 contacts'],
  ])('reads "%s contacts" as "%s"', (n, text) => {
    expect(startButtonText(n)).toBe(text);
  });
});

describe('how many runs a choice can use', () => {
  it.each([
    ['find_email', 3, 3],
    ['find_mobile', 3, 3],
    ['verify_email', 3, 3],
    ['enrich', 3, 6],
    ['enrich', 0, 0],
  ])('%s for %i contacts can use up to %i runs', (objective, n, runs) => {
    expect(runsUpTo(objective, n)).toBe(runs);
  });

  it('warns when the most a choice can use is more than the rep has left', () => {
    expect(quotaWarning('enrich', 3, 4)).toBe('This could use up to 6 runs and you have 4 left today.');
    expect(quotaWarning('find_email', 3, 2)).toBe('This could use up to 3 runs and you have 2 left today.');
  });

  it('says nothing when there are enough runs left, or nothing is picked', () => {
    expect(quotaWarning('enrich', 3, 6)).toBeNull();
    expect(quotaWarning('enrich', 0, 0)).toBeNull();
  });

  it('says one run, not one runs', () => {
    expect(quotaWarning('find_email', 1, 0)).toBe('This could use up to 1 run and you have 0 left today.');
  });
});

describe('telling the rep what a batch did', () => {
  it('says how many runs started', () => {
    expect(batchAlert(result([item('1', 'started'), item('2', 'started')]), NAMES)).toEqual({
      type: 'success',
      title: 'Runs started',
      message: '2 runs started. This card updates when they finish.',
    });
    expect(batchAlert(result([item('1', 'started')]), NAMES).message).toBe('1 run started. This card updates when it finishes.');
  });

  it('names each contact that was skipped, with the reason', () => {
    const alert = batchAlert(result([item('1', 'started'), item('2', 'skipped', { reason: 'Already has an email. Use Verify email.' })]), NAMES);

    expect(alert.type).toBe('success');
    expect(alert.message).toBe('1 run started. This card updates when it finishes. Skipped Sam Ortiz: Already has an email. Use Verify email.');
  });

  it('names each contact whose run could not be started', () => {
    const alert = batchAlert(result([item('1', 'started'), item('2', 'failed', { reason: 'The run could not be queued. Try again in a moment.' })]), NAMES);

    expect(alert.message).toContain('Could not start for Sam Ortiz: The run could not be queued. Try again in a moment.');
  });

  it('says when some were asked in the last day, so the earlier runs are shown', () => {
    const alert = batchAlert(result([item('1', 'started'), item('2', 'reused')]), NAMES);

    expect(alert.message).toBe('1 run started. This card updates when it finishes. 1 was asked in the last day, so the earlier run is shown.');
  });

  it('is an info note, not a success, when nothing new started', () => {
    expect(batchAlert(result([item('1', 'reused'), item('2', 'reused')]), NAMES)).toEqual({
      type: 'info',
      title: 'Already asked',
      message: '2 were asked in the last day, so the earlier runs are shown. Nothing more was spent.',
    });
  });

  it('calls a contact it has no name for by its id', () => {
    const alert = batchAlert(result([item('1', 'started'), item('9', 'skipped', { reason: 'Not linked to this company.' })]), NAMES);

    expect(alert.message).toContain('Skipped Contact 9: Not linked to this company.');
  });
});

describe('job titles', () => {
  it('splits what a rep typed on commas, semicolons and new lines, and drops blanks', () => {
    expect(parseTitles('Medical Director, Dietitian; Social Worker\n  Activities Director ,, ')).toEqual([
      'Medical Director', 'Dietitian', 'Social Worker', 'Activities Director',
    ]);
    expect(parseTitles('')).toEqual([]);
    expect(parseTitles('  ,; ')).toEqual([]);
  });

  it('puts the quick picks first, adds what was typed, and drops repeats in any case', () => {
    expect(combineTitles(['Administrator', 'Director of Nursing'], 'administrator, Dietitian, DIETITIAN')).toEqual([
      'Administrator', 'Director of Nursing', 'Dietitian',
    ]);
    expect(combineTitles([], '')).toEqual([]);
  });

  it.each([
    [0, 'Choose job titles'],
    [1, 'Search for 1 title'],
    [3, 'Search for 3 titles'],
  ])('reads %s titles as "%s"', (n, text) => {
    expect(searchButtonText(n)).toBe(text);
  });

  it.each([
    [0, 'Choose people'],
    [1, 'Add 1 person'],
    [4, 'Add 4 people'],
  ])('reads %s people as "%s"', (n, text) => {
    expect(addButtonText(n)).toBe(text);
  });

  it('asks for the people a search found by company and search', () => {
    expect(candidatesPath('500', 'f'.repeat(32))).toBe(`/company/candidates?companyId=500&runId=${'f'.repeat(32)}`);
  });
});

const find = (reused: boolean): FindResult => ({
  ok: true,
  reused,
  run: { runId: 'f'.repeat(32), contactId: '', objective: 'find_by_title', status: 'queued', outcome: null, message: null, wrote: [], requestedBy: 'rep@example.org', createdAt: '2026-10-26T16:09:00Z', finishedAt: null, companyId: '500', batchId: null, steps: [] },
  quota: { cap: 25, used: 4, remaining: 21 },
});

describe('telling the rep what a search did', () => {
  it('says a new search started', () => {
    expect(findAlert(find(false))).toEqual({ type: 'success', title: 'Search started', message: 'The service is looking. This card shows the people it finds.' });
  });

  it('says when the same search was made in the last day, so its results are shown', () => {
    expect(findAlert(find(true))).toEqual({
      type: 'info',
      title: 'Already searched',
      message: 'The same titles were searched in the last day, so the earlier results are shown. Nothing more was spent.',
    });
  });
});

const aitem = (candidateId: string, status: AddItem['status'], reason: string | null = null): AddItem => ({ candidateId, objective: 'add_candidate', status, reason, run: null });
const aresult = (items: AddItem[]): AddResult => ({ ok: true, batchId: 'b'.repeat(32), items, started: items.filter((i) => i.status === 'started').length, quota: { cap: 25, used: 3, remaining: 22 } });
const PEOPLE = { c1: 'Pat Lee', c2: 'Sam Ortiz' };

describe('telling the rep what an add did', () => {
  it('says how many people are being added', () => {
    expect(addAlert(aresult([aitem('c1', 'started'), aitem('c2', 'started')]), PEOPLE)).toEqual({
      type: 'success',
      title: 'Adding people',
      message: '2 people are being added. This card updates when they finish.',
    });
    expect(addAlert(aresult([aitem('c1', 'started')]), PEOPLE).message).toBe('1 person is being added. This card updates when it finishes.');
  });

  it('names a person who was skipped, or whose add could not be started, with the reason', () => {
    const alert = addAlert(aresult([aitem('c1', 'started'), aitem('c2', 'skipped', 'Not in these search results.'), aitem('c9', 'failed', 'The run could not be queued. Try again in a moment.')]), PEOPLE);

    expect(alert.message).toBe(
      '1 person is being added. This card updates when it finishes. Skipped Sam Ortiz: Not in these search results. Could not start for Person c9: The run could not be queued. Try again in a moment.',
    );
  });

  it('is an info note when everyone chosen was already being added', () => {
    expect(addAlert(aresult([aitem('c1', 'reused')]), PEOPLE)).toEqual({
      type: 'info',
      title: 'Already added',
      message: '1 was already asked for, so the earlier run is shown. Nothing more was spent.',
    });
    expect(addAlert(aresult([aitem('c1', 'reused'), aitem('c2', 'reused')]), PEOPLE).message).toBe('2 were already asked for, so the earlier runs are shown. Nothing more was spent.');
  });

  it('mentions the ones already being added next to the ones that started', () => {
    expect(addAlert(aresult([aitem('c1', 'started'), aitem('c2', 'reused')]), PEOPLE).message).toBe(
      '1 person is being added. This card updates when it finishes. 1 was already asked for, so the earlier run is shown.',
    );
  });
});
