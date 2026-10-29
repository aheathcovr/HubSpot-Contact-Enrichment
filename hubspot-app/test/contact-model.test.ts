// The contact card's logic without HubSpot: reading the service's answers, naming runs, and calling the service.
import { afterEach, describe, expect, it } from 'vitest';
import {
  SERVICE_FUNCTION,
  ServiceError,
  agoText,
  elapsedText,
  liveText,
  makeService,
  objectiveLabel,
  RUN_MAX_POLLS,
  RUN_POLL_EVERY_MS,
  readReply,
  runIsOpen,
  runTag,
  stepVariant,
  stepsOf,
  stepsTitle,
  vendorLabel,
  type RunStep,
  type RunView,
} from '../src/app/cards/contact-model.ts';

const run = (over: Partial<RunView> = {}): RunView => ({
  runId: 'a'.repeat(32),
  contactId: '42',
  objective: 'find_email',
  status: 'done',
  outcome: 'found',
  message: 'Email added.',
  wrote: ['email'],
  requestedBy: 'rep@example.org',
  createdAt: '2026-10-26T16:00:00Z',
  finishedAt: '2026-10-26T16:00:09Z',
  companyId: null,
  batchId: null,
  steps: [],
  ...over,
});

const step = (over: Partial<RunStep> = {}): RunStep => ({ at: '2026-10-26T16:00:02.000Z', vendor: 'fullenrich', result: 'ok', text: 'Searched FullEnrich: 3 people.', ...over });

describe('where the card calls', () => {
  it('is the app function that carries its requests to the service', () => {
    expect(SERVICE_FUNCTION).toBe('service');
  });
});

describe('reading the service\'s answer', () => {
  it('hands back the body of an answer that worked', () => {
    expect(readReply<{ ok: true; n: number }>({ status: 200, json: { ok: true, n: 3 } })).toEqual({ ok: true, n: 3 });
    expect(readReply<{ ok: true }>({ status: 202, json: { ok: true } })).toEqual({ ok: true });
  });

  it('throws the service\'s own message, with its status, when it refused', () => {
    expect.assertions(3);
    try {
      readReply({ status: 409, json: { ok: false, error: 'Already has an email. Use Verify email.' } });
    } catch (err) {
      expect(err).toBeInstanceOf(ServiceError);
      expect((err as ServiceError).message).toBe('Already has an email. Use Verify email.');
      expect((err as ServiceError).status).toBe(409);
    }
  });

  it('carries the quota of a refusal for the daily cap', () => {
    expect.assertions(1);
    try {
      readReply({ status: 429, json: { ok: false, error: 'You have used all 25 runs for today.', quota: { cap: 25, used: 25, remaining: 0 } } });
    } catch (err) {
      expect((err as ServiceError).quota).toEqual({ cap: 25, used: 25, remaining: 0 });
    }
  });

  it.each([
    [401, 'HubSpot\'s request was not accepted by the enrichment service.'],
    [403, 'HubSpot\'s request was not accepted by the enrichment service.'],
    [503, 'The enrichment service is not set up for the app yet.'],
    [404, 'The enrichment service could not find that.'],
    [502, 'The enrichment service answered 502.'],
  ])('says what a %i means when the body has no message', (status, text) => {
    expect(() => readReply({ status, json: null })).toThrow(text);
  });

  it('does not take a redirect for an answer', () => {
    expect(() => readReply({ status: 302, json: { ok: true } })).toThrow('The enrichment service answered 302.');
  });

  it('leaves out the quota of a refusal when it is not a whole one', () => {
    expect.assertions(1);
    try {
      readReply({ status: 429, json: { ok: false, error: 'You have used all 25 runs for today.', quota: { cap: 25 } } });
    } catch (err) {
      expect((err as ServiceError).quota).toBeUndefined();
    }
  });

  it('does not take an answer that says ok but has the wrong status for ok', () => {
    expect(() => readReply({ status: 500, json: { ok: true } })).toThrow('The enrichment service answered 500.');
  });

  it.each([null, 'text', 7, []])('does not take %j for an answer body', (json) => {
    expect(() => readReply({ status: 200, json })).toThrow('The enrichment service sent back something the card could not read.');
  });
});

describe('naming a run', () => {
  it('says which runs are still going', () => {
    expect(runIsOpen(run({ status: 'queued' }))).toBe(true);
    expect(runIsOpen(run({ status: 'running' }))).toBe(true);
    expect(runIsOpen(run({ status: 'done' }))).toBe(false);
    expect(runIsOpen(run({ status: 'failed' }))).toBe(false);
  });

  it.each([
    [{ status: 'queued' }, 'Waiting', 'default'],
    [{ status: 'running' }, 'Running', 'warning'],
    [{ status: 'done', outcome: 'found' }, 'Done', 'success'],
    [{ status: 'done', outcome: 'verified' }, 'Done', 'success'],
    [{ status: 'done', outcome: 'held' }, 'Needs a look', 'warning'],
    [{ status: 'done', outcome: 'not_written' }, 'Needs a look', 'warning'],
    [{ status: 'done', outcome: 'moved' }, 'Needs a look', 'warning'],
    [{ status: 'done', outcome: 'not_found' }, 'Nothing found', 'default'],
    [{ status: 'done', outcome: 'skipped' }, 'Skipped', 'default'],
    [{ status: 'failed', outcome: 'error' }, 'Failed', 'error'],
    [{ status: 'done', outcome: 'something_new' }, 'Done', 'default'],
  ] as const)('shows %j as %s', (over, label, variant) => {
    expect(runTag(run(over as Partial<RunView>))).toEqual({ label, variant });
  });

  it('names the three objectives and passes an unknown one through', () => {
    expect(objectiveLabel('find_email')).toBe('Find email');
    expect(objectiveLabel('find_mobile')).toBe('Find mobile');
    expect(objectiveLabel('verify_email')).toBe('Verify email');
    expect(objectiveLabel('something_new')).toBe('something_new');
  });

  it('names the two runs the company card makes that have no contact to begin with', () => {
    expect(objectiveLabel('find_by_title')).toBe('Find by job title');
    expect(objectiveLabel('add_candidate')).toBe('Add person');
  });
});

describe('how long ago', () => {
  const at = Date.parse('2026-10-26T16:00:00Z');
  it.each([
    [0, 'just now'],
    [59_000, 'just now'],
    [60_000, '1 min ago'],
    [5 * 60_000, '5 min ago'],
    [59 * 60_000, '59 min ago'],
    [60 * 60_000, '1 h ago'],
    [5 * 3_600_000, '5 h ago'],
    [23 * 3_600_000, '23 h ago'],
  ])('writes %i ms as "%s"', (ms, text) => {
    expect(agoText('2026-10-26T16:00:00Z', at + ms)).toBe(text);
  });

  it('gives the date for anything a day old or more', () => {
    expect(agoText('2026-10-26T16:00:00Z', at + 24 * 3_600_000)).toBe('Oct 26');
    expect(agoText('2026-10-20T09:00:00Z', at)).toBe('Oct 20');
  });

  it('gives the date for a time in the future and nothing for a date it cannot read', () => {
    expect(agoText('2026-10-27T16:00:00Z', at)).toBe('Oct 27');
    expect(agoText('soon', at)).toBe('');
    expect(agoText(null, at)).toBe('');
  });
});

describe('dates are UTC, whatever zone the browser is in', () => {
  const zone = process.env.TZ;
  afterEach(() => {
    if (zone === undefined) delete process.env.TZ;
    else process.env.TZ = zone;
  });
  const later = Date.parse('2026-12-20T00:00:00Z');

  it('keeps the day when the local day is the next one', () => {
    process.env.TZ = 'Pacific/Kiritimati'; // UTC+14: 23:30 UTC on the 24th is already the 25th there
    expect(agoText('2026-10-24T23:30:00Z', later)).toBe('Oct 24');
  });

  it('keeps the month when the local month is the one before', () => {
    process.env.TZ = 'Pacific/Pago_Pago'; // UTC-11: 00:30 UTC on 1 November is still 31 October there
    expect(agoText('2026-11-01T00:30:00Z', later)).toBe('Nov 1');
  });
});

describe('calling the service', () => {
  // A fake hubspot.serverless: it records what it was called with and answers as the test says.
  const answering = (answer: unknown, calls: unknown[] = []) =>
    (async (name: string, options: Record<string, unknown>) => {
      calls.push({ name, options });
      return answer;
    }) as never;

  it('calls the service function with the method and the path, and nothing else for a get', async () => {
    const calls: unknown[] = [];
    const service = makeService(answering({ status: 200, json: { ok: true } }, calls));

    const reply = await service('GET', '/contact/status?contactId=42');

    expect(reply).toEqual({ status: 200, json: { ok: true } });
    expect(calls).toEqual([{ name: 'service', options: { parameters: { method: 'GET', path: '/contact/status?contactId=42' } } }]);
  });

  it('sends a body as an object with the method and the path', async () => {
    const calls: { options: Record<string, unknown> }[] = [];
    const service = makeService(answering({ status: 202, json: { ok: true } }, calls));

    await service('POST', '/contact/run', { contactId: '42', objective: 'find_email' });

    expect(calls[0].options).toEqual({ parameters: { method: 'POST', path: '/contact/run', body: { contactId: '42', objective: 'find_email' } } });
  });

  it('sends no body at all on a get', async () => {
    const calls: { options: { parameters: Record<string, unknown> } }[] = [];
    const service = makeService(answering({ status: 200, json: { ok: true } }, calls));

    await service('GET', '/contact/status?contactId=42');

    expect('body' in calls[0].options.parameters).toBe(false);
  });

  it('sends nothing that says who is asking: HubSpot tells the function that', async () => {
    const calls: { options: { parameters: Record<string, unknown> } }[] = [];
    const service = makeService(answering({ status: 200, json: null }, calls));

    await service('POST', '/contact/run', { contactId: '42', objective: 'find_email' });

    expect(Object.keys(calls[0].options.parameters).sort()).toEqual(['body', 'method', 'path']);
  });

  it("hands back the function's refusal, which has the same shape as the service's", async () => {
    const service = makeService(answering({ status: 503, json: { ok: false, error: 'The app is not set up to reach the enrichment service. Ask an admin.' } }));

    expect(await service('GET', '/x')).toEqual({ status: 503, json: { ok: false, error: 'The app is not set up to reach the enrichment service. Ask an admin.' } });
  });

  it('keeps a null body when the service answered with something that was not JSON', async () => {
    const service = makeService(answering({ status: 502, json: null }));

    expect(await service('GET', '/x')).toEqual({ status: 502, json: null });
  });

  it('gives a null body, not no body, when the function sent none', async () => {
    const service = makeService(answering({ status: 204 }));

    expect(await service('GET', '/x')).toEqual({ status: 204, json: null });
  });

  it.each([
    ['nothing', undefined],
    ['null', null],
    ['text', 'ok'],
    ['a list', [200, {}]],
    ['an answer with no status', { json: { ok: true } }],
    ['a status that is not a number', { status: '200', json: {} }],
    ['a status that is not a whole number', { status: 200.5, json: {} }],
  ])('refuses %s as an answer, in words', async (_why, answer) => {
    const service = makeService(answering(answer));

    await expect(service('GET', '/x')).rejects.toThrow('The app sent back something the card could not read.');
  });

  it('lets a failure of the call itself through for the card to report', async () => {
    const service = makeService((async () => { throw new Error('Function execution timed out'); }) as never);

    await expect(service('GET', '/x')).rejects.toThrow('Function execution timed out');
  });
});


describe('the trail of a run', () => {
  it.each([
    ['fullenrich', 'FullEnrich'],
    ['seamless', 'Seamless'],
    ['millionverifier', 'MillionVerifier'],
    ['bounceban', 'BounceBan'],
    ['hubspot', 'HubSpot'],
    ['service', 'Service'],
  ])('names %s as %s', (vendor, label) => {
    expect(vendorLabel(vendor)).toBe(label);
  });

  it('shows a vendor it has no name for as the service sent it', () => {
    expect(vendorLabel('somenewvendor')).toBe('somenewvendor');
  });

  it.each([
    ['ok', 'success'],
    ['failed', 'error'],
    ['skipped', 'warning'],
    ['empty', 'default'],
    ['info', 'default'],
  ] as const)('colours a step that is %s as %s', (result, variant) => {
    expect(stepVariant(result)).toBe(variant);
  });

  it('says how long after a moment another happened, in seconds and then minutes', () => {
    expect(elapsedText('2026-10-26T16:00:00Z', '2026-10-26T16:00:00.900Z')).toBe('0 s');
    expect(elapsedText('2026-10-26T16:00:00Z', '2026-10-26T16:00:14.250Z')).toBe('14 s');
    expect(elapsedText('2026-10-26T16:00:00Z', '2026-10-26T16:01:05Z')).toBe('1 min 5 s');
    expect(elapsedText('2026-10-26T16:00:00Z', Date.parse('2026-10-26T16:00:04Z'))).toBe('4 s');
  });

  it('never says a negative time, and says nothing for a time it cannot read', () => {
    expect(elapsedText('2026-10-26T16:00:10Z', '2026-10-26T16:00:00Z')).toBe('0 s');
    expect(elapsedText('not a time', '2026-10-26T16:00:00Z')).toBe('');
    expect(elapsedText('2026-10-26T16:00:00Z', 'not a time')).toBe('');
  });

  it('treats a run that came with no steps (an older service) as having none', () => {
    expect(stepsOf(run({ steps: undefined as never }))).toEqual([]);
    expect(stepsOf(run({ steps: [step()] }))).toHaveLength(1);
  });

  it('says what a run is doing now from its newest step and how long it has been going', () => {
    const going = run({ status: 'running', outcome: null, finishedAt: null, steps: [step({ text: 'Asked FullEnrich.' }), step({ text: 'Waiting for FullEnrich to finish the lookup (usually under a minute).' })] });

    expect(liveText(going, Date.parse('2026-10-26T16:00:14Z'))).toBe('Waiting for FullEnrich to finish the lookup (usually under a minute) · 14 s');
  });

  it('says a run that has done nothing yet is waiting, or is starting', () => {
    const at = Date.parse('2026-10-26T16:00:04Z');

    expect(liveText(run({ status: 'queued', steps: [] }), at)).toBe('Waiting for the service to pick this up · 4 s');
    expect(liveText(run({ status: 'running', steps: [] }), at)).toBe('Working on it · 4 s');
  });

  it('titles the steps of a finished run with how many there were and how long it took', () => {
    const done = run({ steps: [step(), step(), step()], finishedAt: '2026-10-26T16:00:09Z' });

    expect(stepsTitle(done)).toBe('What happened (3 steps, 9 s)');
    expect(stepsTitle(run({ steps: [step()] }))).toBe('What happened (1 step, 9 s)');
    expect(stepsTitle(run({ steps: [step()], finishedAt: null }))).toBe('What happened (1 step)');
  });

  it('titles the steps of a run that is still going as so far', () => {
    expect(stepsTitle(run({ status: 'running', finishedAt: null, steps: [step(), step()] }))).toBe('What is happening (2 steps so far)');
    expect(stepsTitle(run({ status: 'queued', finishedAt: null, steps: [] }))).toBe('What is happening (0 steps so far)');
  });
});


describe('how closely the cards follow a run', () => {
  it('asks every two seconds, so what the card shows is never far behind what the service is doing', () => {
    expect(RUN_POLL_EVERY_MS).toBe(2000);
  });

  it('keeps asking for at least three minutes, as long as the cards always did before giving up', () => {
    expect(RUN_POLL_EVERY_MS * RUN_MAX_POLLS).toBeGreaterThanOrEqual(180_000);
  });
});
