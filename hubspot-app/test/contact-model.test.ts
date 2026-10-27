// The contact card's logic without HubSpot: reading the service's answers, naming runs, and calling the service.
import { afterEach, describe, expect, it } from 'vitest';
import {
  SERVICE_URL,
  ServiceError,
  agoText,
  makeService,
  objectiveLabel,
  readReply,
  runIsOpen,
  runTag,
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
  ...over,
});

describe('where the card calls', () => {
  it('is the service, under /app', () => {
    expect(SERVICE_URL).toBe('https://hubspot-enrichment-kpqza7tiga-uc.a.run.app/app');
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
  const replying = (status: number, body: unknown, calls: unknown[] = []) =>
    (async (url: string, options: Record<string, unknown>) => {
      calls.push({ url, options });
      return { status, json: async () => body };
    }) as never;

  it('asks for the path under the service, with the method and a timeout', async () => {
    const calls: unknown[] = [];
    const service = makeService(replying(200, { ok: true }, calls));

    const reply = await service('GET', '/contact/status?contactId=42');

    expect(reply).toEqual({ status: 200, json: { ok: true } });
    expect(calls).toEqual([{ url: `${SERVICE_URL}/contact/status?contactId=42`, options: { method: 'GET', timeout: 20000 } }]);
  });

  it('sends a body as an object, which is what hubspot.fetch takes', async () => {
    const calls: { options: Record<string, unknown> }[] = [];
    const service = makeService(replying(202, { ok: true }, calls));

    await service('POST', '/contact/run', { contactId: '42', objective: 'find_email' });

    expect(calls[0].options).toEqual({ method: 'POST', timeout: 20000, body: { contactId: '42', objective: 'find_email' } });
  });

  it('sends no body at all on a get', async () => {
    const calls: { options: Record<string, unknown> }[] = [];
    const service = makeService(replying(200, { ok: true }, calls));

    await service('GET', '/contact/status?contactId=42');

    expect('body' in calls[0].options).toBe(false);
  });

  it('gives a null body when the answer is not JSON', async () => {
    const service = makeService((async () => ({ status: 502, json: async () => { throw new SyntaxError('Unexpected token <'); } })) as never);

    expect(await service('GET', '/x')).toEqual({ status: 502, json: null });
  });

  it('lets a network failure through for the card to report', async () => {
    const service = makeService((async () => { throw new Error('Network down'); }) as never);

    await expect(service('GET', '/x')).rejects.toThrow('Network down');
  });
});
