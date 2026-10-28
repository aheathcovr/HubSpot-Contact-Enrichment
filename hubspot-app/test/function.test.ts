// The enrichment app function against an in-memory HubSpot.
//
// The function does two things for the Enrichment card on a company record: it reports where the company stands
// (when it was last enriched, whether a run is already requested, which contacts it has and how each email checked
// out), and it sets Request to enrich, which makes HubSpot call the enrichment service's webhook. It writes nothing
// else, and it refuses a request the service would only skip.
import { createRequire } from 'node:module';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { COMPANIES, COMPANY_ID, CORPORATION, FACILITY, NOW, company, contact, ctx, ms } from './fixtures.ts';
import { fakeHubSpot } from './hubspot-fake.ts';

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-require-imports
const fn = require('../src/app/functions/enrichment.js');
const { handle, makeRequester, describeFailure } = fn._internals;

const run = (hub: ReturnType<typeof fakeHubSpot>, context: ReturnType<typeof ctx>) => handle(context, hub.api, NOW);
const status = (hub: ReturnType<typeof fakeHubSpot>, over: Parameters<typeof ctx>[1] = {}) => run(hub, ctx('status', over));
const request = (hub: ReturnType<typeof fakeHubSpot>, over: Parameters<typeof ctx>[1] = {}) => run(hub, ctx('request', over));
const hubWith = (properties: Record<string, string | null> = {}, contacts: ReturnType<typeof contact>[] = []) =>
  fakeHubSpot({ companies: [{ ...company(properties), contacts: contacts.map((c) => c.id) }], contacts });

// ── Status: where the company stands ──────────────────────────────────────────

describe('status of a company', () => {
  it('says a company that was never enriched can be requested', async () => {
    const r = await status(hubWith());

    expect(r).toMatchObject({
      ok: true,
      canRequest: true,
      reason: null,
      message: null,
      company: { id: COMPANY_ID, name: 'Elm Court Care Center', kind: 'facility', requested: false, lastEnrichedOn: null, daysSince: null, nextEligibleOn: null },
    });
  });

  it('knows when it was last enriched and when a new request is allowed', async () => {
    const r = await status(hubWith({ revops_enrichment_date: '2026-10-21' }));

    expect(r.company).toMatchObject({ lastEnrichedOn: '2026-10-21', daysSince: 5, nextEligibleOn: '2026-11-20' });
    expect(r).toMatchObject({ canRequest: false, reason: 'recently_enriched' });
    expect(r.message).toMatch(/5 days ago/);
    expect(r.message).toMatch(/2026-11-20/);
  });

  it('reads the date as epoch milliseconds too, the form the service writes it in', async () => {
    const r = await status(hubWith({ revops_enrichment_date: ms(2026, 10, 21) }));

    expect(r.company).toMatchObject({ lastEnrichedOn: '2026-10-21', daysSince: 5 });
  });

  it('reads a date-time by its first ten characters, as the service does', async () => {
    const r = await status(hubWith({ revops_enrichment_date: '2026-10-21T00:00:00.000Z' }));

    expect(r.company).toMatchObject({ lastEnrichedOn: '2026-10-21', daysSince: 5 });
  });

  it('allows a request on the thirtieth day and not on the twenty-ninth', async () => {
    const thirty = await status(hubWith({ revops_enrichment_date: '2026-09-26' }));
    const twentyNine = await status(hubWith({ revops_enrichment_date: '2026-09-27' }));

    expect(thirty.company.daysSince).toBe(30);
    expect(thirty.canRequest).toBe(true);
    expect(twentyNine.company.daysSince).toBe(29);
    expect(twentyNine.canRequest).toBe(false);
  });

  it('counts whole days from midnight UTC, not from the hour of the day', async () => {
    const early = await handle(ctx('status'), hubWith({ revops_enrichment_date: '2026-09-26' }).api, Date.UTC(2026, 9, 26, 0, 0, 1));
    const late = await handle(ctx('status'), hubWith({ revops_enrichment_date: '2026-09-27' }).api, Date.UTC(2026, 9, 26, 23, 59, 59));

    expect(early.company.daysSince).toBe(30);
    expect(late.company.daysSince).toBe(29);
  });

  it('treats a date in the future as recent and does not say "-7 days ago"', async () => {
    const r = await status(hubWith({ revops_enrichment_date: '2026-11-02' }));

    expect(r.canRequest).toBe(false);
    expect(r.reason).toBe('recently_enriched');
    expect(r.message).not.toMatch(/-\d+ days/);
    expect(r.message).toMatch(/2026-12-02/);
  });

  it('says "today" and "1 day ago" in the singular', async () => {
    const today = await status(hubWith({ revops_enrichment_date: '2026-10-26' }));
    const yesterday = await status(hubWith({ revops_enrichment_date: '2026-10-25' }));

    expect(today.message).toMatch(/today/);
    expect(yesterday.message).toMatch(/1 day ago/);
    expect(yesterday.message).not.toMatch(/1 days/);
  });

  it.each(['last Tuesday', '2026-13-45', '2026-02-30', '12/10/2026', '  '])('treats %j as never enriched, as the service does', async (bad) => {
    const r = await status(hubWith({ revops_enrichment_date: bad }));

    expect(r.company).toMatchObject({ lastEnrichedOn: null, daysSince: null, nextEligibleOn: null });
    expect(r.canRequest).toBe(true);
  });

  it('says a run is already requested', async () => {
    const r = await status(hubWith({ request_to_enrich: 'true' }));

    expect(r.company.requested).toBe(true);
    expect(r).toMatchObject({ canRequest: false, reason: 'already_requested' });
    expect(r.message).toMatch(/already requested/i);
  });

  it('puts an already requested run before a recent one', async () => {
    const r = await status(hubWith({ request_to_enrich: 'true', revops_enrichment_date: '2026-10-21' }));

    expect(r.reason).toBe('already_requested');
  });

  it.each(['false', '', null, 'no'])('does not take %j for a request', async (value) => {
    const r = await status(hubWith({ request_to_enrich: value }));

    expect(r.company.requested).toBe(false);
  });

  it.each(['TRUE', 'true ', '1', 'yes'])('takes %j for a request, as the webhook does', async (value) => {
    const r = await status(hubWith({ request_to_enrich: value }));

    expect(r.company.requested).toBe(true);
  });

  it.each([
    [FACILITY, 'facility'],
    [CORPORATION, 'corporation'],
    ['Partner', 'other'],
    [null, 'other'],
  ])('calls a %s company a %s', async (type, kind) => {
    const r = await status(hubWith({ hierarchy_type: type }));

    expect(r.company.kind).toBe(kind);
  });

  it('gives an unnamed company a name to show', async () => {
    const r = await status(hubWith({ name: null }));

    expect(r.company.name).toBe('This company');
  });

  it('prefers the record HubSpot sends over the one the card passed', async () => {
    const hub = hubWith();

    const r = await run(hub, ctx('status', { objectId: '9999', sent: { hs_object_id: COMPANY_ID } }));

    expect(r.ok).toBe(true);
    expect(r.company.id).toBe(COMPANY_ID);
  });
});

// ── Status: the contacts ──────────────────────────────────────────────────────

// ── A company with no usable Company Type ────────────────────────────────────
//
// The service researches a facility's leaders and a corporation's executives differently, so a company that is neither
// (blank, or Partner, Staffing Agency or Other) is asked for its type first. Without that the first run did nothing and
// still stamped the enrichment date, and the rep could not ask again for 30 days.

describe('a company with no usable Company Type', () => {
  it.each([[null], [''], ['   '], ['Partner'], ['Staffing Agency'], ['Staffing Agency2']])('asks for the type before anything else when it is %j', async (type) => {
    const r = await status(hubWith({ hierarchy_type: type }));

    expect(r).toMatchObject({ ok: true, canRequest: false, reason: 'no_company_type', company: { kind: 'other' } });
  });

  it('words a blank type and another type differently', async () => {
    const blank = await status(hubWith({ hierarchy_type: null }));
    const partner = await status(hubWith({ hierarchy_type: 'Partner' }));

    expect(blank.message).toMatch(/no Company Type/);
    expect(blank.message).toMatch(/Facility or Corporation/);
    expect(partner.message).toMatch(/Facility and Corporation companies only/);
    expect(partner.message).not.toMatch(/no Company Type/);
  });

  it('asks for the type even when the company was enriched in the last 30 days', async () => {
    const r = await status(hubWith({ hierarchy_type: null, revops_enrichment_date: ms(2026, 10, 26) }));

    expect(r.reason).toBe('no_company_type');
    expect(r.company).toMatchObject({ lastEnrichedOn: '2026-10-26', nextEligibleOn: '2026-11-25' });
  });

  it('still says a requested run is waiting, whatever the type', async () => {
    const r = await status(hubWith({ hierarchy_type: null, request_to_enrich: 'true' }));

    expect(r).toMatchObject({ canRequest: false, reason: 'already_requested' });
  });

  it.each([[FACILITY], [CORPORATION]])('does not ask a %s company for a type', async (type) => {
    const r = await status(hubWith({ hierarchy_type: type }));

    expect(r).toMatchObject({ canRequest: true, reason: null, message: null });
  });

  it('reads the type the way the service does, ignoring spaces around it', async () => {
    const r = await status(hubWith({ hierarchy_type: ` ${FACILITY} ` }));

    expect(r).toMatchObject({ canRequest: true, reason: null, company: { kind: 'facility' } });
  });

  it('refuses a request without writing anything, and says to set the type', async () => {
    const hub = hubWith({ hierarchy_type: null });

    const r = await request(hub);

    expect(r).toMatchObject({ ok: false, reason: 'no_company_type' });
    expect(r.error).toMatch(/Company Type/);
    expect(hub.writes()).toEqual([]);
  });
});

describe('contacts of a company', () => {
  it('lists them with their email check', async () => {
    const hub = hubWith({}, [
      contact('1', { email_verification_result: 'ok', email_verification_date: '2026-10-21', most_recent_enrichment_date: '2026-10-21' }),
    ]);

    const r = await status(hub);

    expect(r.contacts).toEqual([
      { id: '1', name: 'Pat Lee', title: 'Administrator', email: 'pat.lee@example.org', emailCheck: 'ok', emailCheckedOn: '2026-10-21', enrichedOn: '2026-10-21' },
    ]);
    expect(r.moreContacts).toBe(false);
  });

  it('puts the contacts the service enriched first, newest first, then the rest by name', async () => {
    // HubSpot lists them in no useful order: Baker comes before Adams here, and the older run before the newer.
    const hub = hubWith({}, [
      contact('3', { firstname: 'Ann', lastname: 'Baker' }),
      contact('2', { firstname: 'Old', lastname: 'Run', most_recent_enrichment_date: '2026-09-01' }),
      contact('1', { firstname: 'Zoe', lastname: 'Adams' }),
      contact('4', { firstname: 'New', lastname: 'Run', most_recent_enrichment_date: ms(2026, 10, 21) }),
    ]);

    const r = await status(hub);

    expect(r.contacts.map((c: { id: string }) => c.id)).toEqual(['4', '2', '1', '3']);
    expect(r.contacts.every((c: Record<string, unknown>) => !('sortKey' in c))).toBe(true);
    expect(r.contacts[0].enrichedOn).toBe('2026-10-21');
  });

  it('names a contact from the first and last name, then the email, then says it has no name', async () => {
    const hub = hubWith({}, [
      contact('1', { firstname: 'Pat', lastname: null }),
      contact('2', { firstname: null, lastname: null, email: 'office@example.org' }),
      contact('3', { firstname: null, lastname: null, email: null }),
    ]);

    const r = await status(hub);

    const names = Object.fromEntries(r.contacts.map((c: { id: string; name: string }) => [c.id, c.name]));
    expect(names).toEqual({ '1': 'Pat', '2': 'office@example.org', '3': 'No name' });
  });

  it('gives a blank title or email as null', async () => {
    const hub = hubWith({}, [contact('1', { jobtitle: '  ', email: '' })]);

    const r = await status(hub);

    expect(r.contacts[0]).toMatchObject({ title: null, email: null });
  });

  it('reads at most 100 contacts in one go and says there are more', async () => {
    const many = Array.from({ length: 120 }, (_, i) => contact(String(100 + i), { lastname: `L${String(i).padStart(3, '0')}` }));
    const hub = hubWith({}, many);

    const r = await status(hub);

    expect(r.contacts).toHaveLength(100);
    expect(r.moreContacts).toBe(true);
    const reads = hub.calls.filter((c) => c.path.includes('/associations/contacts'));
    expect(reads).toHaveLength(1);
    expect(reads[0].path).toContain('limit=100');
    expect(hub.calls.filter((c) => c.path.endsWith('/batch/read'))).toHaveLength(1);
  });

  it('shows an empty list for a company with no contacts and asks HubSpot for no contact details', async () => {
    const hub = hubWith();

    const r = await status(hub);

    expect(r.contacts).toEqual([]);
    expect(hub.calls.some((c) => c.path.endsWith('/batch/read'))).toBe(false);
  });

  it('skips a linked contact HubSpot no longer returns', async () => {
    const hub = fakeHubSpot({ companies: [{ ...company(), contacts: ['1', '2'] }], contacts: [contact('2')] });

    const r = await status(hub);

    expect(r.contacts.map((c: { id: string }) => c.id)).toEqual(['2']);
  });

  it('still answers about the company when the contacts cannot be read', async () => {
    const hub = fakeHubSpot({
      companies: [{ ...company({ revops_enrichment_date: '2026-10-21' }), contacts: ['1'] }],
      contacts: [contact('1')],
      fail: (_m, path) => (path.includes('/associations/contacts') ? 403 : null),
    });

    const r = await status(hub);

    expect(r.ok).toBe(true);
    expect(r.company.lastEnrichedOn).toBe('2026-10-21');
    expect(r.contacts).toEqual([]);
    expect(r.contactsError).toMatch(/permission/);
  });

  it('reads only; a status never writes', async () => {
    const hub = hubWith({}, [contact('1')]);

    await status(hub);

    expect(hub.writes()).toEqual([]);
  });
});

// ── Request ───────────────────────────────────────────────────────────────────

describe('requesting enrichment', () => {
  it('sets Request to enrich and changes nothing else', async () => {
    const hub = hubWith();

    const r = await request(hub);

    expect(r.ok).toBe(true);
    expect(hub.writes()).toEqual([{ method: 'PATCH', path: `/crm/v3/objects/companies/${COMPANY_ID}`, body: { properties: { request_to_enrich: 'true' } } }]);
    expect(hub.company(COMPANY_ID).properties).toMatchObject({ request_to_enrich: 'true', name: 'Elm Court Care Center', revops_enrichment_date: null });
  });

  it('answers with the company as requested, without reading its contacts again', async () => {
    const hub = hubWith({}, [contact('1')]);

    const r = await request(hub);

    expect(r).toMatchObject({ ok: true, requested: true, canRequest: false, reason: 'already_requested', company: { id: COMPANY_ID, requested: true } });
    expect(r.contacts).toBeUndefined();
    expect(hub.calls.some((c) => c.path.includes('/associations/'))).toBe(false);
  });

  it('writes nothing when a run is already requested', async () => {
    const hub = hubWith({ request_to_enrich: 'true' });

    const r = await request(hub);

    expect(r).toMatchObject({ ok: false, reason: 'already_requested' });
    expect(r.error).toMatch(/already requested/i);
    expect(hub.writes()).toEqual([]);
  });

  it('writes nothing when the company was enriched in the last 30 days, and says when it can be asked again', async () => {
    const hub = hubWith({ revops_enrichment_date: '2026-10-21' });

    const r = await request(hub);

    expect(r).toMatchObject({ ok: false, reason: 'recently_enriched' });
    expect(r.error).toMatch(/2026-11-20/);
    expect(hub.writes()).toEqual([]);
  });

  it('does not take a name, a value or a property from the card', async () => {
    const hub = hubWith();

    await run(hub, ctx('request', { extra: { property: 'name', value: 'Hijacked', properties: { name: 'Hijacked' } } }));

    expect(hub.writes()).toHaveLength(1);
    expect(hub.writes()[0].body).toEqual({ properties: { request_to_enrich: 'true' } });
  });

  it('says what happened when HubSpot refuses the write', async () => {
    const hub = fakeHubSpot({ companies: [company()], fail: (m) => (m === 'PATCH' ? 403 : null) });

    await expect(request(hub)).rejects.toThrow(/missing a permission/);
  });

  it('says so when the company cannot be found', async () => {
    const hub = fakeHubSpot({ companies: [] });

    await expect(request(hub)).rejects.toThrow(/could not find/);
    expect(hub.writes()).toEqual([]);
  });
});

// ── Refusals before anything is read ──────────────────────────────────────────

describe('what the function refuses', () => {
  it.each(['status', 'request'])('does not run %s on a record that is not a company', async (action) => {
    const hub = hubWith();

    const r = await run(hub, ctx(action, { objectTypeId: '0-1' }));

    expect(r).toEqual({ ok: false, error: 'The Enrichment card works on company records.' });
    expect(hub.calls).toEqual([]);
  });

  it.each(['', 'abc', '12 34', '../5001'])('does not run on the record id %j', async (id) => {
    const hub = hubWith();

    const r = await run(hub, ctx('request', { objectId: id, sent: {} }));

    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/which record/);
    expect(hub.calls).toEqual([]);
  });

  it('does not run an action it does not know', async () => {
    const hub = hubWith();

    const r = await run(hub, ctx('delete'));

    expect(r).toEqual({ ok: false, error: 'The card asked for something this function does not do.' });
    expect(hub.calls).toEqual([]);
  });

  it('works on the company type HubSpot names with its object type id', () => {
    expect(COMPANIES).toBe('0-2');
  });
});

// ── Entry point ───────────────────────────────────────────────────────────────

describe('the entry point', () => {
  const saved = process.env.PRIVATE_APP_ACCESS_TOKEN;
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    if (saved === undefined) delete process.env.PRIVATE_APP_ACCESS_TOKEN;
    else process.env.PRIVATE_APP_ACCESS_TOKEN = saved;
  });

  it('says so when the app has no access token', async () => {
    delete process.env.PRIVATE_APP_ACCESS_TOKEN;

    const r = await fn.main(ctx('status'));

    expect(r).toEqual({ ok: false, error: 'The app has no access token. Reinstall the app.' });
  });

  it('turns a HubSpot error into a message for the card instead of throwing', async () => {
    const hub = fakeHubSpot({ companies: [company()], fail: (m) => (m === 'PATCH' ? 403 : null) });

    const r = await fn._internals.answer(ctx('request'), hub.api, NOW);

    expect(r).toEqual({ ok: false, error: 'The app is missing a permission for setting Request to enrich (403).' });
  });

  it('turns any other error into a plain message and logs it', async () => {
    const broken = { readCompany: async () => { throw new Error('boom'); } };

    const r = await fn._internals.answer(ctx('status'), broken, NOW);

    expect(r).toEqual({ ok: false, error: 'boom' });
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining('enrichment status failed: boom'));
  });

  it('gives a message even when the error has none', async () => {
    const broken = { readCompany: async () => { throw {}; } };

    const r = await fn._internals.answer(ctx('status'), broken, NOW);

    expect(r).toEqual({ ok: false, error: 'Something went wrong.' });
  });
});

// ── The HTTP plumbing ─────────────────────────────────────────────────────────

describe('talking to HubSpot', () => {
  type Reply = { status: number; headers?: Record<string, string>; body?: unknown };
  const transport = (replies: Reply[]) => {
    const seen: { options: Record<string, unknown>; body: string }[] = [];
    let i = 0;
    return {
      seen,
      request(options: Record<string, unknown>, cb: (res: unknown) => void) {
        const reply = replies[Math.min(i++, replies.length - 1)];
        let body = '';
        const handlers: Record<string, ((...a: unknown[]) => void)[]> = {};
        const req = {
          on: (e: string, f: (...a: unknown[]) => void) => ((handlers[e] ??= []).push(f), req),
          write: (b: string) => { body += b; },
          destroy: () => undefined,
          end: () => {
            seen.push({ options, body });
            const res = {
              statusCode: reply.status,
              headers: reply.headers ?? {},
              setEncoding: () => undefined,
              on: (e: string, f: (c?: string) => void) => {
                if (e === 'data') queueMicrotask(() => f(reply.body === undefined ? '' : JSON.stringify(reply.body)));
                if (e === 'end') queueMicrotask(() => queueMicrotask(() => f()));
              },
            };
            cb(res);
          },
        };
        return req;
      },
    };
  };

  it('sends the app token and JSON', async () => {
    const t = transport([{ status: 200, body: { ok: 1 } }]);
    const send = makeRequester('secret-token', { transport: t, sleep: async () => undefined });

    const res = await send('PATCH', '/crm/v3/objects/companies/5', { properties: { a: '1' } });

    expect(res).toMatchObject({ status: 200, json: { ok: 1 } });
    const headers = t.seen[0].options.headers as Record<string, string>;
    expect(headers.Authorization).toBe('Bearer secret-token');
    expect(headers['Content-Type']).toBe('application/json');
    expect(t.seen[0].options).toMatchObject({ hostname: 'api.hubapi.com', method: 'PATCH', path: '/crm/v3/objects/companies/5' });
    expect(JSON.parse(t.seen[0].body)).toEqual({ properties: { a: '1' } });
  });

  it('waits as long as HubSpot asks, up to two seconds, and tries again after a rate limit', async () => {
    const t = transport([{ status: 429, headers: { 'retry-after': '9' } }, { status: 200, body: {} }]);
    const waits: number[] = [];
    const send = makeRequester('t', { transport: t, sleep: async (ms: number) => void waits.push(ms) });

    const res = await send('GET', '/x');

    expect(res.status).toBe(200);
    expect(waits).toEqual([2000]);
  });

  it('retries a server error twice and then returns the last answer', async () => {
    const t = transport([{ status: 502 }]);
    const waits: number[] = [];
    const send = makeRequester('t', { transport: t, sleep: async (ms: number) => void waits.push(ms) });

    const res = await send('GET', '/x');

    expect(res.status).toBe(502);
    expect(t.seen).toHaveLength(3);
    expect(waits).toEqual([500, 1000]);
  });

  it('does not retry a request HubSpot refused', async () => {
    const t = transport([{ status: 403 }]);
    const send = makeRequester('t', { transport: t, sleep: async () => undefined });

    const res = await send('GET', '/x');

    expect(res.status).toBe(403);
    expect(t.seen).toHaveLength(1);
  });

  it.each([
    [401, /refused the app's token/],
    [403, /missing a permission/],
    [404, /could not find/],
    [429, /rate limiting/],
    [500, /returned 500 while reading the company/],
  ])('describes a %i in words a rep can act on', (status, pattern) => {
    expect(describeFailure(status, 'reading the company')).toMatch(pattern);
  });
});
