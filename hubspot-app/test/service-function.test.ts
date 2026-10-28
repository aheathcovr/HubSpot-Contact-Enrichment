// The app function that carries a card's request to the enrichment service.
//
// A static-auth app has no client secret, so HubSpot cannot sign a card's fetch. The card calls this function instead. It
// takes who is asking from HubSpot's context (the user and the portal, which card parameters cannot override), adds
// them to the query, signs the request with a secret it shares with the service, and forwards it. These tests pin what
// matters: only the seven requests the cards make go through, who is asking is only ever HubSpot's word for it, the
// signature is the one the service checks (golden values the service's own tests pin too), and the secret never leaves.
import { createServer, type IncomingMessage, type Server } from 'node:http';
import http from 'node:http';
import { createRequire } from 'node:module';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-require-imports
const fn = require('../src/app/functions/service.js');
const { SERVICE_BASE, ROUTES, SIGNATURE_HEADER, TIMESTAMP_HEADER, TIMEOUT_MS, decodeSignedUri, sign, parseRequest, buildCall, answer, makeSender } = fn._internals;

const SECRET = 'test-app-secret';
const NOW = 1700000000000;
const APP = 'https://hubspot-enrichment-kpqza7tiga-uc.a.run.app/app';

interface Sent {
  method: string;
  url: string;
  headers: Record<string, string>;
  body: string;
}

// A context as HubSpot gives it to a function a card called.
const ctx = (parameters: unknown, over: Record<string, unknown> = {}) => ({
  portalId: 1234,
  user: { userId: 77, email: 'rep@example.org', locale: 'en' },
  parameters,
  ...over,
});

const status = (path = '/contact/status?contactId=42') => ({ method: 'GET', path });
const post = (path: string, body: unknown = {}) => ({ method: 'POST', path, body });

// What the function would send, for a test to read; the service's answer is whatever the test sets.
function recorder(reply: { status: number; json: unknown } = { status: 200, json: { ok: true } }) {
  const sent: Sent[] = [];
  const send = async (call: Sent) => {
    sent.push(call);
    return reply;
  };
  return { sent, send };
}

const run = (context: unknown, deps: Record<string, unknown> = {}) => {
  const r = recorder();
  return answer(context, { secret: SECRET, now: () => NOW, send: r.send, ...deps }).then((result: unknown) => ({ result, sent: r.sent }));
};

// ── The signature ─────────────────────────────────────────────────────────────

describe('what is signed', () => {
  it('decodes the characters HubSpot lists, and only those, before signing the address', () => {
    expect(decodeSignedUri('https://x.example/p?a=%3A%2F%3F%40%21%24%27%28%29%2A%2C%3B&b=%253A%2520%2B')).toBe(
      "https://x.example/p?a=:/?@!$'()*,;&b=%253A%2520%2B",
    );
  });

  it("makes HubSpot's published example signature", () => {
    const body =
      '[{"eventId":531833541,"subscriptionId":3923621,"portalId":48807704,"appId":16111050,' +
      '"occurredAt":1752613920733,"subscriptionType":"contact.creation","attemptNumber":0,"objectId":138017612137,' +
      '"changeFlag":"CREATED","changeSource":"CRM_UI","sourceId":"userId:76023669"}]';

    expect(sign('cfc68c0b-4b4e-4ef8-b764-95350e4ea479', 'POST', 'https://webhook.site/335453f5-94b3-49d9-b684-a55354d4b8df', body, '1752613922216')).toBe(
      'gbj1XPRvUt0noT7i7fXfTzOD4sLzQmf0VT28ZYq0EYg=',
    );
  });

  // The same values test_app_request_auth.py pins, made by the service's own implementation.
  it.each([
    ['GET', `${APP}/contact/status?contactId=42&userId=77&userEmail=rep%40example.org&portalId=1234`, '', '1700000000000', 'QIOhblxs8Xd/ytWw1n9ZS2KIAr8PwE2xHYCdCZs5afI='],
    ['POST', `${APP}/contact/run?userId=77&userEmail=o%27neil%2Brep%40example.org&portalId=1234`, '{"contactId":"42","objective":"find_email"}', '1700000000001', 'tEegZgpE9Gm7X4vCloAlSJPNe/eXBE7nf1ZCaBOl3D0='],
    ['GET', `${APP}/company/status?a=%3A%2F%3F%40%21%24%27%28%29%2A%2C%3B&b=%253A%2520&companyId=500`, '', '1700000000002', 'Vk764+fKfD1RFSlVu2LB8WX7UWQr4VVExvsqebu8vA4='],
    ['POST', `${APP}/company/find?userId=77&portalId=1234`, '{"companyId":"500","titles":["Médical Director"]}', '1700000000003', 'jz1lr+DVn8/WZF6UYsQDVtMysojZWSxO9UH6wQlnmkQ='],
  ])('makes the signature the service expects for %s %s', (method, url, body, timestamp, expected) => {
    expect(sign(SECRET, method, url, body, timestamp)).toBe(expected);
  });

  it('names the two headers as the service reads them', () => {
    expect([SIGNATURE_HEADER, TIMESTAMP_HEADER]).toEqual(['X-Covr-App-Signature', 'X-Covr-App-Timestamp']);
  });
});

// ── Which requests go through ─────────────────────────────────────────────────

describe('the requests a card may make', () => {
  it('lists exactly the seven the cards make', () => {
    expect(ROUTES.map((r: { method: string; path: string }) => `${r.method} ${r.path}`)).toEqual([
      'GET /contact/status', 'POST /contact/run', 'GET /company/status', 'POST /company/run', 'POST /company/find', 'GET /company/candidates', 'POST /company/add',
    ]);
  });

  it.each([
    [status('/contact/status?contactId=42'), 'GET', '/contact/status', [['contactId', '42']]],
    [status('/company/status?companyId=500'), 'GET', '/company/status', [['companyId', '500']]],
    [status(`/company/candidates?companyId=500&runId=${'f'.repeat(32)}`), 'GET', '/company/candidates', [['companyId', '500'], ['runId', 'f'.repeat(32)]]],
    [post('/contact/run', { contactId: '42', objective: 'find_email' }), 'POST', '/contact/run', []],
    [post('/company/run', { companyId: '500', contactIds: ['1'], objective: 'enrich' }), 'POST', '/company/run', []],
    [post('/company/find', { companyId: '500', titles: ['Medical Director'] }), 'POST', '/company/find', []],
    [post('/company/add', { companyId: '500', runId: 'f'.repeat(32), candidateIds: ['aaaaaaaaaaaa'] }), 'POST', '/company/add', []],
  ])('accepts %j', (parameters, method, path, query) => {
    const parsed = parseRequest(parameters);

    expect(parsed).toMatchObject({ ok: true, method, path, query });
  });

  it.each([
    ['nothing at all', undefined],
    ['text', 'GET /contact/status'],
    ['a list', ['GET', '/contact/status']],
    ['no method', { path: '/contact/status?contactId=1' }],
    ['a method that is not allowed', { method: 'DELETE', path: '/contact/status?contactId=1' }],
    ['a lower case method', { method: 'get', path: '/contact/status?contactId=1' }],
    ['no path', { method: 'GET' }],
    ['a path that is not text', { method: 'GET', path: 5 }],
    ['a path with no leading slash', { method: 'GET', path: 'contact/status?contactId=1' }],
    ['another host', { method: 'GET', path: 'https://evil.example/contact/status?contactId=1' }],
    ['a double slash', { method: 'GET', path: '//evil.example/contact/status?contactId=1' }],
    ['a path the cards do not use', { method: 'GET', path: '/run?contactId=1' }],
    ['the work endpoint', { method: 'POST', path: '/run', body: { run_id: 'a'.repeat(32) } }],
    ['a path that climbs out', { method: 'GET', path: '/contact/status/../../run?contactId=1' }],
    ['an encoded path', { method: 'GET', path: '/contact%2Fstatus?contactId=1' }],
    ['a method that does not match the path', { method: 'POST', path: '/contact/status?contactId=1', body: {} }],
    ['a post sent as a get', { method: 'GET', path: '/contact/run' }],
  ])('refuses %s', (_why, parameters) => {
    expect(parseRequest(parameters)).toMatchObject({ ok: false });
  });

  it.each([
    ['a query key the route does not take', '/contact/status?contactId=1&extra=2'],
    ['a key of a different route', '/contact/status?companyId=1'],
    ['a key twice', '/contact/status?contactId=1&contactId=2'],
    ['a value that is too long', `/contact/status?contactId=${'1'.repeat(65)}`],
    ['who is asking, in any case', '/contact/status?contactId=1&userId=2'],
    ['who is asking, in another case', '/contact/status?contactId=1&USERID=2'],
    ['an email, as who is asking', '/contact/status?contactId=1&userEmail=boss%40example.org'],
    ['a portal', '/contact/status?contactId=1&portalId=9'],
    ['an app', '/contact/status?contactId=1&appId=9'],
    ['a key with no value or equals sign', '/contact/status?contactId'],
    ['a key with no equals sign that ends one character past a real key', '/contact/status?contactIdX'],
    ['a key that is only an equals sign', '/contact/status?=1'],
    ['an escape that is not valid', '/contact/status?contactId=%E0%A4%A'],
  ])('refuses a get with %s', (_why, path) => {
    expect(parseRequest({ method: 'GET', path })).toMatchObject({ ok: false });
  });

  it('refuses a body on a get, and a post whose body is not an object', () => {
    expect(parseRequest({ method: 'GET', path: '/contact/status?contactId=1', body: {} })).toMatchObject({ ok: false });
    for (const body of [undefined, null, 'x', 7, ['a']]) {
      expect(parseRequest({ method: 'POST', path: '/contact/run', body })).toMatchObject({ ok: false });
    }
  });

  it('refuses a body that is too large', () => {
    expect(parseRequest(post('/company/find', { companyId: '500', titles: ['x'.repeat(200_000)] }))).toMatchObject({ ok: false });
  });

  it('says why in words a rep can read, never with what was sent', () => {
    const refused = parseRequest({ method: 'GET', path: '/secret-thing?token=abc123' });

    expect(refused.ok).toBe(false);
    expect(refused.error).toBe('That is not a request the app makes.');
  });
});

// ── What is sent ──────────────────────────────────────────────────────────────

describe('the request that is sent', () => {
  it('goes to the service under /app, signed, with who is asking added to the query from HubSpot', async () => {
    const { result, sent } = await run(ctx(status()));

    expect(result).toEqual({ status: 200, json: { ok: true } });
    expect(sent).toHaveLength(1);
    expect(sent[0].method).toBe('GET');
    expect(sent[0].url).toBe(`${APP}/contact/status?contactId=42&userId=77&userEmail=rep%40example.org&portalId=1234`);
    expect(sent[0].body).toBe('');
    expect(sent[0].headers[TIMESTAMP_HEADER]).toBe(String(NOW));
    expect(sent[0].headers[SIGNATURE_HEADER]).toBe('QIOhblxs8Xd/ytWw1n9ZS2KIAr8PwE2xHYCdCZs5afI=');
  });

  it('sends a post body as JSON and signs it', async () => {
    const body = { contactId: '42', objective: 'find_email' };

    const { sent } = await run(ctx(post('/contact/run', body)), { now: () => 1700000000001 });

    expect(sent[0].method).toBe('POST');
    expect(sent[0].body).toBe('{"contactId":"42","objective":"find_email"}');
    expect(sent[0].url).toBe(`${APP}/contact/run?userId=77&userEmail=rep%40example.org&portalId=1234`);
    expect(sent[0].headers['Content-Type']).toBe('application/json');
    expect(sent[0].headers[SIGNATURE_HEADER]).toBe(sign(SECRET, 'POST', sent[0].url, sent[0].body, '1700000000001'));
  });

  it('leaves the email out when HubSpot did not give one, and escapes it when it did', async () => {
    const without = await run(ctx(status(), { user: { userId: 77 } }));
    const odd = await run(ctx(status(), { user: { userId: 77, email: "o'neil+rep@example.org" } }));

    expect(without.sent[0].url).toBe(`${APP}/contact/status?contactId=42&userId=77&portalId=1234`);
    expect(odd.sent[0].url).toContain("userEmail=o'neil%2Brep%40example.org");
  });

  it('takes who is asking from the context and never from what the card sent', async () => {
    const sneaky = { ...status('/contact/status?contactId=42'), userId: 1, user: { userId: 1, email: 'boss@example.org' }, portalId: 9, headers: { 'X-Covr-App-Signature': 'x' } };

    const { sent } = await run(ctx(sneaky));

    expect(sent[0].url).toBe(`${APP}/contact/status?contactId=42&userId=77&userEmail=rep%40example.org&portalId=1234`);
    expect(sent[0].headers[SIGNATURE_HEADER]).not.toBe('x');
  });

  it.each([
    ['zero', 0],
    ['the text zero', '0'],
    ['empty text', ''],
    ['text with a leading zero', '012'],
    ['text that is too long', '1'.repeat(21)],
    ['text with a sign', '+5'],
    ['text with a space', ' 5'],
    ['not a number', null],
    ['an object', {}],
  ])('does not take %s for the user', async (_why, userId) => {
    const { result, sent } = await run(ctx(status(), { user: { userId, email: 'rep@example.org' } }));

    expect(result).toMatchObject({ status: 401 });
    expect(sent).toEqual([]);
  });

  it('takes a user id of one digit and of twenty', async () => {
    for (const userId of ['1', '9'.repeat(20)]) {
      const { sent } = await run(ctx(status(), { user: { userId } }));
      expect(sent[0].url).toContain(`userId=${userId}&portalId=1234`);
    }
  });

  it('trims the email, and leaves it out when it is blank or not text', async () => {
    const trimmed = await run(ctx(status(), { user: { userId: 77, email: '  rep@example.org ' } }));
    expect(trimmed.sent[0].url).toContain('userEmail=rep%40example.org&');

    for (const email of ['   ', '', 5, {}, ['rep@example.org'], null]) {
      const { sent } = await run(ctx(status(), { user: { userId: 77, email } }));
      expect(sent[0].url).toBe(`${APP}/contact/status?contactId=42&userId=77&portalId=1234`);
    }
  });

  it('leaves the portal out when HubSpot did not give one, or gave something that is not a whole number', async () => {
    for (const portalId of [undefined, null, 0, 'abc', 12.5, '']) {
      const { sent } = await run(ctx(status(), { portalId }));
      expect(sent[0].url).toBe(`${APP}/contact/status?contactId=42&userId=77&userEmail=rep%40example.org`);
    }
  });

  it('sends exactly the headers the service reads and a JSON content type, and nothing else', async () => {
    const { sent } = await run(ctx(status()));

    expect(Object.keys(sent[0].headers).sort()).toEqual(['Accept', 'Content-Type', SIGNATURE_HEADER, TIMESTAMP_HEADER].sort());
    expect(sent[0].headers.Accept).toBe('application/json');
  });

  it('sends the digits of the user and portal whether HubSpot gives numbers or text', async () => {
    const { sent } = await run(ctx(status(), { portalId: '1234', user: { userId: '77', email: 'rep@example.org' } }));

    expect(sent[0].url).toContain('userId=77&userEmail=rep%40example.org&portalId=1234');
  });

  it('signs again for each request, with the time of that request', async () => {
    const a = await run(ctx(status()), { now: () => 1700000000000 });
    const b = await run(ctx(status()), { now: () => 1700000000999 });

    expect(a.sent[0].headers[SIGNATURE_HEADER]).not.toBe(b.sent[0].headers[SIGNATURE_HEADER]);
  });

  it('is sent to the service address and nowhere else', () => {
    expect(SERVICE_BASE).toBe('https://hubspot-enrichment-kpqza7tiga-uc.a.run.app');
    for (const route of ROUTES) {
      const call = buildCall({ userId: '77', portalId: '1234' }, { ok: true, method: route.method, path: route.path, query: [], body: route.method === 'POST' ? {} : undefined }, SECRET, NOW);
      expect(call.url.startsWith(`${SERVICE_BASE}/app/`)).toBe(true);
    }
  });
});

// ── When it cannot go ahead ───────────────────────────────────────────────────

describe('when the request is not sent', () => {
  it('says the app is not set up, and sends nothing, when the secret is missing', async () => {
    for (const secret of [undefined, '']) {
      const { result, sent } = await run(ctx(status()), { secret });

      expect(result).toEqual({ status: 503, json: { ok: false, error: 'The app is not set up to reach the enrichment service. Ask an admin.' } });
      expect(sent).toEqual([]);
    }
  });

  it.each([
    ['no user', { user: undefined }],
    ['a user with no id', { user: { email: 'rep@example.org' } }],
    ['a user id that is not a whole number', { user: { userId: 'abc' } }],
    ['a negative user id', { user: { userId: -1 } }],
    ['a user id that is a decimal', { user: { userId: 7.5 } }],
  ])('does not send, and says HubSpot did not say who is asking, with %s', async (_why, over) => {
    const { result, sent } = await run(ctx(status(), over));

    expect(result).toEqual({ status: 401, json: { ok: false, error: 'HubSpot did not say who is asking, so the request was not sent.' } });
    expect(sent).toEqual([]);
  });

  it('does not send a request it refuses, and says it is a bad request', async () => {
    const { result, sent } = await run(ctx({ method: 'GET', path: '/run' }));

    expect(result).toEqual({ status: 400, json: { ok: false, error: 'That is not a request the app makes.' } });
    expect(sent).toEqual([]);
  });

  it('hands the service its refusal back as it came, and its status', async () => {
    const reply = { status: 429, json: { ok: false, error: 'You have used all 25 runs for today.', quota: { cap: 25, used: 25, remaining: 0 } } };

    const { result } = await run(ctx(post('/contact/run', {})), { send: async () => reply });

    expect(result).toEqual(reply);
  });

  it('gives a null body when the service answered with something that is not JSON', async () => {
    const { result } = await run(ctx(status()), { send: async () => ({ status: 502, json: null }) });

    expect(result).toEqual({ status: 502, json: null });
  });

  it('says the service did not answer in time, and says it could not be reached, in words', async () => {
    const slow = Object.assign(new Error('socket hang up'), { timedOut: true });

    const timedOut = await run(ctx(status()), { send: async () => { throw slow; } });
    const down = await run(ctx(status()), { send: async () => { throw new Error('connect ECONNREFUSED 10.0.0.1:443'); } });

    expect(timedOut.result).toEqual({ status: 504, json: { ok: false, error: 'The enrichment service did not answer in time. Try again.' } });
    expect(down.result).toEqual({ status: 502, json: { ok: false, error: 'The enrichment service could not be reached. Try again in a moment.' } });
  });

  it('waits for the service for less than the 15 seconds a function has', () => {
    expect(TIMEOUT_MS).toBeGreaterThan(0);
    expect(TIMEOUT_MS).toBeLessThan(15000);
  });

  it('logs only which route failed and how: not the address, which carries the rep\'s email', async () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});

    await run(ctx(status()), { send: async () => { throw new Error('boom'); } });

    expect(logged.mock.calls).toEqual([['service GET /contact/status could not be reached']]);
    logged.mockRestore();
  });

  it('never returns or logs the secret, the signature or what went wrong inside', async () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const boom = new Error(`failed with ${SECRET} and 10.0.0.1`);

    const { result } = await run(ctx(status()), { send: async () => { throw boom; } });

    expect(JSON.stringify(result)).not.toContain(SECRET);
    expect(JSON.stringify(logged.mock.calls)).not.toContain(SECRET);
    logged.mockRestore();
  });
});

// ── The real thing ────────────────────────────────────────────────────────────

describe('main', () => {
  const saved = process.env.APP_REQUEST_SECRET;
  afterEach(() => {
    if (saved === undefined) delete process.env.APP_REQUEST_SECRET;
    else process.env.APP_REQUEST_SECRET = saved;
  });

  it('reads the secret HubSpot injects and refuses to run without it', async () => {
    delete process.env.APP_REQUEST_SECRET;

    expect(await fn.main(ctx(status()))).toMatchObject({ status: 503 });
  });

  it('is told who is asking by HubSpot, so in local development, where there is no user, it does not send', async () => {
    process.env.APP_REQUEST_SECRET = SECRET;

    expect(await fn.main(ctx(status(), { user: undefined }))).toMatchObject({ status: 401 });
  });
});

describe('the sender', () => {
  let server: Server | null = null;
  afterEach(() => {
    server?.close();
    server = null;
  });

  const listen = (handler: (req: IncomingMessage, body: string, respond: (status: number, text: string) => void) => void) =>
    new Promise<number>((resolve) => {
      server = createServer((req, res) => {
        let body = '';
        req.on('data', (c) => { body += c; });
        req.on('end', () => handler(req, body, (status, text) => { res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.end(text); }));
      }).listen(0, '127.0.0.1', () => resolve((server!.address() as AddressInfo).port));
    });

  const call = (port: number, over: Partial<Sent> = {}): Sent => ({
    method: 'POST', url: `http://127.0.0.1:${port}/app/contact/run?userId=77`, headers: { 'Content-Type': 'application/json', [SIGNATURE_HEADER]: 'sig', [TIMESTAMP_HEADER]: '1' }, body: '{"a":1}', ...over,
  });

  it('sends the method, address, headers and body, and returns the status and the JSON', async () => {
    let seen: { method?: string; url?: string; headers?: http.IncomingHttpHeaders; body?: string } = {};
    const port = await listen((req, body, respond) => {
      seen = { method: req.method, url: req.url, headers: req.headers, body };
      respond(202, '{"ok":true,"run":{"runId":"r1"}}');
    });
    const send = makeSender({ transport: http, timeout: 2000 });

    const result = await send(call(port));

    expect(result).toEqual({ status: 202, json: { ok: true, run: { runId: 'r1' } } });
    expect(seen.method).toBe('POST');
    expect(seen.url).toBe('/app/contact/run?userId=77');
    expect(seen.body).toBe('{"a":1}');
    expect(seen.headers?.['x-covr-app-signature']).toBe('sig');
    expect(seen.headers?.['x-covr-app-timestamp']).toBe('1');
    expect(seen.headers?.['content-length']).toBe('7');
  });

  it('sends a get with no body and no content length', async () => {
    let headers: http.IncomingHttpHeaders = {};
    const port = await listen((req, _body, respond) => { headers = req.headers; respond(200, '{"ok":true}'); });
    const send = makeSender({ transport: http, timeout: 2000 });

    await send(call(port, { method: 'GET', body: '' }));

    expect(headers['content-length']).toBeUndefined();
  });

  it('returns a null body for an answer that is not JSON, and for an empty one', async () => {
    const port = await listen((_req, _body, respond) => respond(502, '<html>Bad gateway</html>'));
    const send = makeSender({ transport: http, timeout: 2000 });

    expect(await send(call(port))).toEqual({ status: 502, json: null });
  });

  it('rejects, marked as a timeout, when the service does not answer in time', async () => {
    const port = await listen(() => {});
    const send = makeSender({ transport: http, timeout: 50 });

    await expect(send(call(port))).rejects.toMatchObject({ timedOut: true });
  });

  it('rejects, not marked as a timeout, when the service cannot be reached', async () => {
    const port = await listen((_req, _body, respond) => respond(200, '{}'));
    server!.close();
    const send = makeSender({ transport: http, timeout: 500 });

    const failure = await send(call(port)).catch((e: Error & { timedOut?: boolean }) => e);

    expect(failure).toBeInstanceOf(Error);
    expect((failure as { timedOut?: boolean }).timedOut).toBeFalsy();
  });
});
