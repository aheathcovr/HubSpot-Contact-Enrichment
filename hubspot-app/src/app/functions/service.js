'use strict';
// Service app function (Covr, Oct 2026).
//
// The cards call this to reach the enrichment service. They cannot call it with hubspot.fetch: a static-auth app has no
// client secret, so HubSpot has nothing to sign a fetch with, and the service would have no way to know who asked.
// So a card calls this function, with { method, path, body } for one of the seven requests the cards make, and this
// function calls the service.
//
// Who is asking is only ever HubSpot's word for it. HubSpot puts the user and the portal in the function's context on
// its own server, and nothing a card sends can override them. They are added to the query (userId, userEmail,
// portalId) and the whole request (method, address, body, time) is signed with a secret shared with the service, the
// way HubSpot's v3 signature does it, so the service can tell this function's requests from anyone else's and cannot
// be told a different rep asked. The service answers 503 until it has the secret, and so does this function.
//
// The secret is APP_REQUEST_SECRET: the same string as the service's, added to HubSpot with `hs secret add` and
// listed in service-hsmeta.json. It is never returned, logged or put in a request. The service's own tests pin the
// signatures this function must make (test_app_request_auth.py), and the tests here pin the same values.
// No dependencies: it calls the service with Node's https module.

const https = require('https');
const crypto = require('crypto');

// The service, and where the app's requests are served. The cards' addresses are relative to APP_PREFIX.
const SERVICE_BASE = 'https://hubspot-enrichment-kpqza7tiga-uc.a.run.app';
const APP_PREFIX = '/app';
// The two headers the service reads the signature from (app_request_auth.py: SIGNATURE_HEADER, TIMESTAMP_HEADER).
// test_hubspot_app_contract.py, in the service's tests, fails if these drift apart.
const SIGNATURE_HEADER = 'X-Covr-App-Signature';
const TIMESTAMP_HEADER = 'X-Covr-App-Timestamp';

// A function has 15 seconds. The service answers a start or a status at once, so this is generous.
const TIMEOUT_MS = 12000;
const MAX_BODY_CHARS = 100000;
const MAX_VALUE_CHARS = 64;

// The only requests that go through, and the query keys each takes. These are the cards' requests and the service's
// /app routes (test_hubspot_app_contract.py fails if they drift apart). Anything else is refused, so this is not a way
// to reach the rest of the service, and who is asking can never come in through the query.
const ROUTES = [
  { method: 'GET', path: '/contact/status', query: ['contactId'] },
  { method: 'POST', path: '/contact/run', query: [] },
  { method: 'GET', path: '/company/status', query: ['companyId'] },
  { method: 'POST', path: '/company/run', query: [] },
  { method: 'POST', path: '/company/find', query: [] },
  { method: 'GET', path: '/company/candidates', query: ['companyId', 'runId'] },
  { method: 'POST', path: '/company/add', query: [] },
];

// HubSpot's published list of what is decoded in the address before it is signed, in the order the service does it.
const DECODED = [
  ['%3A', ':'], ['%2F', '/'], ['%3F', '?'], ['%40', '@'], ['%21', '!'], ['%24', '$'],
  ['%27', "'"], ['%28', '('], ['%29', ')'], ['%2A', '*'], ['%2C', ','], ['%3B', ';'],
];

const reply = (status, error) => ({ status, json: { ok: false, error } });
const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

// ---------- the signature ----------

function decodeSignedUri(url) {
  return DECODED.reduce((text, [encoded, plain]) => text.split(encoded).join(plain), url);
}

// base64 HMAC-SHA256, keyed with the secret, of method + address + body + timestamp.
function sign(secret, method, url, body, timestamp) {
  const message = method.toUpperCase() + decodeSignedUri(url) + body + timestamp;
  return crypto.createHmac('sha256', secret).update(message, 'utf8').digest('base64');
}

// ---------- which requests go through ----------

const REFUSED = 'That is not a request the app makes.';

// { ok: true, method, path, query: [[key, value], ...], body } for a request a card may make, else { ok: false, error }.
function parseRequest(parameters) {
  const refuse = () => ({ ok: false, error: REFUSED });
  if (!isObject(parameters)) return refuse();
  const { method, path, body } = parameters;
  if (typeof method !== 'string' || typeof path !== 'string' || !path.startsWith('/')) return refuse();

  const mark = path.indexOf('?');
  const pathname = mark === -1 ? path : path.slice(0, mark);
  const route = ROUTES.find((r) => r.method === method && r.path === pathname);
  if (!route) return refuse();

  const query = [];
  const text = mark === -1 ? '' : path.slice(mark + 1);
  for (const pair of text === '' ? [] : text.split('&')) {
    const eq = pair.indexOf('=');
    if (eq === -1) return refuse();
    let key;
    let value;
    try {
      key = decodeURIComponent(pair.slice(0, eq));
      value = decodeURIComponent(pair.slice(eq + 1));
    } catch (e) {
      return refuse();
    }
    // A key the route does not take, a key twice, or a long value is refused. That also keeps who is asking out of the
    // query: userId, userEmail, portalId and appId are never keys a card may send.
    if (!route.query.includes(key) || query.some(([k]) => k === key) || value.length > MAX_VALUE_CHARS) return refuse();
    query.push([key, value]);
  }

  if (route.method === 'GET') {
    if (body !== undefined && body !== null) return refuse();
    return { ok: true, method: route.method, path: route.path, query, body: undefined };
  }
  if (!isObject(body) || JSON.stringify(body).length > MAX_BODY_CHARS) return refuse();
  return { ok: true, method: route.method, path: route.path, query, body };
}

// ---------- who is asking ----------

const wholeNumber = (value) => {
  if (typeof value === 'number') return Number.isInteger(value) && value > 0 ? String(value) : '';
  return typeof value === 'string' && /^[1-9]\d{0,19}$/.test(value) ? value : '';
};

// { userId, email, portalId } from HubSpot's context, or null when HubSpot did not say who is asking (as in local
// development, where there is no user). The email can be missing even for a real user, so it is only added when given.
function identityOf(context) {
  const user = context && context.user;
  const userId = wholeNumber(user && user.userId);
  if (!userId) return null;
  const email = user && typeof user.email === 'string' ? user.email.trim() : '';
  return { userId, email, portalId: wholeNumber(context.portalId) };
}

// ---------- the request that is sent ----------

// The signed request for the service: its address (with who is asking added to the query), method, body and headers.
function buildCall(who, request, secret, nowMs) {
  const pairs = [...request.query, ['userId', who.userId]];
  if (who.email) pairs.push(['userEmail', who.email]);
  if (who.portalId) pairs.push(['portalId', who.portalId]);
  const url = `${SERVICE_BASE}${APP_PREFIX}${request.path}?${pairs.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&')}`;
  const body = request.method === 'POST' ? JSON.stringify(request.body) : '';
  const timestamp = String(nowMs);
  return {
    method: request.method,
    url,
    body,
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      [SIGNATURE_HEADER]: sign(secret, request.method, url, body, timestamp),
      [TIMESTAMP_HEADER]: timestamp,
    },
  };
}

// Returns send(call) -> { status, json }. Rejects with timedOut set when the service does not answer in time.
function makeSender(opts = {}) {
  const transport = opts.transport || https;
  const timeout = opts.timeout || TIMEOUT_MS;
  return (call) =>
    new Promise((resolve, reject) => {
      const target = new URL(call.url);
      const headers = { ...call.headers };
      if (call.body) headers['Content-Length'] = Buffer.byteLength(call.body);
      const req = transport.request(
        { hostname: target.hostname, port: target.port || undefined, path: target.pathname + target.search, method: call.method, headers, timeout },
        (res) => {
          let raw = '';
          res.setEncoding('utf8');
          res.on('data', (chunk) => { raw += chunk; });
          res.on('end', () => {
            let json = null;
            try { json = raw ? JSON.parse(raw) : null; } catch (e) { json = null; }
            resolve({ status: res.statusCode, json });
          });
        },
      );
      req.on('timeout', () => req.destroy(Object.assign(new Error('The service did not answer in time.'), { timedOut: true })));
      req.on('error', reject);
      if (call.body) req.write(call.body);
      req.end();
    });
}

// ---------- the function ----------

// Every answer is { status, json }, the service's own or this function's refusal in the same shape, so a card reads
// them the same way. The function never throws for anything that goes wrong on the way.
async function answer(context, deps) {
  const { secret, send, now } = deps;
  if (!secret) return reply(503, 'The app is not set up to reach the enrichment service. Ask an admin.');
  const who = identityOf(context);
  if (!who) return reply(401, 'HubSpot did not say who is asking, so the request was not sent.');
  const request = parseRequest(context.parameters);
  if (!request.ok) return reply(400, request.error);

  const call = buildCall(who, request, secret, now());
  try {
    return await send(call);
  } catch (err) {
    // Only what kind of failure, and which route: not the address (it carries the rep's email), the error text, or the secret.
    const timedOut = Boolean(err && err.timedOut);
    console.error(`service ${request.method} ${request.path} ${timedOut ? 'timed out' : 'could not be reached'}`);
    return timedOut
      ? reply(504, 'The enrichment service did not answer in time. Try again.')
      : reply(502, 'The enrichment service could not be reached. Try again in a moment.');
  }
}

exports.main = async (context = {}) => answer(context, { secret: process.env.APP_REQUEST_SECRET, send: makeSender(), now: Date.now });

exports._internals = {
  SERVICE_BASE, APP_PREFIX, ROUTES, SIGNATURE_HEADER, TIMESTAMP_HEADER, TIMEOUT_MS,
  decodeSignedUri, sign, parseRequest, identityOf, buildCall, makeSender, answer,
};
