'use strict';
// Enrichment app function (Covr, Oct 2026).
//
// The Enrichment card on a company record calls this:
//   { action: 'status' }    where the company stands: when it was last enriched, whether a run is already requested,
//                           and its contacts with each email's check result
//   { action: 'request' }   ask for an enrichment run
//
// A run is requested by setting Request to enrich on the company. HubSpot then calls the enrichment service's
// webhook (webhook_server.py reacts to a change of this property to a true value) and the service queues the run.
// Nothing here talks to the service, so the service did not change for this app.
//
// The function runs on HubSpot's servers with the app's own token. It reads the company, the contacts linked to it,
// and changes one thing: Request to enrich on that one company, and only when the service would not skip the run.
// The service skips a company enriched in the last 30 days (enrichment_processor.py), so asking would do nothing.
// No dependencies: it calls the HubSpot API with Node's https module.

const https = require('https');

// HubSpot's object type id for companies.
const COMPANY_TYPE_ID = '0-2';

// The property and values the service's webhook reacts to (webhook_server.py: TRIGGER_PROPERTY, TRIGGER_VALUES).
// test_hubspot_app_contract.py, in the service's tests, fails if these drift apart.
const TRIGGER_PROPERTY = 'request_to_enrich';
const TRIGGER_VALUE = 'true';
const TRIGGER_VALUES = ['true', '1', 'yes'];
// The date the service stamps when a run finishes, and the days it then leaves a company alone.
const LAST_ENRICHED_PROPERTY = 'revops_enrichment_date';
const REFRESH_DAYS = 30;
const DAY_MS = 86400000;

// Company Type (hierarchy_type) values the service tells apart.
const FACILITY_TYPE = 'Individual Facility / Child';
const CORPORATION_TYPE = 'Corporation / Operator';

const COMPANY_PROPERTIES = ['name', 'hierarchy_type', TRIGGER_PROPERTY, LAST_ENRICHED_PROPERTY];
const CONTACT_PROPERTIES = ['firstname', 'lastname', 'jobtitle', 'email', 'email_verification_result', 'email_verification_date', 'most_recent_enrichment_date'];
// Contacts read for one company. A larger company shows its first 100 and says there are more.
const MAX_CONTACTS = 100;
// HubSpot's batch read takes at most 100 records.
const BATCH = 100;

const fail = (error, extra = {}) => ({ ok: false, error, ...extra });

// ---------- dates ----------

// A HubSpot date as epoch milliseconds, read the way the service reads it: digits are epoch milliseconds, anything
// else is its first ten characters as YYYY-MM-DD. What cannot be read is null, and the service then goes ahead as if
// the company had never been enriched.
function parseDate(raw) {
  const text = String(raw === null || raw === undefined ? '' : raw).trim();
  if (!text) return null;
  if (/^\d+$/.test(text)) return Number(text);
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(text);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const t = Date.UTC(y, mo - 1, d);
  const back = new Date(t);
  // Date.UTC rolls 2026-02-30 over to March; a date that does not exist is not a date.
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return t;
}

const toDay = (ms) => new Date(ms).toISOString().slice(0, 10);
const dayOf = (raw) => {
  const ms = parseDate(raw);
  return ms === null ? null : toDay(ms);
};

function ago(daysSince, on) {
  if (daysSince < 0) return `on ${on}`;
  if (daysSince === 0) return 'today';
  return daysSince === 1 ? '1 day ago' : `${daysSince} days ago`;
}

// ---------- the company ----------

const isRequested = (value) => TRIGGER_VALUES.includes(String(value === null || value === undefined ? '' : value).trim().toLowerCase());

function kindOf(type) {
  if (type === FACILITY_TYPE) return 'facility';
  if (type === CORPORATION_TYPE) return 'corporation';
  return 'other';
}

// Where a company stands, and whether a request would be skipped, and why.
function summarize(id, props, now) {
  const requested = isRequested(props[TRIGGER_PROPERTY]);
  const last = parseDate(props[LAST_ENRICHED_PROPERTY]);
  const daysSince = last === null ? null : Math.floor((now - last) / DAY_MS);
  const lastEnrichedOn = last === null ? null : toDay(last);
  const nextEligibleOn = last === null ? null : toDay(last + REFRESH_DAYS * DAY_MS);
  const company = {
    id,
    name: String(props.name || '').trim() || 'This company',
    kind: kindOf(props.hierarchy_type),
    requested,
    lastEnrichedOn,
    daysSince,
    nextEligibleOn,
  };
  if (requested) {
    return { company, reason: 'already_requested', message: 'A run is already requested for this company. The service starts it within moments and clears the request when it is done.' };
  }
  if (daysSince !== null && daysSince < REFRESH_DAYS) {
    return {
      company,
      reason: 'recently_enriched',
      message: `Enriched ${ago(daysSince, lastEnrichedOn)}. The service skips a company enriched in the last ${REFRESH_DAYS} days, so a new run can be requested from ${nextEligibleOn}.`,
    };
  }
  return { company, reason: null, message: null };
}

// ---------- the contacts ----------

const clean = (value) => {
  const text = String(value === null || value === undefined ? '' : value).trim();
  return text || null;
};

function toContact(row) {
  const p = row.properties || {};
  const first = clean(p.firstname);
  const last = clean(p.lastname);
  const email = clean(p.email);
  const name = [first, last].filter(Boolean).join(' ') || email || 'No name';
  return {
    id: String(row.id),
    name,
    title: clean(p.jobtitle),
    email,
    emailCheck: clean(p.email_verification_result),
    emailCheckedOn: dayOf(p.email_verification_date),
    enrichedOn: dayOf(p.most_recent_enrichment_date),
    // Not sent to the card: how the rest are put in order.
    sortKey: ((last || '') + ' ' + (first || '')).trim().toLowerCase() || (email || '~').toLowerCase(),
  };
}

// The contacts the service has enriched come first, the newest first; the rest follow by last name.
function byEnrichment(a, b) {
  if (a.enrichedOn && b.enrichedOn && a.enrichedOn !== b.enrichedOn) return a.enrichedOn < b.enrichedOn ? 1 : -1;
  if (Boolean(a.enrichedOn) !== Boolean(b.enrichedOn)) return a.enrichedOn ? -1 : 1;
  return a.sortKey < b.sortKey ? -1 : a.sortKey > b.sortKey ? 1 : 0;
}

async function readContacts(api, companyId) {
  const links = await api.contactLinks(companyId, MAX_CONTACTS);
  if (!links.ids.length) return { contacts: [], more: links.more };
  const rows = await api.readContacts(links.ids);
  const contacts = rows.map(toContact).sort(byEnrichment).map(({ sortKey, ...rest }) => rest);
  return { contacts, more: links.more };
}

// ---------- HubSpot API ----------

const sleepFor = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function send(transport, options, body) {
  return new Promise((resolve, reject) => {
    const req = transport.request(options, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        let json = null;
        try { json = raw ? JSON.parse(raw) : null; } catch (e) { json = null; }
        resolve({ status: res.statusCode, headers: res.headers || {}, json });
      });
    });
    req.on('timeout', () => req.destroy(new Error('HubSpot did not answer in time. Try again.')));
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

// Returns request(method, path, body) -> { status, headers, json }. Retries a rate limit or a server error twice.
function makeRequester(token, opts = {}) {
  const transport = opts.transport || https;
  const hostname = opts.hostname || 'api.hubapi.com';
  const retries = opts.retries === undefined ? 2 : opts.retries;
  const sleep = opts.sleep || sleepFor;
  return async function request(method, path, payload) {
    const body = payload === undefined ? null : JSON.stringify(payload);
    const headers = { Authorization: `Bearer ${token}`, Accept: 'application/json' };
    if (body) {
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(body);
    }
    for (let attempt = 0; ; attempt += 1) {
      const res = await send(transport, { hostname, port: opts.port, path, method, headers, timeout: 8000 }, body);
      if ((res.status === 429 || res.status >= 500) && attempt < retries) {
        const after = Number(res.headers['retry-after']);
        await sleep(Math.min(after > 0 ? after * 1000 : 500 * 2 ** attempt, 2000));
        continue;
      }
      return res;
    }
  };
}

function describeFailure(status, doing, detail) {
  if (status === 401) return `HubSpot refused the app's token while ${doing} (401). The app may need to be reinstalled.`;
  if (status === 403) return `The app is missing a permission for ${doing} (403).`;
  if (status === 404) return `HubSpot could not find what it needed while ${doing} (404).`;
  if (status === 429) return 'HubSpot is rate limiting the app. Try again in a minute.';
  return `HubSpot returned ${status} while ${doing}${detail ? `: ${detail}` : ''}.`;
}

function check(res, doing) {
  if (res.status >= 200 && res.status < 300) return res.json;
  const err = new Error(describeFailure(res.status, doing, res.json && res.json.message));
  err.status = res.status;
  throw err;
}

const chunks = (list, size) => {
  const out = [];
  for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size));
  return out;
};

function makeApi(request) {
  return {
    async readCompany(id) {
      const json = check(await request('GET', `/crm/v3/objects/companies/${id}?properties=${COMPANY_PROPERTIES.join(',')}`), 'reading the company');
      return (json && json.properties) || {};
    },
    async requestEnrichment(id) {
      check(await request('PATCH', `/crm/v3/objects/companies/${id}`, { properties: { [TRIGGER_PROPERTY]: TRIGGER_VALUE } }), 'setting Request to enrich');
    },
    // The ids of the contacts linked to a company: the first page of up to `limit`, and whether there are more.
    async contactLinks(companyId, limit) {
      const json = check(await request('GET', `/crm/v4/objects/companies/${companyId}/associations/contacts?limit=${limit}`), 'reading the contacts');
      const ids = ((json && json.results) || []).map((r) => String(r.toObjectId));
      return { ids, more: Boolean(json && json.paging && json.paging.next) };
    },
    async readContacts(ids) {
      const rows = [];
      for (const part of chunks(ids, BATCH)) {
        const json = check(await request('POST', '/crm/v3/objects/contacts/batch/read', { properties: CONTACT_PROPERTIES, inputs: part.map((id) => ({ id })) }), 'reading the contacts');
        rows.push(...((json && json.results) || []));
      }
      return rows;
    },
  };
}

// ---------- the actions ----------

async function handle(context, api, now = Date.now()) {
  const parameters = (context && context.parameters) || {};
  const sent = (context && context.propertiesToSend) || {};
  if (String(parameters.objectTypeId || '') !== COMPANY_TYPE_ID) return fail('The Enrichment card works on company records.');
  // HubSpot fills propertiesToSend from the record the card is on, so prefer it over what the card passed.
  const objectId = String(sent.hs_object_id || parameters.objectId || '');
  if (!/^\d+$/.test(objectId)) return fail('The card could not tell which record it is on.');
  const action = parameters.action;
  if (action !== 'status' && action !== 'request') return fail('The card asked for something this function does not do.');

  const props = await api.readCompany(objectId);
  const { company, reason, message } = summarize(objectId, props, now);

  if (action === 'status') {
    let contacts = [];
    let more = false;
    let contactsError;
    try {
      const read = await readContacts(api, objectId);
      contacts = read.contacts;
      more = read.more;
    } catch (err) {
      contactsError = (err && err.message) || 'The contacts could not be read.';
    }
    return { ok: true, company, canRequest: reason === null, reason, message, contacts, moreContacts: more, ...(contactsError ? { contactsError } : {}) };
  }

  // A request the service would skip, or one that is already waiting, is refused without a write.
  if (reason) return fail(message, { reason });
  await api.requestEnrichment(objectId);
  const requested = summarize(objectId, { ...props, [TRIGGER_PROPERTY]: TRIGGER_VALUE }, now);
  return { ok: true, requested: true, company: requested.company, canRequest: false, reason: requested.reason, message: requested.message };
}

// handle, with every error turned into a message the card can show.
async function answer(context, api, now = Date.now()) {
  try {
    return await handle(context, api, now);
  } catch (err) {
    const message = err && err.message ? err.message : 'Something went wrong.';
    const action = context && context.parameters && context.parameters.action;
    console.error(`enrichment ${action || 'unknown'} failed: ${message}`);
    return fail(message);
  }
}

exports.main = async (context = {}) => {
  // Read outside the try: in local development HubSpot's dev server explains a missing token itself.
  const token = process.env.PRIVATE_APP_ACCESS_TOKEN;
  if (!token) return fail('The app has no access token. Reinstall the app.');
  return answer(context, makeApi(makeRequester(token)));
};

exports._internals = {
  COMPANY_TYPE_ID, TRIGGER_PROPERTY, TRIGGER_VALUE, TRIGGER_VALUES, LAST_ENRICHED_PROPERTY, REFRESH_DAYS, MAX_CONTACTS,
  parseDate, summarize, toContact, byEnrichment, makeRequester, makeApi, describeFailure, handle, answer,
};
