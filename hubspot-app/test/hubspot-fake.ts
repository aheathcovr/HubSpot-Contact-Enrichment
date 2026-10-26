// An in-memory HubSpot for the function tests: companies with their linked contacts, contacts, and every endpoint
// the enrichment function calls. Writes are recorded so a test can check exactly what changed.
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { makeApi } = require('../src/app/functions/enrichment.js')._internals;

export interface Call {
  method: string;
  path: string;
  body?: unknown;
}

export type Props = Record<string, string | null>;

export interface FakeContact {
  id: string;
  properties: Props;
}

export interface FakeCompany {
  id: string;
  properties: Props;
  // Ids of the contacts linked to the company, in the order HubSpot lists them.
  contacts?: string[];
}

const pick = (all: Props, wanted: string[]): Props => Object.fromEntries(wanted.map((name) => [name, all[name] ?? null]));

export function fakeHubSpot(
  opts: {
    companies: FakeCompany[];
    contacts?: FakeContact[];
    // The most links HubSpot returns in one page, whatever limit is asked for.
    pageSize?: number;
    // Force a status for a call, by its method and path.
    fail?: (method: string, path: string) => number | null;
  },
) {
  const calls: Call[] = [];
  const companies = new Map(opts.companies.map((c) => [c.id, structuredClone(c)]));
  const contacts = new Map((opts.contacts ?? []).map((c) => [c.id, c]));
  const pageSize = opts.pageSize ?? 500;
  const json = (status: number, body: unknown) => ({ status, headers: {}, json: body });

  async function request(method: string, path: string, body?: unknown) {
    calls.push({ method, path, body });
    const forced = opts.fail?.(method, path);
    if (forced) return json(forced, { message: `forced ${forced}` });
    const url = new URL(`https://api.hubapi.com${path}`);
    const p = url.pathname;
    let m: RegExpExecArray | null;

    if ((m = /^\/crm\/v3\/objects\/companies\/(\d+)$/.exec(p))) {
      const company = companies.get(m[1]);
      if (!company) return json(404, { message: 'resource not found' });
      if (method === 'GET') {
        const wanted = (url.searchParams.get('properties') ?? '').split(',').filter(Boolean);
        return json(200, { id: company.id, properties: pick(company.properties, wanted) });
      }
      if (method === 'PATCH') {
        Object.assign(company.properties, (body as { properties: Props }).properties);
        return json(200, { id: company.id, properties: company.properties });
      }
    }

    if ((m = /^\/crm\/v4\/objects\/companies\/(\d+)\/associations\/contacts$/.exec(p)) && method === 'GET') {
      const company = companies.get(m[1]);
      if (!company) return json(404, { message: 'resource not found' });
      const ids = company.contacts ?? [];
      const limit = Math.min(Number(url.searchParams.get('limit') ?? 100), pageSize);
      const after = Number(url.searchParams.get('after') ?? 0);
      const page = ids.slice(after, after + limit);
      const more = after + limit < ids.length;
      return json(200, {
        results: page.map((id) => ({ toObjectId: Number(id), associationTypes: [{ category: 'HUBSPOT_DEFINED', typeId: 280, label: null }] })),
        ...(more ? { paging: { next: { after: String(after + limit) } } } : {}),
      });
    }

    if (p === '/crm/v3/objects/contacts/batch/read' && method === 'POST') {
      const b = body as { properties: string[]; inputs: { id: string }[] };
      if (b.inputs.length > 100) return json(400, { message: 'a batch read takes at most 100 records' });
      const results = b.inputs
        .map((i) => contacts.get(i.id))
        .filter((c): c is FakeContact => Boolean(c))
        .map((c) => ({ id: c.id, properties: pick(c.properties, b.properties) }));
      return json(200, { results });
    }

    throw new Error(`the fake HubSpot has no endpoint for ${method} ${path}`);
  }

  return {
    request,
    api: makeApi(request),
    calls,
    company: (id: string) => companies.get(id)!,
    // Everything that is not a read: a batch read is a POST but changes nothing.
    writes: () => calls.filter((c) => c.method !== 'GET' && !c.path.endsWith('/batch/read')),
  };
}
