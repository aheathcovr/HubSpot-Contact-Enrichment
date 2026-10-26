// What the app asks HubSpot for: its permissions, where it shows, and that its pieces point at each other.
// A card that names a function that is not there, or an app that asks for more than it uses, is caught here
// instead of in the portal.
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { COMPANY_TYPE_ID, FUNCTION_NAME } from '../src/app/cards/model.ts';

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const json = (p: string) => JSON.parse(readFileSync(here(p), 'utf8'));
const app = json('../src/app/app-hsmeta.json');
const card = json('../src/app/cards/enrichment-hsmeta.json');
const fn = json('../src/app/functions/enrichment-hsmeta.json');
const project = json('../hsproject.json');

describe('the app', () => {
  it('is private and uses a static token', () => {
    expect(app.type).toBe('app');
    expect(app.config.distribution).toBe('private');
    expect(app.config.auth.type).toBe('static');
  });

  it('asks for the four permissions it uses and no more', () => {
    expect([...app.config.auth.requiredScopes].sort()).toEqual([
      'crm.objects.companies.read',
      'crm.objects.companies.write',
      'crm.objects.contacts.read',
      'oauth',
    ]);
    expect(app.config.auth.optionalScopes).toEqual([]);
    expect(app.config.auth.conditionallyRequiredScopes).toEqual([]);
  });

  it('does not write contacts, and reaches no other site', () => {
    expect(app.config.auth.requiredScopes).not.toContain('crm.objects.contacts.write');
    expect(app.config.permittedUrls).toEqual({ fetch: [], iframe: [], img: [] });
  });

  it('is one project on the platform version the rollout app uses', () => {
    expect(project).toEqual({ name: 'covr-enrichment-app', srcDir: 'src', platformVersion: '2026.03' });
  });
});

describe('the card', () => {
  it('sits in the sidebar of company records', () => {
    expect(card.type).toBe('card');
    expect(card.config.location).toBe('crm.record.sidebar');
    expect(card.config.objectTypes).toEqual(['companies']);
    expect(COMPANY_TYPE_ID).toBe('0-2');
  });

  it('has an entry point that exists', () => {
    const entry = card.config.entrypoint.replace('/app/cards/', '');
    expect(existsSync(here(`../src/app/cards/${entry}`))).toBe(true);
  });
});

describe('the function', () => {
  it('is the one the card calls', () => {
    expect(fn.type).toBe('app-function');
    expect(fn.config.entrypoint).toBe('/app/functions/enrichment.js');
    expect(fn.uid).toBe(FUNCTION_NAME);
    expect(existsSync(here('../src/app/functions/enrichment.js'))).toBe(true);
  });

  it('keeps no secrets of its own: it uses the app token', () => {
    expect(fn.config.secretKeys).toEqual([]);
  });

  it('has no dependencies to install', () => {
    expect(json('../src/app/functions/package.json').dependencies).toEqual({});
  });
});
