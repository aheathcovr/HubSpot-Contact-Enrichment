// The card's logic without HubSpot: how a status becomes words and tags.
import { describe, expect, it } from 'vitest';
import {
  COMPANY_TYPE_ID,
  FUNCTION_NAME,
  SENT_PROPERTIES,
  emailCheckTag,
  formatDay,
  groupContacts,
  messageOf,
  readResult,
  statusTag,
  type CompanySummary,
  type ContactRow,
} from '../src/app/cards/model.ts';

const company = (over: Partial<CompanySummary> = {}): CompanySummary => ({
  id: '5001',
  name: 'Elm Court Care Center',
  kind: 'facility',
  requested: false,
  lastEnrichedOn: null,
  daysSince: null,
  nextEligibleOn: null,
  ...over,
});

const row = (id: string, over: Partial<ContactRow> = {}): ContactRow => ({
  id,
  name: `Contact ${id}`,
  title: null,
  email: null,
  emailCheck: null,
  emailCheckedOn: null,
  enrichedOn: null,
  ...over,
});

describe('what the card sends', () => {
  it('names the app function and asks HubSpot for the record id', () => {
    expect(FUNCTION_NAME).toBe('enrichment');
    expect(SENT_PROPERTIES).toEqual(['hs_object_id']);
    expect(COMPANY_TYPE_ID).toBe('0-2');
  });
});

describe('reading what the function sends back', () => {
  it('hands back an answer that worked', () => {
    expect(readResult<{ ok: true; n: number }>({ ok: true, n: 3 })).toEqual({ ok: true, n: 3 });
  });

  it('throws the function\'s own message when it failed', () => {
    expect(() => readResult({ ok: false, error: 'The app has no access token. Reinstall the app.' })).toThrow('The app has no access token. Reinstall the app.');
  });

  it('throws a plain message when the failure has no text', () => {
    expect(() => readResult({ ok: false })).toThrow('The enrichment app did not give a reason.');
  });

  it.each([null, undefined, 'text', 42, []])('does not take %j for an answer', (bad) => {
    expect(() => readResult(bad)).toThrow('The enrichment app sent back something the card could not read.');
  });
});

describe('messageOf', () => {
  it('takes the message of an error, or a string as it is', () => {
    expect(messageOf(new Error('boom'))).toBe('boom');
    expect(messageOf('plain')).toBe('plain');
  });

  it.each([null, undefined, {}, 7, new Error('')])('gives a plain message for %j', (bad) => {
    expect(messageOf(bad)).toBe('Something went wrong. Try again.');
  });
});

describe('formatDay', () => {
  it('writes a date the way a US rep reads it', () => {
    expect(formatDay('2026-10-21')).toBe('Oct 21, 2026');
    expect(formatDay('2026-01-05')).toBe('Jan 5, 2026');
  });

  it('gives nothing for no date and the text back for one it cannot read', () => {
    expect(formatDay(null)).toBe('');
    expect(formatDay('soon')).toBe('soon');
  });
});

describe('the status tag', () => {
  it('says a run is requested, whatever else is true', () => {
    expect(statusTag(company({ requested: true, lastEnrichedOn: '2026-10-21', daysSince: 5 }))).toEqual({ label: 'Run requested', variant: 'warning' });
  });

  it('says a company that was never enriched has not been', () => {
    expect(statusTag(company())).toEqual({ label: 'Not enriched yet', variant: 'default' });
  });

  it.each([
    [0, 'Enriched today'],
    [1, 'Enriched 1 day ago'],
    [5, 'Enriched 5 days ago'],
    [29, 'Enriched 29 days ago'],
  ])('calls a company enriched %i days ago "%s", in green', (daysSince, label) => {
    expect(statusTag(company({ daysSince, lastEnrichedOn: '2026-10-21' }))).toEqual({ label, variant: 'success' });
  });

  it('calls an older run old, not recent', () => {
    expect(statusTag(company({ daysSince: 30, lastEnrichedOn: '2026-09-26' }))).toEqual({ label: 'Last enriched 30 days ago', variant: 'default' });
    expect(statusTag(company({ daysSince: 45, lastEnrichedOn: '2026-09-11' }))).toEqual({ label: 'Last enriched 45 days ago', variant: 'default' });
  });

  it('gives the date for a run dated in the future', () => {
    expect(statusTag(company({ daysSince: -7, lastEnrichedOn: '2026-11-02' }))).toEqual({ label: 'Enriched Nov 2, 2026', variant: 'success' });
  });
});

describe('the email check tag', () => {
  it.each([
    ['ok', 'Good', 'success'],
    ['catch_all', 'Risky', 'warning'],
    ['unknown', 'Unknown', 'warning'],
    ['invalid', 'Invalid', 'error'],
    ['disposable', 'Disposable', 'error'],
    ['skipped', 'Not checked', 'default'],
    ['error', 'Check failed', 'default'],
  ])('shows %s as %s', (result, label, variant) => {
    expect(emailCheckTag(result)).toEqual({ label, variant });
  });

  it('reads the word without regard to case or spaces', () => {
    expect(emailCheckTag(' OK ')).toEqual({ label: 'Good', variant: 'success' });
  });

  it('shows a word it does not know as it is, capitalised', () => {
    expect(emailCheckTag('role_account')).toEqual({ label: 'Role account', variant: 'default' });
  });

  it.each([null, '', '   '])('shows nothing for %j', (none) => {
    expect(emailCheckTag(none)).toBeNull();
  });
});

describe('grouping contacts', () => {
  it('splits the ones the service enriched from the rest and keeps the order', () => {
    const rows = [row('1'), row('2', { enrichedOn: '2026-10-21' }), row('3'), row('4', { enrichedOn: '2026-09-01' })];

    const { enriched, others } = groupContacts(rows);

    expect(enriched.map((c) => c.id)).toEqual(['2', '4']);
    expect(others.map((c) => c.id)).toEqual(['1', '3']);
  });

  it('gives two empty groups for no contacts', () => {
    expect(groupContacts([])).toEqual({ enriched: [], others: [] });
  });
});
