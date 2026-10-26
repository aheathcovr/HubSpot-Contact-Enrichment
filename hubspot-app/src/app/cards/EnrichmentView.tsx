// Enrichment card: the view. Enrichment.tsx hands it HubSpot's context, actions and the runner for app functions;
// the tests hand it fakes.
import { useCallback, useEffect, useRef, useState } from 'react';
import { Accordion, Button, Divider, EmptyState, ErrorState, Flex, LoadingSpinner, Tag, Text } from '@hubspot/ui-extensions';
import type { CrmContext, ExtensionPointApiActions, ServerlessRunnerParams } from '@hubspot/ui-extensions';
import { COMPANY_TYPE_ID, FUNCTION_NAME, SENT_PROPERTIES, emailCheckTag, formatDay, groupContacts, messageOf, readResult, statusTag } from './model.ts';
import type { ContactRow, RequestResult, StatusResult } from './model.ts';

export type Runner = (name: string, options?: Omit<ServerlessRunnerParams, 'name'>) => Promise<unknown>;

export interface EnrichmentViewProps {
  context: CrmContext;
  actions: ExtensionPointApiActions<'crm.record.sidebar'>;
  run: Runner;
  // While a run is requested the card asks for the status again this often (milliseconds; 0 turns it off), and gives
  // up after maxPolls asks. A run takes from a few seconds to a couple of minutes.
  pollEveryMs?: number;
  maxPolls?: number;
}

type Load = 'first' | 'refresh' | 'poll';

function ContactBlock({ contact }: { contact: ContactRow }) {
  const check = emailCheckTag(contact.emailCheck);
  return (
    <Flex direction="column" gap="flush">
      <Text format={{ fontWeight: 'bold' }}>{contact.name}</Text>
      {contact.title ? <Text variant="microcopy">{contact.title}</Text> : null}
      {contact.email ? (
        <Flex align="center" gap="xs">
          <Text>{contact.email}</Text>
          {check ? <Tag variant={check.variant}>{check.label}</Tag> : null}
        </Flex>
      ) : null}
      {contact.enrichedOn ? <Text variant="microcopy">{`Enriched ${formatDay(contact.enrichedOn)}`}</Text> : null}
    </Flex>
  );
}

export function EnrichmentView({ context, actions, run, pollEveryMs = 10000, maxPolls = 60 }: EnrichmentViewProps) {
  const objectId = String(context.crm.objectId);
  const objectTypeId = context.crm.objectTypeId;
  const isCompany = objectTypeId === COMPANY_TYPE_ID;

  const [data, setData] = useState<StatusResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [requesting, setRequesting] = useState(false);
  const [gaveUp, setGaveUp] = useState(false);
  // Counts the checks made while waiting, so the next one is scheduled whether or not the last one worked.
  const [pollTick, setPollTick] = useState(0);

  const hasData = useRef(false);
  const requestingNow = useRef(false);
  const polls = useRef(0);

  const call = useCallback(
    (parameters: Record<string, string>) => run(FUNCTION_NAME, { propertiesToSend: SENT_PROPERTIES, parameters: { objectId, objectTypeId, ...parameters } }),
    [run, objectId, objectTypeId],
  );

  const load = useCallback(
    async (how: Load) => {
      if (how === 'refresh') setRefreshing(true);
      if (how === 'first') {
        setLoading(true);
        setError(null);
      }
      try {
        const result = readResult<StatusResult>(await call({ action: 'status' }));
        hasData.current = true;
        setData(result);
        setError(null);
      } catch (err) {
        // A check that fails while waiting is skipped: the next one may work. A refresh the rep asked for says so.
        if (how === 'refresh' && hasData.current) actions.addAlert({ type: 'danger', title: 'Status not refreshed', message: messageOf(err) });
        else if (!hasData.current) setError(messageOf(err));
      } finally {
        setLoading(false);
        setRefreshing(false);
        if (how === 'poll') setPollTick((t) => t + 1);
      }
    },
    [call, actions],
  );
  // Effects call the latest load through this ref, so they run once per record and never loop.
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  useEffect(() => {
    if (isCompany) void loadRef.current('first');
  }, [isCompany, objectId]);

  // While a run is requested, ask again every so often until it finishes or the limit is reached.
  const waiting = Boolean(data?.company.requested);
  useEffect(() => {
    if (!isCompany || !pollEveryMs || !waiting) {
      polls.current = 0;
      return undefined;
    }
    if (polls.current >= maxPolls) {
      setGaveUp(true);
      return undefined;
    }
    const timer = setTimeout(() => {
      polls.current += 1;
      void loadRef.current('poll');
    }, pollEveryMs);
    return () => clearTimeout(timer);
  }, [waiting, pollTick, gaveUp, isCompany, pollEveryMs, maxPolls]);

  const refresh = useCallback(() => {
    polls.current = 0;
    setGaveUp(false);
    void load('refresh');
  }, [load]);

  const request = useCallback(async () => {
    if (requestingNow.current) return;
    requestingNow.current = true;
    setRequesting(true);
    try {
      const result = readResult<RequestResult>(await call({ action: 'request' }));
      polls.current = 0;
      setGaveUp(false);
      setData((d) => (d ? { ...d, company: result.company, canRequest: false, reason: result.reason, message: result.message } : d));
      actions.addAlert({ type: 'success', title: 'Enrichment requested', message: 'The service starts the run within moments. This card updates when it finishes.' });
    } catch (err) {
      actions.addAlert({ type: 'danger', title: 'Not requested', message: messageOf(err) });
      // The company may have changed since the card last looked, so look again.
      void loadRef.current('refresh');
    } finally {
      requestingNow.current = false;
      setRequesting(false);
    }
  }, [call, actions]);

  if (!isCompany) {
    return (
      <EmptyState title="Not available here" layout="vertical" imageName="companies" flush>
        <Text>The Enrichment card works on company records.</Text>
      </EmptyState>
    );
  }
  if (loading && !data) return <LoadingSpinner label="Loading enrichment status" showLabel layout="centered" />;
  if (!data) {
    return (
      <ErrorState title="The enrichment status did not load">
        <Text>{error ?? 'Something went wrong. Try again.'}</Text>
        <Button onClick={() => void load('first')}>Try again</Button>
      </ErrorState>
    );
  }

  const { company, canRequest, message, contacts, moreContacts, contactsError } = data;
  const tag = statusTag(company);
  const { enriched, others } = groupContacts(contacts);
  const note = message ?? (company.lastEnrichedOn ? `Last enriched ${formatDay(company.lastEnrichedOn)}.` : 'No enrichment run on record for this company.');

  return (
    <Flex direction="column" gap="sm">
      <Flex align="center" gap="xs">
        <Tag variant={tag.variant}>{tag.label}</Tag>
      </Flex>
      <Text>{note}</Text>
      {gaveUp && company.requested ? <Text>Still waiting for the run to finish. Press Refresh to check again.</Text> : null}
      <Button variant="primary" disabled={!canRequest || requesting} onClick={() => void request()}>
        {requesting ? 'Requesting' : 'Request enrichment'}
      </Button>
      <Divider />
      {contactsError ? <Text>{`Contacts could not be read: ${contactsError}`}</Text> : null}
      {!contactsError && contacts.length === 0 ? <Text>No contacts are linked to this company yet.</Text> : null}
      {contacts.length > 0 && enriched.length === 0 ? <Text>The service has not enriched any contact here yet.</Text> : null}
      {enriched.length > 0 ? (
        <Flex direction="column" gap="sm">
          <Text format={{ fontWeight: 'bold' }}>{`Contacts the service enriched (${enriched.length})`}</Text>
          {enriched.map((c) => (
            <ContactBlock key={c.id} contact={c} />
          ))}
        </Flex>
      ) : null}
      {others.length > 0 ? (
        <Accordion title={`Other contacts (${others.length})`}>
          <Flex direction="column" gap="sm">
            {others.map((c) => (
              <ContactBlock key={c.id} contact={c} />
            ))}
          </Flex>
        </Accordion>
      ) : null}
      {moreContacts ? <Text variant="microcopy">Showing the first 100 contacts. This company has more.</Text> : null}
      <Button size="xs" variant="secondary" disabled={refreshing} onClick={refresh}>
        {refreshing ? 'Refreshing' : 'Refresh'}
      </Button>
    </Flex>
  );
}
