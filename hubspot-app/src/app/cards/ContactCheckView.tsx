// Contact check card: the view. ContactCheck.tsx hands it HubSpot's context, actions and a service that calls the
// enrichment service with hubspot.fetch; the tests hand it fakes.
import { useCallback, useEffect, useRef, useState } from 'react';
import { Button, Divider, EmptyState, ErrorState, Flex, LoadingSpinner, Tag, Text } from '@hubspot/ui-extensions';
import type { CrmContext, ExtensionPointApiActions } from '@hubspot/ui-extensions';
import { emailCheckTag, formatDay, messageOf } from './model.ts';
import {
  CONTACT_TYPE_ID,
  RUN_MAX_POLLS,
  RUN_POLL_EVERY_MS,
  ServiceError,
  agoText,
  objectiveLabel,
  readReply,
  runIsOpen,
  runTag,
} from './contact-model.ts';
import type { ContactInfo, ContactStatus, ObjectiveId, RunView, Service, StartResult } from './contact-model.ts';
import { RunDetails } from './RunDetails.tsx';

export interface ContactCheckViewProps {
  context: CrmContext;
  actions: ExtensionPointApiActions<'crm.record.sidebar'>;
  service: Service;
  // While a run is going the card asks for the status again this often (milliseconds; 0 turns it off), and gives up
  // after maxPolls asks. A run takes from a few seconds to a couple of minutes, and each ask brings the steps it has done
  // so far, so two seconds keeps what the card shows close to what the service is doing (three minutes in all).
  pollEveryMs?: number;
  maxPolls?: number;
  now?: () => number;
}

type Load = 'first' | 'refresh' | 'poll';

function RunBlock({ run, nowMs }: { run: RunView; nowMs: number }) {
  const tag = runTag(run);
  return (
    <Flex direction="column" gap="flush">
      <Flex align="center" gap="xs">
        <Tag variant={tag.variant}>{tag.label}</Tag>
        <Text format={{ fontWeight: 'bold' }}>{objectiveLabel(run.objective)}</Text>
      </Flex>
      {run.message ? <Text>{run.message}</Text> : null}
      <RunDetails run={run} nowMs={nowMs} />
      <Text variant="microcopy">{`Asked by ${run.requestedBy}, ${agoText(run.createdAt, nowMs)}`}</Text>
    </Flex>
  );
}

function ContactBlock({ contact }: { contact: ContactInfo }) {
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
      ) : (
        <Text>No email on record.</Text>
      )}
      {contact.emailCheckedOn ? <Text variant="microcopy">{`Checked ${formatDay(contact.emailCheckedOn)}`}</Text> : null}
      <Text>{contact.mobile ? `Mobile ${contact.mobile}` : 'No mobile on record.'}</Text>
    </Flex>
  );
}

export function ContactCheckView({ context, actions, service, pollEveryMs = RUN_POLL_EVERY_MS, maxPolls = RUN_MAX_POLLS, now = Date.now }: ContactCheckViewProps) {
  const contactId = String(context.crm.objectId);
  const isContact = context.crm.objectTypeId === CONTACT_TYPE_ID;

  const [data, setData] = useState<ContactStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [starting, setStarting] = useState<ObjectiveId | null>(null);
  const [gaveUp, setGaveUp] = useState(false);
  // Counts the checks made while waiting, so the next one is scheduled whether or not the last one worked.
  const [pollTick, setPollTick] = useState(0);

  const hasData = useRef(false);
  const startingNow = useRef(false);
  const polls = useRef(0);

  const load = useCallback(
    async (how: Load) => {
      if (how === 'refresh') setRefreshing(true);
      if (how === 'first') {
        setLoading(true);
        setError(null);
      }
      try {
        const result = readReply<ContactStatus>(await service('GET', `/contact/status?contactId=${contactId}`));
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
    [service, contactId, actions],
  );
  // Effects call the latest load through this ref, so they run once per record and never loop.
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  useEffect(() => {
    if (!isContact) return;
    // A contact the card is moved to starts with all of its patience, whatever the last one used.
    polls.current = 0;
    setGaveUp(false);
    void loadRef.current('first');
  }, [isContact, contactId]);

  // While a run is going, ask again every so often until it finishes or the limit is reached.
  const open = Boolean(data?.runs.some(runIsOpen));
  useEffect(() => {
    if (!isContact || !pollEveryMs || !open) return undefined;
    if (polls.current >= maxPolls) {
      setGaveUp(true);
      return undefined;
    }
    const timer = setTimeout(() => {
      polls.current += 1;
      void loadRef.current('poll');
    }, pollEveryMs);
    return () => clearTimeout(timer);
  }, [open, pollTick, gaveUp, isContact, pollEveryMs, maxPolls]);

  const refresh = useCallback(() => {
    polls.current = 0;
    setGaveUp(false);
    void load('refresh');
  }, [load]);

  const start = useCallback(
    async (objective: ObjectiveId) => {
      if (startingNow.current) return;
      startingNow.current = true;
      setStarting(objective);
      try {
        const result = readReply<StartResult>(await service('POST', '/contact/run', { contactId, objective }));
        polls.current = 0;
        setGaveUp(false);
        setData((d) => (d ? { ...d, quota: result.quota, runs: [result.run, ...d.runs.filter((r) => r.runId !== result.run.runId)].slice(0, 5) } : d));
        actions.addAlert(
          result.reused
            ? { type: 'info', title: 'Already asked', message: 'This was asked of this contact in the last day, so the earlier run is shown. Nothing more was spent.' }
            : { type: 'success', title: 'Run started', message: 'The service is working on it. This card updates when it finishes.' },
        );
      } catch (err) {
        const quota = err instanceof ServiceError ? err.quota : undefined;
        if (quota) setData((d) => (d ? { ...d, quota } : d));
        actions.addAlert({ type: 'danger', title: 'Not started', message: messageOf(err) });
        // The contact may have changed since the card last looked, so look again.
        void loadRef.current('refresh');
      } finally {
        startingNow.current = false;
        setStarting(null);
      }
    },
    [service, contactId, actions],
  );

  if (!isContact) {
    return (
      <EmptyState title="Not available here" layout="vertical" imageName="contacts" flush>
        <Text>The Contact check works on contact records.</Text>
      </EmptyState>
    );
  }
  if (loading && !data) return <LoadingSpinner label="Loading contact check" showLabel layout="centered" />;
  if (!data) {
    return (
      <ErrorState title="The contact check did not load">
        <Text>{error ?? 'Something went wrong. Try again.'}</Text>
        <Button onClick={() => void load('first')}>Try again</Button>
      </ErrorState>
    );
  }

  const { contact, objectives, quota, runs } = data;
  const going = new Set(runs.filter(runIsOpen).map((r) => r.objective));
  const nowMs = now();

  return (
    <Flex direction="column" gap="sm">
      <ContactBlock contact={contact} />
      <Divider />
      <Text variant="microcopy">{quota.remaining === 0 ? `You have used all ${quota.cap} runs for today.` : `${quota.remaining} of ${quota.cap} runs left today.`}</Text>
      {objectives.map((o) => (
        <Flex key={o.id} direction="column" gap="flush">
          <Button
            variant="secondary"
            disabled={!o.available || starting !== null || quota.remaining === 0 || going.has(o.id)}
            onClick={() => void start(o.id)}
          >
            {starting === o.id ? 'Starting' : o.label}
          </Button>
          <Text variant="microcopy">{!o.available ? (o.reason ?? '') : going.has(o.id) ? 'A run is in progress.' : `Uses ${o.spends}.`}</Text>
        </Flex>
      ))}
      {gaveUp && open ? <Text>Still waiting for a run to finish. Press Refresh to check again.</Text> : null}
      <Divider />
      <Text format={{ fontWeight: 'bold' }}>Recent runs</Text>
      {runs.length === 0 ? <Text>No runs on this contact yet.</Text> : null}
      {runs.map((r) => (
        <RunBlock key={r.runId} run={r} nowMs={nowMs} />
      ))}
      <Button size="xs" variant="secondary" disabled={refreshing} onClick={refresh}>
        {refreshing ? 'Refreshing' : 'Refresh'}
      </Button>
    </Flex>
  );
}
