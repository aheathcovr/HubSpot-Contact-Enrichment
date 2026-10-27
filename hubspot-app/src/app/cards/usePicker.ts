// Company card, picking contacts: what the card does with the enrichment service. It loads the company's runs and
// quota, remembers which contacts are ticked and what to do with them, starts the runs, and checks again while one
// is going. EnrichmentView.tsx draws it; the tests hand it a fake service.
import { useCallback, useEffect, useRef, useState } from 'react';
import type { ExtensionPointApiActions } from '@hubspot/ui-extensions';
import { messageOf } from './model.ts';
import type { ContactRow } from './model.ts';
import { ServiceError, readReply, runIsOpen } from './contact-model.ts';
import type { Quota, RunView, Service } from './contact-model.ts';
import { batchAlert, statusPath } from './company-model.ts';
import type { BatchResult, CompanyStatus } from './company-model.ts';

const DEFAULT_OBJECTIVE = 'enrich';
const DEFAULT_MAX_CONTACTS = 10;

// A quiet load looks again without the patience counting or a refusal being shown: the title search uses it after it
// starts something, so the runs and quota here follow.
type Load = 'first' | 'refresh' | 'poll' | 'quiet';

export interface PickerOptions {
  service: Service | undefined;
  actions: ExtensionPointApiActions<'crm.record.sidebar'>;
  companyId: string;
  // False off a company record: nothing is asked.
  active: boolean;
  contacts: ContactRow[];
  // While a run is going the card asks for the status again this often (milliseconds; 0 turns it off), and gives up
  // after maxPolls asks.
  pollEveryMs: number;
  maxPolls: number;
}

export interface Picker {
  // Whether there is a service to ask at all. Without one the card does no picking.
  enabled: boolean;
  data: CompanyStatus | null;
  // Why picking is off, when the first look at the service failed.
  unavailable: string | null;
  picked: string[];
  objective: string;
  starting: boolean;
  gaveUp: boolean;
  atLimit: boolean;
  toggle: (contactId: string, checked: boolean) => void;
  setObjective: (id: string) => void;
  start: () => void;
  // Look again, with all the patience back. The caller's own refresh calls this.
  refresh: () => void;
  // For the title search: look again quietly, and take the quota a start or a refusal sent.
  reload: () => void;
  setQuota: (quota: Quota) => void;
}

const newestFirst = (runs: RunView[]): RunView[] => [...runs].sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt));

export function usePicker({ service, actions, companyId, active, contacts, pollEveryMs, maxPolls }: PickerOptions): Picker {
  const enabled = Boolean(service);
  const on = active && enabled;

  const [data, setData] = useState<CompanyStatus | null>(null);
  const [unavailable, setUnavailable] = useState<string | null>(null);
  const [picked, setPicked] = useState<string[]>([]);
  const [objective, setObjective] = useState<string>(DEFAULT_OBJECTIVE);
  const [starting, setStarting] = useState(false);
  const [gaveUp, setGaveUp] = useState(false);
  // Counts the checks made while waiting, so the next one is scheduled whether or not the last one worked.
  const [pollTick, setPollTick] = useState(0);

  const hasData = useRef(false);
  const startingNow = useRef(false);
  const polls = useRef(0);

  const load = useCallback(
    async (how: Load) => {
      if (!service) return;
      try {
        const result = readReply<CompanyStatus>(await service('GET', statusPath(companyId)));
        hasData.current = true;
        setData(result);
        setUnavailable(null);
      } catch (err) {
        // A check that fails while waiting is skipped: the next one may work. A refresh the rep asked for says so.
        if (how === 'refresh' && hasData.current) actions.addAlert({ type: 'danger', title: 'Status not refreshed', message: messageOf(err) });
        else if (!hasData.current) setUnavailable(messageOf(err));
      } finally {
        if (how === 'poll') setPollTick((t) => t + 1);
      }
    },
    [service, companyId, actions],
  );
  // Effects call the latest load through this ref, so they run once per record and never loop.
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  useEffect(() => {
    if (!on) return;
    // A company the card is moved to starts with all of its patience, whatever the last one used.
    polls.current = 0;
    setGaveUp(false);
    hasData.current = false;
    setData(null);
    setPicked([]);
    void loadRef.current('first');
  }, [on, companyId]);

  // While a run is going, ask again every so often until it finishes or the limit is reached.
  const open = Boolean(data?.runs.some(runIsOpen));
  useEffect(() => {
    if (!on || !pollEveryMs || !open) return undefined;
    if (polls.current >= maxPolls) {
      setGaveUp(true);
      return undefined;
    }
    const timer = setTimeout(() => {
      polls.current += 1;
      void loadRef.current('poll');
    }, pollEveryMs);
    return () => clearTimeout(timer);
  }, [open, pollTick, gaveUp, on, pollEveryMs, maxPolls]);

  const maxContacts = data?.limits.maxContacts ?? DEFAULT_MAX_CONTACTS;

  const toggle = useCallback(
    (contactId: string, checked: boolean) => {
      setPicked((prev) => {
        if (!checked) return prev.filter((id) => id !== contactId);
        return prev.includes(contactId) || prev.length >= maxContacts ? prev : [...prev, contactId];
      });
    },
    [maxContacts],
  );

  const refresh = useCallback(() => {
    polls.current = 0;
    setGaveUp(false);
    void load('refresh');
  }, [load]);

  const reload = useCallback(() => void loadRef.current('quiet'), []);
  const setQuota = useCallback((quota: Quota) => setData((d) => (d ? { ...d, quota } : d)), []);

  const start = useCallback(async () => {
    if (!service || startingNow.current) return;
    // In the order the contacts are listed, and only the ones still listed.
    const contactIds = contacts.map((c) => c.id).filter((id) => picked.includes(id));
    if (contactIds.length === 0) return;
    startingNow.current = true;
    setStarting(true);
    try {
      const result = readReply<BatchResult>(await service('POST', '/company/run', { companyId, contactIds, objective }));
      const names = Object.fromEntries(contacts.map((c) => [c.id, c.name]));
      polls.current = 0;
      setGaveUp(false);
      setPicked([]);
      const fresh = result.items.flatMap((i) => (i.run ? [i.run] : []));
      setData((d) =>
        d
          ? { ...d, quota: result.quota, runs: newestFirst([...fresh, ...d.runs.filter((r) => !fresh.some((f) => f.runId === r.runId))]) }
          : d,
      );
      actions.addAlert(batchAlert(result, names));
    } catch (err) {
      const quota = err instanceof ServiceError ? err.quota : undefined;
      if (quota) setData((d) => (d ? { ...d, quota } : d));
      actions.addAlert({ type: 'danger', title: 'Not started', message: messageOf(err) });
      // The contacts may have changed since the card last looked, so look again.
      void loadRef.current('refresh');
    } finally {
      startingNow.current = false;
      setStarting(false);
    }
  }, [service, companyId, contacts, picked, objective, actions]);

  return {
    enabled,
    data,
    unavailable,
    picked,
    objective,
    starting,
    gaveUp,
    atLimit: picked.length >= maxContacts,
    toggle,
    setObjective,
    start: () => void start(),
    refresh,
    reload,
    setQuota,
  };
}
