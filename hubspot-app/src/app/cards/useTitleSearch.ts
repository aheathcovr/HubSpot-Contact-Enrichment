// Company card, finding people by job title: what the card does with the enrichment service. It asks for a search for
// the titles a rep chose, reads the people the search found, remembers who is chosen, and asks for those people to be
// added, checking again while a search or an add is going. The card writes nothing itself and the search writes nothing
// to HubSpot: only an add does, and only for the people chosen. EnrichmentView.tsx draws it through TitlePanel.tsx.
import { useCallback, useEffect, useRef, useState } from 'react';
import type { ExtensionPointApiActions } from '@hubspot/ui-extensions';
import { messageOf } from './model.ts';
import { ServiceError, readReply, runIsOpen } from './contact-model.ts';
import type { Quota, RunView, Service } from './contact-model.ts';
import { addAlert, candidatesPath, combineTitles, findAlert } from './company-model.ts';
import type { AddResult, Candidate, CandidateList, CompanyStatus, FindResult } from './company-model.ts';

const FIND_OBJECTIVE = 'find_by_title';
// The service gives the same answer to the same search for a day, so a search this old or newer is the one to bring back.
const RESUME_WITHIN_MS = 24 * 60 * 60 * 1000;

type Load = 'first' | 'refresh' | 'poll';

interface Search {
  runId: string;
  run: RunView;
  candidates: Candidate[];
  ready: boolean;
}

export interface TitleSearchOptions {
  service: Service | undefined;
  actions: ExtensionPointApiActions<'crm.record.sidebar'>;
  companyId: string;
  // The service's company status, once loaded: the quick picks, the limits and the runs.
  status: CompanyStatus | null;
  // Take the quota a start or a refusal sent, and look at the status again.
  applyQuota: (quota: Quota) => void;
  reloadStatus: () => void;
  pollEveryMs: number;
  maxPolls: number;
  now: () => number;
}

export interface TitleSearch {
  picks: string[];
  typed: string;
  titles: string[];
  setPicks: (titles: string[]) => void;
  setTyped: (text: string) => void;
  search: Search | null;
  // Why the people of the last search are gone, when the service has let them expire.
  gone: string | null;
  searching: boolean;
  adding: boolean;
  chosen: string[];
  gaveUp: boolean;
  open: boolean;
  find: () => void;
  toggle: (candidateId: string, checked: boolean) => void;
  add: () => void;
  refresh: () => void;
}

// A person whose add is going or done cannot be chosen again; one whose add failed can.
export const isBeingAdded = (c: Candidate): boolean => Boolean(c.added && c.added.status !== 'failed');

export function useTitleSearch({ service, actions, companyId, status, applyQuota, reloadStatus, pollEveryMs, maxPolls, now }: TitleSearchOptions): TitleSearch {
  const [picks, setPicks] = useState<string[]>([]);
  const [typed, setTyped] = useState('');
  const [search, setSearch] = useState<Search | null>(null);
  const [gone, setGone] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);
  const [adding, setAdding] = useState(false);
  const [chosen, setChosen] = useState<string[]>([]);
  const [gaveUp, setGaveUp] = useState(false);
  // Counts the checks made while waiting, so the next one is scheduled whether or not the last one worked.
  const [pollTick, setPollTick] = useState(0);

  const busy = useRef(false);
  const polls = useRef(0);
  const adopted = useRef(false);

  const on = Boolean(service && status);
  const titles = combineTitles(picks, typed);
  const maxPeople = status?.limits.maxContacts ?? 10;

  const loadPeople = useCallback(
    async (runId: string, how: Load) => {
      if (!service) return;
      try {
        const list = readReply<CandidateList>(await service('GET', candidatesPath(companyId, runId)));
        setSearch({ runId, run: list.run, candidates: list.candidates, ready: list.ready });
        setGone(null);
      } catch (err) {
        if (err instanceof ServiceError && err.status === 410) {
          setSearch(null);
          setGone(messageOf(err));
        } else if (how === 'refresh') {
          actions.addAlert({ type: 'danger', title: 'Status not refreshed', message: messageOf(err) });
        }
      } finally {
        if (how === 'poll') setPollTick((t) => t + 1);
      }
    },
    [service, companyId, actions],
  );
  const loadRef = useRef(loadPeople);
  useEffect(() => {
    loadRef.current = loadPeople;
  }, [loadPeople]);

  // A company the card is moved to starts clean, with all of its patience.
  useEffect(() => {
    polls.current = 0;
    adopted.current = false;
    setGaveUp(false);
    setSearch(null);
    setGone(null);
    setChosen([]);
    setPicks([]);
    setTyped('');
  }, [companyId]);

  // Once the status is in, bring back the newest search that did not fail, if it was made in the last day.
  useEffect(() => {
    if (!on || adopted.current || !status) return;
    adopted.current = true;
    const newest = status.runs.find((r) => r.objective === FIND_OBJECTIVE && r.status !== 'failed' && now() - Date.parse(r.createdAt) <= RESUME_WITHIN_MS);
    if (newest) void loadRef.current(newest.runId, 'first');
  }, [on, status, now]);

  const waitingForPeople = Boolean(search && !search.ready && runIsOpen(search.run));
  const waitingForAdds = Boolean(search?.candidates.some((c) => c.added && runIsOpen(c.added)));
  const open = waitingForPeople || waitingForAdds;
  useEffect(() => {
    if (!search || !pollEveryMs || !open) return undefined;
    if (polls.current >= maxPolls) {
      setGaveUp(true);
      return undefined;
    }
    const timer = setTimeout(() => {
      polls.current += 1;
      void loadRef.current(search.runId, 'poll');
    }, pollEveryMs);
    return () => clearTimeout(timer);
  }, [search?.runId, open, pollTick, gaveUp, pollEveryMs, maxPolls]);

  const refresh = useCallback(() => {
    polls.current = 0;
    setGaveUp(false);
    if (search) void loadPeople(search.runId, 'refresh');
  }, [search?.runId, loadPeople]);

  const find = useCallback(async () => {
    if (!service || busy.current || titles.length === 0) return;
    busy.current = true;
    setSearching(true);
    try {
      const result = readReply<FindResult>(await service('POST', '/company/find', { companyId, titles }));
      polls.current = 0;
      setGaveUp(false);
      setGone(null);
      setChosen([]);
      setSearch({ runId: result.run.runId, run: result.run, candidates: [], ready: false });
      applyQuota(result.quota);
      reloadStatus();
      actions.addAlert(findAlert(result));
      // A search made earlier is already done: read its people now rather than wait for a check.
      if (result.reused) void loadRef.current(result.run.runId, 'first');
    } catch (err) {
      const quota = err instanceof ServiceError ? err.quota : undefined;
      if (quota) applyQuota(quota);
      actions.addAlert({ type: 'danger', title: 'Not started', message: messageOf(err) });
    } finally {
      busy.current = false;
      setSearching(false);
    }
  }, [service, companyId, titles.join('\n'), actions, applyQuota, reloadStatus]);

  const toggle = useCallback(
    (candidateId: string, checked: boolean) => {
      setChosen((prev) => {
        if (!checked) return prev.filter((id) => id !== candidateId);
        return prev.includes(candidateId) || prev.length >= maxPeople ? prev : [...prev, candidateId];
      });
    },
    [maxPeople],
  );

  const add = useCallback(async () => {
    if (!service || busy.current || !search) return;
    // In the order the people are listed, and only the ones still listed.
    const candidateIds = search.candidates.filter((c) => chosen.includes(c.candidateId)).map((c) => c.candidateId);
    if (candidateIds.length === 0) return;
    busy.current = true;
    setAdding(true);
    try {
      const result = readReply<AddResult>(await service('POST', '/company/add', { companyId, runId: search.runId, candidateIds }));
      const names = Object.fromEntries(search.candidates.map((c) => [c.candidateId, c.name]));
      polls.current = 0;
      setGaveUp(false);
      setChosen([]);
      setSearch((s) =>
        s ? { ...s, candidates: s.candidates.map((c) => ({ ...c, added: result.items.find((i) => i.candidateId === c.candidateId && i.run)?.run ?? c.added })) } : s,
      );
      applyQuota(result.quota);
      reloadStatus();
      actions.addAlert(addAlert(result, names));
    } catch (err) {
      const quota = err instanceof ServiceError ? err.quota : undefined;
      if (quota) applyQuota(quota);
      actions.addAlert({ type: 'danger', title: 'Not started', message: messageOf(err) });
    } finally {
      busy.current = false;
      setAdding(false);
    }
  }, [service, companyId, search, chosen, actions, applyQuota, reloadStatus]);

  return {
    picks,
    typed,
    titles,
    setPicks,
    setTyped,
    search,
    gone,
    searching,
    adding,
    chosen,
    gaveUp,
    open,
    find: () => void find(),
    toggle,
    add: () => void add(),
    refresh,
  };
}
