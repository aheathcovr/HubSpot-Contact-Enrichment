// Company card, finding people by job title: the part below the picking controls. A rep chooses titles (the ones the
// company runs look for, and any of their own), asks for a search, ticks the people it found, and adds them. The state
// is in useTitleSearch.ts; nothing is written to HubSpot until someone is added.
import { Button, Checkbox, Divider, Flex, Input, MultiSelect, Tag, Text } from '@hubspot/ui-extensions';
import { runTag } from './contact-model.ts';
import type { RunView } from './contact-model.ts';
import { addButtonText, quotaWarning, searchButtonText } from './company-model.ts';
import type { Candidate, CompanyStatus } from './company-model.ts';
import { RunDetails } from './RunDetails.tsx';
import { isBeingAdded } from './useTitleSearch.ts';
import type { TitleSearch } from './useTitleSearch.ts';

function AddedLine({ run, nowMs }: { run: RunView; nowMs: number }) {
  const tag = runTag(run);
  return (
    <Flex direction="column" gap="flush">
      <Tag variant={tag.variant}>{tag.label}</Tag>
      {run.message ? <Text variant="microcopy">{run.message}</Text> : null}
      <RunDetails run={run} nowMs={nowMs} />
    </Flex>
  );
}

function Person({ candidate, ticked, readOnly, nowMs, onChange }: { candidate: Candidate; ticked: boolean; readOnly: boolean; nowMs: number; onChange: (checked: boolean) => void }) {
  return (
    <Flex direction="column" gap="flush">
      <Checkbox name={`cand-${candidate.candidateId}`} checked={ticked} readOnly={readOnly} onChange={(checked) => onChange(checked)}>
        {candidate.name}
      </Checkbox>
      {candidate.title ? <Text variant="microcopy">{candidate.title}</Text> : null}
      {candidate.linkedContactId ? <Text variant="microcopy">Already a contact here.</Text> : null}
      {candidate.added ? <AddedLine run={candidate.added} nowMs={nowMs} /> : null}
    </Flex>
  );
}

export interface TitlePanelProps {
  data: CompanyStatus;
  titles: TitleSearch;
  // The time the card shows how long a run has been going against.
  nowMs: number;
}

export function TitlePanel({ data, titles: t, nowMs }: TitlePanelProps) {
  const { quota, limits, titlePresets } = data;
  const tooMany = t.titles.length > limits.maxTitles;
  const search = t.search;
  const candidates = search?.candidates ?? [];
  const warning = quotaWarning('add_candidate', t.chosen.length, quota.remaining);
  const atLimit = t.chosen.length >= limits.maxContacts;

  return (
    <Flex direction="column" gap="sm">
      <Divider />
      <Text format={{ fontWeight: 'bold' }}>Find people by job title</Text>
      <MultiSelect
        name="title-presets"
        label="Titles to look for"
        value={t.picks}
        options={titlePresets.map((title) => ({ label: title, value: title }))}
        onChange={(values) => t.setPicks(values.map(String))}
      />
      <Input name="title-other" label="Other titles" placeholder="Medical Director, Dietitian" value={t.typed} onInput={(text) => t.setTyped(text)} onChange={(text) => t.setTyped(text)} />
      <Text variant="microcopy">A search uses 1 of your runs. Nothing is added until you choose people.</Text>
      {tooMany ? <Text variant="microcopy">{`Choose at most ${limits.maxTitles} titles.`}</Text> : null}
      <Button variant="secondary" disabled={t.titles.length === 0 || tooMany || t.searching || quota.remaining === 0} onClick={t.find}>
        {t.searching ? 'Searching' : searchButtonText(t.titles.length)}
      </Button>
      {t.gone ? <Text>{t.gone}</Text> : null}
      {search && !search.ready ? <Text>{search.run.status === 'failed' ? (search.run.message ?? 'The search did not finish.') : 'Searching for people. This card updates when it finishes.'}</Text> : null}
      {search?.ready && candidates.length === 0 ? <Text>{search.run.message ?? 'No one found for these titles at this company.'}</Text> : null}
      {search ? <RunDetails run={search.run} nowMs={nowMs} /> : null}
      {candidates.length > 0 ? (
        <Flex direction="column" gap="sm">
          <Text format={{ fontWeight: 'bold' }}>{`Found ${candidates.length} ${candidates.length === 1 ? 'person' : 'people'}`}</Text>
          {candidates.map((c) => (
            <Person
              key={c.candidateId}
              candidate={c}
              ticked={t.chosen.includes(c.candidateId)}
              readOnly={isBeingAdded(c) || (atLimit && !t.chosen.includes(c.candidateId))}
              nowMs={nowMs}
              onChange={(checked) => t.toggle(c.candidateId, checked)}
            />
          ))}
          {warning ? <Text variant="microcopy">{warning}</Text> : null}
          <Button variant="secondary" disabled={t.chosen.length === 0 || t.adding || quota.remaining === 0} onClick={t.add}>
            {t.adding ? 'Adding' : addButtonText(t.chosen.length)}
          </Button>
        </Flex>
      ) : null}
      {t.gaveUp && t.open ? <Text>Still waiting for the service to finish. Press Refresh to check again.</Text> : null}
    </Flex>
  );
}
