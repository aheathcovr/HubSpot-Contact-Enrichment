// Company card, picking contacts: the part below the contact list. A rep ticks contacts above, chooses what to do with
// them here, and starts the runs; the runs on this company follow. The state is in usePicker.ts.
import { Button, Divider, Flex, Select, Tag, Text } from '@hubspot/ui-extensions';
import { agoText, objectiveLabel, runIsOpen, runTag } from './contact-model.ts';
import type { RunView } from './contact-model.ts';
import { quotaWarning, startButtonText } from './company-model.ts';
import type { Picker } from './usePicker.ts';
import { RunDetails } from './RunDetails.tsx';

// The runs shown under the controls. The service sends more; these are the newest.
const SHOWN_RUNS = 5;

function RunBlock({ run, name, nowMs, details }: { run: RunView; name: string; nowMs: number; details: boolean }) {
  const tag = runTag(run);
  return (
    <Flex direction="column" gap="flush">
      <Flex align="center" gap="xs">
        <Tag variant={tag.variant}>{tag.label}</Tag>
        <Text format={{ fontWeight: 'bold' }}>{name ? `${name} · ${objectiveLabel(run.objective)}` : objectiveLabel(run.objective)}</Text>
      </Flex>
      {run.message ? <Text>{run.message}</Text> : null}
      {details ? <RunDetails run={run} nowMs={nowMs} /> : null}
      <Text variant="microcopy">{`Asked by ${run.requestedBy}, ${agoText(run.createdAt, nowMs)}`}</Text>
    </Flex>
  );
}

export interface PickPanelProps {
  picker: Picker;
  // Contact id to name, for the runs. A contact the card has no name for is shown by its id; a run with no contact
  // (a search by job title, an add that has not made its contact yet) is shown by what it is.
  names: Record<string, string>;
  nowMs: number;
  // Runs whose steps are shown somewhere else on the card (the search and the adds below), so they are not shown twice.
  shownElsewhere?: string[];
}

export function PickPanel({ picker, names, nowMs, shownElsewhere = [] }: PickPanelProps) {
  const { data, unavailable } = picker;
  if (!data) return unavailable ? <Text>{`Picking contacts is not available: ${unavailable}`}</Text> : null;

  const { quota, runs, objectives } = data;
  const chosen = objectives.find((o) => o.id === picker.objective);
  const picked = picker.picked.length;
  const warning = quotaWarning(picker.objective, picked, quota.remaining);
  const waiting = runs.some(runIsOpen);

  return (
    <Flex direction="column" gap="sm">
      <Divider />
      <Text format={{ fontWeight: 'bold' }}>Enrich contacts you choose</Text>
      <Select
        name="objective"
        label="What to do"
        value={picker.objective}
        options={objectives.map((o) => ({ label: o.label, value: o.id }))}
        onChange={(value) => picker.setObjective(String(value))}
      />
      {chosen ? <Text variant="microcopy">{`Uses ${chosen.spends}.`}</Text> : null}
      <Text variant="microcopy">{quota.remaining === 0 ? `You have used all ${quota.cap} runs for today.` : `${quota.remaining} of ${quota.cap} runs left today.`}</Text>
      {warning ? <Text variant="microcopy">{warning}</Text> : null}
      <Button variant="secondary" disabled={picked === 0 || picker.starting || quota.remaining === 0} onClick={picker.start}>
        {picker.starting ? 'Starting' : startButtonText(picked)}
      </Button>
      {picker.gaveUp && waiting ? <Text>Still waiting for the runs to finish. Press Refresh to check again.</Text> : null}
      {runs.length > 0 ? (
        <Flex direction="column" gap="sm">
          <Text format={{ fontWeight: 'bold' }}>Recent runs</Text>
          {runs.slice(0, SHOWN_RUNS).map((r) => (
            <RunBlock key={r.runId} run={r} name={r.contactId ? (names[r.contactId] ?? `Contact ${r.contactId}`) : ''} nowMs={nowMs} details={!shownElsewhere.includes(r.runId)} />
          ))}
        </Flex>
      ) : null}
    </Flex>
  );
}
