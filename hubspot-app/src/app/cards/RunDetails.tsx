// What a run is doing and did, as the cards show it: a spinner with the newest step while the run is going, and every step
// under a heading (open while it is going, closed afterwards). The steps are the service's trail of the run
// (run_trail.py): which vendor was asked what, and what came back. Nothing here is personal: the service keeps counts,
// statuses and reasons, never an email address, a phone number or a name.
import { Accordion, Flex, LoadingSpinner, Tag, Text } from '@hubspot/ui-extensions';
import { elapsedText, liveText, runIsOpen, stepVariant, stepsOf, stepsTitle, vendorLabel } from './contact-model.ts';
import type { RunView } from './contact-model.ts';

// What a run that is going is doing right now, and how long since the rep asked. Nothing for a run that has finished.
export function RunProgress({ run, nowMs }: { run: RunView; nowMs: number }) {
  if (!runIsOpen(run)) return null;
  return <LoadingSpinner label={liveText(run, nowMs)} showLabel size="xs" layout="inline" />;
}

// Every step of a run, each with the vendor it concerns, how it went, and how long after the start it happened. Nothing
// for a run that has no steps (one made before the service kept them).
export function RunSteps({ run }: { run: RunView }) {
  const steps = stepsOf(run);
  if (steps.length === 0) return null;
  return (
    <Accordion title={stepsTitle(run)} defaultOpen={runIsOpen(run)} size="xs">
      <Flex direction="column" gap="xs">
        {steps.map((step, index) => (
          <Flex key={`${step.at}-${index}`} direction="column" gap="flush">
            <Flex align="center" gap="xs">
              <Tag variant={stepVariant(step.result)}>{vendorLabel(step.vendor)}</Tag>
              <Text variant="microcopy">{`+${elapsedText(run.createdAt, step.at)}`}</Text>
            </Flex>
            <Text>{step.text}</Text>
          </Flex>
        ))}
      </Flex>
    </Accordion>
  );
}

export function RunDetails({ run, nowMs }: { run: RunView; nowMs: number }) {
  return (
    <>
      <RunProgress run={run} nowMs={nowMs} />
      <RunSteps run={run} />
    </>
  );
}
