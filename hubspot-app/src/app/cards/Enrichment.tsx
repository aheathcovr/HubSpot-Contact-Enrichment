// Enrichment card (Covr, Oct 2026). HubSpot loads this file in the sidebar of company records.
// It shows where the company stands (last enriched, whether a run is waiting, its contacts and their email checks)
// and asks for a run. It calls the enrichment app function (src/app/functions/enrichment.js) for the company's status
// and the request, and the enrichment service, through the service function (functions/service.js), to start runs for
// the contacts a rep ticks and to find people by job title.
import { hubspot } from '@hubspot/ui-extensions';
import { EnrichmentView } from './EnrichmentView.tsx';
import { makeService } from './contact-model.ts';

const service = makeService((name, options) => hubspot.serverless(name, options));

hubspot.extend<'crm.record.sidebar'>(({ context, actions }) => (
  <EnrichmentView context={context} actions={actions} run={hubspot.serverless} service={service} />
));
