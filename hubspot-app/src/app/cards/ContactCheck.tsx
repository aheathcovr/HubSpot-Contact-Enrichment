// Contact check card (Covr, Oct 2026). HubSpot loads this file in the sidebar of contact records.
// A rep chooses what to check for this contact: Find email, Find mobile, or Verify email. The card calls the enrichment
// service through the app's service function (functions/service.js, which says who is asking from HubSpot's context
// and signs the request), shows the runs on this contact, and checks again while one is going.
import { hubspot } from '@hubspot/ui-extensions';
import { ContactCheckView } from './ContactCheckView.tsx';
import { makeService } from './contact-model.ts';

const service = makeService((name, options) => hubspot.serverless(name, options));

hubspot.extend<'crm.record.sidebar'>(({ context, actions }) => (
  <ContactCheckView context={context} actions={actions} service={service} />
));
