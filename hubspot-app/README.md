# Enrichment card (HubSpot app)

A HubSpot developer project (platform 2026.03) with one card and one app function. **Phase 1 of the plan on the
HubSpot app tab of the Enrichment Service Map. It has not been uploaded to HubSpot.** Nothing here runs in HubSpot
until someone approves an upload.

- **Enrichment** (right sidebar of company records). A rep sees when the company was last enriched, whether a run is
  already waiting, and the contacts linked to it, with each email's check result. One button asks for a run.

It is modelled on Noah's `covr-rollout-checklist` app: one project, a private app with static auth, a thin card, the
logic in a model file, one app function with no dependencies, an in-memory HubSpot for tests, and a changelog with one
entry per build.

## How it works

- **Asking for a run** sets `request_to_enrich` to `true` on the company. HubSpot reacts to that property change and
  calls the enrichment service's `/webhook`, which queues the run (`webhook_server.py`). **The service did not change
  for this app.** When the run ends the service sets the property back to `false` and stamps
  `revops_enrichment_date`; the card reads both.
- **The 30-day rule.** The service skips a company it enriched in the last 30 days (`enrichment_processor.py`), so a
  request would do nothing. The function refuses it up front and the card says when a new run can be asked for. A
  date is read the way the service reads it: digits are epoch milliseconds, anything else is its first ten characters
  as `YYYY-MM-DD`; a date that cannot be read counts as never enriched. A run is "waiting" while the property is true.
- **The card** asks the function for the status when it opens, and again every 10 seconds while a run is waiting,
  for up to 10 minutes. Then it says it is still waiting and leaves it to the rep to press Refresh. A check that fails
  while waiting is skipped without a word; a Refresh that fails says so.
- **Contacts** are the first 100 linked to the company. The ones the service enriched (those with
  `most_recent_enrichment_date`) come first, newest first, then the rest by last name. The email check is
  MillionVerifier's own word for the address (`ok`, `catch_all`, `unknown`, `invalid`, ...), shown as Good, Risky,
  Unknown, Invalid and so on.
- The function reads the company and its contacts, and writes exactly one thing: `request_to_enrich` on that one
  company. It takes no property name or value from the card. It runs on HubSpot's servers with the app's own token; no
  keys are kept in this folder.

## Files

| File | What it is |
| --- | --- |
| `hsproject.json` | Developer project, platform 2026.03 |
| `src/app/app-hsmeta.json` | The app: private, static auth. Scopes: companies read and write (the flag), contacts read |
| `src/app/cards/enrichment-hsmeta.json` | The card: sidebar of company records |
| `src/app/cards/Enrichment.tsx`, `EnrichmentView.tsx` | The card |
| `src/app/cards/model.ts` | What the card sends the function, and how an answer becomes words and tags |
| `src/app/cards/tsconfig.json` | Keep it. Without it the build compiles the card wrongly and it crashes |
| `src/app/functions/enrichment.js` | The app function: `status` and `request`. Holds the property names and the 30 days |
| `test/` | Tests. Not uploaded |
| `../State Association Contact Enrichment/test_hubspot_app_contract.py` | Fails if the app and the service disagree on the property, its values, or the 30 days |

## Tests

`cd hubspot-app/test && npm install && npm run check` runs:

- a type check of the card against HubSpot's component types,
- the function against an in-memory HubSpot (every endpoint it calls; reads, the one write, paging, and HubSpot's
  refusals), the card logic, the app's permissions, and the card rendered with HubSpot's test renderer and a fake
  function (loading, requesting, waiting and checking again, giving up, refusals, errors, contacts),
- a build of the card with HubSpot's own card build, checking the bundle.

The service's own suite has the contract test above: `python3 -m pytest "State Association Contact Enrichment/test_hubspot_app_contract.py"`.

## Ship it

**Do not upload without the owner's approval.** When it is approved:

1. Run the checks above.
2. `cd hubspot-app && hs project upload`. A "Project lint" warning about lint packages is expected.
3. Approve the app's permissions (Connect App) in HubSpot. Until then the card shows a 403 message.
4. Add the card to the sidebar of company records in the page editor.
5. Check it read only on one test company first: open the card, check it shows the right date and contacts. Request a
   run only on a company the owner names.

## Change it

Edit, run the tests, then upload again. Things to keep in step:

- The property, its values, or the 30 days changed in the service: `TRIGGER_PROPERTY`, `TRIGGER_VALUES` and
  `REFRESH_DAYS` in `enrichment.js` (the contract test says when).
- A new email check word from the verifier: `CHECKS` in `model.ts`.
- A new permission: `app-hsmeta.json` and the permissions test in `test/config.test.ts`.

## Limits

- Anyone who can open a company, view-only seats included, can ask for a run. A run spends vendor credits. The
  function acts with the app's access, not the user's, and does not record who asked. A cap per person, and who asked
  on the run's note, are phase 2.
- Two reps pressing the button in the same second can both pass the check. HubSpot only calls the webhook when the
  property changes, so the second press does not start a second run.
- The card cannot clear a request that is stuck at `true`. The service clears it after every run, whether it ended,
  was skipped or failed. If the webhook subscription is off, the card waits ten minutes and says it is still waiting.
- Only the first 100 linked contacts are read. A larger company says so.
- Dates are in UTC, as the service writes them.
- BounceBan's second opinion is not shown separately: the card shows the verification result the service saved.
- Nothing here has run against a real HubSpot portal.
