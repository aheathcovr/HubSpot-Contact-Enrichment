# Enrichment card (HubSpot app)

A HubSpot developer project (platform 2026.03) with two cards and one app function. **It has not been uploaded to
HubSpot.** Nothing here runs in HubSpot until someone approves an upload.

- **Enrichment** (right sidebar of company records, phase 1). A rep sees when the company was last enriched, whether
  a run is already waiting, and the contacts linked to it, with each email's check result. One button asks for a run.
- **Contact check** (right sidebar of contact records, first slice of phase 2). A rep chooses what to check for one
  contact: **Find email**, **Find mobile** or **Verify email**. The card shows what the contact has, how many runs the
  rep has left today, and the recent runs on the contact. The design is in `DESIGN-contact-card.md`.

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
- **The Contact check card** calls the enrichment service with `hubspot.fetch`, not an app function. HubSpot signs each
  request and adds who is asking; the service (`hubspot_request_auth.py`, `contact_objectives.py`, `app_run_store.py`
  and the `/app` routes in `webhook_server.py`) checks the signature, takes a place in the rep's daily cap, queues a
  Cloud Task and answers at once. The card then asks for the status every 5 seconds while a run is going, for up to
  3 minutes. The service does the looking and the writing with its own HubSpot token, under the company runs' rules:
  fill blanks only, never delete, no invalid email, a mobile only if Seamless says it is one. The app itself writes
  nothing for this card.
- The function reads the company and its contacts, and writes exactly one thing: `request_to_enrich` on that one
  company. It takes no property name or value from the card. It runs on HubSpot's servers with the app's own token; no
  keys are kept in this folder.

## Files

| File | What it is |
| --- | --- |
| `hsproject.json` | Developer project, platform 2026.03 |
| `src/app/app-hsmeta.json` | The app: private, static auth. Scopes: companies read and write (the flag), contacts read. One permitted fetch URL: the enrichment service's `/app/` |
| `src/app/cards/enrichment-hsmeta.json` | The Enrichment card: sidebar of company records |
| `src/app/cards/contact-check-hsmeta.json` | The Contact check card: sidebar of contact records |
| `src/app/cards/Enrichment.tsx`, `EnrichmentView.tsx` | The Enrichment card |
| `src/app/cards/ContactCheck.tsx`, `ContactCheckView.tsx` | The Contact check card |
| `src/app/cards/model.ts` | What the Enrichment card sends the function, and how an answer becomes words and tags |
| `src/app/cards/contact-model.ts` | The Contact check card: how it calls the service, reads its answers and names a run |
| `src/app/cards/tsconfig.json` | Keep it. Without it the build compiles the card wrongly and it crashes |
| `src/app/functions/enrichment.js` | The app function: `status` and `request`. Holds the property names and the 30 days |
| `test/` | Tests. Not uploaded |
| `DESIGN-contact-card.md` | The design of the Contact check card and the `/app` endpoints, with the defaults chosen |
| `../State Association Contact Enrichment/test_hubspot_app_contract.py` | Fails if the app and the service disagree on the property, its values, the 30 days, or the names the contact card relies on |

## Tests

`cd hubspot-app/test && npm install && npm run check` runs:

- a type check of the card against HubSpot's component types,
- the function against an in-memory HubSpot (every endpoint it calls; reads, the one write, paging, and HubSpot's
  refusals), both cards' logic, the app's permissions and permitted URL, and both cards rendered with HubSpot's test
  renderer and a fake function or fake service (loading, requesting, waiting and checking again, giving up, refusals,
  errors, contacts, the daily cap),
- a build of each card with HubSpot's own card build, checking the bundle.

The service's own tests for the Contact check card are `test_hubspot_request_auth.py`, `test_app_run_store.py`,
`test_contact_objectives.py`, `test_app_endpoints.py` and `test_hubspot_contact_for_app.py` in `State Association
Contact Enrichment/`.

The service's own suite has the contract test above: `python3 -m pytest "State Association Contact Enrichment/test_hubspot_app_contract.py"`.

## Ship it

**Do not upload without the owner's approval.** When it is approved:

1. Run the checks above.
2. `cd hubspot-app && hs project upload`. A "Project lint" warning about lint packages is expected.
3. Approve the app's permissions (Connect App) in HubSpot. Until then the cards show a 403 message.
4. Add the Enrichment card to the sidebar of company records, and the Contact check card to the sidebar of contact
   records, in the page editor.
5. Check them read only on one test company and one test contact first: open the cards, check they show the right
   dates, contacts and email. Request a run only on a company or contact the owner names.

For the Contact check card to work, the service must be turned on for it (see "To turn it on" in
`DESIGN-contact-card.md`): a deployed build that has the `/app` routes, the app's client secret as
`HUBSPOT_APP_CLIENT_SECRET`, and the two Firestore TTL policies. Until the secret is set the `/app` routes answer 503
and the card says the service is not set up. It is not yet confirmed that a private app with static auth can send
signed requests; that needs a check in the portal.

## Change it

Edit, run the tests, then upload again. Things to keep in step:

- The property, its values, or the 30 days changed in the service: `TRIGGER_PROPERTY`, `TRIGGER_VALUES` and
  `REFRESH_DAYS` in `enrichment.js` (the contract test says when).
- A new email check word from the verifier: `CHECKS` in `model.ts`.
- A new permission: `app-hsmeta.json` and the permissions test in `test/config.test.ts`.

## Limits

- Anyone who can open a company, view-only seats included, can ask for a run. A run spends vendor credits. The
  function acts with the app's access, not the user's, and does not record who asked. (The Contact check card is
  different: the service knows who asked from HubSpot's signed request, caps each rep at 25 runs a day and names the
  rep on the run and on the contact's note.)
- Two reps pressing the button in the same second can both pass the check. HubSpot only calls the webhook when the
  property changes, so the second press does not start a second run.
- The card cannot clear a request that is stuck at `true`. The service clears it after every run, whether it ended,
  was skipped or failed. If the webhook subscription is off, the card waits ten minutes and says it is still waiting.
- Only the first 100 linked contacts are read. A larger company says so.
- Dates are in UTC, as the service writes them.
- BounceBan's second opinion is not shown separately: the card shows the verification result the service saved.
- The Contact check card does not offer "still works there", a general enrich, or the company objectives yet. A run
  that is still going after 3 minutes is left to the rep to check with Refresh.
- Nothing here has run against a real HubSpot portal, and nothing has been deployed for the `/app` routes.
