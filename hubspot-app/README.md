# Enrichment card (HubSpot app)

A HubSpot developer project (platform 2026.03) with two cards and one app function. **It has not been uploaded to
HubSpot.** Nothing here runs in HubSpot until someone approves an upload.

- **Enrichment** (right sidebar of company records). A rep sees when the company was last enriched, whether a run is
  already waiting, and the contacts linked to it, with each email's check result. One button asks for a whole-company
  run. Build 3 adds two more things: **tick contacts and choose what to do with them** (Find email, Find mobile, Verify
  email, or Enrich, which fills what is missing), and **find people by job title** (a preview of who FullEnrich has,
  then add the ones chosen). The design is in `DESIGN-company-card.md`.
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
- **The Contact check card** calls the enrichment service through the app's `service` function (`service.js`), not
  `hubspot.fetch`: a static-auth app has no client secret, so HubSpot cannot sign a fetch. The function says who is
  asking from HubSpot's own context, forwards only the seven requests the cards make, and signs each with a secret it
  shares with the service; the service (`app_request_auth.py`, `contact_objectives.py`, `app_run_store.py` and the
  `/app` routes in `webhook_server.py`) checks the signature, takes a place in the rep's daily cap, queues a Cloud
  Task and answers at once. The card then asks for the status every 5 seconds while a run is going, for up to
  3 minutes. The service does the looking and the writing with its own HubSpot token, under the company runs' rules:
  fill blanks only, never delete, no invalid email, a mobile only if Seamless says it is one. The app itself writes
  nothing for this card.
- **The Enrichment card's picking and title search** call the enrichment service through the same function, like the
  Contact check card (`usePicker.ts`, `useTitleSearch.ts`, `company-model.ts` and the `/app/company/...` routes in
  `webhook_server.py`, with `company_titles.py` and `contact_objectives.py` behind them). A pick queues one run per
  contact. A title search writes nothing to HubSpot: it keeps the people it found for two days, and only adding one
  does a write, through the step a company run uses for a person it found. The card reads the company's contacts
  from the app function and the runs, quota and search results from the service.
- The function reads the company and its contacts, and writes exactly one thing: `request_to_enrich` on that one
  company. It takes no property name or value from the card. It runs on HubSpot's servers with the app's own token; no
  keys are kept in this folder.

## Files

| File | What it is |
| --- | --- |
| `hsproject.json` | Developer project, platform 2026.03 |
| `src/app/app-hsmeta.json` | The app: private, static auth. Scopes: companies read and write (the flag), contacts read. No permitted fetch URLs: the cards reach the service through the service function |
| `src/app/cards/enrichment-hsmeta.json` | The Enrichment card: sidebar of company records |
| `src/app/cards/contact-check-hsmeta.json` | The Contact check card: sidebar of contact records |
| `src/app/cards/Enrichment.tsx`, `EnrichmentView.tsx` | The Enrichment card |
| `src/app/cards/ContactCheck.tsx`, `ContactCheckView.tsx` | The Contact check card |
| `src/app/functions/service.js`, `service-hsmeta.json` | The service function: carries the cards' requests to the enrichment service, says who is asking from HubSpot's context and signs them with `APP_REQUEST_SECRET`. Holds the service's address and the seven requests it forwards |
| `../setup_app_secret.sh` | Makes `APP_REQUEST_SECRET` (dry run by default): Secret Manager for the service, your clipboard for `hs secret add`. Never prints the value |
| `src/app/cards/company-model.ts` | The Enrichment card's picking and title search: what it asks the service and how it words the answers |
| `src/app/cards/usePicker.ts`, `PickPanel.tsx` | Ticking contacts, choosing what to do, the runs on the company |
| `src/app/cards/useTitleSearch.ts`, `TitlePanel.tsx` | Choosing titles, the search, the people found, adding them |
| `src/app/cards/model.ts` | What the Enrichment card sends the function, and how an answer becomes words and tags |
| `src/app/cards/contact-model.ts` | The Contact check card: how it calls the service, reads its answers and names a run |
| `src/app/cards/tsconfig.json` | Keep it. Without it the build compiles the card wrongly and it crashes |
| `src/app/functions/enrichment.js` | The app function: `status` and `request`. Holds the property names and the 30 days |
| `test/` | Tests. Not uploaded |
| `DESIGN-contact-card.md` | The design of the Contact check card and the `/app` endpoints, with the defaults chosen |
| `DESIGN-company-card.md` | The design of picking contacts and finding people by job title, with the defaults chosen |
| `../State Association Contact Enrichment/test_hubspot_app_contract.py` | Fails if the app and the service disagree on the property, its values, the 30 days, or the names the contact card relies on |

## Tests

`cd hubspot-app/test && npm install && npm run check` runs:

- a type check of the card against HubSpot's component types,
- the enrichment function against an in-memory HubSpot (every endpoint it calls; reads, the one write, paging, and
  HubSpot's refusals), the service function (which requests go through, who is asking, the signature, the sender),
  both cards' logic, the app's permissions and functions, and both cards rendered with HubSpot's test
  renderer and a fake function or fake service (loading, requesting, waiting and checking again, giving up, refusals,
  errors, contacts, the daily cap),
- a build of each card with HubSpot's own card build, checking the bundle.

The service's own tests for the Contact check card are `test_app_request_auth.py`, `test_app_run_store.py`,
`test_contact_objectives.py`, `test_app_endpoints.py`, `test_hubspot_contact_for_app.py` and `test_setup_app_secret.py`
in `State Association Contact Enrichment/`; for the company card's picking and title search, `test_app_company_endpoints.py`,
`test_app_title_endpoints.py` and `test_company_titles.py` (and a few more cases in the run store, objectives and
FullEnrich tests).

The service's own suite has the contract test above: `python3 -m pytest "State Association Contact Enrichment/test_hubspot_app_contract.py"`.

## Ship it

**Do not upload without the owner's approval.** When it is approved:

1. Run the checks above.
2. Add the function's secret first, in each account (`../setup_app_secret.sh` copies it for you; see
   `DESIGN-contact-card.md`), then upload from a path with no hidden folder in it (the CLI skips `.claude/`): `hs
   project upload --account <id>`. A "Project lint" warning about lint packages is expected.
3. Approve the app's permissions (Connect App) in HubSpot. Until then the cards show a 403 message.
4. Add the Enrichment card to the sidebar of company records, and the Contact check card to the sidebar of contact
   records, in the page editor. The Enrichment card's picking and title search need the service turned on as well
   (see "To turn it on" in the two design notes); until then the card says picking is not available and the rest of it
   works.
5. Check them read only on one test company and one test contact first: open the cards, check they show the right
   dates, contacts and email. Request a run only on a company or contact the owner names.

For the Contact check card to work, the service must be turned on for it (see "To turn it on" in
`DESIGN-contact-card.md`): a deployed build that has the `/app` routes, the shared secret as `APP_REQUEST_SECRET` (in
Secret Manager and as the function's HubSpot secret), and the Firestore TTL policies. Until the secret is set the `/app`
routes answer 503 and the card says the service is not set up. The service accepts only the portal named in
`HUBSPOT_PORTAL_ID` (the deploy sets it to Covr_HubSpot, 24434722), so the app works in that account only. Not yet confirmed in a real portal: that the service
function can call out to the service and be accepted. That is only proven after a deploy.

## Change it

Edit, run the tests, then upload again. Things to keep in step:

- The property, its values, or the 30 days changed in the service: `TRIGGER_PROPERTY`, `TRIGGER_VALUES` and
  `REFRESH_DAYS` in `enrichment.js` (the contract test says when).
- A new email check word from the verifier: `CHECKS` in `model.ts`.
- A new permission: `app-hsmeta.json` and the permissions test in `test/config.test.ts`.
- A request the cards make, added or removed: `ROUTES` in `service.js` and the service's `/app` routes (the contract test
  says when). The service's address: `SERVICE_BASE` in `service.js`.

## Limits

- Anyone who can open a company, view-only seats included, can ask for a run. A run spends vendor credits. The
  function acts with the app's access, not the user's, and does not record who asked. (The Contact check card is
  different: the service function passes on who asked from HubSpot's own context and signs it, the service caps each rep at 25 runs a day and names the
  rep on the run and on the contact's note.)
- Two reps pressing the button in the same second can both pass the check. HubSpot only calls the webhook when the
  property changes, so the second press does not start a second run.
- The card cannot clear a request that is stuck at `true`. The service clears it after every run, whether it ended,
  was skipped or failed. If the webhook subscription is off, the card waits ten minutes and says it is still waiting.
- Only the first 100 linked contacts are read. A larger company says so.
- Dates are in UTC, as the service writes them.
- BounceBan's second opinion is not shown separately: the card shows the verification result the service saved.
- Nothing offers "still works there" yet, and a title search does not check that a person found still works at the
  company. A run that is still going after 3 minutes is left to the rep to check with Refresh.
- A search or a run spends one of the rep's 25 places a day, and anyone who can open a company can ask. A title search
  is only as good as FullEnrich's coverage of the titles asked for; that has not been measured for custom titles.
- Nothing here has run against a real HubSpot portal, and nothing has been deployed for the `/app` routes.
