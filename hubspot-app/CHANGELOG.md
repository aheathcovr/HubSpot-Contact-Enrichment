# Changelog

One entry per build of the Covr Enrichment app: HubSpot developer project `covr-enrichment-app`. Every build is its
own commit on `main`, so `main` is what has been approved to upload. The tests are
`cd hubspot-app/test && npm install && npm run check`.

| Build | Uploaded | Commit | Tests | New permissions |
| --- | --- | --- | --- | --- |
| 4 | Not uploaded | this branch | 422 JavaScript, 34 contract | None. Removes the permitted fetch URL; adds a function secret, `APP_REQUEST_SECRET` |
| 3 | Merged as #23, not uploaded | `c518009` | 323 JavaScript, 27 contract | None (the same permitted fetch URL) |
| 2 | Test account only (HubSpot build #2; build #1 failed to package) | `363af0b` | 218 JavaScript, 13 contract | None (one permitted fetch URL: the service's `/app/`) |
| 1 | Merged as #21, not uploaded | `dbe8d3b` | 137 JavaScript, 7 contract | First install: companies read and write, contacts read |

## Build 4: the cards reach the service through an app function

- A static-auth app has no client secret (its page has no Auth tab), so HubSpot cannot sign a card's `hubspot.fetch` and
  Builds 2 and 3, which called the service that way, could not have worked. The cards now call a new app function,
  `service`, which says who is asking from HubSpot's own context (the user and the portal, set by HubSpot and not
  overridable by a card), forwards only the seven requests the cards make, and signs each with a secret it shares with
  the service. The cards' views and logic did not change, only the transport.
- The service checks that signature (HubSpot's v3 scheme, so its published example checks both ends) in
  `app_request_auth.py` (renamed from `hubspot_request_auth.py`), with `X-Covr-App-Signature` and
  `X-Covr-App-Timestamp`. The secret is `APP_REQUEST_SECRET` (was `HUBSPOT_APP_CLIENT_SECRET`, which cannot exist). The
  address it checks is the one the caller used, so no setting is needed.
- `setup_app_secret.sh` (dry run by default, not run) makes the secret: Secret Manager for the service, your clipboard
  for HubSpot's hidden `hs secret add` prompt. It never prints the value or replaces an existing secret.
- The app no longer asks for any permitted fetch URL.
- Checked: the function against fakes and a local server, the signature against HubSpot's published example and four
  values the service's code made, and the real function run in Node against the real check (accepted when untouched,
  refused once who asked or what was asked is changed). Not checked: the function calling the service from a real
  HubSpot portal, which needs a deploy, the secret and an upload.

## Build 3: Company card, pick contacts and find by job title

- The Enrichment card on company records gains a checkbox beside each contact, a choice of what to do (Find email,
  Find mobile, Verify email, or Enrich, which fills what is missing) and a start button for up to 10 contacts.
- It also gains "Find people by job title": quick picks of the titles the company runs look for, a box for the rep's
  own, a search that previews the people FullEnrich has (nothing is written), and an add step for the ones chosen.
  An add goes through the step a company run uses for a person it found, so the identity gate, the email check and
  the rule for reusing an existing contact all apply.
- Both are runs like the contact card's: queued, one place each in the rep's 25 a day, shown with who asked. The
  service gains `/app/company/status`, `/app/company/run`, `/app/company/find`, `/app/company/candidates` and
  `/app/company/add`, `company_titles.py`, a planner for Enrich in `contact_objectives.py`, and `app_candidates` in
  Firestore (names and titles, 2 days; its TTL policy is in `setup_firestore.sh`, dry run by default, not applied).
- No new permissions: the card calls the service under the same permitted URL. The design is
  `DESIGN-company-card.md`.
- Checked: the service pieces and both cards against fakes of HubSpot, FullEnrich, Cloud Tasks and Firestore, and with
  HubSpot's test renderer; every deliberate break of the new code fails a test. Not checked: a real portal, real
  Firestore, a deployed build, or FullEnrich's coverage of custom job titles.

## Build 2: Contact check card, first slice

- The Contact check card in the sidebar of contact records: Find email, Find mobile and Verify email for one contact,
  what the contact has, the rep's runs left today, and the recent runs on the contact with who asked.
- The card calls the enrichment service with `hubspot.fetch`. The app gains one permitted URL, the service's `/app/`;
  no new scopes. The design is `DESIGN-contact-card.md`.
- The service gains `/app/contact/status`, `/app/contact/run` and `/app/run`: HubSpot's request signature and the
  rep's identity (`hubspot_request_auth.py`), run records and a 25-a-day cap per rep in Firestore
  (`app_run_store.py`), and the three objectives under the company runs' rules (`contact_objectives.py`). They answer
  503 until `HUBSPOT_APP_CLIENT_SECRET` is set.
- `setup_firestore.sh` gains TTL policies for `app_runs` and `app_quota` (dry run by default; not applied).
- Checked: the service pieces against fakes of HubSpot, the vendors, Cloud Tasks and Firestore, and the card with
  HubSpot's test renderer. Not checked: real Firestore or a deployed build. Uploaded to the `covr-handoff-test`
  account only (HubSpot build #2), where nothing reaches the service.

## Build 1: Enrichment card, phase 1

- The Enrichment card in the sidebar of company records: when the company was last enriched, whether a run is
  waiting, the contacts linked to it with each email's check result, and a Request enrichment button.
- The app function `enrichment` with two actions. `status` reads the company and up to 100 contacts. `request` sets
  Request to enrich on the company, and only when the service would not skip the run: not when one is already waiting,
  not within 30 days of the last one.
- No change to the enrichment service. HubSpot's own property-change webhook starts the run.
- The service's tests gained `test_hubspot_app_contract.py`, which fails if the app and the service disagree on the
  property, its values or the 30 days.
- Checked: the function against an in-memory HubSpot, the card with HubSpot's test renderer, a type check, and a
  build of the card with HubSpot's own build. Not checked: any real HubSpot portal.
