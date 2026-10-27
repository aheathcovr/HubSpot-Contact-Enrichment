# Changelog

One entry per build of the Covr Enrichment app: HubSpot developer project `covr-enrichment-app`. Every build is its
own commit on `main`, so `main` is what has been approved to upload. The tests are
`cd hubspot-app/test && npm install && npm run check`.

| Build | Uploaded | Commit | Tests | New permissions |
| --- | --- | --- | --- | --- |
| 2 | Not uploaded | this branch | 218 JavaScript, 13 contract | None (one permitted fetch URL: the service's `/app/`) |
| 1 | Merged as #21, not uploaded | `dbe8d3b` | 137 JavaScript, 7 contract | First install: companies read and write, contacts read |

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
  HubSpot's test renderer. Not checked: a real portal, real Firestore, or a deployed build.

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
