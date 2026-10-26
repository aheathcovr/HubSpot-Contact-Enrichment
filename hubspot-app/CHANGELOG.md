# Changelog

One entry per build of the Covr Enrichment app: HubSpot developer project `covr-enrichment-app`. Every build is its
own commit on `main`, so `main` is what has been approved to upload. The tests are
`cd hubspot-app/test && npm install && npm run check`.

| Build | Uploaded | Commit | Tests | New permissions |
| --- | --- | --- | --- | --- |
| 1 | Not uploaded | this branch | 137 JavaScript, 7 contract | First install: companies read and write, contacts read |

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
