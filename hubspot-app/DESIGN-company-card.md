# Company card: pick contacts and find people by job title

Build 3 of the Covr Enrichment app. **Built test-first. Not uploaded to HubSpot, not deployed, never run against a real
portal.** It adds two things to the Enrichment card on company records, both made of the same runs the contact card
makes (queued, done by the same worker, shown with the same tags). It stands on the contact card's pieces (signed
requests, run records, the daily cap), which are in the pull request before it.

## What a rep can do

**Pick contacts.** The card already lists the company's contacts. Each gets a checkbox. The rep chooses what to do
and starts it for the ones ticked (up to 10).

| Choice | What it runs, per contact |
| --- | --- |
| Find email | the contact card's Find email, if the contact has no email |
| Find mobile | Find mobile, if the contact has no mobile |
| Verify email | Verify email, if the contact has an email |
| Enrich (the default) | whichever of these the contact is missing: a missing email is looked for, an email that has never been checked is checked, a missing mobile is looked for. An email that already has a check is left alone. Up to 2 runs per contact |

A contact the choice does not apply to is skipped with the reason, and costs nothing. A contact that is not linked to
this company is never run.

**Find people by job title.** The rep picks from the titles the company runs look for (the facility's three on a
facility, the corporate list on a corporation) and can type their own, up to 5 titles. Then two steps:

1. **Search.** The service asks FullEnrich for people holding those titles at this company. A preview: **nothing is
   written to HubSpot.** The card shows the people found (name, title, and whether they are already a contact here).
2. **Add.** The rep ticks who to add (up to 10). Each person goes through the step a company run uses for a person it
   found: the identity gate on any vendor email, MillionVerifier, an existing HubSpot contact reused only when
   `contact_match_guard` says it is safe, otherwise a new contact, with the same title routing deciding where it is
   linked (a corporate title on a facility goes to the operator). The new contact gets a note saying who asked and what
   was searched for.

## The rules

They are the company runs' rules, not new ones:

- Fill blanks only, never delete, no invalid email, a mobile only if Seamless says it is one.
- Nobody is created or linked unless a rep chose them in the add step.
- Custom titles are searched as the rep wrote them. A corporation's own title list keeps the seniority filters the
  company run uses; any other title is searched without them, because they would silently drop a manager the rep asked
  for.
- A search that cannot run (no FullEnrich key, no credits left) says so. It never reports "no one found".
- These runs do not set `revops_enrichment_date` and are not held back by the 30-day company guard.

## Endpoints

All reach the service through the app's service function, which signs them (`app_request_auth.py`; see "Authentication
and identity" in `DESIGN-contact-card.md`); the rep is the one the signed query names. Until `APP_REQUEST_SECRET` is
set they answer 503.

| Route | Body or query | Answer |
| --- | --- | --- |
| `GET /app/company/status` | `companyId` | company (id, name, kind), the choices, `titlePresets`, limits (`maxContacts` 10, `maxTitles` 5), the rep's quota, the company's recent runs |
| `POST /app/company/run` | `{companyId, contactIds, objective}` | 202 `{batchId, items, started, quota}`; one item per run or skip |
| `POST /app/company/find` | `{companyId, titles}` | 202 `{reused, run, quota}` |
| `GET /app/company/candidates` | `companyId`, `runId` | `{ready, run, candidates}`; 410 once the results have expired |
| `POST /app/company/add` | `{companyId, runId, candidateIds}` | 202 `{batchId, items, started, quota}` |

Refusals: 400 for an ill-formed request, 404 for a company or search that is not there (or not this company's), 409
when nothing could be started, the search is not finished, or (for a search) the company has no usable Company Type, 410 for expired results, 429 when the cap would be
passed, 502 when HubSpot or the queue is down.

## Defaults chosen (easy to change)

- **Cap.** Every run takes one place in the rep's 25 a day, as on the contact card: each contact run, each search, each
  add. A request that needs more places than are left starts nothing (all or none); places taken for runs that cannot be
  queued are given back.
- **Repeats.** The same thing asked of the same contact within 24 hours is the earlier run, and costs nothing. The same
  titles searched on the same company within 24 hours (any order or case) are the earlier search. An add is the earlier
  run for the same person *from the same search*; a failed run never stands in for a new one.
- **Limits.** 10 contacts or people at a time, 5 titles of at most 80 characters.
- **Where the people are kept.** A search keeps the people it found in `app_candidates` for **2 days** (names, titles,
  LinkedIn addresses). That is the one place this app stores people's names. Run records still hold ids, statuses and
  short messages only (no names, emails or numbers), for 30 days.
- **The card brings back** the newest search made in the last 24 hours that did not fail, when it is opened again.

## What a run shows

Every run, on this card and the contact card, keeps a trail of steps (what each vendor said back) that the card shows live and
afterwards; see "What a run shows: the trail" in DESIGN-contact-card.md. On this card the title search's steps sit with the
search, each added person's with that person, and the recent runs show the rest (so no run is shown twice). The card asks again
every 2 seconds, for up to 3 minutes.

## When something goes wrong

| What happened | What the rep sees |
| --- | --- |
| The company's Company Type is blank, or is not Facility or Corporation | The card says so and shows HubSpot's own editor for Company Type; the request and the contact tools wait until the rep sets it and presses Refresh. A request that slips through (a workflow, an import) gets a note asking for the type, no research, and its enrichment date is left alone |
| The service is not set up (no secret) | "Picking contacts is not available" and the rest of the card works |
| No FullEnrich key, or no credits | The search run fails with that reason in words |
| A search that could not be done (a rejected key, a vendor that was down) and found nobody | The run is failed: "FullEnrich could not be searched, so this is not a finding that no one is there: see What happened." Nothing is kept. One that found people but could not search a title says "Part of the search could not be done" beside them |
| Nobody found | "No one found for these titles at this company.", with the searches made and why each person who came back was not used under "What happened" |
| The results expired | "Those search results are no longer available. Search again." |
| The cap would be passed | "This needs N runs and you have M left today." and nothing starts |
| A person in a request is not in the results, or a contact is not on the company | Skipped, named, with the reason |
| A run cannot be queued | That one is marked failed and gives its place back; the others go ahead |
| A check fails while waiting | Skipped quietly; "Still waiting" after 3 minutes, and Refresh starts it again |

## Not verified

- How well FullEnrich covers non-standard titles. Its name-and-domain lookup missed 11 of 12 people earlier and its
  title search has not been measured on custom titles. Try about 20 known people against a few custom titles before
  relying on it.
- A search may return someone who has since left. The add step applies the same identity gate as a company run, but a
  title search does not check that a person still works there.
- Nothing here has run against a real HubSpot portal, real Firestore or a deployed build.

## To turn it on

Everything in "To turn it on" in `DESIGN-contact-card.md`, and also:

1. `FULLENRICH_API_KEY` is set on the service and has credits (the company runs already need it).
2. Apply `setup_firestore.sh` so `app_candidates` has its TTL policy (it also covers `app_runs` and `app_quota`).
3. Upload the app to HubSpot, with approval.
