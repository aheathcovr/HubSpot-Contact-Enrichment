# Contact card, first slice: design

Status: built test-first and merged. **Not uploaded to HubSpot and not deployed.** The service endpoints answer 503
until the shared secret `APP_REQUEST_SECRET` is configured (see "To turn it on"). Since Build 4 the card reaches the
service through the app's service function, not `hubspot.fetch` (see "Authentication and identity").

## What this slice does

A sidebar card on **contact** records with three objectives a rep can choose:

| Objective | What it does | Spends |
| --- | --- | --- |
| **Find email** | Looks for a work email for this contact: FullEnrich first, Seamless as a fallback. The address must be at the facility's or operator's domain and must not verify as invalid. | Up to 2 lookups and 1 check |
| **Find mobile** | Asks Seamless for a mobile number. | 1 lookup |
| **Verify email** | Checks the email the contact already has (MillionVerifier, and BounceBan when MillionVerifier cannot settle it) and saves the answer on the contact. | 1 check, sometimes 2 |

The card also shows what the contact has, how many runs the rep has left today, and the recent runs on this contact
(who asked, what happened). Not in this slice: "still works there", a general enrich, and the company objectives.

## Rules the service keeps (unchanged from the company runs)

- **Fill blanks only.** Find email is not offered when the contact has an email (use Verify). Find mobile is not offered
  when the contact has a mobile. The service re-reads the contact right before it writes, so an address added in the
  meantime is never replaced.
- **Never delete.** Nothing removes or overwrites a value. Verify changes only the two verification properties.
- **Identity gate.** A vendor's email that is not at the facility's or operator's domain is held, not written. The
  note on the contact says so.
- **No invalid email is written.** MillionVerifier INVALID suppresses it.
- **Dialable phones only.** Seamless's own screening applies: not do-not-call, not the facility's main line, North
  American, above the confidence floor. For this objective the number must also be of type mobile. Any other type is
  reported and not written.
- **Someone who has moved.** If Seamless shows the person at another organisation, nothing is used, and the card says
  so.
- **A note on the contact** for every run: who asked, what was asked, what happened. Notes carry the values; the run
  records do not.
- **Writes are limited to** `email` (when blank), `mobilephone` (when blank), `email_verification_result`,
  `email_verification_date`, and the note. A run does **not** set `most_recent_enrichment_date`: that stamp belongs to
  company runs and gates them.

## Defaults chosen (to confirm)

- **Cap:** 25 runs per person per UTC day (`APP_DAILY_RUN_CAP`). A run counts when it is accepted. A run that could
  not be queued is given back.
- **No double spend:** asking for the same objective on the same contact within 24 hours returns the earlier run (not
  a failed one) and spends nothing.
- **Mobile only** for Find mobile.
- **Everyone who can open a contact can ask**, within the cap. The service cannot tell roles apart yet.
- **Run records hold ids, statuses and short messages, not email addresses or phone numbers.** They expire after 30
  days.

## Flow

```
card -- hubspot.serverless --> service function (adds who is asking from HubSpot's context, signs)
         service function -- https --> GET  /app/contact/status?contactId=...   what the contact has, quota, recent runs
         service function -- https --> POST /app/contact/run                    accepts a run, queues a Cloud Task
Cloud Tasks (Google-signed) ---------> POST /app/run                           does the work, updates the run record
card polls /app/contact/status every 5 s while a run is queued or running
```

A run takes from a few seconds to over a minute, and HubSpot cuts a fetch off at 15 seconds by default and retries a
5xx once. So the start call returns at once (202) and the work runs in a Cloud Task, as company runs do. The retry is
harmless: a repeat start for the same contact and objective finds the earlier run.

### `GET /app/contact/status?contactId=<id>`

```json
{ "ok": true,
  "contact": { "id": "1", "name": "Pat Lee", "title": "Administrator", "email": "pat.lee@example.org", "mobile": null,
               "emailCheck": "ok", "emailCheckedOn": "2026-10-21" },
  "objectives": [ { "id": "find_email", "label": "Find email", "available": false, "reason": "Already has an email. Use Verify email.", "spends": "up to 2 lookups and 1 check" } ],
  "quota": { "cap": 25, "used": 3, "remaining": 22 },
  "runs": [ { "runId": "…", "objective": "find_mobile", "status": "done", "outcome": "found", "message": "Mobile number added.",
              "wrote": ["mobilephone"], "requestedBy": "rep@example.org", "createdAt": "2026-10-26T16:00:00Z", "finishedAt": "…" } ] }
```

### `POST /app/contact/run` body `{ "contactId": "1", "objective": "find_email" }`

- `202 { ok, run, quota, reused }` accepted (or an earlier run returned, `reused: true`).
- `400` bad contact id or objective. `409` the objective is not available (`reason` says why). `429` the cap is used
  up (`quota` says how many). `502` the run could not be queued. `503` the service is not configured for the app.

### `POST /app/run` (Cloud Tasks only)

Body `{ "run_id": "…" }`. Claims the run (queued to running, once), does it, records the outcome, and answers 200
whether it worked or not, so Cloud Tasks never retries paid work.

## Authentication and identity

**Why not `hubspot.fetch`.** HubSpot signs a card's fetch with the app's client secret. A static-auth app has none (its
page in HubSpot has no Auth tab; found 2026-10-26, and HubSpot's function reference says OAuth apps cannot use the
2026.03 functions this app needs), so a fetch would reach the service unsigned and the service could not tell who asked.

**What it does instead.** The card calls the app's `service` function (`functions/service.js`) with `{ method, path, body }`.
HubSpot puts the user and the portal in the function's context on its own server, and card parameters cannot override
them (HubSpot's docs say to authorize on `portalId` and `user.userId`, as the service does, because the email can be
missing). The function:

1. Forwards only the seven requests the cards make (`ROUTES`), with only the query keys each takes. Anything else, and
   any query key that says who is asking, is refused. It is not a way to reach the rest of the service.
2. Adds `userId`, `userEmail` (when HubSpot gives one) and `portalId` to the query from HubSpot's context.
3. Signs method + address + body + timestamp with the shared secret `APP_REQUEST_SECRET`, using HubSpot's v3 scheme
   (base64 HMAC-SHA256; the characters HubSpot lists decoded before signing), and sends it in `X-Covr-App-Signature` and
   `X-Covr-App-Timestamp`.
4. Returns the service's answer as `{ status, json }`, or its own refusal in the same shape (503 not set up, 401 HubSpot
   did not say who is asking, 400 not a request the app makes, 504 or 502 the service did not answer). It never
   returns, logs or sends the secret.

The service (`app_request_auth.py`, used by `webhook_server.py`):

1. Fails closed (503) when `APP_REQUEST_SECRET` is not set.
2. Rejects (401) a missing, stale (over 5 minutes) or wrong signature. The address checked is the one the caller used,
   built from the Host header and the scheme Cloud Run's front end reports, so it needs no setting.
3. Rejects (403) a request for another portal when `HUBSPOT_PORTAL_ID` is set.
4. Takes the rep's identity from the signed query, never from the body.

The scheme is HubSpot's own, so its published example checks both implementations, and four golden signatures made by
the service's code are pinned in the function's tests too (see "Tests"). A leaked secret would let someone act as any
user, so it lives in two places only: Secret Manager and the function's HubSpot secret. `setup_app_secret.sh` makes it
without ever printing it.

`/app/run` is not signed by the function; like `/enrich` it accepts only Google-signed Cloud Tasks requests.

## Run records and the cap

Firestore collection `app_runs` (ids, statuses, short messages, `expire_at` 30 days out) and `app_quota`
(`<day>_<userId>` counters, `expire_at` 2 days out). The cap is taken in a transaction, so two requests at once cannot
both take the last place. `setup_firestore.sh` gains the two TTL policies (dry run by default; not applied).

## When it goes wrong

| What | What the rep sees |
| --- | --- |
| Vendor key missing or vendor down | Run ends `error` with a plain message; the cap place is kept (the call was made) |
| Contact has no name, or is linked to no company | The objective is shown as unavailable with the reason; a run that finds this late ends `skipped` |
| Cloud Tasks cannot be reached | 502, the place in the cap is given back, no run record is left running |
| Task delivered twice | The second delivery sees the run already claimed and does nothing |
| Service restarts mid-run | The run stays `running`; the card stops waiting after 3 minutes and says so |

## To turn it on (needs the owner; nothing here has been done)

1. Make the shared secret: `./setup_app_secret.sh` is a dry run; with `APPLY=1` it creates
   the secret in Secret Manager, lets the service's runtime account read it, and copies the value to your clipboard for
   `hs secret add APP_REQUEST_SECRET --account <id>` (a hidden prompt; paste, then press Enter to clear the clipboard).
   Repeat the paste for each HubSpot account the app goes to (`COPY_EXISTING=1`). Never replaced if it exists.
2. Add `--set-secrets=APP_REQUEST_SECRET=APP_REQUEST_SECRET:latest` to `cloudbuild.yaml`, only once the secret exists
   (a deploy cannot start without it). Until then the endpoints answer 503.
3. Apply `setup_firestore.sh` for the TTL policies.
4. Deploy a build from main and move traffic to it. The new endpoints exist only in a build that has this code.
5. Upload the app, after `hs secret add`, with approval; add the card to contact records, and check it read only on one
   test contact.

## Tests

- Service: `test_app_request_auth.py`, `test_app_run_store.py`, `test_contact_objectives.py`,
  `test_app_endpoints.py`, `test_hubspot_contact_for_app.py`, `test_setup_app_secret.py` (the secret script, against
  fake `gcloud` and clipboard commands), and the contact-card and service-function parts of
  `test_hubspot_app_contract.py`. Vendors, HubSpot and Cloud Tasks are faked; Firestore has an in-memory twin with the
  same contract. The contract tests also run the real function in Node and check that the service accepts what it
  builds and refuses it once who asked or what was asked is changed.
- App: the card against a fake service, the model, the service function (`service-function.test.ts`: which requests
  go through, who is asking, the signature, and the sender against a local server), and the config (no permitted URLs;
  the function and the secret it names).
