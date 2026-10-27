# Contact card, first slice: design

Status: built test-first on branch `feat/contact-card-first-slice`. **Not uploaded to HubSpot and not deployed.** The
service endpoints answer 503 until the app's client secret is configured (see "To turn it on").

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
card -- hubspot.fetch (signed) --> GET  /app/contact/status?contactId=...   what the contact has, quota, recent runs
card -- hubspot.fetch (signed) --> POST /app/contact/run                    accepts a run, queues a Cloud Task
Cloud Tasks (Google-signed) ------> POST /app/run                           does the work, updates the run record
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

HubSpot signs every `hubspot.fetch` request (`X-HubSpot-Signature-v3`, HMAC-SHA256 of method + URI + body + timestamp
with the app's client secret, base64) and adds `userId`, `userEmail`, `portalId` and `appId` to the query string. The
signature covers the URI, so those values cannot be forged. The service:

1. Fails closed (503) when `HUBSPOT_APP_CLIENT_SECRET` is not set.
2. Rejects (401) a missing, stale (over 5 minutes) or wrong signature. The URI is compared with the characters HubSpot
   lists decoded (`%40` as `@` and so on), which the webhook's check does not need.
3. Rejects (403) a request for another portal when `HUBSPOT_PORTAL_ID` is set.
4. Takes the rep's identity from the signed query, never from the body.

`/app/run` is not signed by HubSpot; like `/enrich` it accepts only Google-signed Cloud Tasks requests.

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

1. Find out whether the private app can send signed requests, and get its client secret. If it cannot, the fallback is
   to route the call through the app function with a shared secret.
2. Create the secret `HUBSPOT_APP_CLIENT_SECRET` (the owner pastes it), let the runtime account read it, add it to
   `cloudbuild.yaml` (`--set-secrets`) and `setup_runtime_service_account.sh`. Until then the endpoints answer 503.
3. Apply `setup_firestore.sh` for the two TTL policies.
4. Deploy a build from main and move traffic to it. The new endpoints exist only in a build that has this code.
5. Upload the app (approval required), add the card to contact records, check it read only on one test contact.

## Tests

- Service: `test_hubspot_request_auth.py`, `test_app_run_store.py`, `test_contact_objectives.py`,
  `test_app_endpoints.py`, `test_hubspot_contact_for_app.py`, and the contact-card parts of `test_hubspot_app_contract.py`. Vendors, HubSpot and Cloud Tasks are faked; Firestore has an in-memory twin with the same
  contract.
- App: the card against a fake service client, the model, and the config (the one permitted URL).
