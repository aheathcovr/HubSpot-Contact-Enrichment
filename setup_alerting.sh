#!/bin/bash
# setup_alerting.sh
#
# Log-based metrics and alert policies for the hubspot-enrichment Cloud Run service.
#
# Until now nothing alerted on this service: no log-based metrics, no alert
# policies, no notification channel. The code already tags the failures that
# matter (jsonPayload.alert = enrichment_failure / enrich_unauthorized), and the
# webhook swallows enqueue failures and still answers HubSpot 200, so a broken
# permission or a rejected token would otherwise stop enrichment silently.
#
# DRY RUN BY DEFAULT: prints every command and policy body, changes nothing.
#
#   NOTIFY_EMAIL=you@example.com ./setup_alerting.sh            # show what would run
#   NOTIFY_EMAIL=you@example.com APPLY=1 ./setup_alerting.sh    # do it
#
# Safe to re-run: existing metrics are updated, existing policies and the channel
# are found by name and skipped.
#
# Each filter below was run against the last 30 days of real logs before this was
# written (see the match counts in the comments).

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gen-lang-client-0844868008}"
NOTIFY_EMAIL="${NOTIFY_EMAIL:-}"
APPLY="${APPLY:-0}"
SERVICE="hubspot-enrichment"
CHANNEL_NAME="HubSpot enrichment alerts (email)"

SVC='resource.type="cloud_run_revision" AND resource.labels.service_name="'"${SERVICE}"'"'

# Print a command the way you could paste it; run it only when APPLY=1.
run() {
  if [[ "${APPLY}" == "1" ]]; then
    "$@"
  else
    printf '  +'
    for a in "$@"; do
      case "$a" in *[\ \"\&\(\)\<\>\|\;]*) printf " '%s'" "$a" ;; *) printf ' %s' "$a" ;; esac
    done
    printf '\n'
  fi
}

mode() { [[ "${APPLY}" == "1" ]] && echo "APPLYING" || echo "DRY RUN (nothing is changed)"; }

echo "==> $(mode)   project=${PROJECT_ID}   service=${SERVICE}"
echo ""

if [[ "${APPLY}" == "1" && -z "${NOTIFY_EMAIL}" ]]; then
  echo "NOTIFY_EMAIL is required with APPLY=1 (where alerts are sent)." >&2
  exit 1
fi

# ── 1. Log-based metrics (counters) ───────────────────────────────────────────
create_metric() {
  local name="$1" description="$2" filter="$3"
  if [[ "${APPLY}" == "1" ]] && gcloud logging metrics describe "${name}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "--- ${name}: exists, updating"
    run gcloud logging metrics update "${name}" --description="${description}" --log-filter="${filter}" --project="${PROJECT_ID}"
  else
    echo "--- ${name}"
    run gcloud logging metrics create "${name}" --description="${description}" --log-filter="${filter}" --project="${PROJECT_ID}"
  fi
}

echo "### Metrics"

# run_enrichment() raised. _run_enrichment_safe tags it and resets request_to_enrich.
# 30-day matches: 0.
create_metric hubspot_enrichment_failures \
  "An enrichment run raised and the company was left for manual review" \
  "${SVC} AND jsonPayload.alert=\"enrichment_failure\""

# HubSpot was told "accepted" (200) but no Cloud Task was created, so nothing runs.
# 30-day matches: 0.
create_metric hubspot_enqueue_failures \
  "A webhook was accepted but its Cloud Task could not be created" \
  "${SVC} AND severity>=ERROR AND jsonPayload.message:\"Failed to enqueue Cloud Task\""

# Every /enrich call refused for a missing or wrong Cloud Tasks token, from any source.
# 30-day matches: 2 (both my own curl probes).
create_metric hubspot_enrich_unauthorized \
  "/enrich rejected a call: missing or invalid Cloud Tasks OIDC token (any source)" \
  "${SVC} AND jsonPayload.alert=\"enrich_unauthorized\""

# The same refusal when the caller is Cloud Tasks itself: real queued work being
# rejected (a token/audience/account mismatch), as opposed to someone probing.
# Read from the request log, which carries the user agent.
create_metric hubspot_enrich_unauthorized_cloud_tasks \
  "/enrich returned 401 to Cloud Tasks: queued enrichment is being rejected" \
  "${SVC} AND httpRequest.requestUrl:\"/enrich\" AND httpRequest.status=401 AND httpRequest.userAgent:\"Google-Cloud-Tasks\""

# Everything else at ERROR. Most failures inside run_enrichment are caught, logged
# and the request still returns 200 (an expired HubSpot key logs "Cannot load
# company" and moves on), so the tagged alert above would never see them.
# Excludes the two cases that already have their own alert, so one failure pages once.
# 30-day matches: 1 (my own test company).
create_metric hubspot_unhandled_errors \
  "An ERROR that is not an enrichment_failure or a failed enqueue" \
  "${SVC} AND severity>=ERROR AND NOT jsonPayload.alert=\"enrichment_failure\" AND NOT jsonPayload.message:\"Failed to enqueue Cloud Task\""

# A paid source or a data source refusing us or running dry. The service tags these:
#   vendor_not_configured          a revision started without a vendor's key (logged once per vendor per start)
#   fullenrich_credits_exhausted   FullEnrich balance is zero, lookups are skipped
#   millionverifier_credits_low    under 200 left; MillionVerifier credits do not renew
#   bounceban_credits_low          under 200 left
#   bounceban_unavailable          key rejected, out of credits, or account blocked
#   seamless_unavailable           key rejected, or out of credits (422 insufficientCredits)
#   seamless_credits_low           under 25 left: no further lookups are made
# plus the untagged refusals: MillionVerifier refused a request, a FullEnrich POST or
# search failed, a Definitive query failed. (millionverifier_test_mode is an ERROR, so
# the "unexpected errors" policy already covers it; so does hubspot_mirror_stale.)
# 30-day matches: 7 (the Definitive Full Feed table that no longer exists, since removed).
create_metric hubspot_vendor_or_data_failures \
  "A vendor refused a request or ran low on credits, or a Definitive Healthcare query failed" \
  "${SVC} AND severity>=WARNING AND (jsonPayload.alert=(\"vendor_not_configured\" OR \"fullenrich_credits_exhausted\" OR \"millionverifier_credits_low\" OR \"bounceban_credits_low\" OR \"bounceban_unavailable\" OR \"seamless_unavailable\" OR \"seamless_credits_low\") OR jsonPayload.message:\"MillionVerifier refused\" OR jsonPayload.message:\"FullEnrich enrich POST failed\" OR jsonPayload.message:\"FullEnrich search failed\" OR jsonPayload.message:\"DH verification query\" OR jsonPayload.message:\"DH facility contact query failed\" OR jsonPayload.message:\"table query failed\" OR jsonPayload.message:\"Overview query failed\")"

# The HubSpot app's cards, or the runs they start, being refused. A card request is signed by the app's function with
# APP_REQUEST_SECRET; the service answers 401 for a wrong signature or a stale one, 403 for another HubSpot account,
# and 503 when it has no secret. The first two are logged ("/app request refused"), the 503 is not, so it is read from
# the request log. "Missing signature" is left out on purpose: a real card call always carries a signature, so a
# request without one is someone probing the public address. The last clause is the Cloud Tasks callback that finishes
# a run: a 401 to Cloud Tasks there means runs start and never finish.
# 30-day matches: 0 before one deliberate request with a made-up signature, then 1. The unsigned probes made while
# testing (2) were correctly left out. The 503 and Cloud Tasks clauses were not triggered: that would break production.
create_metric hubspot_app_requests_refused \
  "A card request or a run callback was refused: bad or stale signature, another HubSpot account, no secret, or a rejected Cloud Tasks token" \
  "${SVC} AND ((jsonPayload.message:\"/app request refused\" AND NOT jsonPayload.message:\"Missing signature\") OR (httpRequest.requestUrl:\"/app/\" AND httpRequest.status=503) OR (httpRequest.requestUrl:\"/app/run\" AND httpRequest.status=401 AND httpRequest.userAgent:\"Google-Cloud-Tasks\"))"

# ── 2. Notification channel ───────────────────────────────────────────────────
echo ""
echo "### Notification channel"
CHANNEL="projects/${PROJECT_ID}/notificationChannels/<created-on-apply>"
if [[ "${APPLY}" == "1" ]]; then
  CHANNEL="$(gcloud beta monitoring channels list --project="${PROJECT_ID}" \
    --filter="displayName=\"${CHANNEL_NAME}\"" --format='value(name)' | head -1)"
fi
if [[ "${APPLY}" == "1" && -n "${CHANNEL}" ]]; then
  echo "--- exists: ${CHANNEL}"
else
  echo "--- create an email channel for ${NOTIFY_EMAIL:-<NOTIFY_EMAIL>}"
  if [[ "${APPLY}" == "1" ]]; then
    CHANNEL="$(gcloud beta monitoring channels create --project="${PROJECT_ID}" \
      --display-name="${CHANNEL_NAME}" --type=email \
      --channel-labels="email_address=${NOTIFY_EMAIL}" --format='value(name)')"
    echo "    created ${CHANNEL}"
  else
    run gcloud beta monitoring channels create --project="${PROJECT_ID}" \
      --display-name="${CHANNEL_NAME}" --type=email \
      --channel-labels="email_address=${NOTIFY_EMAIL:-<NOTIFY_EMAIL>}"
  fi
fi

# ── 3. Alert policies ─────────────────────────────────────────────────────────
# Counter metrics, summed over the window; "greater than N" fires at N+1 or more.
# Thresholds come from the 30-day counts: these events are rare, so one is enough
# to alert, except for unauthorized calls from arbitrary sources.
policy_json() {
  CHANNEL="${CHANNEL}" PROJECT_ID="${PROJECT_ID}" python3 -c '
import json, os, sys
name, severity, documentation = sys.argv[1:4]
conditions = []
for spec in sys.argv[4:]:
    label, metric, threshold, period = spec.split("|")
    conditions.append({
        "displayName": label,
        "conditionThreshold": {
            "filter": f"metric.type=\"logging.googleapis.com/user/{metric}\" AND resource.type=\"cloud_run_revision\"",
            "aggregations": [{"alignmentPeriod": f"{period}s", "perSeriesAligner": "ALIGN_DELTA",
                              "crossSeriesReducer": "REDUCE_SUM"}],
            "comparison": "COMPARISON_GT",
            "thresholdValue": float(threshold),
            "duration": "0s",
            "trigger": {"count": 1},
        },
    })
print(json.dumps({
    "displayName": name,
    "severity": severity,
    "combiner": "OR",
    "enabled": True,
    "conditions": conditions,
    "notificationChannels": [os.environ["CHANNEL"]],
    "alertStrategy": {"autoClose": "1800s"},
    "documentation": {"mimeType": "text/markdown", "content": documentation},
}, indent=2))
' "$@"
}

create_policy() {
  local name="$1"
  if [[ "${APPLY}" == "1" ]] && [[ -n "$(gcloud beta monitoring policies list --project="${PROJECT_ID}" \
        --filter="displayName=\"${name}\"" --format='value(name)')" ]]; then
    echo "--- ${name}: exists, skipping"
    return
  fi
  echo "--- ${name}"
  local file
  file="$(mktemp)"
  policy_json "$@" > "${file}"
  if [[ "${APPLY}" != "1" ]]; then sed 's/^/    /' "${file}"; fi
  run gcloud beta monitoring policies create --project="${PROJECT_ID}" --policy-from-file="${file}"
  rm -f "${file}"
}

echo ""
echo "### Policies"

create_policy "HubSpot enrichment: run failed" ERROR \
"An enrichment run raised an exception. The company's \`request_to_enrich\` was reset to false, so it will not retry on its own.

1. Logs: \`jsonPayload.alert=\"enrichment_failure\"\`. The entry carries \`company_id\`, \`error\` and \`traceback\`.
2. Fix the cause, then set \`request_to_enrich\` back to true on that company." \
  "enrichment failures|hubspot_enrichment_failures|0|300"

create_policy "HubSpot enrichment: webhook accepted but task not queued" CRITICAL \
"HubSpot got a 200 so it will not retry, but no Cloud Task was created. **Nothing is running for that company.**

1. Logs: \`jsonPayload.message:\"Failed to enqueue Cloud Task\"\` (names the company and the error).
2. Usual causes: the runtime account lost \`cloudtasks.enqueuer\` on the queue or act-as on the tasks account, \`CLOUD_RUN_SERVICE_URL\` / \`CLOUD_TASKS_SA_EMAIL\` unset, queue paused.
3. Re-trigger by setting \`request_to_enrich\` true again." \
  "enqueue failures|hubspot_enqueue_failures|0|300"

create_policy "HubSpot enrichment: /enrich rejecting calls" ERROR \
"Two conditions. A 401 to **Cloud Tasks** means real queued work is being rejected: the token's audience or account no longer matches \`CLOUD_RUN_SERVICE_URL\` / \`CLOUD_TASKS_SA_EMAIL\`, or the tasks account changed. Five or more 401s from anything else in ten minutes is probing of the public endpoint.

Logs: \`jsonPayload.alert=\"enrich_unauthorized\"\`. Cloud Tasks retries each rejected task 5 times." \
  "Cloud Tasks deliveries rejected|hubspot_enrich_unauthorized_cloud_tasks|0|300" \
  "repeated unauthorized calls from other sources|hubspot_enrich_unauthorized|4|600"

create_policy "HubSpot enrichment: unexpected errors" ERROR \
"An ERROR that is not already covered by the run-failed or enqueue alerts. Most failures inside a run are caught and logged while the request still returns 200: an expired HubSpot key shows up as \`Cannot load company\`, a failed write as \`Failed to create note\` / \`Failed to update company\`.

Logs: \`severity>=ERROR\` on the service." \
  "unexpected errors|hubspot_unhandled_errors|0|600"

create_policy "HubSpot enrichment: vendor or data source failing" WARNING \
"A paid vendor or a Definitive table is refusing us, or credits are running out. Credits for MillionVerifier, FullEnrich and BounceBan do not renew. MillionVerifier \`refused\` or BounceBan \`unavailable\` usually means out of credits or a rejected key; FullEnrich POST/search failures mean a bad key or no credits; a Definitive \`query failed\` usually means a table was renamed or dropped.

Enrichment keeps running without that source, so the output quietly gets thinner. Logs: the matching warning." \
  "vendor or data failures|hubspot_vendor_or_data_failures|0|3600"

create_policy "HubSpot app: card requests refused" ERROR \
"The HubSpot app's cards, or the runs they start, are being refused by the service. Reps see an error on the card and nothing else tells you.

1. Logs: \`/app request refused (401): Invalid signature.\` means APP_REQUEST_SECRET in HubSpot (\`hs secret add APP_REQUEST_SECRET --account 24434722\`) is not the same value as the one in Secret Manager. \`The request is too old\` is a clock problem. \`(403) ... another HubSpot account\` is a card in a different account (the test account is refused on purpose).
2. A 503 on \`/app/\` in the request log is the service having no APP_REQUEST_SECRET (the service does not log that one), or Cloud Run itself failing: check that the serving revision has the secret.
3. A 401 to Cloud Tasks on \`/app/run\` means runs start and never finish: the token's audience or account no longer matches \`CLOUD_RUN_SERVICE_URL\` / \`CLOUD_TASKS_SA_EMAIL\`.

Requests with no signature at all are not counted: that is anyone probing the public address." \
  "app requests refused|hubspot_app_requests_refused|0|600"

echo ""
if [[ "${APPLY}" == "1" ]]; then
  echo "Done. Test one: write a matching log line, or trigger the failure, and watch the policy."
else
  echo "Dry run only: nothing was changed. Re-run with APPLY=1 (and NOTIFY_EMAIL) to apply."
fi
