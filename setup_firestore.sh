#!/bin/bash
# setup_firestore.sh
#
# Firestore for the FullEnrich webhook path.
#
# The service is moving from "submit a FullEnrich lookup and poll it for up to 90 s"
# to "submit it, finish the run, and finish the contact when FullEnrich's signed
# callback arrives". Between those two moments something has to remember what the
# submit was for (which HubSpot contact, which run, what has already been paid for
# and verified), because the callback can land on any instance, or on a new one.
# One small Firestore document per submit does that, and also makes the callback
# idempotent and gives the run a counter of jobs still pending.
#
# What this sets up, and nothing else:
#   1. the Firestore API
#   2. the (default) database, Firestore Native mode, in the Cloud Run region,
#      with delete protection on. A database's location cannot be changed later.
#   3. roles/datastore.user for the Cloud Run runtime account. Firestore has one
#      database here and it will hold only job records (ids and statuses), so the
#      role is granted on the project; there is nothing else for it to reach.
#   4. a TTL policy on fullenrich_jobs.expire_at, so finished jobs delete
#      themselves (the service sets expire_at 30 days out).
#
# Not part of this script, each needing its own go-ahead: the separate finish queue,
# min-instances, and switching the service over to the webhook path.
#
# DRY RUN BY DEFAULT: reads the project's current state, prints every command it
# would run, changes nothing.
#
#   ./setup_firestore.sh              # show what would run
#   APPLY=1 ./setup_firestore.sh      # do it
#
# Safe to re-run: anything already in place is left alone. It refuses to continue
# if a (default) database already exists in Datastore mode, and never deletes.
#
# Needs a logged-in gcloud (gcloud auth login) with permission to enable services,
# create databases and edit the project's IAM policy.

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gen-lang-client-0844868008}"
LOCATION="${LOCATION:-us-central1}"      # the Cloud Run region
DATABASE="(default)"
COLLECTION="fullenrich_jobs"
TTL_FIELD="expire_at"
RUNTIME_SA="hubspot-enrichment-runtime@${PROJECT_ID}.iam.gserviceaccount.com"
APPLY="${APPLY:-0}"

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

echo "==> $(mode)"
echo "==> Project:   ${PROJECT_ID}"
echo "==> Database:  ${DATABASE} in ${LOCATION} (Firestore Native)"
echo "==> Runtime:   ${RUNTIME_SA}"
echo ""

# ── 1. The API ────────────────────────────────────────────────────────────────
echo "--- Firestore API"
if gcloud services list --enabled --project="${PROJECT_ID}" \
     --filter="config.name=firestore.googleapis.com" --format="value(config.name)" | grep -q firestore; then
  echo "    Already enabled."
else
  run gcloud services enable firestore.googleapis.com --project="${PROJECT_ID}"
fi

# ── 2. The database ───────────────────────────────────────────────────────────
echo "--- Database ${DATABASE}"
# Describe fails when there is no database, or when the API is not enabled yet:
# both mean "nothing to look at".
existing_type="$(gcloud firestore databases describe --database="${DATABASE}" --project="${PROJECT_ID}" \
                   --format="value(type)" 2>/dev/null || true)"
existing_location="$(gcloud firestore databases describe --database="${DATABASE}" --project="${PROJECT_ID}" \
                       --format="value(locationId)" 2>/dev/null || true)"
if [[ -z "${existing_type}" ]]; then
  run gcloud firestore databases create --database="${DATABASE}" --location="${LOCATION}" \
        --type=firestore-native --delete-protection --project="${PROJECT_ID}"
elif [[ "${existing_type}" != "FIRESTORE_NATIVE" ]]; then
  echo "    A ${DATABASE} database already exists in ${existing_type} mode." >&2
  echo "    This service needs Native mode. Not touching it; decide by hand." >&2
  exit 1
else
  echo "    Already exists (Native mode, ${existing_location})."
  if [[ "${existing_location}" != "${LOCATION}" ]]; then
    echo "    Note: it is in ${existing_location}, not ${LOCATION}, where Cloud Run runs. Works, with cross-region latency." >&2
  fi
fi

# ── 3. The runtime account may read and write documents ───────────────────────
echo "--- roles/datastore.user for ${RUNTIME_SA}"
run gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="serviceAccount:${RUNTIME_SA}" \
      --role="roles/datastore.user" \
      --condition=None --quiet

# ── 4. Finished jobs delete themselves ────────────────────────────────────────
echo "--- TTL on ${COLLECTION}.${TTL_FIELD}"
if gcloud firestore fields ttls list --database="${DATABASE}" --project="${PROJECT_ID}" \
     --format="value(name)" 2>/dev/null | grep -q "/collectionGroups/${COLLECTION}/fields/${TTL_FIELD}"; then
  echo "    Already set."
else
  run gcloud firestore fields ttls update "${TTL_FIELD}" \
        --collection-group="${COLLECTION}" --enable-ttl \
        --database="${DATABASE}" --project="${PROJECT_ID}"
fi

# ── Read back what is now in place ────────────────────────────────────────────
if [[ "${APPLY}" == "1" ]]; then
  echo ""
  echo "--- Result"
  gcloud firestore databases describe --database="${DATABASE}" --project="${PROJECT_ID}" \
    --format="table(name, type, locationId, deleteProtectionState)"
  gcloud firestore fields ttls list --database="${DATABASE}" --project="${PROJECT_ID}" \
    --format="table(name, ttlConfig.state)"
  echo -n "datastore.user held by runtime account: "
  gcloud projects get-iam-policy "${PROJECT_ID}" --flatten="bindings[].members" \
    --filter="bindings.role=roles/datastore.user AND bindings.members=serviceAccount:${RUNTIME_SA}" \
    --format="value(bindings.role)" | grep -q datastore.user && echo yes || echo NO
fi
