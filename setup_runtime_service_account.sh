#!/bin/bash
# setup_runtime_service_account.sh
#
# Gives the Cloud Run service its own least-privilege identity.
#
# Until now the service ran as the project's default Compute Engine service
# account, which holds roles/editor (plus run.admin and iam.serviceAccountUser)
# on the whole project. The service accepts public traffic (the HubSpot
# webhook), so any flaw in it would have reached every dataset and secret in the
# project.
#
# This script creates `hubspot-enrichment-runtime` and grants it only what the
# service does at runtime. Everything is granted at the narrowest scope that
# works, and the script is safe to re-run.
#
#   Secret Manager   secretAccessor on each of the five secrets the service mounts
#   BigQuery         jobUser on the project, plus READER on the three datasets the
#                    matcher queries (read-only; the service never writes)
#   Cloud Tasks      enqueuer on the one queue
#   Service accounts serviceAccountUser on the Cloud Tasks invoker account, which
#                    creating a task with an OIDC token for it requires
#
# Not granted, deliberately: logging (Cloud Run writes container logs itself),
# Cloud Storage (GCS_CACHE_BUCKET is not set, so the LLM cache is disk-only), and
# any project-wide role.
#
# The script does NOT switch the service over and does NOT touch the default
# Compute account's roles; other things in the project may still rely on it.
#
# Usage:
#   CLOUDSDK_PYTHON=<python 3.10+> ./setup_runtime_service_account.sh [PROJECT_ID] [REGION]
#
# Defaults:
#   PROJECT_ID = gen-lang-client-0844868008
#   REGION     = us-central1

set -euo pipefail

PROJECT_ID="${1:-gen-lang-client-0844868008}"
REGION="${2:-us-central1}"

SERVICE_NAME="hubspot-enrichment"
QUEUE_NAME="hubspot-enrichment"
RUNTIME_SA_NAME="hubspot-enrichment-runtime"
RUNTIME_SA="${RUNTIME_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
TASKS_SA="hubspot-enrichment-tasks@${PROJECT_ID}.iam.gserviceaccount.com"
BUILD_SA="hubspot-enrichment-build@${PROJECT_ID}.iam.gserviceaccount.com"

# Must match the --set-secrets list in cloudbuild.yaml.
SECRETS=(
  HUBSPOT_API_KEY
  OPENROUTER_API_KEY
  MILLIONVERIFIER_API_KEY
  FULLENRICH_API_KEY
  HUBSPOT_WEBHOOK_SECRET
)

# Secrets the service reads when they exist. A missing one is skipped, not an error:
# BOUNCEBAN_API_KEY turns on the BounceBan second opinion behind MillionVerifier, and
# the service runs the same without it. Creating one is a separate step (see the PR).
OPTIONAL_SECRETS=(
  BOUNCEBAN_API_KEY
)

# Datasets the matcher reads: HubSpot_Airbyte (companies, contacts),
# definitive_healthcare (the DH tables), state_associations (state association table).
DATASETS=(
  HubSpot_Airbyte
  definitive_healthcare
  state_associations
)

echo "==> Project:         ${PROJECT_ID}"
echo "==> Runtime account: ${RUNTIME_SA}"
echo ""

# ── 1. Create the service account ─────────────────────────────────────────────
echo "--- Creating service account..."
if gcloud iam service-accounts describe "${RUNTIME_SA}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    Already exists, skipping."
else
  gcloud iam service-accounts create "${RUNTIME_SA_NAME}" \
    --display-name="HubSpot Enrichment Cloud Run runtime" \
    --project="${PROJECT_ID}"
  echo "    Created."
fi

# ── 2. Secret Manager: read access to the five mounted secrets, per secret ────
echo "--- Granting secretAccessor on each secret..."
for secret in "${SECRETS[@]}"; do
  gcloud secrets add-iam-policy-binding "${secret}" \
    --member="serviceAccount:${RUNTIME_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="${PROJECT_ID}" \
    --condition=None --quiet >/dev/null
  echo "    ${secret}"
done

echo "--- Granting secretAccessor on optional secrets that exist..."
for secret in "${OPTIONAL_SECRETS[@]}"; do
  if gcloud secrets describe "${secret}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud secrets add-iam-policy-binding "${secret}" \
      --member="serviceAccount:${RUNTIME_SA}" \
      --role="roles/secretmanager.secretAccessor" \
      --project="${PROJECT_ID}" \
      --condition=None --quiet >/dev/null
    echo "    ${secret}"
  else
    echo "    ${secret}: no such secret yet, skipped"
  fi
done

# ── 3. BigQuery: permission to run query jobs ─────────────────────────────────
echo "--- Granting bigquery.jobUser on the project..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/bigquery.jobUser" \
  --condition=None --quiet >/dev/null
echo "    Done."

# ── 4. BigQuery: READER on each dataset the service queries ───────────────────
# Dataset access is a list on the dataset itself. Read the whole list, add one
# entry, write it back: `bq update --source` replaces the list, so appending to
# what is there (never composing it from scratch) is what keeps every existing
# entry, including authorized-dataset entries, intact.
echo "--- Granting READER on each dataset..."
for dataset in "${DATASETS[@]}"; do
  tmp="$(mktemp)"
  bq --project_id="${PROJECT_ID}" show --format=prettyjson "${dataset}" > "${tmp}"
  changed="$(python3 - "${tmp}" "${RUNTIME_SA}" <<'PY'
import json, sys
path, sa = sys.argv[1], sys.argv[2]
d = json.load(open(path))
access = d.get("access", [])
if any(a.get("userByEmail", "").lower() == sa.lower() and a.get("role") == "READER" for a in access):
    print("no")
else:
    access.append({"role": "READER", "userByEmail": sa})
    d["access"] = access
    json.dump(d, open(path, "w"))
    print("yes")
PY
)"
  if [[ "${changed}" == "yes" ]]; then
    bq --project_id="${PROJECT_ID}" update --source "${tmp}" "${dataset}" >/dev/null
    echo "    ${dataset}: granted."
  else
    echo "    ${dataset}: already granted."
  fi
  rm -f "${tmp}"
done

# ── 5. Cloud Tasks: enqueue on the one queue ──────────────────────────────────
echo "--- Granting cloudtasks.enqueuer on queue ${QUEUE_NAME}..."
gcloud tasks queues add-iam-policy-binding "${QUEUE_NAME}" \
  --location="${REGION}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/cloudtasks.enqueuer" \
  --project="${PROJECT_ID}" --quiet >/dev/null
echo "    Done."

# ── 6. Act-as the Cloud Tasks invoker account (needed to attach its OIDC token) ─
echo "--- Granting serviceAccountUser on ${TASKS_SA}..."
gcloud iam service-accounts add-iam-policy-binding "${TASKS_SA}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/iam.serviceAccountUser" \
  --project="${PROJECT_ID}" --quiet >/dev/null
echo "    Done."

# ── 7. Let the deploy account keep the service on this identity ───────────────
# Updating a service that runs as ${RUNTIME_SA} requires act-as on it.
echo "--- Granting serviceAccountUser on ${RUNTIME_SA} to ${BUILD_SA}..."
gcloud iam service-accounts add-iam-policy-binding "${RUNTIME_SA}" \
  --member="serviceAccount:${BUILD_SA}" \
  --role="roles/iam.serviceAccountUser" \
  --project="${PROJECT_ID}" --quiet >/dev/null
echo "    Done."

echo ""
echo "============================================================"
echo "  ${RUNTIME_SA} is ready. The service is NOT switched yet."
echo "============================================================"
echo ""
echo "  1. Verify the account can do its job before switching. Impersonating it"
echo "     needs roles/iam.serviceAccountTokenCreator on it (Owner does not include"
echo "     that); grant it temporarily and remove it afterwards:"
echo "       CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=${RUNTIME_SA} \\"
echo "         bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`${PROJECT_ID}.HubSpot_Airbyte.companies\`'"
echo ""
echo "  2. Switch the service (creates a new revision):"
echo "       gcloud run services update ${SERVICE_NAME} --region=${REGION} \\"
echo "         --service-account=${RUNTIME_SA} --project=${PROJECT_ID}"
echo ""
echo "  3. Roll back if needed (route traffic to the previous revision):"
echo "       gcloud run services update-traffic ${SERVICE_NAME} --region=${REGION} \\"
echo "         --to-revisions=<previous-revision>=100 --project=${PROJECT_ID}"
echo ""
