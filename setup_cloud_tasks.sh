#!/bin/bash
# setup_cloud_tasks.sh
#
# One-time setup for the Cloud Tasks migration.
# Run this before deploying the updated Cloud Run service.
#
# Usage:
#   ./setup_cloud_tasks.sh [PROJECT_ID] [REGION]
#
# Defaults:
#   PROJECT_ID = gen-lang-client-0844868008
#   REGION     = us-central1

set -euo pipefail

PROJECT_ID="${1:-gen-lang-client-0844868008}"
REGION="${2:-us-central1}"

SERVICE_NAME="hubspot-enrichment"
QUEUE_NAME="hubspot-enrichment"
SA_NAME="hubspot-enrichment-tasks"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Project:  ${PROJECT_ID}"
echo "==> Region:   ${REGION}"
echo "==> Queue:    ${QUEUE_NAME}"
echo "==> SA:       ${SA_EMAIL}"
echo ""

# ── 1. Create the service account ─────────────────────────────────────────────
echo "--- Creating service account..."
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    Already exists, skipping."
else
  gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="HubSpot Enrichment Cloud Tasks Invoker" \
    --project="${PROJECT_ID}"
  echo "    Created."
fi

# ── 2. Grant the SA permission to invoke the Cloud Run service ─────────────────
echo "--- Granting roles/run.invoker to ${SA_EMAIL}..."
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
  --region="${REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker" \
  --project="${PROJECT_ID}"
echo "    Done."

# ── 3. Find the Cloud Run service's own service account ────────────────────────
echo "--- Looking up Cloud Run service account..."
CLOUD_RUN_SA=$(gcloud run services describe "${SERVICE_NAME}" \
  --region="${REGION}" \
  --format="value(spec.template.spec.serviceAccountName)" \
  --project="${PROJECT_ID}" 2>/dev/null || true)

# Fall back to the Compute Engine default SA if no dedicated SA is set
if [[ -z "${CLOUD_RUN_SA}" ]]; then
  PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
  CLOUD_RUN_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
  echo "    No dedicated SA found, using default Compute SA: ${CLOUD_RUN_SA}"
else
  echo "    Found: ${CLOUD_RUN_SA}"
fi

# ── 4. Grant the Cloud Run SA permission to enqueue tasks ──────────────────────
echo "--- Granting roles/cloudtasks.enqueuer to Cloud Run SA..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${CLOUD_RUN_SA}" \
  --role="roles/cloudtasks.enqueuer"
echo "    Done."

# ── 5. Create the Cloud Tasks queue ───────────────────────────────────────────
echo "--- Creating Cloud Tasks queue '${QUEUE_NAME}'..."
if gcloud tasks queues describe "${QUEUE_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    Already exists, skipping."
else
  gcloud tasks queues create "${QUEUE_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --max-attempts=5 \
    --min-backoff=10s \
    --max-backoff=300s \
    --max-doublings=3
  echo "    Created."
fi

# ── 6. Get the Cloud Run service URL ──────────────────────────────────────────
echo "--- Fetching Cloud Run service URL..."
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region="${REGION}" \
  --format="value(status.url)" \
  --project="${PROJECT_ID}" 2>/dev/null || true)

echo ""
echo "============================================================"
echo "  Setup complete!"
echo "============================================================"
echo ""
echo "  Set these as substitution variables in your Cloud Build trigger:"
echo ""
echo "    _CLOUD_TASKS_SA_EMAIL  = ${SA_EMAIL}"
if [[ -n "${SERVICE_URL}" ]]; then
echo "    _CLOUD_RUN_SERVICE_URL = ${SERVICE_URL}"
else
echo "    _CLOUD_RUN_SERVICE_URL = (deploy the service first, then re-run to get the URL)"
fi
echo ""
echo "  Cloud Build trigger substitution variables can be set at:"
echo "  https://console.cloud.google.com/cloud-build/triggers?project=${PROJECT_ID}"
echo ""
