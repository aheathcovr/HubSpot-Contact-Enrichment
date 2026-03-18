"""
FastAPI webhook server for HubSpot contact enrichment.

Receives HubSpot company property-change webhooks, enqueues a Cloud Tasks job,
and returns 200 immediately so HubSpot doesn't retry. Cloud Tasks then delivers
a POST /enrich request to this service, which runs the enrichment synchronously.

Endpoints:
  GET  /health   — health check (used by Cloud Run)
  POST /webhook  — HubSpot company webhook receiver (enqueues Cloud Task)
  POST /enrich   — Cloud Tasks delivery endpoint (runs enrichment synchronously)

Environment variables:
  HUBSPOT_API_KEY           HubSpot Private App token (required)
  HUBSPOT_WEBHOOK_SECRET    HMAC secret for signature validation (required)
  OPENROUTER_API_KEY        OpenRouter / Perplexity key (required for research)
  GCP_PROJECT_ID            GCP project (default: gen-lang-client-0844868008)
  GCP_LOCATION              GCP region (default: us-central1)
  CLOUD_TASKS_QUEUE         Cloud Tasks queue name (default: hubspot-enrichment)
  CLOUD_RUN_SERVICE_URL     Base URL of this Cloud Run service (required for Cloud Tasks)
  CLOUD_TASKS_SA_EMAIL      Service account email for OIDC auth on /enrich (required)
  BQ_LOCATION               BigQuery location (default: US)
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("webhook_server")

# ── Startup validation ────────────────────────────────────────────────────────
HUBSPOT_WEBHOOK_SECRET: str = ""
TRIGGER_PROPERTY = "request_to_enrich"
TRIGGER_VALUES = {"true", "1", "yes"}

_COMPANY_ID_RE = re.compile(r'^\d{1,20}$')


@asynccontextmanager
async def lifespan(app: FastAPI):
    global HUBSPOT_WEBHOOK_SECRET
    secret = os.getenv("HUBSPOT_WEBHOOK_SECRET", "").strip()
    if not secret:
        raise RuntimeError(
            "HUBSPOT_WEBHOOK_SECRET is required. "
            "Set it to the HubSpot Private App webhook signing secret."
        )
    HUBSPOT_WEBHOOK_SECRET = secret
    logger.info("Webhook signature validation enabled")
    yield


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="HubSpot Contact Enrichment",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)


# ── Signature validation ──────────────────────────────────────────────────────


def _validate_hubspot_signature(
    secret: str,
    method: str,
    url: str,
    body: bytes,
    timestamp: str,
    signature_v3: str,
) -> bool:
    """
    Validate a HubSpot v3 webhook signature.

    HubSpot computes: HMAC-SHA256(secret, method + url + body + timestamp)
    and base64-encodes it. We compare with hmac.compare_digest to prevent
    timing attacks.

    Also checks that the timestamp is within 5 minutes to prevent replay attacks.
    """
    try:
        ts_int = int(timestamp)
        if abs(time.time() * 1000 - ts_int) > 5 * 60 * 1000:
            logger.warning("HubSpot webhook timestamp out of acceptable range")
            return False
    except (ValueError, TypeError):
        return False

    message = method.upper() + url + body.decode("utf-8", errors="replace") + timestamp
    expected = base64.b64encode(
        hmac.new(
            secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")

    return hmac.compare_digest(expected, signature_v3)


# ── Background task wrapper ───────────────────────────────────────────────────


def _run_enrichment_safe(company_id: str) -> None:
    """
    Top-level wrapper for run_enrichment.

    Catches all exceptions so the background task never dies silently.
    On any uncaught error, attempts a last-resort reset of request_to_enrich
    so the record doesn't get stuck in a permanently triggered state.
    """
    try:
        from enrichment_processor import run_enrichment

        run_enrichment(company_id)
    except Exception as exc:
        logger.exception(f"Unhandled error in enrichment for company {company_id}: {exc}")
        # Last-resort: reset the trigger flag
        try:
            from hubspot_client import HubSpotClient

            HubSpotClient().update_company(company_id, {"request_to_enrich": "false"})
            logger.info(f"Last-resort: reset request_to_enrich for company {company_id}")
        except Exception as reset_exc:
            logger.error(
                f"Last-resort flag reset also failed for company {company_id}: {reset_exc}"
            )


# ── Cloud Tasks enqueue ───────────────────────────────────────────────────────


def _enqueue_enrichment(company_id: str) -> None:
    """
    Create a Cloud Tasks HTTP task that delivers POST /enrich to this service.

    Cloud Tasks owns the job lifecycle: it retries on non-2xx responses and
    guarantees at-least-once delivery, so enrichment jobs survive instance
    shutdowns.

    Required env vars: CLOUD_RUN_SERVICE_URL, CLOUD_TASKS_SA_EMAIL
    Optional env vars: GCP_PROJECT_ID, GCP_LOCATION, CLOUD_TASKS_QUEUE
    """
    from google.cloud import tasks_v2

    project  = os.getenv("GCP_PROJECT_ID", "gen-lang-client-0844868008")
    location = os.getenv("GCP_LOCATION", "us-central1")
    queue    = os.getenv("CLOUD_TASKS_QUEUE", "hubspot-enrichment")
    base_url = os.getenv("CLOUD_RUN_SERVICE_URL", "").rstrip("/")
    sa_email = os.getenv("CLOUD_TASKS_SA_EMAIL", "")

    if not base_url:
        raise RuntimeError("CLOUD_RUN_SERVICE_URL is required for Cloud Tasks enqueue")
    if not sa_email:
        raise RuntimeError("CLOUD_TASKS_SA_EMAIL is required for Cloud Tasks enqueue")

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{base_url}/enrich",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"company_id": company_id}).encode(),
            "oidc_token": {
                "service_account_email": sa_email,
                "audience": base_url,
            },
        }
    }

    response = client.create_task(request={"parent": parent, "task": task})
    logger.info(f"Enqueued Cloud Task {response.name} for company_id={company_id}")


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health() -> dict:
    """Health check endpoint — used by Cloud Run startup/liveness probes."""
    return {"status": "ok"}


@app.post("/webhook")
async def handle_webhook(request: Request) -> JSONResponse:
    """
    Receive a HubSpot company property-change webhook.

    HubSpot sends a JSON array of event objects. We filter for events where
    propertyName == "request_to_enrich" and propertyValue is truthy, then
    enqueue a Cloud Task for each unique company ID.

    Returns 200 immediately — enrichment runs via Cloud Tasks delivery to /enrich.
    Requests without a valid HubSpot HMAC-SHA256 signature are rejected with 401.
    """
    body = await request.body()

    # ── Signature validation (always enforced) ────────────────────────────────
    sig_v3 = request.headers.get("X-HubSpot-Signature-V3", "")
    timestamp = request.headers.get("X-HubSpot-Request-Timestamp", "")
    if not sig_v3:
        logger.warning("Webhook received without X-HubSpot-Signature-V3 header")
        return JSONResponse({"error": "Missing signature"}, status_code=401)

    url = str(request.url)
    logger.info(f"Validating signature against url={url!r}")
    valid = _validate_hubspot_signature(
        secret=HUBSPOT_WEBHOOK_SECRET,
        method="POST",
        url=url,
        body=body,
        timestamp=timestamp,
        signature_v3=sig_v3,
    )
    if not valid:
        logger.warning("Invalid HubSpot webhook signature — request rejected")
        return JSONResponse({"error": "Invalid signature"}, status_code=401)

    # ── Parse payload ─────────────────────────────────────────────────────────
    try:
        payload: Any = json.loads(body)
    except json.JSONDecodeError as exc:
        logger.error(f"Webhook body is not valid JSON: {exc}")
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # HubSpot sends either a list of events or a single event object
    events: list[dict] = payload if isinstance(payload, list) else [payload]

    queued_ids: set[str] = set()
    for event in events:
        prop_name = event.get("propertyName", "")
        prop_value = str(event.get("propertyValue", "")).strip().lower()
        obj_id = str(event.get("objectId", "") or event.get("companyId", "")).strip()

        if not _COMPANY_ID_RE.match(obj_id):
            if obj_id:
                logger.warning(f"Ignoring event with invalid company_id format: {obj_id!r}")
            continue

        if prop_name == TRIGGER_PROPERTY and prop_value in TRIGGER_VALUES:
            queued_ids.add(obj_id)

    for company_id in queued_ids:
        logger.info(f"Enqueuing Cloud Task for company_id={company_id}")
        try:
            _enqueue_enrichment(company_id)
        except Exception as exc:
            logger.exception(f"Failed to enqueue Cloud Task for company {company_id}: {exc}")

    logger.info(
        f"Webhook processed: {len(events)} event(s), {len(queued_ids)} enrichment(s) enqueued"
    )
    return JSONResponse({"status": "accepted", "queued": len(queued_ids)})


@app.post("/enrich")
async def handle_enrich(request: Request) -> JSONResponse:
    """
    Cloud Tasks delivery endpoint — runs enrichment synchronously.

    Cloud Tasks delivers POST /enrich with {"company_id": "<id>"} in the body.
    This endpoint runs enrichment to completion before returning 200, so Cloud
    Tasks knows the job succeeded. On failure, _run_enrichment_safe handles
    cleanup (resetting request_to_enrich) and logs the error.

    Cloud Run's --no-allow-unauthenticated + OIDC token on the task ensures
    only Cloud Tasks (via the configured service account) can call this endpoint.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    company_id = str(body.get("company_id", "")).strip()
    if not _COMPANY_ID_RE.match(company_id):
        logger.warning(f"/enrich received invalid company_id: {company_id!r}")
        return JSONResponse({"error": "Invalid company_id"}, status_code=400)

    logger.info(f"Running enrichment for company_id={company_id}")
    _run_enrichment_safe(company_id)
    return JSONResponse({"status": "done", "company_id": company_id})
