"""
FastAPI webhook server for HubSpot contact enrichment.

Receives HubSpot company property-change webhooks and fires the enrichment
workflow as a background task, returning 200 immediately so HubSpot doesn't
retry.

Endpoints:
  GET  /health   — health check (used by Cloud Run)
  POST /webhook  — HubSpot company webhook receiver

Environment variables:
  HUBSPOT_API_KEY           HubSpot Private App token (required)
  HUBSPOT_WEBHOOK_SECRET    HMAC secret for signature validation (optional)
  OPENROUTER_API_KEY        OpenRouter / Perplexity key (required for research)
  BQ_PROJECT_ID             GCP project (default: gen-lang-client-0844868008)
  BQ_LOCATION               BigQuery location (default: US)
"""

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Request, Response
from fastapi.responses import JSONResponse

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("webhook_server")

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="HubSpot Contact Enrichment", docs_url=None, redoc_url=None)

HUBSPOT_WEBHOOK_SECRET = os.getenv("HUBSPOT_WEBHOOK_SECRET", "")
TRIGGER_PROPERTY = "request_to_enrich"
TRIGGER_VALUES = {"true", "1", "yes"}


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
    expected = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

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


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health() -> dict:
    """Health check endpoint — used by Cloud Run startup/liveness probes."""
    return {"status": "ok"}


@app.post("/webhook")
async def handle_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    """
    Receive a HubSpot company property-change webhook.

    HubSpot sends a JSON array of event objects. We filter for events where
    propertyName == "request_to_enrich" and propertyValue is truthy, then
    enqueue an enrichment background task for each unique company ID.

    Returns 200 immediately — enrichment runs asynchronously.
    """
    body = await request.body()

    # ── Optional signature validation ─────────────────────────────────────────
    if HUBSPOT_WEBHOOK_SECRET:
        sig_v3 = request.headers.get("X-HubSpot-Signature-V3", "")
        timestamp = request.headers.get("X-HubSpot-Request-Timestamp", "")
        if not sig_v3:
            logger.warning("Webhook received without X-HubSpot-Signature-V3 header")
            return JSONResponse({"error": "Missing signature"}, status_code=401)

        url = str(request.url)
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

        if prop_name == TRIGGER_PROPERTY and prop_value in TRIGGER_VALUES and obj_id:
            queued_ids.add(obj_id)

    for company_id in queued_ids:
        logger.info(f"Queuing enrichment for company_id={company_id}")
        background_tasks.add_task(_run_enrichment_safe, company_id)

    logger.info(
        f"Webhook processed: {len(events)} event(s), {len(queued_ids)} enrichment(s) queued"
    )
    return JSONResponse({"status": "accepted", "queued": len(queued_ids)})
