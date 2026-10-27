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

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import requests as _http_requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

import app_run_store
import company_titles
import contact_objectives
import hubspot_request_auth
from hubspot_client import HubSpotError


# ── Structured JSON logging (Cloud Logging compatible) ────────────────────────

class _CloudLoggingFormatter(logging.Formatter):
    """Emit one JSON object per log line for Cloud Logging structured ingestion."""

    _SEVERITY = {
        logging.DEBUG:    "DEBUG",
        logging.INFO:     "INFO",
        logging.WARNING:  "WARNING",
        logging.ERROR:    "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "severity": self._SEVERITY.get(record.levelno, "DEFAULT"),
            "message":  record.getMessage(),
            "logger":   record.name,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # Callers can attach extra structured fields via extra={"json_fields": {...}}
        json_fields = getattr(record, "json_fields", None)
        if json_fields and isinstance(json_fields, dict):
            payload.update(json_fields)
        return json.dumps(payload, default=str)


def _setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_CloudLoggingFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)


_setup_logging()
logger = logging.getLogger("webhook_server")

# ── OIDC verification for /enrich ─────────────────────────────────────────────

# Reusable transport so Google's public-key cache persists across requests.
# Lock-guarded: with Cloud Run --concurrency > 1, two /enrich deliveries can
# now genuinely race on first use within the same warm instance.
_google_auth_transport = None
_google_auth_transport_lock = threading.Lock()

def _get_google_auth_transport():
    global _google_auth_transport
    if _google_auth_transport is None:
        with _google_auth_transport_lock:
            if _google_auth_transport is None:
                from google.auth.transport import requests as _gar
                _google_auth_transport = _gar.Request(session=_http_requests.Session())
    return _google_auth_transport


def _verify_cloud_tasks_oidc(request: Request) -> bool:
    """
    Verify the Google OIDC bearer token that Cloud Tasks attaches to every
    /enrich delivery.

    The service is --allow-unauthenticated so HubSpot can POST to /webhook,
    which means /enrich has no Cloud Run IAM layer protecting it. This function
    provides the equivalent application-level gate.

    Checks:
      1. Authorization: Bearer <token> header is present.
      2. Token is a valid Google-signed OIDC JWT.
      3. Token audience matches CLOUD_RUN_SERVICE_URL.
      4. Token email matches CLOUD_TASKS_SA_EMAIL.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False

    token = auth_header[7:]
    audience = os.getenv("CLOUD_RUN_SERVICE_URL", "").rstrip("/")
    sa_email = os.getenv("CLOUD_TASKS_SA_EMAIL", "")

    if not audience or not sa_email:
        logger.error(
            "OIDC verification skipped — CLOUD_RUN_SERVICE_URL or "
            "CLOUD_TASKS_SA_EMAIL is not configured",
        )
        return False

    try:
        from google.oauth2 import id_token as _id_token
        info = _id_token.verify_oauth2_token(token, _get_google_auth_transport(), audience=audience)
        return info.get("email") == sa_email
    except Exception as exc:
        logger.warning(f"OIDC token verification failed: {exc}")
        return False


# ── Startup validation ────────────────────────────────────────────────────────
HUBSPOT_WEBHOOK_SECRET: str = ""
TRIGGER_PROPERTY = "request_to_enrich"
TRIGGER_VALUES = {"true", "1", "yes"}

_COMPANY_ID_RE = re.compile(r'^\d{1,20}$')

# Set to True once lifespan startup completes; checked by /health readiness probe.
_ready: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global HUBSPOT_WEBHOOK_SECRET, _ready
    secret = os.getenv("HUBSPOT_WEBHOOK_SECRET", "").strip()
    if not secret:
        raise RuntimeError(
            "HUBSPOT_WEBHOOK_SECRET is required. "
            "Set it to the HubSpot Private App webhook signing secret."
        )
    HUBSPOT_WEBHOOK_SECRET = secret
    logger.info("Webhook signature validation enabled")
    _ready = True
    yield
    _ready = False


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

    Failures are logged as structured ERROR records with alert="enrichment_failure"
    so a Cloud Logging log-based alert can notify on-call when a company gets stuck.
    To wire up alerting: create a log-based metric on
      jsonPayload.alert="enrichment_failure"
    then attach a Cloud Monitoring alerting policy to that metric.
    """
    try:
        from enrichment_processor import run_enrichment

        run_enrichment(company_id)
    except Exception as exc:
        logger.error(
            "Enrichment failed — company stuck; manual review required",
            extra={
                "json_fields": {
                    "alert":      "enrichment_failure",
                    "company_id": company_id,
                    "error":      str(exc),
                    "traceback":  traceback.format_exc(),
                }
            },
        )
        # Last-resort: reset the trigger flag so the record isn't permanently stuck.
        # Reuses the same shared client run_enrichment() uses (see
        # enrichment_processor._get_hubspot_client) instead of building a second
        # fresh Session just for this one call.
        try:
            from enrichment_processor import _get_hubspot_client

            _get_hubspot_client().update_company(company_id, {"request_to_enrich": "false"})
            logger.info(
                "Last-resort: reset request_to_enrich after enrichment failure",
                extra={"json_fields": {"company_id": company_id}},
            )
        except Exception as reset_exc:
            logger.error(
                "Last-resort flag reset failed after enrichment failure",
                extra={
                    "json_fields": {
                        "alert":      "enrichment_failure",
                        "company_id": company_id,
                        "error":      str(reset_exc),
                    }
                },
            )


# ── Cloud Tasks enqueue ───────────────────────────────────────────────────────

# A task may run as long as the Cloud Run service may (--timeout=1800s). Cloud Tasks
# gives an HTTP task 10 minutes unless told otherwise; a corporation run that took
# longer was cut off and retried, and a retry pays the paid vendors a second time.
# 30 minutes is the most Cloud Tasks allows for an HTTP target.
_DISPATCH_DEADLINE_SECONDS = 1800

# One client for the whole process. Building a CloudTasksClient opens a gRPC channel
# and looks up credentials; doing that on every webhook was most of the 1 to 7 seconds
# the 2026-09-24 webhooks took, against a 5 second timeout on HubSpot's side.
# Lock-guarded for the same reason as _google_auth_transport above.
_tasks_client = None
_tasks_client_lock = threading.Lock()


def _get_tasks_client():
    global _tasks_client
    if _tasks_client is None:
        with _tasks_client_lock:
            if _tasks_client is None:
                from google.cloud import tasks_v2

                _tasks_client = tasks_v2.CloudTasksClient()
    return _tasks_client


def _event_key(event: dict) -> str:
    """Identify one HubSpot event across its redeliveries.

    HubSpot retries a webhook it thinks timed out, up to ten times, with the same
    occurredAt and a higher attemptNumber. Its docs say eventId alone "is not
    guaranteed to be unique", so the key is the event time, plus the eventId when
    there is one. Two separate triggers of one company differ in occurredAt.
    Empty when the event carries no time: such a task is unnamed and not deduplicated.
    """
    occurred = str(event.get("occurredAt", "") or "").strip()
    if not occurred:
        return ""
    event_id = str(event.get("eventId", "") or "").strip()
    return f"{occurred}-{event_id}" if event_id else occurred


def _task_id(company_id: str, event_key: str) -> str:
    """A Cloud Tasks task id (letters, digits, - and _ only), or "" for an unkeyed event."""
    if not event_key:
        return ""
    return f"enrich-{company_id}-{re.sub(r'[^A-Za-z0-9_-]', '-', event_key)}"[:200]


def _enqueue_enrichment(company_id: str, event_key: str = "") -> None:
    """
    Create a Cloud Tasks HTTP task that delivers POST /enrich to this service.

    Cloud Tasks owns the job lifecycle: it retries on non-2xx responses and
    guarantees at-least-once delivery, so enrichment jobs survive instance
    shutdowns.

    When the event has a key the task is named after it, so a second delivery of the
    same HubSpot event is refused by Cloud Tasks (AlreadyExists) instead of becoming a
    second enrichment run. That refusal is the success case here: the first task is
    already queued.

    Required env vars: CLOUD_RUN_SERVICE_URL, CLOUD_TASKS_SA_EMAIL
    Optional env vars: GCP_PROJECT_ID, GCP_LOCATION, CLOUD_TASKS_QUEUE
    """
    from google.api_core import exceptions as gexc
    from google.cloud import tasks_v2
    from google.protobuf import duration_pb2

    project  = os.getenv("GCP_PROJECT_ID", "gen-lang-client-0844868008")
    location = os.getenv("GCP_LOCATION", "us-central1")
    queue    = os.getenv("CLOUD_TASKS_QUEUE", "hubspot-enrichment")
    base_url = os.getenv("CLOUD_RUN_SERVICE_URL", "").rstrip("/")
    sa_email = os.getenv("CLOUD_TASKS_SA_EMAIL", "")

    if not base_url:
        raise RuntimeError("CLOUD_RUN_SERVICE_URL is required for Cloud Tasks enqueue")
    if not sa_email:
        raise RuntimeError("CLOUD_TASKS_SA_EMAIL is required for Cloud Tasks enqueue")

    client = _get_tasks_client()
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
        },
        "dispatch_deadline": duration_pb2.Duration(seconds=_DISPATCH_DEADLINE_SECONDS),
    }
    task_id = _task_id(company_id, event_key)
    if task_id:
        task["name"] = client.task_path(project, location, queue, task_id)

    try:
        response = client.create_task(request={"parent": parent, "task": task})
    except gexc.AlreadyExists:
        logger.info(
            f"Cloud Task {task_id} already queued for company_id={company_id}; "
            f"ignoring the duplicate HubSpot delivery"
        )
        return
    logger.info(f"Enqueued Cloud Task {response.name} for company_id={company_id}")


def _enqueue_app_run(run_id: str) -> None:
    """
    Create a Cloud Tasks HTTP task that delivers POST /app/run for one HubSpot app run.

    Same queue, service account and OIDC audience as _enqueue_enrichment. The task is named after the run, so a second
    enqueue of the same run is refused (AlreadyExists) instead of becoming a second task.
    """
    from google.api_core import exceptions as gexc
    from google.cloud import tasks_v2
    from google.protobuf import duration_pb2

    project  = os.getenv("GCP_PROJECT_ID", "gen-lang-client-0844868008")
    location = os.getenv("GCP_LOCATION", "us-central1")
    queue    = os.getenv("CLOUD_TASKS_QUEUE", "hubspot-enrichment")
    base_url = os.getenv("CLOUD_RUN_SERVICE_URL", "").rstrip("/")
    sa_email = os.getenv("CLOUD_TASKS_SA_EMAIL", "")
    if not base_url:
        raise RuntimeError("CLOUD_RUN_SERVICE_URL is required for Cloud Tasks enqueue")
    if not sa_email:
        raise RuntimeError("CLOUD_TASKS_SA_EMAIL is required for Cloud Tasks enqueue")

    client = _get_tasks_client()
    task = {
        "name": client.task_path(project, location, queue, f"app-run-{run_id}"),
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{base_url}/app/run",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"run_id": run_id}).encode(),
            "oidc_token": {"service_account_email": sa_email, "audience": base_url},
        },
        "dispatch_deadline": duration_pb2.Duration(seconds=_DISPATCH_DEADLINE_SECONDS),
    }
    try:
        client.create_task(request={"parent": client.queue_path(project, location, queue), "task": task})
    except gexc.AlreadyExists:
        logger.info(f"Cloud Task for app run {run_id} already queued")


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health() -> JSONResponse:
    """
    Readiness/liveness probe for Cloud Run.

    Returns 503 until the lifespan startup hook has run (env vars loaded,
    secrets validated). Cloud Run startup probes will retry until 200 is
    returned, so no traffic is routed to a cold instance before it is ready.
    """
    if not _ready:
        return JSONResponse({"status": "starting"}, status_code=503)
    return JSONResponse({"status": "ok"})


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

    queued_ids: dict[str, str] = {}   # company id -> the key of the event that asked for it
    for event in events:
        prop_name = event.get("propertyName", "")
        prop_value = str(event.get("propertyValue", "")).strip().lower()
        obj_id = str(event.get("objectId", "") or event.get("companyId", "")).strip()

        if not _COMPANY_ID_RE.match(obj_id):
            if obj_id:
                logger.warning(f"Ignoring event with invalid company_id format: {obj_id!r}")
            continue

        if prop_name == TRIGGER_PROPERTY and prop_value in TRIGGER_VALUES:
            queued_ids.setdefault(obj_id, _event_key(event))

    # Cloud Tasks' client is blocking, so each enqueue runs on a worker thread: on the
    # event loop it stalled every other request on the instance, and HubSpot gives the
    # whole webhook 5 seconds. The companies are enqueued side by side, and one failing
    # does not stop the others.
    async def _enqueue_one(company_id: str, event_key: str) -> None:
        logger.info(f"Enqueuing Cloud Task for company_id={company_id}")
        try:
            await run_in_threadpool(_enqueue_enrichment, company_id, event_key)
        except Exception as exc:
            logger.exception(f"Failed to enqueue Cloud Task for company {company_id}: {exc}")

    await asyncio.gather(*(_enqueue_one(c, k) for c, k in queued_ids.items()))

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

    Enrichment itself is synchronous/blocking (requests, time.sleep polling,
    BigQuery). It runs via run_in_threadpool so it doesn't block this process's
    single asyncio event loop — without that, a single long-running enrichment
    would stall /health and every other concurrent request on the same
    instance, regardless of Cloud Run's --concurrency setting.

    The service is --allow-unauthenticated (so HubSpot can reach /webhook), so
    /enrich is guarded at the application layer by verifying the Google OIDC
    bearer token that Cloud Tasks attaches to every delivery.
    """
    if not _verify_cloud_tasks_oidc(request):
        logger.warning(
            "/enrich rejected: missing or invalid Cloud Tasks OIDC token",
            extra={"json_fields": {"alert": "enrich_unauthorized"}},
        )
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    company_id = str(body.get("company_id", "")).strip()
    if not _COMPANY_ID_RE.match(company_id):
        logger.warning(f"/enrich received invalid company_id: {company_id!r}")
        return JSONResponse({"error": "Invalid company_id"}, status_code=400)

    logger.info(f"Running enrichment for company_id={company_id}")
    await run_in_threadpool(_run_enrichment_safe, company_id)
    return JSONResponse({"status": "done", "company_id": company_id})


# ── HubSpot app: the contact card ─────────────────────────────────────────────
#
# The card calls /app/contact/status and /app/contact/run with hubspot.fetch, which HubSpot signs (see
# hubspot_request_auth). The run itself is a Cloud Task delivered to /app/run, like /enrich, so the card's call
# returns at once. Nothing here is reachable until HUBSPOT_APP_CLIENT_SECRET is set: the endpoints answer 503.

_CONTACT_ID_RE = re.compile(r"^\d{1,20}$")
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_APP_DEFAULT_DAILY_CAP = 25
_APP_REUSE_WINDOW = timedelta(hours=24)
_APP_RECENT_RUNS = 5
_APP_BATCH_MAX_CONTACTS = 10
_APP_COMPANY_RUNS = 20
_APP_COMPANY_ADD_RUNS = 100   # runs of a company read to find the latest add or earlier search


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_hs_client = None
_hs_client_lock = threading.Lock()


def _hubspot():
    """One HubSpot client for the process, built when first needed (it reads HUBSPOT_API_KEY)."""
    global _hs_client
    if _hs_client is None:
        with _hs_client_lock:
            if _hs_client is None:
                from hubspot_client import HubSpotClient

                _hs_client = HubSpotClient()
    return _hs_client


def _run_store():
    return app_run_store.get_run_store()


def _daily_cap() -> int:
    """Runs a rep may start per UTC day: APP_DAILY_RUN_CAP, or 25 when it is unset or not a positive number."""
    try:
        cap = int(os.getenv("APP_DAILY_RUN_CAP", "").strip())
    except ValueError:
        return _APP_DEFAULT_DAILY_CAP
    return cap if cap > 0 else _APP_DEFAULT_DAILY_CAP


def _app_error(message: str, status: int, **extra) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message, **extra}, status_code=status)


def _quota(store, who, now: datetime) -> dict:
    cap = _daily_cap()
    used = store.used_today(who.user_id, app_run_store.day_of(now))
    return {"cap": cap, "used": used, "remaining": max(0, cap - used)}


def _hubspot_failure(exc: HubSpotError, what: str = "contact") -> tuple:
    if exc.status_code == 404:
        return 404, {"ok": False, "error": f"HubSpot has no such {what}."}
    logger.warning(f"HubSpot call for the app failed: {exc}")
    return 502, {"ok": False, "error": "HubSpot could not be reached. Try again in a moment."}


async def _authenticate_app_request(request: Request):
    """(identity, body, None) for a request HubSpot signed, or (None, body, the refusal to send)."""
    body = await request.body()
    try:
        who = hubspot_request_auth.authenticate(
            secret=os.getenv("HUBSPOT_APP_CLIENT_SECRET", ""),
            method=request.method,
            url=str(request.url),
            body=body,
            signature=request.headers.get("X-HubSpot-Signature-v3"),
            timestamp=request.headers.get("X-HubSpot-Request-Timestamp"),
            expected_portal=os.getenv("HUBSPOT_PORTAL_ID", "").strip(),
        )
    except hubspot_request_auth.AuthError as exc:
        if exc.status_code != 503:
            logger.warning(f"/app request refused ({exc.status_code}): {exc.message}")
        return None, body, _app_error(exc.message, exc.status_code)
    return who, body, None


def _app_status(contact_id: str, who) -> tuple:
    store, now = _run_store(), _utcnow()
    try:
        contact = _hubspot().get_contact_for_app(contact_id)
    except HubSpotError as exc:
        return _hubspot_failure(exc)
    return 200, {
        "ok": True,
        "contact": {"id": contact_id, **contact_objectives.contact_summary(contact)},
        "objectives": contact_objectives.objectives_for(contact),
        "quota": _quota(store, who, now),
        "runs": [app_run_store.run_view(r) for r in store.recent_runs(contact_id, _APP_RECENT_RUNS)],
    }


def _app_start(contact_id: str, objective: str, who) -> tuple:
    store, now = _run_store(), _utcnow()
    try:
        contact = _hubspot().get_contact_for_app(contact_id)
    except HubSpotError as exc:
        return _hubspot_failure(exc)
    shown = next(o for o in contact_objectives.objectives_for(contact) if o["id"] == objective)
    if not shown["available"]:
        return 409, {"ok": False, "error": shown["reason"], "reason": shown["reason"]}

    # The same thing asked of the same contact within a day is the same run: nothing more is spent, and a double
    # click or a retry by HubSpot (it retries a 5xx once) finds the first run.
    repeat = store.find_recent_run(contact_id, objective, now - _APP_REUSE_WINDOW)
    if repeat is not None:
        return 202, {"ok": True, "reused": True, "run": app_run_store.run_view(repeat), "quota": _quota(store, who, now)}

    day = app_run_store.day_of(now)
    taken, _ = store.take_place(who.user_id, day, _daily_cap(), now)
    if not taken:
        quota = _quota(store, who, now)
        return 429, {"ok": False, "error": f"You have used all {quota['cap']} runs for today.", "quota": quota}

    run_id = uuid.uuid4().hex
    store.create_run({
        "run_id": run_id, "contact_id": contact_id, "objective": objective, "requested_by_id": who.user_id,
        "requested_by": who.email, "portal_id": who.portal_id, "created_at": now,
    })
    try:
        _enqueue_app_run(run_id)
    except Exception:
        logger.exception(f"Could not queue app run {run_id}")
        store.give_back_place(who.user_id, day)
        store.finish_run(run_id, _utcnow(), status="failed", outcome="error", message="The run could not be queued. Try again.", wrote=[])
        return 502, {"ok": False, "error": "The run could not be queued. Try again in a moment."}
    return 202, {"ok": True, "reused": False, "run": app_run_store.run_view(store.get_run(run_id)), "quota": _quota(store, who, now)}


def _execute_app_run(run_id: str) -> str:
    """Do one queued run. "skipped" when it is not queued (done already, running, or not there)."""
    store = _run_store()
    if not store.claim_run(run_id, _utcnow()):
        return "skipped"
    run = store.get_run(run_id)
    requested_by = run.get("requested_by") or f"HubSpot user {run.get('requested_by_id', '')}"
    try:
        if run["objective"] == company_titles.FIND_OBJECTIVE:
            outcome = company_titles.run_find_by_title(run, hs=_hubspot(), store=store, now=_utcnow())
        elif run["objective"] == company_titles.ADD_OBJECTIVE:
            outcome = company_titles.run_add_candidate(run, hs=_hubspot(), store=store)
        else:
            outcome = contact_objectives.run_objective(run["objective"], run["contact_id"], hs=_hubspot(), requested_by=requested_by)
    except Exception:
        logger.exception(f"App run {run_id} crashed")
        outcome = contact_objectives.Outcome("failed", "error", "Something went wrong. Nothing was changed. Try again.")
    store.finish_run(run_id, _utcnow(), status=outcome.status, outcome=outcome.outcome, message=outcome.message, wrote=outcome.wrote,
                     contact_id=outcome.contact_id or None)
    logger.info(f"App run {run_id} ({run['objective']}) ended {outcome.status}/{outcome.outcome}")
    return "done"


@app.get("/app/contact/status")
async def handle_app_contact_status(request: Request) -> JSONResponse:
    """What a contact has, which objectives are open, the rep's runs left today, and the latest runs on the contact."""
    who, _, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    contact_id = request.query_params.get("contactId", "").strip()
    if not _CONTACT_ID_RE.match(contact_id):
        return _app_error("contactId must be the contact's id, digits only.", 400)
    status, payload = await run_in_threadpool(_app_status, contact_id, who)
    return JSONResponse(payload, status_code=status)


@app.post("/app/contact/run")
async def handle_app_contact_run(request: Request) -> JSONResponse:
    """Accept a run for one contact and one objective, and queue it. Answers at once; the card then asks for status."""
    who, body, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _app_error("The request body is not JSON.", 400)
    if not isinstance(payload, dict):
        return _app_error("The request body must be an object.", 400)
    raw_id, objective = payload.get("contactId"), payload.get("objective")
    contact_id = str(raw_id).strip() if isinstance(raw_id, (str, int)) and not isinstance(raw_id, bool) else ""
    if not _CONTACT_ID_RE.match(contact_id):
        return _app_error("contactId must be the contact's id, digits only.", 400)
    if not isinstance(objective, str) or objective not in {o[0] for o in contact_objectives.OBJECTIVES}:
        return _app_error("objective must be find_email, find_mobile or verify_email.", 400)
    status, result = await run_in_threadpool(_app_start, contact_id, objective, who)
    return JSONResponse(result, status_code=status)


# ── HubSpot app: the company card ─────────────────────────────────────────────
#
# Two things from the company card, both made of the same runs the contact card makes (queued, done by the worker at
# /app/run, shown by run_view):
#
#   picking contacts   a rep ticks contacts and chooses an objective. Each run is one contact and one of find_email,
#                      find_mobile, verify_email. Enrich is only a plan for which of those a contact needs, so it is
#                      never stored.
#   finding by title   a rep sends job titles; a search is queued (it writes nothing to HubSpot) and the card reads the
#                      people it found; the rep chooses some and one add is queued for each (company_titles.py).

_CANDIDATE_ID_RE = re.compile(r"^[0-9a-f]{12}$")


def _id_of(value) -> str:
    """An id as digits-only text, or "" for anything else (numbers are what HubSpot gives; a bool is not an id)."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    text = str(value).strip()
    return text if _CONTACT_ID_RE.match(text) else ""


def _cap_refusal(store, who, now: datetime, needed: int) -> dict:
    quota = _quota(store, who, now)
    if quota["remaining"] == 0:
        return {"ok": False, "error": f"You have used all {quota['cap']} runs for today.", "quota": quota}
    return {"ok": False, "error": f"This needs {needed} runs and you have {quota['remaining']} left today.", "quota": quota}


def _take_places(store, who, now: datetime, count: int) -> bool:
    """One place in the rep's cap for each run, all or none: if they do not all fit, the ones taken are given back."""
    day, cap, taken = app_run_store.day_of(now), _daily_cap(), 0
    for _ in range(count):
        ok, _used = store.take_place(who.user_id, day, cap, now)
        if not ok:
            break
        taken += 1
    if taken < count:
        for _ in range(taken):
            store.give_back_place(who.user_id, day)
        return False
    return True


def _queue_run(store, who, now: datetime, record: dict) -> dict:
    """Create the run and queue it; the run as the card shows it. A run that cannot be queued is failed and gives its place back."""
    run_id = uuid.uuid4().hex
    store.create_run({
        **record, "run_id": run_id, "requested_by_id": who.user_id, "requested_by": who.email, "portal_id": who.portal_id, "created_at": now,
    })
    try:
        _enqueue_app_run(run_id)
    except Exception:
        logger.exception(f"Could not queue app run {run_id}")
        store.give_back_place(who.user_id, app_run_store.day_of(now))
        store.finish_run(run_id, _utcnow(), status="failed", outcome="error", message="The run could not be queued. Try again.", wrote=[])
    return app_run_store.run_view(store.get_run(run_id))


def _queue_entries(store, who, now: datetime, entries: list, what: str) -> tuple:
    """
    Queue the runs a request planned. An entry with a "record" is a run to start (it takes a place); the others are
    already settled (reused, skipped or failed). 202 when anything started or was reused, else why nothing did.
    """
    to_start = [e for e in entries if "record" in e]
    if not _take_places(store, who, now, len(to_start)):
        return 429, _cap_refusal(store, who, now, len(to_start))
    batch_id = uuid.uuid4().hex
    for entry in to_start:
        view = _queue_run(store, who, now, {**entry["record"], "batch_id": batch_id})
        entry["item"]["run"] = view
        if view["status"] == "failed":
            entry["item"].update(status="failed", reason="The run could not be queued. Try again in a moment.")
        else:
            entry["item"]["status"] = "started"

    items = [e["item"] for e in entries]
    started = sum(1 for i in items if i["status"] == "started")
    quota = _quota(store, who, now)
    if started or any(i["status"] == "reused" for i in items):
        return 202, {"ok": True, "batchId": batch_id, "items": items, "started": started, "quota": quota}
    if any(i["status"] == "failed" for i in items):
        return 502, {"ok": False, "error": "Nothing could be started. Try again in a moment.", "items": items, "quota": quota}
    return 409, {"ok": False, "error": f"Nothing to start for these {what}.", "items": items, "quota": quota}


def _read_json_object(body: bytes):
    """(payload, None), or (None, the 400 to send) for a body that is not a JSON object."""
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _app_error("The request body is not JSON.", 400)
    if not isinstance(payload, dict):
        return None, _app_error("The request body must be an object.", 400)
    return payload, None


# ── Picking contacts ──────────────────────────────────────────────────────────

def _app_company_status(company_id: str, who) -> tuple:
    store, now = _run_store(), _utcnow()
    try:
        props = _hubspot().get_company_properties(company_id)
    except HubSpotError as exc:
        return _hubspot_failure(exc, "company")
    kind = contact_objectives.company_kind(props.get("hierarchy_type", ""))
    return 200, {
        "ok": True,
        "company": {"id": company_id, "name": props.get("name") or "", "kind": kind},
        "objectives": [{"id": oid, "label": label, "spends": spends} for oid, label, spends in contact_objectives.BATCH_OBJECTIVES],
        "titlePresets": company_titles.presets_for(kind),
        "limits": {"maxContacts": _APP_BATCH_MAX_CONTACTS, "maxTitles": company_titles.MAX_TITLES},
        "quota": _quota(store, who, now),
        "runs": [app_run_store.run_view(r) for r in store.runs_for_company(company_id, _APP_COMPANY_RUNS)],
    }


def _item(contact_id: str, objective: str, status: str, reason=None, run=None) -> dict:
    return {"contactId": contact_id, "objective": objective, "status": status, "reason": reason, "run": run}


def _plan_company_runs(hs, store, company_id: str, contact_ids: list, objective: str, now: datetime) -> list:
    """
    One entry per run a request means, in the order the rep ticked: to start (it has a record), reused, skipped or
    failed. Nothing is created here and no place is taken.
    """
    entries = []
    for contact_id in contact_ids:
        try:
            if company_id not in [str(c) for c in hs.get_contact_company_ids(contact_id)]:
                entries.append({"item": _item(contact_id, objective, "skipped", "Not linked to this company.")})
                continue
            contact = hs.get_contact_for_app(contact_id)
        except HubSpotError as exc:
            logger.warning(f"HubSpot could not be read for contact {contact_id} in a company run: {exc}")
            entries.append({"item": _item(contact_id, objective, "failed", "HubSpot could not be read for this contact. Try again in a moment.")})
            continue
        steps, reason = contact_objectives.plan_for(objective, contact)
        if not steps:
            entries.append({"item": _item(contact_id, objective, "skipped", reason)})
            continue
        for step in steps:
            repeat = store.find_recent_run(contact_id, step, now - _APP_REUSE_WINDOW)
            if repeat is not None:
                entries.append({"item": _item(contact_id, step, "reused", None, app_run_store.run_view(repeat))})
            else:
                entries.append({"item": _item(contact_id, step, "pending"), "record": {"contact_id": contact_id, "objective": step, "company_id": company_id}})
    return entries


def _app_company_start(company_id: str, contact_ids: list, objective: str, who) -> tuple:
    store, now = _run_store(), _utcnow()
    entries = _plan_company_runs(_hubspot(), store, company_id, contact_ids, objective, now)
    return _queue_entries(store, who, now, entries, "contacts")


# ── Finding by title ──────────────────────────────────────────────────────────

def _search_of(store, company_id: str, run_id: str):
    """The title search a request names, or None when there is no such search for this company."""
    run = store.get_run(run_id)
    if not run or run.get("company_id") != company_id or run.get("objective") != company_titles.FIND_OBJECTIVE:
        return None
    return run


def _latest_adds(store, company_id: str, run_id: str) -> dict:
    """Candidate id to the newest add run for it from this search (runs_for_company is newest first)."""
    latest = {}
    for run in store.runs_for_company(company_id, _APP_COMPANY_ADD_RUNS):
        if run.get("objective") == company_titles.ADD_OBJECTIVE and run.get("source_run_id") == run_id:
            latest.setdefault(run.get("candidate_id"), run)
    return latest


def _app_find_start(company_id: str, titles: list, who) -> tuple:
    store, now = _run_store(), _utcnow()
    try:
        _hubspot().get_company_properties(company_id)
    except HubSpotError as exc:
        return _hubspot_failure(exc, "company")

    # The same search within a day is the same run: nothing more is spent, and a double click or a retry by HubSpot finds it.
    key = company_titles.titles_key(titles)
    since = now - _APP_REUSE_WINDOW
    repeat = next((r for r in store.runs_for_company(company_id, _APP_COMPANY_ADD_RUNS)
                   if r.get("objective") == company_titles.FIND_OBJECTIVE and r.get("titles_key") == key and r["created_at"] >= since and r["status"] != "failed"), None)
    if repeat is not None:
        return 202, {"ok": True, "reused": True, "run": app_run_store.run_view(repeat), "quota": _quota(store, who, now)}

    if not _take_places(store, who, now, 1):
        return 429, _cap_refusal(store, who, now, 1)
    view = _queue_run(store, who, now, {"contact_id": "", "objective": company_titles.FIND_OBJECTIVE, "titles": titles, "titles_key": key, "company_id": company_id})
    quota = _quota(store, who, now)
    if view["status"] == "failed":
        return 502, {"ok": False, "error": "The search could not be queued. Try again in a moment.", "quota": quota}
    return 202, {"ok": True, "reused": False, "run": view, "quota": quota}


_GONE = {"ok": False, "error": company_titles.SEARCH_GONE}


def _app_candidates(company_id: str, run_id: str) -> tuple:
    store = _run_store()
    run = _search_of(store, company_id, run_id)
    if run is None:
        return 404, {"ok": False, "error": "No such search."}
    view = app_run_store.run_view(run)
    if run["status"] != "done":
        return 200, {"ok": True, "ready": False, "run": view, "candidates": []}
    kept = store.get_candidates(run_id)
    if kept is None or kept.get("company_id") != company_id:
        return 410, dict(_GONE)
    adds = _latest_adds(store, company_id, run_id)
    return 200, {
        "ok": True, "ready": True, "run": view,
        "candidates": [
            {
                "candidateId": c["candidate_id"], "name": c["full_name"], "title": c.get("title") or None, "linkedinUrl": c.get("linkedin_url") or None,
                "updatedAt": c.get("updated_at") or None, "linkedContactId": c.get("linked_contact_id") or None,
                "added": app_run_store.run_view(adds[c["candidate_id"]]) if c["candidate_id"] in adds else None,
            }
            for c in kept.get("candidates", [])
        ],
    }


def _add_item(candidate_id: str, status: str, reason=None, run=None) -> dict:
    return {"candidateId": candidate_id, "objective": company_titles.ADD_OBJECTIVE, "status": status, "reason": reason, "run": run}


def _app_add_start(company_id: str, run_id: str, candidate_ids: list, who) -> tuple:
    store, now = _run_store(), _utcnow()
    run = _search_of(store, company_id, run_id)
    if run is None:
        return 404, {"ok": False, "error": "No such search."}
    if run["status"] != "done":
        return 409, {"ok": False, "error": "The search has not finished."}
    kept = store.get_candidates(run_id)
    if kept is None or kept.get("company_id") != company_id:
        return 410, dict(_GONE)

    known = {c["candidate_id"] for c in kept.get("candidates", [])}
    adds = _latest_adds(store, company_id, run_id)
    entries = []
    for candidate_id in candidate_ids:
        if candidate_id not in known:
            entries.append({"item": _add_item(candidate_id, "skipped", "Not in these search results.")})
        elif candidate_id in adds and adds[candidate_id]["status"] != "failed":
            entries.append({"item": _add_item(candidate_id, "reused", None, app_run_store.run_view(adds[candidate_id]))})
        else:
            entries.append({
                "item": _add_item(candidate_id, "pending"),
                "record": {"contact_id": "", "objective": company_titles.ADD_OBJECTIVE, "company_id": company_id, "source_run_id": run_id, "candidate_id": candidate_id},
            })
    return _queue_entries(store, who, now, entries, "people")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/app/company/status")
async def handle_app_company_status(request: Request) -> JSONResponse:
    """What the company card needs beyond its contact list: the company's kind, what can be asked, the rep's quota and the runs."""
    who, _, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    company_id = request.query_params.get("companyId", "").strip()
    if not _CONTACT_ID_RE.match(company_id):
        return _app_error("companyId must be the company's id, digits only.", 400)
    status, payload = await run_in_threadpool(_app_company_status, company_id, who)
    return JSONResponse(payload, status_code=status)


@app.post("/app/company/run")
async def handle_app_company_run(request: Request) -> JSONResponse:
    """Accept runs for the contacts a rep picked on a company, and queue them. Answers at once; the card then asks for status."""
    who, body, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    payload, bad = _read_json_object(body)
    if bad is not None:
        return bad
    company_id = _id_of(payload.get("companyId"))
    if not company_id:
        return _app_error("companyId must be the company's id, digits only.", 400)
    raw_ids = payload.get("contactIds")
    if not isinstance(raw_ids, list) or not raw_ids or any(not _id_of(c) for c in raw_ids):
        return _app_error("contactIds must be a list of contact ids, digits only.", 400)
    contact_ids = list(dict.fromkeys(_id_of(c) for c in raw_ids))
    if len(contact_ids) > _APP_BATCH_MAX_CONTACTS:
        return _app_error(f"Pick at most {_APP_BATCH_MAX_CONTACTS} contacts at a time.", 400)
    objective = payload.get("objective")
    if not isinstance(objective, str) or objective not in {o[0] for o in contact_objectives.BATCH_OBJECTIVES}:
        return _app_error("objective must be find_email, find_mobile, verify_email or enrich.", 400)
    status, result = await run_in_threadpool(_app_company_start, company_id, contact_ids, objective, who)
    return JSONResponse(result, status_code=status)


@app.post("/app/company/find")
async def handle_app_company_find(request: Request) -> JSONResponse:
    """Queue a search for the people holding some job titles at a company. Writes nothing to HubSpot; the card then reads the people."""
    who, body, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    payload, bad = _read_json_object(body)
    if bad is not None:
        return bad
    company_id = _id_of(payload.get("companyId"))
    if not company_id:
        return _app_error("companyId must be the company's id, digits only.", 400)
    titles, problem = company_titles.clean_titles(payload.get("titles"))
    if problem:
        return _app_error(problem, 400)
    status, result = await run_in_threadpool(_app_find_start, company_id, titles, who)
    return JSONResponse(result, status_code=status)


@app.get("/app/company/candidates")
async def handle_app_company_candidates(request: Request) -> JSONResponse:
    """The people a finished search found, with how adding each is going."""
    who, _, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    company_id = request.query_params.get("companyId", "").strip()
    run_id = request.query_params.get("runId", "").strip()
    if not _CONTACT_ID_RE.match(company_id):
        return _app_error("companyId must be the company's id, digits only.", 400)
    if not _RUN_ID_RE.match(run_id):
        return _app_error("runId must be the search's id.", 400)
    status, payload = await run_in_threadpool(_app_candidates, company_id, run_id)
    return JSONResponse(payload, status_code=status)


@app.post("/app/company/add")
async def handle_app_company_add(request: Request) -> JSONResponse:
    """Queue an add for each person a rep chose from a search's results."""
    who, body, refused = await _authenticate_app_request(request)
    if refused is not None:
        return refused
    payload, bad = _read_json_object(body)
    if bad is not None:
        return bad
    company_id = _id_of(payload.get("companyId"))
    if not company_id:
        return _app_error("companyId must be the company's id, digits only.", 400)
    run_id = payload.get("runId")
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        return _app_error("runId must be the search's id.", 400)
    raw = payload.get("candidateIds")
    if not isinstance(raw, list) or not raw or any(not isinstance(c, str) or not _CANDIDATE_ID_RE.match(c) for c in raw):
        return _app_error("candidateIds must be a list of the people's ids from the search.", 400)
    candidate_ids = list(dict.fromkeys(raw))
    if len(candidate_ids) > _APP_BATCH_MAX_CONTACTS:
        return _app_error(f"Choose at most {_APP_BATCH_MAX_CONTACTS} people at a time.", 400)
    status, result = await run_in_threadpool(_app_add_start, company_id, run_id, candidate_ids, who)
    return JSONResponse(result, status_code=status)


@app.post("/app/run")
async def handle_app_run(request: Request) -> JSONResponse:
    """
    Cloud Tasks delivery for one app run. Like /enrich it answers 200 whether the run worked or not, so Cloud Tasks
    never retries paid work, and it accepts only Cloud Tasks' own OIDC token.
    """
    if not _verify_cloud_tasks_oidc(request):
        logger.warning("/app/run rejected: missing or invalid Cloud Tasks OIDC token")
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    run_id = body.get("run_id") if isinstance(body, dict) else None
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        return JSONResponse({"error": "Invalid run_id"}, status_code=400)
    result = await run_in_threadpool(_execute_app_run, run_id)
    return JSONResponse({"status": result, "run_id": run_id})
