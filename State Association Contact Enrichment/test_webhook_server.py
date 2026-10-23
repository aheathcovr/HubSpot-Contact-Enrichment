"""
Tests for webhook_server: the public HubSpot webhook, the Cloud Tasks delivery
endpoint, and the task the webhook enqueues.

The service is internet-facing (HubSpot has to reach /webhook), so the two
gates matter most: a webhook without a valid HubSpot signature must be refused,
and /enrich must refuse anything not carrying Cloud Tasks' OIDC token. These
tests pin that behaviour so a dependency upgrade to FastAPI/Starlette cannot
change it unnoticed.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_webhook_server.py" -v
"""

import base64
import hashlib
import hmac
import json
import os
import sys
import time
from unittest.mock import create_autospec

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import webhook_server

_SECRET = "test-webhook-secret"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("HUBSPOT_WEBHOOK_SECRET", _SECRET)
    with TestClient(webhook_server.app) as c:   # entering runs the lifespan startup
        yield c


def _signed_headers(body: bytes, secret: str = _SECRET, timestamp: str | None = None) -> dict:
    """Headers HubSpot would send: v3 signature = base64(HMAC-SHA256(method+url+body+timestamp))."""
    timestamp = timestamp or str(int(time.time() * 1000))
    message = "POST" + "http://testserver/webhook" + body.decode() + timestamp
    digest = hmac.new(secret.encode(), message.encode(), hashlib.sha256).digest()
    return {
        "X-HubSpot-Signature-V3": base64.b64encode(digest).decode(),
        "X-HubSpot-Request-Timestamp": timestamp,
        "Content-Type": "application/json",
    }


def _trigger_event(company_id="12345", value="true", prop="request_to_enrich") -> bytes:
    return json.dumps([{"objectId": int(company_id), "propertyName": prop, "propertyValue": value}]).encode()


@pytest.fixture
def enqueued(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(webhook_server, "_enqueue_enrichment", calls.append)
    return calls


# ── /health ───────────────────────────────────────────────────────────────────

def test_health_is_ok_once_started(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ── /webhook: the public gate ─────────────────────────────────────────────────

def test_webhook_without_a_signature_is_refused(client, enqueued):
    response = client.post("/webhook", content=_trigger_event())

    assert response.status_code == 401
    assert enqueued == []


def test_webhook_with_a_wrong_signature_is_refused(client, enqueued):
    body = _trigger_event()

    response = client.post("/webhook", content=body, headers=_signed_headers(body, secret="not-the-secret"))

    assert response.status_code == 401
    assert enqueued == []


def test_webhook_with_a_stale_timestamp_is_refused(client, enqueued):
    body = _trigger_event()
    ten_minutes_ago = str(int((time.time() - 600) * 1000))

    response = client.post("/webhook", content=body, headers=_signed_headers(body, timestamp=ten_minutes_ago))

    assert response.status_code == 401
    assert enqueued == []


def test_signed_trigger_enqueues_one_task_for_the_company(client, enqueued):
    body = _trigger_event(company_id="12345")

    response = client.post("/webhook", content=body, headers=_signed_headers(body))

    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "queued": 1}
    assert enqueued == ["12345"]


def test_signed_event_for_another_property_or_a_false_value_enqueues_nothing(client, enqueued):
    for body in (_trigger_event(value="false"), _trigger_event(prop="lifecyclestage")):
        response = client.post("/webhook", content=body, headers=_signed_headers(body))

        assert response.status_code == 200
        assert response.json()["queued"] == 0
    assert enqueued == []


def test_signed_webhook_with_a_malformed_body_is_a_bad_request(client, enqueued):
    body = b"not json"

    response = client.post("/webhook", content=body, headers=_signed_headers(body))

    assert response.status_code == 400
    assert enqueued == []


# ── /enrich: the Cloud Tasks delivery endpoint ────────────────────────────────

def test_enrich_without_a_cloud_tasks_token_is_refused(client, monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)

    response = client.post("/enrich", json={"company_id": "12345"})

    assert response.status_code == 401
    assert ran == []


def test_enrich_with_a_valid_token_runs_enrichment_and_waits_for_it(client, monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)

    response = client.post("/enrich", json={"company_id": "12345"})

    assert response.status_code == 200
    assert response.json() == {"status": "done", "company_id": "12345"}
    assert ran == ["12345"]


def test_enrich_rejects_a_company_id_that_is_not_digits(client, monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)

    response = client.post("/enrich", json={"company_id": "12345; DROP TABLE"})

    assert response.status_code == 400
    assert ran == []


# ── OIDC token check ──────────────────────────────────────────────────────────

class _FakeRequest:
    def __init__(self, authorization: str):
        self.headers = {"Authorization": authorization}


def _patch_google_verifier(monkeypatch, claims):
    from google.oauth2 import id_token

    monkeypatch.setattr(id_token, "verify_oauth2_token", lambda token, transport, audience=None: claims)
    monkeypatch.setenv("CLOUD_RUN_SERVICE_URL", "https://svc.example.run.app")
    monkeypatch.setenv("CLOUD_TASKS_SA_EMAIL", "tasks@proj.iam.gserviceaccount.com")


def test_oidc_check_accepts_the_tasks_account(monkeypatch):
    _patch_google_verifier(monkeypatch, {"email": "tasks@proj.iam.gserviceaccount.com"})

    assert webhook_server._verify_cloud_tasks_oidc(_FakeRequest("Bearer abc")) is True


def test_oidc_check_refuses_a_different_account(monkeypatch):
    _patch_google_verifier(monkeypatch, {"email": "someone-else@proj.iam.gserviceaccount.com"})

    assert webhook_server._verify_cloud_tasks_oidc(_FakeRequest("Bearer abc")) is False


def test_oidc_check_refuses_a_missing_bearer_token(monkeypatch):
    _patch_google_verifier(monkeypatch, {"email": "tasks@proj.iam.gserviceaccount.com"})

    assert webhook_server._verify_cloud_tasks_oidc(_FakeRequest("")) is False


# ── The task the webhook enqueues ─────────────────────────────────────────────

def test_enqueued_task_posts_to_enrich_with_an_oidc_token(monkeypatch):
    """Built against the real Cloud Tasks client's signatures, with only the network stubbed."""
    from google.cloud import tasks_v2

    fake_client = create_autospec(tasks_v2.CloudTasksClient, instance=True)
    fake_client.queue_path.side_effect = tasks_v2.CloudTasksClient.queue_path
    fake_client.create_task.return_value.name = "projects/p/locations/l/queues/q/tasks/t"
    monkeypatch.setattr(tasks_v2, "CloudTasksClient", lambda: fake_client)
    monkeypatch.setenv("GCP_PROJECT_ID", "proj")
    monkeypatch.setenv("GCP_LOCATION", "us-central1")
    monkeypatch.setenv("CLOUD_TASKS_QUEUE", "hubspot-enrichment")
    monkeypatch.setenv("CLOUD_RUN_SERVICE_URL", "https://svc.example.run.app/")
    monkeypatch.setenv("CLOUD_TASKS_SA_EMAIL", "tasks@proj.iam.gserviceaccount.com")

    webhook_server._enqueue_enrichment("12345")

    request = fake_client.create_task.call_args.kwargs["request"]
    assert request["parent"] == "projects/proj/locations/us-central1/queues/hubspot-enrichment"
    http_request = request["task"]["http_request"]
    assert http_request["http_method"] == tasks_v2.HttpMethod.POST
    assert http_request["url"] == "https://svc.example.run.app/enrich"
    assert json.loads(http_request["body"]) == {"company_id": "12345"}
    assert http_request["oidc_token"] == {
        "service_account_email": "tasks@proj.iam.gserviceaccount.com",
        "audience": "https://svc.example.run.app",
    }
