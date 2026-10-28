"""
Tests for webhook_server: the public HubSpot webhook, the Cloud Tasks delivery
endpoint, and the task the webhook enqueues.

The service is internet-facing (HubSpot has to reach /webhook), so the two
gates matter most: a webhook without a valid HubSpot signature must be refused,
and /enrich must refuse anything not carrying Cloud Tasks' OIDC token. These
tests pin that behaviour so a dependency upgrade to FastAPI/Starlette cannot
change it unnoticed.

They also pin what keeps the webhook inside HubSpot's 5 second window: one reused
Cloud Tasks client, an enqueue that never blocks the event loop, and a task name
that makes HubSpot's redeliveries of one event collapse into a single task.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_webhook_server.py" -v
"""

import base64
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from unittest.mock import create_autospec

import pytest
from fastapi.testclient import TestClient
from google.api_core import exceptions as gexc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_claim
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


def _trigger_event(company_id="12345", value="true", prop="request_to_enrich", **extra) -> bytes:
    event = {"objectId": int(company_id), "propertyName": prop, "propertyValue": value, **extra}
    return json.dumps([event]).encode()


def _trigger_events(*company_ids) -> bytes:
    return json.dumps([
        {"objectId": int(c), "propertyName": "request_to_enrich", "propertyValue": "true"} for c in company_ids
    ]).encode()


@pytest.fixture(autouse=True)
def _fresh_tasks_client():
    """The Cloud Tasks client is built once per process; every test starts without one."""
    webhook_server._tasks_client = None
    yield
    webhook_server._tasks_client = None


@pytest.fixture(autouse=True)
def _memory_claims(monkeypatch):
    """/enrich takes a claim on the company; the tests keep it in memory instead of Firestore."""
    monkeypatch.setattr(enrichment_claim, "_store", enrichment_claim.MemoryClaimStore())


@pytest.fixture
def enqueued(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(webhook_server, "_enqueue_enrichment",
                        lambda company_id, event_key="": calls.append(company_id))
    return calls


@pytest.fixture
def enqueued_with_keys(monkeypatch):
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(webhook_server, "_enqueue_enrichment",
                        lambda company_id, event_key="": calls.append((company_id, event_key)))
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


def test_a_second_task_for_a_company_already_running_does_nothing(client, monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)
    enrichment_claim.get_claim_store().claim("12345", "task-a", webhook_server._utcnow())

    response = client.post("/enrich", json={"company_id": "12345"}, headers={"X-CloudTasks-TaskName": "task-b"})

    assert response.status_code == 200
    assert response.json() == {"status": "skipped", "reason": "already_running", "company_id": "12345"}
    assert ran == []


def test_cloud_tasks_retrying_the_task_that_holds_the_company_is_let_back_in(client, monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)
    enrichment_claim.get_claim_store().claim("12345", "task-a", webhook_server._utcnow())

    response = client.post("/enrich", json={"company_id": "12345"}, headers={"X-CloudTasks-TaskName": "task-a"})

    assert response.json()["status"] == "done"
    assert ran == ["12345"]


def test_a_company_is_given_back_when_its_run_ends_so_the_next_trigger_can_run(client, monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)

    client.post("/enrich", json={"company_id": "12345"}, headers={"X-CloudTasks-TaskName": "task-a"})
    client.post("/enrich", json={"company_id": "12345"}, headers={"X-CloudTasks-TaskName": "task-b"})

    assert ran == ["12345", "12345"]


def test_a_company_is_given_back_even_when_its_run_raises(client, monkeypatch):
    def boom(company_id):
        raise RuntimeError("the run died")

    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", boom)

    with pytest.raises(RuntimeError):
        client.post("/enrich", json={"company_id": "12345"}, headers={"X-CloudTasks-TaskName": "task-a"})

    assert enrichment_claim.get_claim_store().claim("12345", "task-b", webhook_server._utcnow()) is True


def test_enrichment_still_runs_when_the_claim_store_is_down(client, monkeypatch):
    ran: list[str] = []

    class Down:
        def claim(self, *a):
            raise RuntimeError("firestore unavailable")

        def release(self, *a):
            raise AssertionError("nothing was claimed, so nothing is released")

    monkeypatch.setattr(webhook_server, "_verify_cloud_tasks_oidc", lambda request: True)
    monkeypatch.setattr(webhook_server, "_run_enrichment_safe", ran.append)
    monkeypatch.setattr(enrichment_claim, "_store", Down())

    response = client.post("/enrich", json={"company_id": "12345"})

    assert response.json()["status"] == "done"
    assert ran == ["12345"]


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


# ── /webhook: what it hands to the enqueue ───────────────────────────────────

def test_the_hubspot_event_time_travels_with_the_company_so_redeliveries_can_be_recognised(client, enqueued_with_keys):
    body = _trigger_event(company_id="12345", occurredAt=1760000000000, eventId=77)

    client.post("/webhook", content=body, headers=_signed_headers(body))

    assert enqueued_with_keys == [("12345", "1760000000000-77")]


def test_an_event_without_a_timestamp_carries_no_key(client, enqueued_with_keys):
    body = _trigger_event(company_id="12345")

    client.post("/webhook", content=body, headers=_signed_headers(body))

    assert enqueued_with_keys == [("12345", "")]


def test_every_company_in_one_webhook_is_enqueued_even_when_one_enqueue_fails(client, monkeypatch):
    seen: list[str] = []

    def flaky(company_id, event_key=""):
        seen.append(company_id)
        if company_id == "222":
            raise RuntimeError("Cloud Tasks is having a moment")

    monkeypatch.setattr(webhook_server, "_enqueue_enrichment", flaky)
    body = _trigger_events("111", "222", "333")

    response = client.post("/webhook", content=body, headers=_signed_headers(body))

    assert response.status_code == 200
    assert sorted(seen) == ["111", "222", "333"]


def test_a_slow_enqueue_does_not_stall_the_event_loop(client, monkeypatch):
    """HubSpot gives the webhook 5 seconds. A blocking Cloud Tasks call on the loop made every other
    request on the instance wait behind it: 9 enqueueing webhooks took 1 to 7.4 seconds on 2026-09-24."""
    release, started = threading.Event(), threading.Event()

    def slow(company_id, event_key=""):
        started.set()
        release.wait(5)

    monkeypatch.setattr(webhook_server, "_enqueue_enrichment", slow)
    body = _trigger_event()
    poster = threading.Thread(
        target=lambda: client.post("/webhook", content=body, headers=_signed_headers(body)))
    poster.start()
    try:
        assert started.wait(2), "the enqueue never started"
        began = time.monotonic()
        health = client.get("/health")
        waited = time.monotonic() - began
    finally:
        release.set()
        poster.join(5)

    assert health.status_code == 200
    assert waited < 1.0, f"/health waited {waited:.1f}s behind the enqueue"


# ── The task the webhook enqueues ─────────────────────────────────────────────

def _fake_tasks_client(monkeypatch):
    """The real Cloud Tasks client's signatures, with only the network stubbed.
    Returns (client, constructions): how many times the service built a client."""
    from google.cloud import tasks_v2

    fake_client = create_autospec(tasks_v2.CloudTasksClient, instance=True)
    fake_client.queue_path.side_effect = tasks_v2.CloudTasksClient.queue_path
    fake_client.task_path.side_effect = tasks_v2.CloudTasksClient.task_path
    fake_client.create_task.return_value.name = "projects/p/locations/l/queues/q/tasks/t"
    constructions: list[int] = []

    def build():
        constructions.append(1)
        return fake_client

    monkeypatch.setattr(tasks_v2, "CloudTasksClient", build)
    monkeypatch.setenv("GCP_PROJECT_ID", "proj")
    monkeypatch.setenv("GCP_LOCATION", "us-central1")
    monkeypatch.setenv("CLOUD_TASKS_QUEUE", "hubspot-enrichment")
    monkeypatch.setenv("CLOUD_RUN_SERVICE_URL", "https://svc.example.run.app/")
    monkeypatch.setenv("CLOUD_TASKS_SA_EMAIL", "tasks@proj.iam.gserviceaccount.com")
    return fake_client, constructions


def test_enqueued_task_posts_to_enrich_with_an_oidc_token(monkeypatch):
    from google.cloud import tasks_v2

    fake_client, _ = _fake_tasks_client(monkeypatch)

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


def test_the_cloud_tasks_client_is_built_once_and_reused(monkeypatch):
    """A new client per enqueue meant a new gRPC channel and a credentials lookup on every webhook."""
    fake_client, constructions = _fake_tasks_client(monkeypatch)

    webhook_server._enqueue_enrichment("111")
    webhook_server._enqueue_enrichment("222")
    webhook_server._enqueue_enrichment("333")

    assert len(constructions) == 1
    assert fake_client.create_task.call_count == 3


def test_the_task_is_named_after_the_hubspot_event_so_a_redelivery_cannot_queue_a_second_run(monkeypatch):
    """HubSpot retries a webhook it thinks timed out, with the same occurredAt. The same name makes
    Cloud Tasks refuse the second task."""
    fake_client, _ = _fake_tasks_client(monkeypatch)

    webhook_server._enqueue_enrichment("12345", "1760000000000-77")

    task = fake_client.create_task.call_args.kwargs["request"]["task"]
    assert task["name"] == "projects/proj/locations/us-central1/queues/hubspot-enrichment/tasks/enrich-12345-1760000000000-77"


def test_a_task_name_only_ever_contains_characters_cloud_tasks_allows(monkeypatch):
    fake_client, _ = _fake_tasks_client(monkeypatch)

    webhook_server._enqueue_enrichment("12345", "17600/00:00 77")

    task_id = fake_client.create_task.call_args.kwargs["request"]["task"]["name"].rsplit("/", 1)[-1]
    assert task_id == "enrich-12345-17600-00-00-77"


def test_an_event_without_a_key_makes_an_unnamed_task(monkeypatch):
    fake_client, _ = _fake_tasks_client(monkeypatch)

    webhook_server._enqueue_enrichment("12345")

    assert "name" not in fake_client.create_task.call_args.kwargs["request"]["task"]


def test_a_duplicate_delivery_is_not_an_error(monkeypatch):
    fake_client, _ = _fake_tasks_client(monkeypatch)
    fake_client.create_task.side_effect = gexc.AlreadyExists("task exists")

    webhook_server._enqueue_enrichment("12345", "1760000000000-77")   # must not raise

    assert fake_client.create_task.call_count == 1


def test_any_other_enqueue_failure_still_surfaces(monkeypatch):
    fake_client, _ = _fake_tasks_client(monkeypatch)
    fake_client.create_task.side_effect = gexc.PermissionDenied("no")

    with pytest.raises(gexc.PermissionDenied):
        webhook_server._enqueue_enrichment("12345", "1760000000000-77")


def test_the_task_may_run_as_long_as_the_service_does(monkeypatch):
    """Cloud Tasks gives an HTTP task 10 minutes by default and Cloud Run 30. A long corporation run cut
    off at 10 was retried, and a retry pays the vendors again."""
    from google.cloud import tasks_v2

    fake_client, _ = _fake_tasks_client(monkeypatch)

    webhook_server._enqueue_enrichment("12345", "1760000000000-77")

    request = fake_client.create_task.call_args.kwargs["request"]
    proto = tasks_v2.CreateTaskRequest(request)     # the real message: a malformed field raises here
    assert proto.task.dispatch_deadline.seconds == 1800
