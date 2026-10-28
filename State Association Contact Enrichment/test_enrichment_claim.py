"""
One enrichment run per company at a time.

One flip of request_to_enrich once reached the service as two webhooks and the company ran twice side by side.
A run takes a claim on the company; a second run finds it held and does nothing.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_enrichment_claim.py" -v
"""

import os
import sys
import threading
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_claim as ec

T0 = datetime(2026, 10, 28, 16, 32, 0, tzinfo=timezone.utc)


# ── A twin of the Firestore calls the store makes ─────────────────────────────

class _Snap:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class _Doc:
    def __init__(self, db, collection, doc_id):
        self._db, self._collection, self.id = db, collection, doc_id

    @property
    def _docs(self):
        return self._db.data.setdefault(self._collection, {})

    def get(self, transaction=None):
        return _Snap(self._docs.get(self.id))

    def set(self, data):
        self._docs[self.id] = dict(data)

    def delete(self):
        self._docs.pop(self.id, None)


class _Collection:
    def __init__(self, db, name):
        self._db, self._name = db, name

    def document(self, doc_id):
        return _Doc(self._db, self._name, doc_id)


class _Transaction:
    def set(self, ref, data):
        ref.set(data)

    def delete(self, ref):
        ref.delete()


class FakeFirestore:
    def __init__(self):
        self.data = {}

    def collection(self, name):
        return _Collection(self, name)

    def transaction(self):
        return _Transaction()


def _transactional(fn):
    """What firestore.transactional does, without Firestore: run the function with a transaction."""
    def run(transaction, *args, **kwargs):
        return fn(transaction, *args, **kwargs)
    return run


@pytest.fixture(params=["memory", "firestore"])
def store(request):
    if request.param == "memory":
        return ec.MemoryClaimStore()
    return ec.FirestoreClaimStore(client=FakeFirestore(), transactional=_transactional)


# ── The claim ─────────────────────────────────────────────────────────────────

def test_a_company_nobody_holds_can_be_claimed(store):
    assert store.claim("100", "task-a", T0) is True


def test_a_company_that_is_held_cannot_be_claimed_by_another_task(store):
    store.claim("100", "task-a", T0)

    assert store.claim("100", "task-b", T0 + timedelta(seconds=2)) is False


def test_the_task_that_holds_a_company_can_claim_it_again_when_cloud_tasks_retries_it(store):
    store.claim("100", "task-a", T0)

    assert store.claim("100", "task-a", T0 + timedelta(minutes=1)) is True


def test_two_companies_are_claimed_independently(store):
    store.claim("100", "task-a", T0)

    assert store.claim("200", "task-b", T0) is True


def test_a_claim_lapses_after_the_lease_so_a_killed_instance_does_not_lock_the_company(store):
    store.claim("100", "task-a", T0)

    assert store.claim("100", "task-b", T0 + ec.CLAIM_LEASE - timedelta(seconds=1)) is False
    assert store.claim("100", "task-b", T0 + ec.CLAIM_LEASE) is True


def test_the_lease_outlasts_the_longest_run_the_service_allows():
    assert ec.CLAIM_LEASE > timedelta(minutes=30)


# ── Giving it back ────────────────────────────────────────────────────────────

def test_a_released_company_can_be_claimed_by_anyone(store):
    store.claim("100", "task-a", T0)

    store.release("100", "task-a")

    assert store.claim("100", "task-b", T0 + timedelta(seconds=1)) is True


def test_only_the_holder_can_release_a_company(store):
    store.claim("100", "task-a", T0)

    store.release("100", "task-b")

    assert store.claim("100", "task-b", T0 + timedelta(seconds=1)) is False


def test_releasing_a_company_nobody_holds_is_harmless(store):
    store.release("100", "task-a")

    assert store.claim("100", "task-a", T0) is True


def test_two_runs_starting_at_once_cannot_both_take_the_company():
    store = ec.MemoryClaimStore()
    results, barrier = [], threading.Barrier(8)

    def go(n):
        barrier.wait()
        results.append(store.claim("100", f"task-{n}", T0))

    threads = [threading.Thread(target=go, args=(n,)) for n in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert results.count(True) == 1


# ── Firestore layout ──────────────────────────────────────────────────────────

def test_the_firestore_store_keeps_one_document_per_company_with_its_owner_and_lease():
    db = FakeFirestore()
    store = ec.FirestoreClaimStore(client=db, transactional=_transactional)

    store.claim("100", "task-a", T0)

    assert db.data[ec.CLAIMS_COLLECTION]["100"] == {
        "owner": "task-a", "claimed_at": T0, "expire_at": T0 + ec.CLAIM_LEASE,
    }
