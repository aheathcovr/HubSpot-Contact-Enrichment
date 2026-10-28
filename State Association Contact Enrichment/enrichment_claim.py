"""
One enrichment run per company at a time.

A trigger can become two Cloud Tasks (on 2026-10-28 one flip of request_to_enrich reached the service as two
webhooks, 2 seconds apart, and the company ran twice side by side: every Sonar and FullEnrich call paid twice, two
notes, two sets of contacts). Naming the task after the HubSpot event stops a redelivery of the same event, but not two
different events. So /enrich takes a claim on the company before it runs and gives it back after: a second run that
finds the claim held does nothing.

The claim is a lease. It lapses after CLAIM_LEASE (a run may last as long as the service allows, 30 minutes), so an
instance killed mid-run does not lock the company for good. The claim names its owner, the Cloud Tasks task that holds
it, so Cloud Tasks retrying that same task (after a killed instance) is let back in at once instead of waiting out
the lease.

Two stores keep the same contract: MemoryClaimStore (tests, local use, one process) and FirestoreClaimStore (the
service). A claim is one document per company in enrichment_claims.
"""

import os
import threading
from datetime import datetime, timedelta
from typing import Optional

CLAIMS_COLLECTION = "enrichment_claims"
# The Cloud Run request timeout (--timeout=1800s) plus a margin: past this the run is certainly not still going.
CLAIM_LEASE = timedelta(minutes=35)


def _free(claim: Optional[dict], owner: str, now: datetime) -> bool:
    """Whether *owner* may take the company: nobody holds it, the lease lapsed, or *owner* already holds it."""
    return claim is None or claim["expire_at"] <= now or claim["owner"] == owner


class MemoryClaimStore:
    def __init__(self):
        self._claims: dict[str, dict] = {}
        self._lock = threading.Lock()

    def claim(self, company_id: str, owner: str, now: datetime) -> bool:
        with self._lock:
            if not _free(self._claims.get(company_id), owner, now):
                return False
            self._claims[company_id] = {"owner": owner, "claimed_at": now, "expire_at": now + CLAIM_LEASE}
            return True

    def release(self, company_id: str, owner: str) -> None:
        with self._lock:
            held = self._claims.get(company_id)
            if held and held["owner"] == owner:
                del self._claims[company_id]


class FirestoreClaimStore:
    """
    The same contract in Firestore, inside a transaction so two runs starting at once cannot both take the company.

    *client* and *transactional* are only passed by the tests; the service lets them default.
    """

    def __init__(self, client=None, transactional=None):
        self._client = client
        self._transactional = transactional

    def _db(self):
        if self._client is None:
            from google.cloud import firestore
            self._client = firestore.Client()
        return self._client

    def _tx(self, fn):
        if self._transactional is None:
            from google.cloud import firestore
            self._transactional = firestore.transactional
        return self._transactional(fn)

    def _ref(self, company_id: str):
        return self._db().collection(CLAIMS_COLLECTION).document(company_id)

    def claim(self, company_id: str, owner: str, now: datetime) -> bool:
        ref = self._ref(company_id)

        def take(transaction):
            snap = ref.get(transaction=transaction)
            if not _free(snap.to_dict() if snap.exists else None, owner, now):
                return False
            transaction.set(ref, {"owner": owner, "claimed_at": now, "expire_at": now + CLAIM_LEASE})
            return True

        return self._tx(take)(self._db().transaction())

    def release(self, company_id: str, owner: str) -> None:
        ref = self._ref(company_id)

        def give_back(transaction):
            snap = ref.get(transaction=transaction)
            if snap.exists and snap.to_dict().get("owner") == owner:
                transaction.delete(ref)

        self._tx(give_back)(self._db().transaction())


_store = None
_store_lock = threading.Lock()


def get_claim_store():
    """The store the service uses: Firestore, or memory when ENRICHMENT_CLAIM_STORE=memory (one process only)."""
    global _store
    with _store_lock:
        if _store is None:
            memory = os.getenv("ENRICHMENT_CLAIM_STORE", "firestore").strip().lower() == "memory"
            _store = MemoryClaimStore() if memory else FirestoreClaimStore()
        return _store
