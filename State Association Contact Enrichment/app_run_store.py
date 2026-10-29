"""
Run records and the per-person cap for the HubSpot app.

A rep's request becomes a run record. It holds ids, a status and a short message, never an email address or a phone
number (those are in the note the run leaves on the HubSpot contact), and it expires after 30 days. Each request also
takes one place in the rep's quota for the UTC day; the quota is a counter per rep per day, taken inside a transaction
so two requests made at once cannot both take the last place.

A title search's candidates are kept apart from the run record, with a shorter expiry, because they are the one place
this app stores people's names: the run record itself holds ids, statuses and short messages only.

Two stores keep the same contract: MemoryRunStore (tests, local use, one process) and FirestoreRunStore (the service).
"""

import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

RUNS_COLLECTION = "app_runs"
QUOTA_COLLECTION = "app_quota"
CANDIDATES_COLLECTION = "app_candidates"
RUN_TTL = timedelta(days=30)
QUOTA_TTL = timedelta(days=2)
# The people a title search found (names and titles) are kept only as long as a rep needs to choose among them.
CANDIDATES_TTL = timedelta(days=2)
# A contact's runs read to find the latest ones. A contact has a handful in 30 days; this is a safety limit.
_RUNS_READ = 100
# What a step of a run's trail carries (run_trail.py), and all the card is shown of it.
STEP_FIELDS = ("at", "vendor", "result", "text")


def day_of(moment: datetime) -> str:
    """The UTC date a moment falls on, as YYYY-MM-DD: the day a quota is counted over."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _text(moment: Optional[datetime]) -> Optional[str]:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if moment else None


def run_view(record: dict) -> dict:
    """A run as the card is shown it: camel case, text dates, and no ids of the rep."""
    return {
        "runId": record["run_id"],
        "contactId": record["contact_id"],
        "objective": record["objective"],
        "status": record["status"],
        "outcome": record.get("outcome"),
        "message": record.get("message"),
        "wrote": list(record.get("wrote") or []),
        "requestedBy": record.get("requested_by") or f"HubSpot user {record.get('requested_by_id', '')}",
        "createdAt": _text(record["created_at"]),
        "finishedAt": _text(record.get("finished_at")),
        "companyId": record.get("company_id") or None,
        "batchId": record.get("batch_id") or None,
        "steps": [{key: step[key] for key in STEP_FIELDS if key in step} for step in record.get("steps") or []],
    }


def _with_defaults(record: dict) -> dict:
    full = {
        "status": "queued", "outcome": None, "message": None, "wrote": [], "started_at": None, "finished_at": None,
        "steps": [],
        **record,
    }
    full["expire_at"] = full["created_at"] + RUN_TTL
    return full


def _copy_run(run: dict) -> dict:
    """A run as a caller may keep it: a later step added to the stored run does not appear in what was already read."""
    return {**run, "steps": [dict(step) for step in run.get("steps") or []]}


def _newest_first(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: r["created_at"], reverse=True)


class MemoryRunStore:
    """Everything in this process. Used by the tests, and by local runs that set APP_RUN_STORE=memory."""

    def __init__(self):
        self._lock = threading.RLock()
        self._runs: dict[str, dict] = {}
        self._used: dict[str, int] = {}
        self._candidates: dict[str, dict] = {}

    def create_run(self, record: dict) -> None:
        with self._lock:
            self._runs[record["run_id"]] = _with_defaults(record)

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._lock:
            run = self._runs.get(run_id)
            return _copy_run(run) if run else None

    def claim_run(self, run_id: str, now: datetime) -> bool:
        with self._lock:
            run = self._runs.get(run_id)
            if not run or run["status"] != "queued":
                return False
            run.update(status="running", started_at=now)
            return True

    def finish_run(self, run_id: str, now: datetime, *, status: str, outcome: str, message: str, wrote: list, contact_id: Optional[str] = None) -> None:
        fields = {"status": status, "outcome": outcome, "message": message, "wrote": list(wrote), "finished_at": now}
        if contact_id:
            fields["contact_id"] = contact_id
        with self._lock:
            self._runs[run_id].update(fields)

    def append_step(self, run_id: str, step: dict) -> None:
        with self._lock:
            self._runs[run_id].setdefault("steps", []).append(dict(step))

    def save_candidates(self, run_id: str, company_id: str, candidates: list, now: datetime) -> None:
        with self._lock:
            self._candidates[run_id] = {"run_id": run_id, "company_id": company_id, "candidates": list(candidates), "expire_at": now + CANDIDATES_TTL}

    def get_candidates(self, run_id: str) -> Optional[dict]:
        with self._lock:
            found = self._candidates.get(run_id)
            return dict(found) if found else None

    def recent_runs(self, contact_id: str, limit: int = 5) -> list[dict]:
        with self._lock:
            rows = [_copy_run(r) for r in self._runs.values() if r["contact_id"] == contact_id]
        return _newest_first(rows)[:limit]

    def runs_for_company(self, company_id: str, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = [_copy_run(r) for r in self._runs.values() if r.get("company_id") == company_id]
        return _newest_first(rows)[:limit]

    def find_recent_run(self, contact_id: str, objective: str, since: datetime) -> Optional[dict]:
        rows = [r for r in self.recent_runs(contact_id, limit=_RUNS_READ) if r["objective"] == objective and r["created_at"] >= since and r["status"] != "failed"]
        return rows[0] if rows else None

    def take_place(self, user_id: str, day: str, cap: int, now: datetime) -> tuple[bool, int]:
        key = f"{day}_{user_id}"
        with self._lock:
            used = self._used.get(key, 0)
            if used >= cap:
                return False, used
            self._used[key] = used + 1
            return True, used + 1

    def give_back_place(self, user_id: str, day: str) -> None:
        key = f"{day}_{user_id}"
        with self._lock:
            self._used[key] = max(0, self._used.get(key, 0) - 1)

    def used_today(self, user_id: str, day: str) -> int:
        with self._lock:
            return self._used.get(f"{day}_{user_id}", 0)


class FirestoreRunStore:
    """
    The same contract in Firestore: collection app_runs (one document per run) and app_quota (one counter per rep per
    day). Both carry expire_at, which a TTL policy (setup_firestore.sh) uses to delete them.

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

    def _run_ref(self, run_id: str):
        return self._db().collection(RUNS_COLLECTION).document(run_id)

    def create_run(self, record: dict) -> None:
        self._run_ref(record["run_id"]).set(_with_defaults(record))

    def get_run(self, run_id: str) -> Optional[dict]:
        snap = self._run_ref(run_id).get()
        return snap.to_dict() if snap.exists else None

    def claim_run(self, run_id: str, now: datetime) -> bool:
        ref = self._run_ref(run_id)

        def claim(transaction):
            snap = ref.get(transaction=transaction)
            if not snap.exists or snap.to_dict().get("status") != "queued":
                return False
            transaction.update(ref, {"status": "running", "started_at": now})
            return True

        return self._tx(claim)(self._db().transaction())

    def finish_run(self, run_id: str, now: datetime, *, status: str, outcome: str, message: str, wrote: list, contact_id: Optional[str] = None) -> None:
        fields = {"status": status, "outcome": outcome, "message": message, "wrote": list(wrote), "finished_at": now}
        if contact_id:
            fields["contact_id"] = contact_id
        self._run_ref(run_id).update(fields)

    def append_step(self, run_id: str, step: dict) -> None:
        # An array union appends in one atomic write, with no read first, so a step costs one write however long the run is.
        from google.cloud import firestore
        self._run_ref(run_id).update({"steps": firestore.ArrayUnion([dict(step)])})

    def save_candidates(self, run_id: str, company_id: str, candidates: list, now: datetime) -> None:
        self._db().collection(CANDIDATES_COLLECTION).document(run_id).set(
            {"run_id": run_id, "company_id": company_id, "candidates": list(candidates), "expire_at": now + CANDIDATES_TTL}
        )

    def get_candidates(self, run_id: str) -> Optional[dict]:
        snap = self._db().collection(CANDIDATES_COLLECTION).document(run_id).get()
        return snap.to_dict() if snap.exists else None

    def _runs_where(self, field: str, value: str) -> list[dict]:
        from google.cloud.firestore_v1.base_query import FieldFilter
        query = self._db().collection(RUNS_COLLECTION).where(filter=FieldFilter(field, "==", value)).limit(_RUNS_READ)
        return [snap.to_dict() for snap in query.stream()]

    def recent_runs(self, contact_id: str, limit: int = 5) -> list[dict]:
        # Read by contact only and ordered here: ordering in the query would need a composite index to be created.
        return _newest_first(self._runs_where("contact_id", contact_id))[:limit]

    def runs_for_company(self, company_id: str, limit: int = 20) -> list[dict]:
        return _newest_first(self._runs_where("company_id", company_id))[:limit]

    def find_recent_run(self, contact_id: str, objective: str, since: datetime) -> Optional[dict]:
        rows = [r for r in self.recent_runs(contact_id, limit=_RUNS_READ) if r["objective"] == objective and r["created_at"] >= since and r["status"] != "failed"]
        return rows[0] if rows else None

    def _quota_ref(self, user_id: str, day: str):
        return self._db().collection(QUOTA_COLLECTION).document(f"{day}_{user_id}")

    def take_place(self, user_id: str, day: str, cap: int, now: datetime) -> tuple[bool, int]:
        ref = self._quota_ref(user_id, day)

        def take(transaction):
            snap = ref.get(transaction=transaction)
            used = (snap.to_dict() or {}).get("used", 0) if snap.exists else 0
            if used >= cap:
                return False, used
            if snap.exists:
                transaction.update(ref, {"used": used + 1})
            else:
                transaction.set(ref, {"user_id": user_id, "day": day, "used": 1, "expire_at": now + QUOTA_TTL})
            return True, used + 1

        return self._tx(take)(self._db().transaction())

    def give_back_place(self, user_id: str, day: str) -> None:
        ref = self._quota_ref(user_id, day)

        def give_back(transaction):
            snap = ref.get(transaction=transaction)
            used = (snap.to_dict() or {}).get("used", 0) if snap.exists else 0
            if used > 0:
                transaction.update(ref, {"used": used - 1})

        self._tx(give_back)(self._db().transaction())

    def used_today(self, user_id: str, day: str) -> int:
        snap = self._quota_ref(user_id, day).get()
        return (snap.to_dict() or {}).get("used", 0) if snap.exists else 0


_store = None
_store_lock = threading.Lock()


def get_run_store():
    """The store the service uses: Firestore, or memory when APP_RUN_STORE=memory (one process only)."""
    global _store
    with _store_lock:
        if _store is None:
            _store = MemoryRunStore() if os.getenv("APP_RUN_STORE", "firestore").strip().lower() == "memory" else FirestoreRunStore()
        return _store
