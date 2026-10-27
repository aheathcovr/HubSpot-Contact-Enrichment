"""
Run records and the per-person cap for the HubSpot app.

A rep's request becomes a run record (ids, status and a short message, never an email or phone number), and takes one
place in that rep's quota for the day. The same contract is kept by an in-memory store (tests, local use) and a
Firestore store, so both are tested here with the same cases; Firestore is driven through a small in-memory twin of
the calls the store makes.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_app_run_store.py" -v
"""

import os
import sys
import threading
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app_run_store as rs

T0 = datetime(2026, 10, 26, 16, 0, 0, tzinfo=timezone.utc)
DAY = "2026-10-26"


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

    def update(self, fields):
        if self.id not in self._docs:
            raise KeyError(self.id)
        self._docs[self.id].update(fields)


class _Query:
    def __init__(self, db, collection, filters=None, limit=None):
        self._db, self._collection, self._filters, self._limit = db, collection, filters or [], limit

    def where(self, *, filter):
        return _Query(self._db, self._collection, self._filters + [filter], self._limit)

    def limit(self, n):
        return _Query(self._db, self._collection, self._filters, n)

    def stream(self):
        rows = list(self._db.data.get(self._collection, {}).values())
        for f in self._filters:
            assert f.op_string == "==", "the store only asks for equality"
            rows = [r for r in rows if r.get(f.field_path) == f.value]
        return [_Snap(r) for r in (rows[: self._limit] if self._limit else rows)]


class _Collection(_Query):
    def __init__(self, db, name):
        super().__init__(db, name)

    def document(self, doc_id):
        return _Doc(self._db, self._collection, doc_id)


class _Transaction:
    def __init__(self, db):
        self._db = db

    def set(self, ref, data):
        ref.set(data)

    def update(self, ref, fields):
        ref.update(fields)


class FakeFirestore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def collection(self, name):
        return _Collection(self, name)

    def transaction(self):
        return _Transaction(self)


def _transactional(fn):
    """What firestore.transactional does, without Firestore: run the function with a transaction."""
    def run(transaction, *args, **kwargs):
        return fn(transaction, *args, **kwargs)
    return run


@pytest.fixture(params=["memory", "firestore"])
def store(request):
    if request.param == "memory":
        return rs.MemoryRunStore()
    return rs.FirestoreRunStore(client=FakeFirestore(), transactional=_transactional)


def _run(run_id="r1", contact="42", objective="find_email", user="77", **over):
    record = {
        "run_id": run_id, "contact_id": contact, "objective": objective, "status": "queued",
        "requested_by_id": user, "requested_by": "rep@example.org", "portal_id": "1234", "created_at": T0,
    }
    record.update(over)
    return record


# ── Run records ───────────────────────────────────────────────────────────────

def test_a_run_can_be_read_back(store):
    store.create_run(_run())

    got = store.get_run("r1")

    assert got["run_id"] == "r1" and got["contact_id"] == "42" and got["objective"] == "find_email"
    assert got["status"] == "queued" and got["requested_by"] == "rep@example.org"


def test_a_run_nobody_made_is_none(store):
    assert store.get_run("nope") is None


def test_a_run_expires_thirty_days_after_it_was_made(store):
    store.create_run(_run())

    assert store.get_run("r1")["expire_at"] == T0 + timedelta(days=30)


def test_a_run_is_claimed_once(store):
    store.create_run(_run())

    assert store.claim_run("r1", T0 + timedelta(seconds=5)) is True
    assert store.claim_run("r1", T0 + timedelta(seconds=6)) is False
    got = store.get_run("r1")
    assert got["status"] == "running" and got["started_at"] == T0 + timedelta(seconds=5)


def test_a_run_that_is_finished_cannot_be_claimed(store):
    store.create_run(_run())
    store.claim_run("r1", T0)
    store.finish_run("r1", T0 + timedelta(seconds=9), status="done", outcome="found", message="Mobile number added.", wrote=["mobilephone"])

    assert store.claim_run("r1", T0 + timedelta(seconds=10)) is False


def test_a_run_nobody_made_cannot_be_claimed(store):
    assert store.claim_run("nope", T0) is False


def test_finishing_a_run_records_how_it_went(store):
    store.create_run(_run())
    store.claim_run("r1", T0)

    store.finish_run("r1", T0 + timedelta(seconds=9), status="done", outcome="found", message="Mobile number added.", wrote=["mobilephone"])

    got = store.get_run("r1")
    assert got["status"] == "done" and got["outcome"] == "found" and got["message"] == "Mobile number added."
    assert got["wrote"] == ["mobilephone"] and got["finished_at"] == T0 + timedelta(seconds=9)


def test_recent_runs_are_this_contacts_newest_first(store):
    store.create_run(_run("a", created_at=T0))
    store.create_run(_run("b", created_at=T0 + timedelta(minutes=1)))
    store.create_run(_run("c", created_at=T0 + timedelta(minutes=2)))
    store.create_run(_run("other", contact="99", created_at=T0 + timedelta(minutes=3)))

    got = store.recent_runs("42", limit=2)

    assert [r["run_id"] for r in got] == ["c", "b"]


def test_a_companys_runs_are_those_started_from_it_newest_first(store):
    store.create_run(_run("a", contact="1", company_id="500", created_at=T0))
    store.create_run(_run("b", contact="2", company_id="500", created_at=T0 + timedelta(minutes=1)))
    store.create_run(_run("c", contact="3", company_id="500", created_at=T0 + timedelta(minutes=2)))
    store.create_run(_run("elsewhere", contact="4", company_id="600", created_at=T0 + timedelta(minutes=3)))
    store.create_run(_run("from-the-contact-card", contact="5", created_at=T0 + timedelta(minutes=4)))

    got = store.runs_for_company("500", limit=2)

    assert [r["run_id"] for r in got] == ["c", "b"]


def test_a_company_with_no_runs_has_none(store):
    assert store.runs_for_company("500") == []


def test_no_recent_runs_for_a_contact_with_none(store):
    assert store.recent_runs("42") == []


def test_a_recent_run_of_the_same_objective_is_found(store):
    store.create_run(_run("a", created_at=T0))

    found = store.find_recent_run("42", "find_email", since=T0 - timedelta(hours=1))

    assert found["run_id"] == "a"


@pytest.mark.parametrize("contact, objective", [("99", "find_email"), ("42", "verify_email")])
def test_a_run_for_another_contact_or_objective_is_not_a_repeat(store, contact, objective):
    store.create_run(_run("a", created_at=T0))

    assert store.find_recent_run(contact, objective, since=T0 - timedelta(hours=1)) is None


def test_a_run_older_than_the_window_is_not_a_repeat(store):
    store.create_run(_run("a", created_at=T0 - timedelta(hours=25)))

    assert store.find_recent_run("42", "find_email", since=T0 - timedelta(hours=24)) is None


def test_a_failed_run_is_not_a_repeat(store):
    store.create_run(_run("a", created_at=T0))
    store.claim_run("a", T0)
    store.finish_run("a", T0, status="failed", outcome="error", message="Seamless did not answer.", wrote=[])

    assert store.find_recent_run("42", "find_email", since=T0 - timedelta(hours=1)) is None


def test_the_newest_of_several_repeats_is_returned(store):
    store.create_run(_run("a", created_at=T0))
    store.create_run(_run("b", created_at=T0 + timedelta(minutes=5)))

    assert store.find_recent_run("42", "find_email", since=T0 - timedelta(hours=1))["run_id"] == "b"


# ── A run that ends up with a contact, and the people a title search found ────

def test_a_finished_run_can_say_which_contact_it_made(store):
    store.create_run(_run(contact=""))
    store.claim_run("r1", T0)

    store.finish_run("r1", T0, status="done", outcome="found", message="Added.", wrote=["contact"], contact_id="901")

    assert store.get_run("r1")["contact_id"] == "901"


def test_a_finished_run_keeps_its_contact_when_it_does_not_say(store):
    store.create_run(_run())
    store.claim_run("r1", T0)

    store.finish_run("r1", T0, status="done", outcome="found", message="Mobile number added.", wrote=["mobilephone"])

    assert store.get_run("r1")["contact_id"] == "42"


def test_the_people_a_title_search_found_can_be_read_back(store):
    people = [{"candidate_id": "c1", "full_name": "Pat Lee", "title": "Medical Director"}]

    store.save_candidates("r1", "500", people, T0)

    got = store.get_candidates("r1")
    assert got["run_id"] == "r1" and got["company_id"] == "500" and got["candidates"] == people


def test_a_search_nobody_saved_people_for_has_none(store):
    assert store.get_candidates("nope") is None


def test_the_people_a_search_found_are_kept_for_two_days(store):
    store.save_candidates("r1", "500", [], T0)

    assert store.get_candidates("r1")["expire_at"] == T0 + timedelta(days=2)


# ── The cap ───────────────────────────────────────────────────────────────────

def test_places_are_given_until_the_cap_is_reached(store):
    results = [store.take_place("77", DAY, 3, T0) for _ in range(5)]

    assert results == [(True, 1), (True, 2), (True, 3), (False, 3), (False, 3)]


def test_each_rep_and_each_day_has_places_of_their_own(store):
    store.take_place("77", DAY, 1, T0)

    assert store.take_place("88", DAY, 1, T0) == (True, 1)
    assert store.take_place("77", "2026-10-27", 1, T0) == (True, 1)
    assert store.take_place("77", DAY, 1, T0) == (False, 1)


def test_a_place_given_back_can_be_taken_again(store):
    store.take_place("77", DAY, 1, T0)

    store.give_back_place("77", DAY)

    assert store.used_today("77", DAY) == 0
    assert store.take_place("77", DAY, 1, T0) == (True, 1)


def test_giving_back_never_goes_below_none(store):
    store.give_back_place("77", DAY)
    store.take_place("77", DAY, 5, T0)
    store.give_back_place("77", DAY)
    store.give_back_place("77", DAY)

    assert store.used_today("77", DAY) == 0


def test_a_rep_who_has_asked_for_nothing_has_used_nothing(store):
    assert store.used_today("77", DAY) == 0


def test_a_cap_of_zero_gives_no_places(store):
    assert store.take_place("77", DAY, 0, T0) == (False, 0)


def test_two_reps_asking_at_once_cannot_both_take_the_last_place():
    store = rs.MemoryRunStore()
    results = []

    def ask():
        results.append(store.take_place("77", DAY, 5, T0)[0])

    threads = [threading.Thread(target=ask) for _ in range(40)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert results.count(True) == 5 and store.used_today("77", DAY) == 5


def test_the_day_is_the_utc_date():
    assert rs.day_of(datetime(2026, 10, 26, 23, 59, 59, tzinfo=timezone.utc)) == "2026-10-26"
    assert rs.day_of(datetime(2026, 10, 27, 0, 0, 1, tzinfo=timezone.utc)) == "2026-10-27"
    assert rs.day_of(datetime(2026, 10, 26, 20, 0, 0, tzinfo=timezone(timedelta(hours=-6)))) == "2026-10-27"


# ── Firestore specifics ───────────────────────────────────────────────────────

def test_the_firestore_store_keeps_quota_counters_that_expire_after_two_days():
    db = FakeFirestore()
    store = rs.FirestoreRunStore(client=db, transactional=_transactional)

    store.take_place("77", DAY, 5, T0)

    doc = db.data[rs.QUOTA_COLLECTION]["2026-10-26_77"]
    assert doc["used"] == 1 and doc["expire_at"] == T0 + timedelta(days=2)


def test_the_firestore_store_keeps_runs_in_their_own_collection():
    db = FakeFirestore()
    store = rs.FirestoreRunStore(client=db, transactional=_transactional)

    store.create_run(_run())

    assert list(db.data[rs.RUNS_COLLECTION]) == ["r1"]


def test_a_run_record_holds_no_email_or_phone_field():
    """The record is ids, statuses and a message. The values live in the note on the HubSpot contact."""
    store = rs.MemoryRunStore()
    store.create_run(_run())

    assert not {"email", "phone", "mobile", "mobilephone"} & set(store.get_run("r1"))


# ── How a run is shown ────────────────────────────────────────────────────────

def test_a_run_is_shown_to_the_card_with_camel_case_names_and_text_dates():
    record = _run(status="done", outcome="found", message="Mobile number added.", wrote=["mobilephone"],
                  finished_at=T0 + timedelta(seconds=9), started_at=T0)

    shown = rs.run_view(record)

    assert shown == {
        "runId": "r1", "contactId": "42", "objective": "find_email", "status": "done", "outcome": "found",
        "message": "Mobile number added.", "wrote": ["mobilephone"], "requestedBy": "rep@example.org",
        "createdAt": "2026-10-26T16:00:00Z", "finishedAt": "2026-10-26T16:00:09Z", "companyId": None, "batchId": None,
    }


def test_a_run_started_from_a_company_is_shown_with_its_company_and_batch():
    shown = rs.run_view(_run(company_id="500", batch_id="b" * 32))

    assert shown["companyId"] == "500" and shown["batchId"] == "b" * 32


def test_a_run_not_finished_has_no_finish_time_or_outcome():
    shown = rs.run_view(_run())

    assert shown["finishedAt"] is None and shown["outcome"] is None and shown["message"] is None and shown["wrote"] == []


def test_a_run_made_by_someone_with_no_email_is_shown_by_user_id():
    shown = rs.run_view(_run(requested_by=""))

    assert shown["requestedBy"] == "HubSpot user 77"
