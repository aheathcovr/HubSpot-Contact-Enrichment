"""
What FullEnrich says back, as the trail of a run records it.

A title search that "found no one" and one that could not be done look the same to the rep unless the trail says which:
these tests pin the step each call leaves (a count, a status, a credit balance, the reason a person was not used), that a
failed call is marked as failed and not as empty, and that no person's name, email address or phone number is in any step.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_fullenrich_trail.py" -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fullenrich_client as fe
import run_trail
from test_fullenrich_client import fake  # noqa: F401  (fixture: a local FullEnrich that counts what it is asked)

# A person as FullEnrich sends one. The names are made up; the tests check none of them reaches a step.
JANE = {"full_name": "Jane Quillfeather", "employment": {"current": {"title": "Administrator"}}, "location": {"city": "Laramie", "region": "Wyoming"}}
JOHN = {"full_name": "John Pennyworth", "employment": {"current": {"title": "Administrator"}}, "location": {"city": "Tulsa", "region": "Oklahoma"}}
NOLOC = {"full_name": "Mary Fiddlesticks", "employment": {"current": {"title": "Administrator"}}}
TECH = {"full_name": "Tom Bytewright", "employment": {"current": {"title": "Database Administrator"}}, "location": {"city": "Laramie", "region": "Wyoming"}}
NAMES = ("Quillfeather", "Pennyworth", "Fiddlesticks", "Bytewright", "doe", "@")


class _Resp:
    def __init__(self, status=200, body=None):
        self.status_code, self._body, self.headers, self.text = status, body if body is not None else {}, {}, ""

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            err = fe.requests.exceptions.HTTPError(f"{self.status_code} for url: https://x.example/?q=Jane+Doe")
            err.response = self
            raise err


def recorded(call):
    steps, box = [], {}
    with run_trail.recording(steps.append):
        box["result"] = call()
        box["failures"] = run_trail.failures("fullenrich")
    return steps, box


def texts(steps):
    return [s["text"] for s in steps]


def asked_step(steps):
    """The step that says a lookup was submitted (the credit balance comes before it)."""
    return next(s for s in steps if s["text"].startswith("Asked FullEnrich"))


def search_returns(monkeypatch, *people_per_call):
    """Make the people search answer with these lists of people, one per call, in order."""
    queue = list(people_per_call)
    monkeypatch.setattr(fe, "_post_waiting_out_rate_limit", lambda url, payload, key: _Resp(200, {"people": queue.pop(0) if queue else []}))


def assert_no_personal_detail(steps):
    for s in steps:
        assert not any(n.lower() in s["text"].lower() for n in NAMES), s["text"]


# ── One title searched ───────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("people, words", [([JANE, JOHN, NOLOC], "3 people"), ([JANE], "1 person"), ([], "no one")])
def test_a_search_says_what_was_searched_for_and_how_many_people_came_back(monkeypatch, people, words):
    search_returns(monkeypatch, people)

    steps, _ = recorded(lambda: fe._search_one_title("key", "Administrator", domain="valleyview.example"))

    assert len(steps) == 1 and steps[0]["vendor"] == "fullenrich"
    assert steps[0]["result"] == ("ok" if people else "empty")
    assert 'Administrator' in steps[0]["text"] and "valleyview.example" in steps[0]["text"] and words in steps[0]["text"]
    assert_no_personal_detail(steps)


def test_a_search_by_company_name_says_so(monkeypatch):
    search_returns(monkeypatch, [JANE])

    steps, _ = recorded(lambda: fe._search_one_title("key", "Director of Nursing", company_name="Valley View Care"))

    assert "Director of Nursing" in steps[0]["text"] and "Valley View Care" in steps[0]["text"] and "company name" in steps[0]["text"]


@pytest.mark.parametrize("status, words", [(503, "HTTP 503"), (401, "key was rejected"), (402, "out of credits"), (429, "rate limited")])
def test_a_search_that_failed_is_a_failure_with_its_reason_not_an_empty_result(monkeypatch, status, words):
    monkeypatch.setattr(fe, "_post_waiting_out_rate_limit", lambda url, payload, key: _Resp(status))

    steps, box = recorded(lambda: fe._search_one_title("key", "Administrator", domain="valleyview.example"))

    assert box["result"] == [] and len(steps) == 1
    assert steps[0]["result"] == "failed" and words in steps[0]["text"] and "failed" in steps[0]["text"]
    assert box["failures"] == 1
    assert "no one" not in steps[0]["text"]


def test_a_search_that_timed_out_says_so(monkeypatch):
    def slow(url, payload, key):
        raise fe.requests.exceptions.ReadTimeout("read timed out")
    monkeypatch.setattr(fe, "_post_waiting_out_rate_limit", slow)

    steps, box = recorded(lambda: fe._search_one_title("key", "Administrator", domain="valleyview.example"))

    assert steps[0]["result"] == "failed" and "timed out" in steps[0]["text"] and box["failures"] == 1


def test_a_reply_that_is_not_a_list_of_people_is_a_failure(monkeypatch):
    monkeypatch.setattr(fe, "_post_waiting_out_rate_limit", lambda url, payload, key: _Resp(200, {"people": "oops"}))

    steps, box = recorded(lambda: fe._search_one_title("key", "Administrator", domain="valleyview.example"))

    assert steps[0]["result"] == "failed" and "unreadable" in steps[0]["text"] and box["failures"] == 1


def test_the_real_http_failure_path_is_a_failure_too(fake):
    fake(post_status=503)

    steps, box = recorded(lambda: fe._search_one_title("test-key", "Administrator", domain="valleyview.example"))

    assert steps[0]["result"] == "failed" and "HTTP 503" in steps[0]["text"] and box["failures"] == 1


# ── The balance ──────────────────────────────────────────────────────────────────────────────────────────────────────

def test_the_credits_left_are_reported(fake, monkeypatch):
    fake(credits_balance=1234)
    search_returns(monkeypatch, [])

    steps, _ = recorded(lambda: fe.search_facility_contacts("Valley View", "Laramie", "WY", ["Administrator"], domain="valleyview.example"))

    assert "FullEnrich credits left: 1,234" in texts(steps)
    assert next(s for s in steps if "credits left" in s["text"])["result"] == "info"


def test_an_empty_balance_is_a_failure_and_no_search_is_made(fake):
    server = fake(credits_balance=0)

    steps, box = recorded(lambda: fe.search_facility_contacts("Valley View", "Laramie", "WY", ["Administrator"], domain="valleyview.example"))

    assert server.posts == [] and box["result"] == []
    assert steps[0]["result"] == "failed" and "no credits" in steps[0]["text"] and box["failures"] == 1


def test_a_service_with_no_key_says_so(monkeypatch):
    monkeypatch.delenv("FULLENRICH_API_KEY", raising=False)

    steps, _ = recorded(lambda: fe.search_facility_contacts("Valley View", "Laramie", "WY", ["Administrator"], domain="valleyview.example"))

    assert steps[0]["vendor"] == "fullenrich" and steps[0]["result"] == "skipped" and "not set up" in steps[0]["text"]


# ── Why a person who came back was not used ──────────────────────────────────────────────────────────────────────────

def _find(monkeypatch, fake_balance, people_per_call, city="Laramie", state="WY", domain="valleyview.example", name="Valley View"):
    fake_balance(credits_balance=500)
    search_returns(monkeypatch, *people_per_call)
    return recorded(lambda: fe.search_facility_contacts(name, city, state, ["Administrator"], domain=domain))


def test_a_person_confirmed_at_the_facility_is_kept_and_the_location_match_is_said(fake, monkeypatch):
    steps, box = _find(monkeypatch, fake, [[JANE]])

    assert len(box["result"]) == 1
    kept = next(s for s in steps if "best match" in s["text"].lower())
    assert kept["result"] == "ok" and "city and state match" in kept["text"]
    assert_no_personal_detail(steps)


def test_a_person_with_no_location_cannot_be_tied_to_the_facility_and_that_is_said(fake, monkeypatch):
    steps, box = _find(monkeypatch, fake, [[NOLOC], []])

    assert box["result"] == []
    why = next(s for s in steps if "no location" in s["text"])
    assert why["result"] == "empty" and "Laramie, WY" in why["text"] and "Administrator" in why["text"]
    assert_no_personal_detail(steps)


def test_a_person_listed_in_another_state_is_rejected_and_that_is_said(fake, monkeypatch):
    steps, box = _find(monkeypatch, fake, [[JOHN], []])

    assert box["result"] == []
    why = next(s for s in steps if "another state" in s["text"])
    assert "Laramie, WY" in why["text"] and why["result"] == "empty"
    assert_no_personal_detail(steps)


def test_people_who_are_all_it_roles_are_skipped_and_that_is_said(fake, monkeypatch):
    steps, box = _find(monkeypatch, fake, [[TECH], []])

    assert box["result"] == []
    assert any("IT roles" in s["text"] for s in steps)
    assert_no_personal_detail(steps)


def test_a_website_that_returns_no_one_falls_back_to_the_company_name_and_that_is_said(fake, monkeypatch):
    steps, box = _find(monkeypatch, fake, [[], [JANE]])

    assert len(box["result"]) == 1
    assert any("company name" in s["text"] and "no one" in s["text"] for s in steps)
    assert any("trying the company name" in s["text"].lower() for s in steps)


def test_a_website_whose_people_do_not_fit_falls_back_to_the_company_name_and_that_is_said(fake, monkeypatch):
    steps, _ = _find(monkeypatch, fake, [[JOHN], [JANE]])

    assert any("trying the company name" in s["text"].lower() for s in steps)


# ── A contact lookup ─────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_lookup_that_finds_an_email_says_so_without_the_address(fake):
    fake()

    steps, box = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", company_name="Valley View", domain="valleyview.example"))

    assert box["result"]["email"] == "jane.doe@facility.com"
    asked = asked_step(steps)
    assert asked["result"] == "info" and "work email" in asked["text"] and "valleyview.example" in asked["text"]
    assert steps[-1]["result"] == "ok" and "found a work email" in steps[-1]["text"] and "deliverable" in steps[-1]["text"].lower()
    assert_no_personal_detail(steps)


def test_a_lookup_by_company_name_and_one_with_a_linkedin_profile_say_so(fake):
    fake()

    by_name, _ = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", company_name="Valley View Care"))
    with_profile, _ = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example", linkedin_url="https://www.linkedin.com/in/jq"))

    assert "company name" in asked_step(by_name)["text"] and "Valley View Care" in asked_step(by_name)["text"]
    assert "LinkedIn" in asked_step(with_profile)["text"] and "linkedin.com" not in asked_step(with_profile)["text"]


def test_a_lookup_that_could_not_be_submitted_is_a_failure(fake):
    fake(post_status=503)

    steps, box = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert box["result"]["email"] == "" and box["failures"] == 1
    assert steps[-1]["result"] == "failed" and "HTTP 503" in steps[-1]["text"]


def _poll_with(monkeypatch, *replies):
    """Make the submit succeed and the polls answer with these (status, body) pairs, then repeat the last."""
    monkeypatch.setenv("FULLENRICH_API_KEY", "test-key")
    fe._credits_cache.clear()
    monkeypatch.setattr(fe, "_post_waiting_out_rate_limit", lambda url, payload, key: _Resp(200, {"enrichment_id": "enr-1"}))
    queue = list(replies)

    class Session:
        def get(self, url, headers=None, timeout=None):
            if url.endswith("/credits"):
                return _Resp(200, {"balance": 50})
            status, body = queue.pop(0) if len(queue) > 1 else queue[0]
            return _Resp(status, body)
    monkeypatch.setattr(fe, "_session", Session())
    monkeypatch.setattr(fe, "_POLL_INTERVAL", 0)


def test_a_lookup_that_finished_with_no_email_says_it_found_none(monkeypatch):
    _poll_with(monkeypatch, (200, {"status": "FINISHED", "data": [{"contact_info": {}}]}))

    steps, box = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert box["result"]["email"] == "" and box["failures"] == 0
    assert steps[-1]["result"] == "empty" and "no work email" in steps[-1]["text"]


@pytest.mark.parametrize("status", ["FAILED", "ERROR", "CANCELLED"])
def test_a_lookup_that_ended_in_error_is_a_failure(monkeypatch, status):
    _poll_with(monkeypatch, (200, {"status": status}))

    steps, box = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert box["failures"] == 1 and steps[-1]["result"] == "failed" and status in steps[-1]["text"]


def test_a_lookup_that_never_finished_is_a_failure_that_says_how_long_it_waited(monkeypatch):
    _poll_with(monkeypatch, (200, {"status": "IN_PROGRESS"}))
    monkeypatch.setattr(fe, "_POLL_MAX", 3)
    monkeypatch.setattr(fe, "_POLL_INTERVAL", 5)
    monkeypatch.setattr(fe, "_sleep", lambda s: None)

    steps, box = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert box["failures"] == 1 and steps[-1]["result"] == "failed"
    assert "not finished" in steps[-1]["text"] and "15 s" in steps[-1]["text"]


def test_a_lookup_that_waits_says_it_is_waiting_so_a_card_has_something_to_show(monkeypatch):
    _poll_with(monkeypatch, (200, {"status": "IN_PROGRESS"}), (200, {"status": "FINISHED", "data": [{"contact_info": {}}]}))

    steps, _ = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert any("waiting for fullenrich" in s["text"].lower() for s in steps)


def test_a_rate_limited_poll_says_it_is_waiting_it_out(monkeypatch):
    _poll_with(monkeypatch, (429, {}), (200, {"status": "FINISHED", "data": [{"contact_info": {}}]}))
    monkeypatch.setattr(fe, "_sleep", lambda s: None)

    steps, _ = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert any("rate limited" in s["text"] for s in steps)


def test_a_lookup_with_no_key_says_so(monkeypatch):
    monkeypatch.delenv("FULLENRICH_API_KEY", raising=False)

    steps, _ = recorded(lambda: fe.enrich_contact_info("Jane", "Quillfeather", domain="valleyview.example"))

    assert steps[0]["result"] == "skipped" and "not set up" in steps[0]["text"]


# ── Outside a run nothing is recorded ────────────────────────────────────────────────────────────────────────────────

def test_a_search_outside_a_run_records_nothing_and_still_works(fake, monkeypatch):
    fake(credits_balance=500)
    search_returns(monkeypatch, [JANE])

    people = fe.search_facility_contacts("Valley View", "Laramie", "WY", ["Administrator"], domain="valleyview.example")

    assert len(people) == 1 and run_trail.failures() == 0
