"""
run_trail: the step-by-step record of what a HubSpot-app run did and what each vendor said back.

The card used to say "Nothing found" and nothing else. The trail is what lets it say which vendor was asked what and what
came back (a count, a status, a credit balance, a reason). It is kept on the run record, which is stored without any
person's email address, phone number or name, so these tests pin that nothing of the sort gets in, that recording a step
can never break a run, and that vendor code outside a run (a webhook, a company workflow) records nothing and costs
nothing.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_run_trail.py" -v
"""

import threading
from datetime import datetime, timezone

import pytest

import run_trail


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    """A clock that moves a second at a time, so steps have distinct, predictable times."""
    state = {"n": 0}

    def tick():
        state["n"] += 1
        return datetime(2026, 10, 28, 23, 14, state["n"], 250_000, tzinfo=timezone.utc)

    monkeypatch.setattr(run_trail, "_utcnow", tick)


def record(calls):
    """Run *calls* with a recording sink; the steps it was given."""
    steps = []
    with run_trail.recording(steps.append):
        calls()
    return steps


# ── Outside a run ────────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_note_outside_a_run_does_nothing_and_does_not_raise():
    run_trail.note("fullenrich", "Searched for Administrator: 0 people", "empty")
    assert run_trail.failures() == 0


def test_a_note_after_a_run_ended_is_not_recorded():
    steps = record(lambda: run_trail.note("fullenrich", "first"))
    run_trail.note("fullenrich", "second")
    assert [s["text"] for s in steps] == ["first"]


# ── What a step is ───────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_step_says_when_who_what_and_how_it_went():
    steps = record(lambda: run_trail.note("fullenrich", 'Searched "Administrator": 3 people', "ok"))
    assert steps == [{"at": "2026-10-28T23:14:01.250Z", "vendor": "fullenrich", "result": "ok", "text": 'Searched "Administrator": 3 people'}]


def test_steps_arrive_in_the_order_they_happened():
    def calls():
        run_trail.note("fullenrich", "one")
        run_trail.note("seamless", "two")
        run_trail.note("millionverifier", "three")

    steps = record(calls)
    assert [s["text"] for s in steps] == ["one", "two", "three"]
    assert [s["at"] for s in steps] == sorted(s["at"] for s in steps)


def test_a_vendor_or_result_the_trail_does_not_know_is_filed_as_the_service_and_info():
    steps = record(lambda: run_trail.note("some-new-vendor", "x", "weird"))
    assert steps[0]["vendor"] == "service" and steps[0]["result"] == "info"


def test_the_result_defaults_to_info():
    assert record(lambda: run_trail.note("service", "Started"))[0]["result"] == "info"


# ── Nothing personal gets in ─────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text", [
    "Found jane.doe@example.com at the facility",
    "Seamless gave +1 (555) 123-4567 as a direct line",
    "Seamless gave 555-123-4567",
    "Seamless gave 5551234567",
    "Seamless gave 555.123.4567 ext 2",
])
def test_an_email_address_or_phone_number_is_hidden(text):
    shown = record(lambda: run_trail.note("seamless", text))[0]["text"]
    assert "@" not in shown and "555" not in shown and "example.com" not in shown


def test_the_numbers_a_rep_needs_are_not_mistaken_for_phone_numbers():
    shown = record(lambda: run_trail.note("millionverifier", "HTTP 402 after 12 s; 1,234 credits left; confidence 90"))[0]["text"]
    assert shown == "HTTP 402 after 12 s; 1,234 credits left; confidence 90"


def test_a_few_digits_with_brackets_and_spaces_are_not_a_phone_number():
    shown = record(lambda: run_trail.note("service", "Tried the title (1) (2) (3) of 5 searches"))[0]["text"]

    assert shown == "Tried the title (1) (2) (3) of 5 searches"


def test_a_long_text_is_cut_and_a_line_break_cannot_split_a_step():
    shown = record(lambda: run_trail.note("service", "line one\n\nline   two\t" + "x" * 500))[0]["text"]
    assert len(shown) <= run_trail.TEXT_MAX
    assert shown.startswith("line one line two x") and shown.endswith("…")
    assert "\n" not in shown and "\t" not in shown


# ── Never breaks the run ─────────────────────────────────────────────────────────────────────────────────────────────

def test_a_sink_that_fails_does_not_stop_the_run_or_later_steps():
    seen = []

    def sink(step):
        if step["text"] == "second":
            raise RuntimeError("Firestore is down")
        seen.append(step["text"])

    with run_trail.recording(sink):
        run_trail.note("service", "first")
        run_trail.note("service", "second")
        run_trail.note("service", "third")
    assert seen == ["first", "third"]


def test_a_run_that_raises_still_ends_the_recording():
    steps = []
    with pytest.raises(ValueError):
        with run_trail.recording(steps.append):
            run_trail.note("service", "before")
            raise ValueError("boom")
    run_trail.note("service", "after")
    assert [s["text"] for s in steps] == ["before"]


# ── Too many steps ───────────────────────────────────────────────────────────────────────────────────────────────────

def test_steps_past_the_limit_are_dropped_with_one_notice():
    def calls():
        for i in range(run_trail.STEP_CAP + 20):
            run_trail.note("service", f"step {i}")

    steps = record(calls)
    assert len(steps) == run_trail.STEP_CAP + 1
    assert steps[run_trail.STEP_CAP - 1]["text"] == f"step {run_trail.STEP_CAP - 1}"
    assert "more" in steps[-1]["text"].lower()


# ── Telling a failed lookup from an empty one ────────────────────────────────────────────────────────────────────────

def test_failures_counts_the_steps_that_failed_in_this_run():
    def calls():
        run_trail.note("fullenrich", "a", "failed")
        run_trail.note("fullenrich", "b", "empty")
        run_trail.note("seamless", "c", "failed")
        assert run_trail.failures() == 2
        assert run_trail.failures("fullenrich") == 1
        assert run_trail.failures("seamless") == 1
        assert run_trail.failures("bounceban") == 0

    record(calls)


def test_a_new_run_starts_with_no_failures():
    record(lambda: run_trail.note("fullenrich", "a", "failed"))
    seen = []
    record(lambda: seen.append(run_trail.failures()))
    assert seen == [0]


def test_failures_are_counted_even_when_the_step_was_dropped_for_length():
    def calls():
        for _ in range(run_trail.STEP_CAP + 5):
            run_trail.note("fullenrich", "x", "failed")
        assert run_trail.failures("fullenrich") == run_trail.STEP_CAP + 5

    record(calls)


# ── One run at a time per thread ─────────────────────────────────────────────────────────────────────────────────────

def test_a_run_in_another_thread_does_not_record_into_this_one():
    steps, other = [], []

    def elsewhere():
        run_trail.note("service", "from another thread")
        other.append(run_trail.failures())

    with run_trail.recording(steps.append):
        thread = threading.Thread(target=elsewhere)
        thread.start()
        thread.join()
        run_trail.note("service", "from this thread")
    assert [s["text"] for s in steps] == ["from this thread"]
    assert other == [0]


# ── Saying why a call failed, without the address it went to ─────────────────────────────────────────────────────────

class _Http(Exception):
    def __init__(self, code):
        super().__init__(f"{code} Client Error for url: https://vendor.example/lookup?name=Jane+Doe")
        self.response = type("R", (), {"status_code": code})()


class ReadTimeout(Exception):
    pass


class ConnectionError(Exception):    # noqa: A001  (named as requests names it, which is what why() reads)
    pass


@pytest.mark.parametrize("code, words", [
    (401, "HTTP 401 (the key was rejected)"),
    (403, "HTTP 403 (the key was rejected)"),
    (402, "HTTP 402 (out of credits)"),
    (429, "HTTP 429 (rate limited)"),
    (503, "HTTP 503 (the vendor had a problem)"),
    (404, "HTTP 404"),
])
def test_a_failed_call_is_explained_by_its_status(code, words):
    assert run_trail.why(_Http(code)) == words


def test_the_reason_never_carries_the_address_or_the_error_text():
    assert "vendor.example" not in run_trail.why(_Http(404)) and "Jane" not in run_trail.why(_Http(404))


def test_a_timeout_and_a_failed_connection_are_told_apart():
    assert run_trail.why(ReadTimeout("read timed out: Jane Doe")) == "timed out"
    assert run_trail.why(ConnectionError("could not reach jane.doe@example.com")) == "could not connect"


def test_anything_else_is_an_unexpected_error_with_no_detail():
    assert run_trail.why(ValueError("Jane Doe's record is broken")) == "an unexpected error"


# ── Counting what happened, by vendor and result ─────────────────────────────────────────────────────────────────────

def test_count_tallies_steps_by_result_and_vendor_in_this_run():
    def calls():
        run_trail.note("fullenrich", "a", "failed")
        run_trail.note("fullenrich", "b", "skipped")
        run_trail.note("fullenrich", "c", "empty")
        run_trail.note("seamless", "d", "skipped")
        assert run_trail.count("failed") == 1
        assert run_trail.count("skipped") == 2
        assert run_trail.count("skipped", "fullenrich") == 1
        assert run_trail.count("empty", "seamless") == 0

    record(calls)


def test_count_is_zero_outside_a_run_and_between_runs():
    assert run_trail.count("failed") == 0
    record(lambda: run_trail.note("fullenrich", "a", "failed"))
    seen = []
    record(lambda: seen.append(run_trail.count("failed")))
    assert seen == [0]


def test_count_includes_steps_dropped_for_length():
    def calls():
        for _ in range(run_trail.STEP_CAP + 5):
            run_trail.note("seamless", "x", "skipped")
        assert run_trail.count("skipped", "seamless") == run_trail.STEP_CAP + 5

    record(calls)
