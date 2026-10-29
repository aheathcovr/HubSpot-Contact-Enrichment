"""
How the trail changes what a run says it found.

"Nothing found" is a finding only when the vendors were asked and answered. A search or a lookup that could not be done
(a rejected key, no credits, a vendor that was down, a vendor that is not set up) is not that, and a rep who is told
"nothing found" will wrongly give up on the person. These tests pin that a run whose lookups could not be done ends as
failed, with nothing written, and says to look at what happened; that a run where some of them could not be done says so
beside what it did find; that a clean empty answer is still "nothing found"; and that each decision the service makes (an
address held for being at another domain, one that failed the check, one written) leaves a step with no address in it.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_run_trail_outcomes.py" -v
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import company_titles as ct
import contact_objectives as co
import enrichment_processor
import run_trail
from test_company_titles import NOW, _add_run, _find_run, _person, hs as titles_hs, kept, processor, store  # noqa: F401  (fixtures)
from test_contact_objectives import _FACILITY, _OK, _INVALID, _ON_DOMAIN, _fe, _hs, _note, _sl, _written

PERSONAL = ("pat", "lee", "sam", "ortiz", "@")


def _steps(run):
    steps = []
    with run_trail.recording(steps.append):
        box = {"out": run()}
    for s in steps:
        assert not any(p in s["text"].lower().split() or p == "@" and "@" in s["text"] for p in PERSONAL), s["text"]
    return steps, box["out"]


def _texts(steps):
    return [s["text"] for s in steps]


# ── The title search ─────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def hs(titles_hs):
    return titles_hs


def _search(monkeypatch, people, notes=()):
    """A FullEnrich search that leaves *notes* ((vendor, text, result) steps) and finds *people*."""
    def fake(**kwargs):
        for vendor, text, result in notes:
            run_trail.note(vendor, text, result)
        return list(people)
    monkeypatch.setattr(ct, "search_facility_contacts", fake)
    monkeypatch.setattr(ct, "search_unavailable_reason", lambda: None)


FAILED_SEARCH = (("fullenrich", 'Search for "Medical Director" at elmcourt.example.org failed: HTTP 503 (the vendor had a problem).', "failed"),)


def test_a_search_that_could_not_be_done_is_a_failure_not_a_finding_that_no_one_is_there(hs, store, monkeypatch):
    _search(monkeypatch, [], FAILED_SEARCH)

    steps, out = _steps(lambda: ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW))

    assert (out.status, out.outcome) == ("failed", "error")
    assert "could not be searched" in out.message and "What happened" in out.message and "No one found" not in out.message
    assert store.get_candidates("f" * 32) is None


def test_a_search_that_was_done_and_found_no_one_is_still_a_finding(hs, store, monkeypatch):
    _search(monkeypatch, [], (("fullenrich", 'Searched FullEnrich for "Medical Director" at elmcourt.example.org: no one.', "empty"),))

    steps, out = _steps(lambda: ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW))

    assert (out.status, out.outcome, out.message) == ("done", "not_found", "No one found for these titles at this company.")
    assert store.get_candidates("f" * 32)["candidates"] == []


def test_a_search_that_found_people_but_could_not_search_one_title_says_so_beside_what_it_found(hs, store, monkeypatch):
    _search(monkeypatch, [_person()], FAILED_SEARCH)

    _, out = _steps(lambda: ct.run_find_by_title(_find_run(titles=("Medical Director", "Dietitian")), hs=hs, store=store, now=NOW))

    assert (out.status, out.outcome) == ("done", "found")
    assert out.message == "Found 1 person. Choose who to add. Part of the search could not be done: see What happened."
    assert len(store.get_candidates("f" * 32)["candidates"]) == 1


def test_a_search_says_which_titles_it_is_looking_for_and_where(hs, store, monkeypatch):
    _search(monkeypatch, [])

    steps, _ = _steps(lambda: ct.run_find_by_title(_find_run(titles=("Medical Director", "Dietitian")), hs=hs, store=store, now=NOW))

    first = steps[0]
    assert first["vendor"] == "service" and first["result"] == "info"
    assert "Medical Director" in first["text"] and "Dietitian" in first["text"] and "Elm Court Care Center" in first["text"]


def test_an_unavailable_search_leaves_no_failure_of_its_own_to_misread(hs, store, monkeypatch):
    monkeypatch.setattr(ct, "search_unavailable_reason", lambda: "FullEnrich has no credits left, so people cannot be searched for.")

    _, out = _steps(lambda: ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW))

    assert out.message == "FullEnrich has no credits left, so people cannot be searched for."


# ── An added person ──────────────────────────────────────────────────────────────────────────────────────────────────

def test_an_add_says_what_was_done_to_hubspot_in_a_step(hs, kept, processor):
    steps, out = _steps(lambda: ct.run_add_candidate(_add_run(), hs=hs, store=kept))

    last = steps[-1]
    assert last["vendor"] == "hubspot" and last["result"] == "ok" and "Created a new contact" in last["text"]
    assert out.outcome == "found"


def test_an_add_that_added_nothing_says_so_as_an_empty_step(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="no_contact_found")

    steps, _ = _steps(lambda: ct.run_add_candidate(_add_run(), hs=hs, store=kept))

    assert steps[-1]["result"] == "empty" and "Nothing was added" in steps[-1]["text"]


# ── Find email ───────────────────────────────────────────────────────────────────────────────────────────────────────

def _vendor(value, notes=()):
    def call(*args, **kwargs):
        for vendor, text, result in notes:
            run_trail.note(vendor, text, result)
        return value
    return call


FE_DOWN = (("fullenrich", "The FullEnrich lookup request failed: HTTP 503 (the vendor had a problem).", "failed"),)
FE_OFF = (("fullenrich", "FullEnrich is not set up on the service, so no lookup was made.", "skipped"),)
SL_DOWN = (("seamless", "Seamless answered HTTP 500 (the vendor had a problem).", "failed"),)
SL_PAUSED = (("seamless", "Seamless is paused for a while after an earlier problem, so it was not asked.", "skipped"),)
FE_EMPTY = (("fullenrich", "FullEnrich finished and found no work email.", "empty"),)
SL_EMPTY = (("seamless", "Seamless ended the research with status notfound; nothing to use.", "empty"),)


def _find_email(hs, *, fe, sl, ev=None):
    fe_value, fe_notes = fe
    sl_value, sl_notes = sl
    with patch.object(co, "enrich_contact_info", side_effect=_vendor(fe_value, fe_notes)), \
         patch.object(co, "seamless_lookup", side_effect=_vendor(sl_value, sl_notes)), \
         patch.object(co, "verify_email_millionverifier", side_effect=_vendor(ev or dict(_OK), (("millionverifier", "MillionVerifier says the address is deliverable.", "ok"),))):
        return _steps(lambda: co.run_objective("find_email", "42", hs=hs, requested_by="rep@example.org"))


def test_when_no_vendor_could_be_asked_the_run_fails_and_writes_nothing(monkeypatch):
    hs = _hs()

    steps, out = _find_email(hs, fe=(_fe(), FE_DOWN), sl=(None, SL_DOWN))

    assert (out.status, out.outcome) == ("failed", "error")
    assert "FullEnrich and Seamless could not be asked" in out.message and "What happened" in out.message and "Try again later" in out.message
    assert "nothing usable" not in out.message
    assert hs.update_contact.call_count == 0 and hs.create_note_on_contact.call_count == 0


@pytest.mark.parametrize("fe_notes, sl_notes", [(FE_OFF, SL_PAUSED), (FE_DOWN, SL_PAUSED), (FE_OFF, SL_DOWN)])
def test_a_vendor_that_was_not_set_up_or_is_paused_counts_as_not_asked(fe_notes, sl_notes):
    _, out = _find_email(_hs(), fe=(_fe(), fe_notes), sl=(None, sl_notes))

    assert (out.status, out.outcome) == ("failed", "error")


def test_when_one_vendor_could_not_be_asked_the_empty_answer_of_the_other_is_not_passed_off_as_both():
    hs = _hs()

    _, out = _find_email(hs, fe=(_fe(), FE_DOWN), sl=(_sl(), SL_EMPTY))

    assert (out.status, out.outcome) == ("done", "not_found")
    assert out.message == "No email found. Seamless had nothing usable. FullEnrich could not be asked: see What happened."
    assert "<b>No email found.</b>" in _note(hs)


def test_when_seamless_is_the_one_that_could_not_be_asked_the_message_says_so():
    _, out = _find_email(_hs(), fe=(_fe(), FE_EMPTY), sl=(None, SL_DOWN))

    assert out.message == "No email found. FullEnrich had nothing usable. Seamless could not be asked: see What happened."


def test_when_both_answered_with_nothing_the_message_is_what_it_always_was():
    _, out = _find_email(_hs(), fe=(_fe(), FE_EMPTY), sl=(_sl(), SL_EMPTY))

    assert out.message == "No email found. FullEnrich and Seamless had nothing usable."


def test_outside_a_run_the_message_is_what_it_always_was():
    with patch.object(co, "enrich_contact_info", return_value=_fe()), patch.object(co, "seamless_lookup", return_value=None):
        out = co.run_objective("find_email", "42", hs=_hs(), requested_by="rep@example.org")

    assert out.message == "No email found. FullEnrich and Seamless had nothing usable."


def test_a_find_says_which_company_and_website_it_looked_at(monkeypatch):
    steps, _ = _find_email(_hs(), fe=(_fe(), FE_EMPTY), sl=(_sl(), SL_EMPTY))

    first = steps[0]
    assert first["vendor"] == "service" and "Elm Court Care Center" in first["text"] and "elmcourt.example.org" in first["text"]


def test_an_address_at_another_domain_is_held_and_a_step_says_so_without_the_address():
    steps, out = _find_email(_hs(), fe=(_fe("pat.lee@elsewhere-example.com"), ()), sl=(None, ()))

    assert out.outcome == "held"
    held = next(s for s in steps if "held" in s["text"].lower())
    assert "FullEnrich" in held["text"] and "domain" in held["text"] and "elsewhere" not in held["text"]


def test_an_address_that_fails_the_check_is_not_added_and_a_step_says_so():
    steps, out = _find_email(_hs(), fe=(_fe(_ON_DOMAIN), ()), sl=(None, ()), ev=dict(_INVALID))

    assert out.outcome == "not_written"
    assert any("failed the check" in t and "not added" in t for t in _texts(steps))


def test_an_address_that_is_written_leaves_a_hubspot_step():
    hs = _hs()

    steps, out = _find_email(hs, fe=(_fe(_ON_DOMAIN), ()), sl=(None, ()))

    assert out.outcome == "found" and _written(hs)["email"] == _ON_DOMAIN
    assert steps[-1]["vendor"] == "hubspot" and steps[-1]["result"] == "ok" and "Added the email" in steps[-1]["text"]


def test_an_address_hubspot_will_not_save_is_a_failed_hubspot_step_and_not_a_success():
    from hubspot_client import HubSpotError

    hs = _hs()
    hs.update_contact.side_effect = HubSpotError("conflict", 409)

    steps, out = _find_email(hs, fe=(_fe(_ON_DOMAIN), ()), sl=(None, ()))

    assert out.outcome == "not_written"
    refused = next(s for s in steps if s["vendor"] == "hubspot")
    assert refused["result"] == "failed" and "would not save" in refused["text"]
    assert not any("Added the email" in t for t in _texts(steps))


def test_a_contact_that_already_has_an_email_says_nothing_was_looked_up():
    steps, out = _find_email(_hs(contact={"email": "pat.lee@somewhere.example"}), fe=(_fe(), ()), sl=(None, ()))

    assert out.outcome == "skipped"
    assert steps[0]["result"] == "skipped" and "Already has an email" in steps[0]["text"]


def test_a_contact_linked_to_no_company_says_why_nobody_was_asked():
    steps, out = _find_email(_hs(company_ids=()), fe=(_fe(), ()), sl=(None, ()))

    assert out.outcome == "skipped" and steps[0]["result"] == "skipped" and "not linked to a company" in steps[0]["text"]


# ── Find mobile ──────────────────────────────────────────────────────────────────────────────────────────────────────

def _find_mobile(hs, *, sl):
    sl_value, sl_notes = sl
    with patch.object(co, "seamless_lookup", side_effect=_vendor(sl_value, sl_notes)):
        return _steps(lambda: co.run_objective("find_mobile", "42", hs=hs, requested_by="rep@example.org"))


def test_a_mobile_search_that_could_not_ask_seamless_fails_and_writes_nothing():
    hs = _hs()

    steps, out = _find_mobile(hs, sl=(None, SL_DOWN))

    assert (out.status, out.outcome) == ("failed", "error")
    assert out.message == "Seamless could not be asked: see What happened. Try again later."
    assert hs.update_contact.call_count == 0 and hs.create_note_on_contact.call_count == 0


def test_a_mobile_search_that_found_nothing_after_seamless_answered_is_still_nothing_found():
    _, out = _find_mobile(_hs(), sl=(_sl(), SL_EMPTY))

    assert (out.status, out.outcome) == ("done", "not_found")


def test_a_mobile_that_is_added_leaves_a_hubspot_step():
    hs = _hs()

    steps, out = _find_mobile(hs, sl=(_sl(phone="3307020110", phone_type="mobile", phone_confidence=96.0), ()))

    assert out.outcome == "found"
    assert steps[-1]["vendor"] == "hubspot" and "Added the mobile number" in steps[-1]["text"]


def test_a_number_that_is_not_a_mobile_says_so_in_a_step_without_the_number():
    steps, out = _find_mobile(_hs(), sl=(_sl(phone="3307020110", phone_type="direct", phone_confidence=96.0), ()))

    assert out.outcome == "not_written"
    assert any("direct line, not a mobile" in t for t in _texts(steps)) and not any("330" in t for t in _texts(steps))


# ── Verify email ─────────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_check_says_what_it_is_checking_and_that_the_answer_was_saved():
    hs = _hs(contact={"email": "pat.lee@elmcourt.example.org"})
    with patch.object(co, "verify_email_millionverifier", side_effect=_vendor(dict(_OK), (("millionverifier", "MillionVerifier says the address is deliverable.", "ok"),))):
        steps, out = _steps(lambda: co.run_objective("verify_email", "42", hs=hs, requested_by="rep@example.org"))

    assert out.outcome == "verified"
    assert steps[0]["vendor"] == "service" and "Checking the email" in steps[0]["text"]
    assert steps[-1]["vendor"] == "hubspot" and "Saved the check" in steps[-1]["text"]
