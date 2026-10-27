"""
Finding people by job title from the company card, and adding the ones a rep chooses.

A rep types or picks job titles on a company. The service searches FullEnrich for people holding them (a preview: nothing
is written to HubSpot), the rep ticks who to add, and each person goes through the same step a company run uses for a
person it found (enrichment_processor._process_found_contact): identity gate, MillionVerifier, reuse of an existing
contact only when it is safe, otherwise a new contact. These tests fake FullEnrich and HubSpot and pin what matters: bad
titles are refused, the search says honestly when it cannot run, custom titles are not silently narrowed, nothing is
written by a search, the stored candidates are the only place names are kept, and an add takes the company run's path.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_company_titles.py" -v
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app_run_store
import company_titles as ct
import config
import enrichment_processor
from hubspot_client import HubSpotError

NOW = datetime(2026, 10, 26, 16, 0, 0, tzinfo=timezone.utc)
FACILITY = "Individual Facility / Child"
CORPORATION = "Corporation / Operator"

COMPANIES = {
    "500": {"name": "Elm Court Care Center", "hierarchy_type": FACILITY, "website": "https://www.elmcourt.example.org/",
            "city": "Tulsa", "state": "OK", "hs_parent_company_id": "600"},
    "600": {"name": "Elm Holdings", "hierarchy_type": CORPORATION, "website": "https://www.elmholdings.example.org/",
            "city": "", "state": "OK", "hs_parent_company_id": ""},
    "700": {"name": "Birch Hill Center", "hierarchy_type": "", "website": "", "city": "", "state": "", "hs_parent_company_id": ""},
}


def _person(name="Pat Lee", title="Medical Director", linkedin="https://www.linkedin.com/in/pat-lee-example", updated="2026-09-01"):
    return {"full_name": name, "title": title, "linkedin_url": linkedin, "updated_at": updated, "current_start": "2024-03-01"}


@pytest.fixture
def hs():
    h = MagicMock()
    h.get_company_properties.side_effect = lambda cid: dict(COMPANIES[cid])
    h.get_associated_contacts.return_value = []
    return h


@pytest.fixture
def search(monkeypatch):
    """The FullEnrich search, recording how it was asked and answering with whatever a test sets."""
    calls = []
    answers = []

    def fake(**kwargs):
        calls.append(kwargs)
        return list(answers.pop(0)) if answers else []

    monkeypatch.setattr(ct, "search_facility_contacts", fake)
    monkeypatch.setattr(ct, "search_unavailable_reason", lambda: None)
    fake.calls, fake.answers = calls, answers
    return fake


# ── Which titles are accepted ─────────────────────────────────────────────────

def test_titles_are_trimmed_squeezed_and_kept_in_order():
    assert ct.clean_titles(["  Medical   Director ", "Social\nWorker"]) == (["Medical Director", "Social Worker"], None)


def test_blank_titles_and_repeats_in_any_case_are_dropped():
    assert ct.clean_titles(["Medical Director", "", "   ", "medical director", "MEDICAL DIRECTOR", "Dietitian"]) == (["Medical Director", "Dietitian"], None)


@pytest.mark.parametrize("raw", [None, [], ["", "  "], "Medical Director", 5])
def test_nothing_to_search_for_is_refused(raw):
    assert ct.clean_titles(raw) == ([], "Choose at least one job title.")


def test_more_than_five_titles_is_refused():
    titles, error = ct.clean_titles([f"Title {n}" for n in range(6)])

    assert titles == [] and error == "Choose at most 5 job titles."


def test_five_titles_are_accepted():
    assert ct.clean_titles([f"Title {n}" for n in range(5)])[1] is None


def test_repeats_do_not_count_against_the_limit():
    titles, error = ct.clean_titles(["A one", "a one", "B two", "C three", "D four", "E five"])

    assert error is None and len(titles) == 5


def test_a_title_longer_than_eighty_characters_is_refused():
    assert ct.clean_titles(["x" * 81]) == ([], "A job title can be at most 80 characters.")
    assert ct.clean_titles(["x" * 80])[1] is None


@pytest.mark.parametrize("raw", [[7], [None], [["Medical Director"]], ["Medical\x00Director"], ["Medical\x07Director"]])
def test_a_title_that_is_not_plain_text_is_refused(raw):
    assert ct.clean_titles(raw) == ([], "Job titles must be plain text.")


def test_the_same_titles_in_any_order_or_case_make_the_same_key():
    assert ct.titles_key(["Medical Director", "Dietitian"]) == ct.titles_key(["dietitian", "MEDICAL DIRECTOR"])
    assert ct.titles_key(["Medical Director"]) != ct.titles_key(["Medical Director", "Dietitian"])


def test_the_presets_are_the_titles_the_company_runs_look_for():
    assert ct.presets_for("facility") == config.FACILITY_LEADERSHIP_TITLES
    assert ct.presets_for("corporation") == config.W2_CORPORATE_TITLES
    assert ct.presets_for("other") == config.FACILITY_LEADERSHIP_TITLES


# ── Finding people ────────────────────────────────────────────────────────────

def test_a_facility_is_searched_the_way_a_company_run_searches_it_with_the_reps_titles(hs, search):
    search.answers.append([_person()])

    ct.find_candidates(hs, "500", COMPANIES["500"], "facility", ["Medical Director", "Dietitian"])

    assert search.calls == [{
        "company_name": "Elm Court Care Center", "city": "Tulsa", "state": "OK",
        "titles": ["Medical Director", "Dietitian"], "domain": "https://www.elmcourt.example.org/",
    }]


def test_a_corporation_is_searched_for_its_own_titles_with_the_corporate_filters_and_the_rest_without(hs, search):
    search.answers.extend([[_person("Sam Ortiz", "Chief Executive Officer")], [_person("Kim Day", "Quality Director")]])

    ct.find_candidates(hs, "600", COMPANIES["600"], "corporation", ["chief executive officer", "Quality Director", "President"])

    assert [(c["titles"], c["corporate_mode"], c["city"], c["state"]) for c in search.calls] == [
        (["chief executive officer", "President"], True, "", ""),
        (["Quality Director"], False, "", ""),
    ]


def test_a_corporation_asked_only_for_its_own_titles_is_searched_once(hs, search):
    ct.find_candidates(hs, "600", COMPANIES["600"], "corporation", ["President"])

    assert len(search.calls) == 1 and search.calls[0]["corporate_mode"] is True


def test_a_corporation_asked_only_for_custom_titles_is_searched_once_without_the_corporate_filters(hs, search):
    ct.find_candidates(hs, "600", COMPANIES["600"], "corporation", ["Quality Director"])

    assert len(search.calls) == 1 and search.calls[0]["corporate_mode"] is False


def test_a_company_that_is_neither_is_searched_like_a_facility(hs, search):
    ct.find_candidates(hs, "700", COMPANIES["700"], "other", ["Medical Director"])

    assert search.calls[0]["company_name"] == "Birch Hill Center" and search.calls[0].get("corporate_mode", False) is False


def test_each_person_found_gets_a_stable_id_and_what_the_card_shows(hs, search):
    search.answers.append([_person()])

    found = ct.find_candidates(hs, "500", COMPANIES["500"], "facility", ["Medical Director"])

    assert found == [{
        "candidate_id": ct.candidate_id(_person()), "full_name": "Pat Lee", "title": "Medical Director",
        "linkedin_url": "https://www.linkedin.com/in/pat-lee-example", "updated_at": "2026-09-01", "linked_contact_id": "",
    }]
    assert len(found[0]["candidate_id"]) == 12 and ct.candidate_id(_person()) == ct.candidate_id(_person())
    assert ct.candidate_id(_person()) != ct.candidate_id(_person("Sam Ortiz"))


def test_a_person_with_no_update_date_is_shown_with_when_they_started(hs, search):
    search.answers.append([{**_person(), "updated_at": ""}])

    assert ct.find_candidates(hs, "500", COMPANIES["500"], "facility", ["Medical Director"])[0]["updated_at"] == "2024-03-01"


def test_a_person_found_under_two_searches_is_listed_once(hs, search):
    search.answers.extend([[_person("Pat Lee", "Chief Executive Officer")], [_person("pat lee", "Owner")]])

    found = ct.find_candidates(hs, "600", COMPANIES["600"], "corporation", ["President", "Owner Operator"])

    assert [c["full_name"] for c in found] == ["Pat Lee"]


def test_a_person_who_is_already_a_contact_of_the_company_is_marked_with_that_contact(hs, search):
    hs.get_associated_contacts.return_value = [{"id": "901", "firstname": "Pat", "lastname": "Lee", "email": "", "phone": "", "jobtitle": "Medical Director"}]
    search.answers.append([_person(), _person("Sam Ortiz", "Dietitian", "")])

    found = ct.find_candidates(hs, "500", COMPANIES["500"], "facility", ["Medical Director", "Dietitian"])

    assert [(c["full_name"], c["linked_contact_id"]) for c in found] == [("Pat Lee", "901"), ("Sam Ortiz", "")]
    hs.get_associated_contacts.assert_called_once_with("500", limit=100)


def test_a_search_that_finds_nobody_returns_nothing(hs, search):
    assert ct.find_candidates(hs, "500", COMPANIES["500"], "facility", ["Medical Director"]) == []


# ── The search run ────────────────────────────────────────────────────────────

def _find_run(company="500", titles=("Medical Director",), run_id="f" * 32):
    return {"run_id": run_id, "company_id": company, "contact_id": "", "objective": "find_by_title", "titles": list(titles),
            "requested_by": "rep@example.org", "requested_by_id": "77"}


@pytest.fixture
def store():
    return app_run_store.MemoryRunStore()


def test_a_search_says_so_and_looks_for_nobody_when_it_cannot_run(hs, search, store, monkeypatch):
    monkeypatch.setattr(ct, "search_unavailable_reason", lambda: "FullEnrich has no credits left, so people cannot be searched for.")

    out = ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW)

    assert (out.status, out.outcome) == ("failed", "error") and out.message == "FullEnrich has no credits left, so people cannot be searched for."
    assert search.calls == [] and store.get_candidates("f" * 32) is None


def test_a_search_that_finds_people_keeps_them_for_the_rep_to_choose_from_and_says_how_many(hs, search, store):
    search.answers.append([_person(), _person("Sam Ortiz", "Dietitian", "")])

    out = ct.run_find_by_title(_find_run(titles=("Medical Director", "Dietitian")), hs=hs, store=store, now=NOW)

    assert (out.status, out.outcome, out.message) == ("done", "found", "Found 2 people. Choose who to add.")
    kept = store.get_candidates("f" * 32)
    assert kept["company_id"] == "500" and [c["full_name"] for c in kept["candidates"]] == ["Pat Lee", "Sam Ortiz"]


def test_one_person_is_said_in_the_singular(hs, search, store):
    search.answers.append([_person()])

    assert ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW).message == "Found 1 person. Choose who to add."


def test_the_message_of_a_search_never_carries_a_name(hs, search, store):
    search.answers.append([_person()])

    assert "Pat" not in ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW).message


def test_a_search_that_finds_nobody_says_so_and_keeps_an_empty_list(hs, search, store):
    out = ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW)

    assert (out.status, out.outcome, out.message) == ("done", "not_found", "No one found for these titles at this company.")
    assert store.get_candidates("f" * 32)["candidates"] == []


def test_a_search_writes_nothing_to_hubspot(hs, search, store):
    search.answers.append([_person()])

    ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW)

    for name in ("create_contact", "update_contact", "associate_contact_to_company", "create_note_on_contact", "create_note_on_company", "update_company"):
        assert getattr(hs, name).call_count == 0, name


def test_a_search_for_a_company_hubspot_will_not_read_fails_plainly_and_leaks_nothing(hs, search, store):
    hs.get_company_properties.side_effect = HubSpotError("boom: secret detail", 500)

    out = ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW)

    assert (out.status, out.outcome) == ("failed", "error") and "secret detail" not in out.message


def test_a_search_that_crashes_fails_plainly(hs, search, store):
    search.answers.append(None)   # list(None) raises inside the search step

    out = ct.run_find_by_title(_find_run(), hs=hs, store=store, now=NOW)

    assert (out.status, out.outcome) == ("failed", "error") and out.message == "Something went wrong. Nothing was changed. Try again."


# ── Adding a person ───────────────────────────────────────────────────────────

def _add_run(company="500", candidate=None, source="f" * 32, run_id="a" * 32):
    return {"run_id": run_id, "company_id": company, "contact_id": "", "objective": "add_candidate", "source_run_id": source,
            "candidate_id": candidate or ct.candidate_id(_person()), "requested_by": "rep@example.org", "requested_by_id": "77"}


@pytest.fixture
def processor(monkeypatch):
    """The company run's step for a person it found, faked: it records how it was called and answers as a test says."""
    calls = []
    answer = {"action": enrichment_processor.ContactAction(action="created_new", contact_id="901")}

    def fake(**kwargs):
        calls.append(kwargs)
        return answer["action"]

    monkeypatch.setattr(enrichment_processor, "_process_found_contact", fake)
    fake.calls, fake.answer = calls, answer
    return fake


@pytest.fixture
def kept(store):
    store.save_candidates("f" * 32, "500", [{**_person(), "candidate_id": ct.candidate_id(_person()), "linked_contact_id": ""}], NOW)
    return store


def test_an_add_takes_the_company_runs_path_for_a_found_person(hs, kept, processor):
    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert len(processor.calls) == 1
    call = processor.calls[0]
    assert call["hs"] is hs and call["found_name"] == "Pat Lee" and call["found_title"] == "Medical Director"
    assert call["found_linkedin"] == "https://www.linkedin.com/in/pat-lee-example" and call["found_email"] == "" and call["found_phone"] == ""
    assert call["source_path"] == "fullenrich_people_search" and call["confidence"] == "low_fullenrich" and call["fe_people_search_ran"] is False
    assert call["fe_updated_at"] == "2026-09-01" and call["pre_populated_contact_ids"] == "" and call["research_findings"] == ""
    assert call["facility_name"] == "Elm Court Care Center" and call["facility_domain"] == "https://www.elmcourt.example.org/"
    assert call["operator_name"] == "Elm Holdings" and call["target_company_id"] == "500"
    assert set(call["related_company_ids"]) == {"500", "600"}
    assert (out.status, out.outcome) == ("done", "found")


def test_a_corporate_title_on_a_facility_is_linked_where_the_company_run_would_link_it(hs, store, processor):
    person = _person("Sam Ortiz", "Regional Vice President", "")
    store.save_candidates("f" * 32, "500", [{**person, "candidate_id": ct.candidate_id(person), "linked_contact_id": ""}], NOW)

    out = ct.run_add_candidate(_add_run(candidate=ct.candidate_id(person)), hs=hs, store=store)

    assert processor.calls[0]["target_company_id"] == "600" and "Elm Holdings" in out.message


def test_a_person_added_from_a_corporation_is_linked_to_it_with_itself_as_the_operator(hs, store, processor):
    store.save_candidates("f" * 32, "600", [{**_person(), "candidate_id": ct.candidate_id(_person()), "linked_contact_id": ""}], NOW)

    ct.run_add_candidate(_add_run(company="600"), hs=hs, store=store)

    call = processor.calls[0]
    assert call["target_company_id"] == "600" and call["facility_name"] == "Elm Holdings" and call["operator_name"] == "Elm Holdings"


def test_a_new_contact_is_reported_with_its_id_and_where_it_was_linked(hs, kept, processor):
    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert out.contact_id == "901" and out.wrote == ["contact"]
    assert out.message == "Created a new contact and linked it to Elm Court Care Center."


def test_an_existing_contact_that_was_reused_is_reported_as_linked(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="associated_existing", contact_id="902")

    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert (out.status, out.outcome, out.contact_id) == ("done", "found", "902")
    assert out.message == "Linked an existing contact to Elm Court Care Center."


def test_an_email_that_was_found_is_said_but_never_shown(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="created_new", contact_id="901", found_email="pat.lee@elmcourt.example.org",
                                                                    email_validation_status="VERIFIED")

    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert out.message.endswith("An email was added.") and "pat.lee" not in out.message


def test_an_email_that_checked_as_invalid_is_not_claimed(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="created_new", contact_id="901", found_email="pat.lee@elmcourt.example.org",
                                                                    email_validation_status="INVALID")

    assert "email was added" not in ct.run_add_candidate(_add_run(), hs=hs, store=kept).message


def test_a_person_nothing_could_be_done_for_is_reported_not_found(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="no_contact_found", detail="Could not parse name")

    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert (out.status, out.outcome, out.wrote) == ("done", "not_found", []) and out.contact_id == ""


def test_an_add_that_errors_fails_plainly_without_the_detail(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="error", detail="HubSpot said: secret detail")

    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert (out.status, out.outcome) == ("failed", "error") and "secret detail" not in out.message


def test_an_add_leaves_a_note_on_the_contact_saying_who_asked_and_what_was_searched(hs, store, processor):
    store.save_candidates("f" * 32, "500", [{**_person(), "candidate_id": ct.candidate_id(_person()), "linked_contact_id": ""}], NOW)
    store.create_run({"run_id": "f" * 32, "contact_id": "", "objective": "find_by_title", "titles": ["Medical Director", "Dietitian"], "requested_by_id": "77",
                      "requested_by": "rep@example.org", "portal_id": "1", "company_id": "500", "created_at": NOW})

    ct.run_add_candidate(_add_run(), hs=hs, store=store)

    contact_id, body = hs.create_note_on_contact.call_args.args
    assert contact_id == "901" and "rep@example.org" in body and "Medical Director" in body and "Dietitian" in body


def test_the_note_escapes_what_the_rep_typed(hs, store, processor):
    store.save_candidates("f" * 32, "500", [{**_person(), "candidate_id": ct.candidate_id(_person()), "linked_contact_id": ""}], NOW)
    store.create_run({"run_id": "f" * 32, "contact_id": "", "objective": "find_by_title", "titles": ["<b>x</b>"], "requested_by_id": "77",
                      "requested_by": "", "portal_id": "1", "company_id": "500", "created_at": NOW})

    ct.run_add_candidate({**_add_run(), "requested_by": "<i>me</i>"}, hs=hs, store=store)

    body = hs.create_note_on_contact.call_args.args[1]
    assert "<b>x</b>" not in body and "&lt;b&gt;x&lt;/b&gt;" in body and "<i>me</i>" not in body


def test_a_note_that_cannot_be_saved_does_not_undo_the_add(hs, kept, processor):
    hs.create_note_on_contact.side_effect = HubSpotError("no", 500)

    assert ct.run_add_candidate(_add_run(), hs=hs, store=kept).outcome == "found"


def test_no_note_is_left_when_nothing_was_made(hs, kept, processor):
    processor.answer["action"] = enrichment_processor.ContactAction(action="no_contact_found")

    ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert hs.create_note_on_contact.call_count == 0


@pytest.mark.parametrize("why, run, saved", [
    ("results expired", _add_run(), None),
    ("another company's results", _add_run(company="600"), "500"),
    ("a person not in the results", _add_run(candidate="0" * 12), "500"),
])
def test_an_add_that_cannot_find_its_person_does_nothing_and_says_to_search_again(hs, store, processor, why, run, saved):
    if saved:
        store.save_candidates("f" * 32, saved, [{**_person(), "candidate_id": ct.candidate_id(_person()), "linked_contact_id": ""}], NOW)

    out = ct.run_add_candidate(run, hs=hs, store=store)

    assert (out.status, out.outcome) == ("failed", "error") and out.message == "Those search results are no longer available. Search again."
    assert processor.calls == [] and hs.create_contact.call_count == 0


def test_an_add_that_crashes_fails_plainly(hs, kept, processor):
    processor.answer["action"] = None   # .action on None raises

    out = ct.run_add_candidate(_add_run(), hs=hs, store=kept)

    assert (out.status, out.outcome) == ("failed", "error") and out.message == "Something went wrong. Nothing was changed. Try again."
