"""
The three things a rep can ask of one contact from the HubSpot app: Find email, Find mobile, Verify email.

Each one reuses what the company runs already do (FullEnrich and Seamless to look, MillionVerifier and BounceBan to
check, the identity gate to hold an address that is not at the facility or its operator) and keeps the same rules:
fill blanks only, never delete, no invalid email, a dialable mobile only, nothing used for someone who has moved. The
vendors and HubSpot are faked here; the identity gate is the real one.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_contact_objectives.py" -v
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import contact_objectives as co
from hubspot_client import HubSpotError
from identity_gate import bare_domain

_FACILITY = "Individual Facility / Child"
_CORPORATION = "Corporation / Operator"
_SITE = "https://www.elmcourt.example.org/"
_DOMAIN = bare_domain(_SITE)
_ON_DOMAIN = f"pat.lee@{_DOMAIN}"
_OK = {"result": "ok", "status": "VERIFIED", "quality": "good"}
_INVALID = {"result": "invalid", "status": "INVALID", "quality": "bad"}

_CONTACT = {
    "firstname": "Pat", "lastname": "Lee", "jobtitle": "Administrator", "email": "", "mobilephone": "", "phone": "",
    "hs_linkedin_url": "https://www.linkedin.com/in/pat-lee", "email_verification_result": "", "email_verification_date": "",
}
_COMPANIES = {
    "500": {"name": "Elm Court Care Center", "hierarchy_type": _FACILITY, "website": _SITE, "hs_parent_company_id": "600"},
    "600": {"name": "Elm Holdings", "hierarchy_type": _CORPORATION, "website": "", "hs_parent_company_id": ""},
    "700": {"name": "Birch Hill Center", "hierarchy_type": _FACILITY, "website": "birchhill.example.com", "hs_parent_company_id": ""},
}


def _hs(contact=None, company_ids=("500",), companies=None, reads=None):
    """A HubSpot with one contact. *reads* lets a test change what the contact looks like on the second read."""
    hs = MagicMock()
    first = {**_CONTACT, **(contact or {})}
    if reads:
        hs.get_contact_for_app.side_effect = [first, *reads]
    else:
        hs.get_contact_for_app.return_value = first
    hs.get_contact_company_ids.return_value = list(company_ids)
    table = companies or _COMPANIES
    hs.get_company_properties.side_effect = lambda cid: dict(table[cid])
    return hs


def _fe(email="", status="DELIVERABLE"):
    return {"email": email, "email_status": status, "personal_email": "", "phone": ""}


def _sl(**over):
    base = {"status": "done", "moved": False, "company": "Elm Court Care Center", "company_domain": _DOMAIN, "title": "Administrator",
            "linkedin": "", "phone": "", "phone_type": "", "phone_confidence": None, "phone_rejected": "", "email": "",
            "email_confidence": None, "email_rejected": ""}
    base.update(over)
    return base


class _Vendors:
    def __init__(self, fe, sl, ev):
        self.fe, self.sl, self.ev = fe, sl, ev


def _run(objective, hs, *, fe=None, sl=None, ev=None, by="rep@example.org"):
    with patch.object(co, "enrich_contact_info", return_value=fe if fe is not None else _fe()) as f, \
         patch.object(co, "seamless_lookup", return_value=sl) as s, \
         patch.object(co, "verify_email_millionverifier", return_value=ev if ev is not None else dict(_OK)) as v:
        outcome = co.run_objective(objective, "42", hs=hs, requested_by=by)
    return outcome, _Vendors(f, s, v)


def _written(hs):
    merged = {}
    for call in hs.update_contact.call_args_list:
        merged.update(call.args[1])
    return merged


def _note(hs):
    return " ".join(str(c.args[1]) for c in hs.create_note_on_contact.call_args_list)


# ── What is open to a rep ─────────────────────────────────────────────────────

def _open(contact, objective):
    view = {o["id"]: o for o in co.objectives_for({**_CONTACT, **contact})}
    return view[objective]


def test_the_three_objectives_come_in_the_same_order_with_their_labels_and_costs():
    view = co.objectives_for(dict(_CONTACT))

    assert [(o["id"], o["label"], o["spends"]) for o in view] == [
        ("find_email", "Find email", "up to 2 lookups and 1 check"),
        ("find_mobile", "Find mobile", "1 lookup"),
        ("verify_email", "Verify email", "1 check"),
    ]


def test_find_email_is_open_for_a_named_contact_with_no_email():
    assert _open({}, "find_email") == {"id": "find_email", "label": "Find email", "available": True, "reason": None, "spends": "up to 2 lookups and 1 check"}


def test_find_email_is_closed_when_the_contact_has_an_email_and_points_to_verify():
    got = _open({"email": "pat.lee@example.org"}, "find_email")

    assert got["available"] is False and "Verify email" in got["reason"]


def test_find_mobile_is_closed_when_the_contact_has_a_mobile():
    got = _open({"mobilephone": "405-555-0101"}, "find_mobile")

    assert got["available"] is False and "mobile" in got["reason"].lower()


def test_a_phone_that_is_not_a_mobile_does_not_close_find_mobile():
    assert _open({"phone": "405-555-0100"}, "find_mobile")["available"] is True


@pytest.mark.parametrize("missing", [{"firstname": ""}, {"lastname": ""}, {"firstname": "", "lastname": ""}])
@pytest.mark.parametrize("objective", ["find_email", "find_mobile"])
def test_a_contact_without_a_first_and_last_name_cannot_be_looked_up(objective, missing):
    got = _open(missing, objective)

    assert got["available"] is False and "first and last name" in got["reason"]


def test_verify_email_needs_an_email():
    assert _open({}, "verify_email") == {"id": "verify_email", "label": "Verify email", "available": False, "reason": "No email to check.", "spends": "1 check"}
    assert _open({"email": "pat.lee@example.org"}, "verify_email")["available"] is True


def test_the_card_is_given_a_summary_of_the_contact():
    got = co.contact_summary({**_CONTACT, "email": "pat.lee@example.org", "mobilephone": "405-555-0101",
                              "email_verification_result": "ok", "email_verification_date": "2026-10-21"})

    assert got == {"name": "Pat Lee", "title": "Administrator", "email": "pat.lee@example.org", "mobile": "405-555-0101",
                   "emailCheck": "ok", "emailCheckedOn": "2026-10-21"}


def test_a_contact_with_nothing_is_summarised_with_nulls():
    got = co.contact_summary({k: "" for k in _CONTACT})

    assert got == {"name": "No name", "title": None, "email": None, "mobile": None, "emailCheck": None, "emailCheckedOn": None}


# ── Find email ────────────────────────────────────────────────────────────────

def test_find_email_adds_a_work_email_at_the_facility_and_records_its_check():
    hs = _hs()

    out, v = _run("find_email", hs, fe=_fe(_ON_DOMAIN))

    assert out.status == "done" and out.outcome == "found" and out.message.startswith("Email added")
    written = _written(hs)
    assert written["email"] == _ON_DOMAIN and written["email_verification_result"] == "ok" and "email_verification_date" in written
    assert out.wrote == ["email", "email_verification_result", "email_verification_date"]
    v.sl.assert_not_called()


def test_find_email_asks_fullenrich_about_this_person_at_this_facility():
    _, v = _run("find_email", _hs(), fe=_fe(_ON_DOMAIN))

    v.fe.assert_called_once_with("Pat", "Lee", company_name="Elm Court Care Center", domain=_DOMAIN, linkedin_url="https://www.linkedin.com/in/pat-lee")


def test_find_email_falls_back_to_seamless_when_fullenrich_finds_nothing():
    hs = _hs()

    out, v = _run("find_email", hs, fe=_fe(""), sl=_sl(email=_ON_DOMAIN))

    assert out.outcome == "found" and _written(hs)["email"] == _ON_DOMAIN
    v.sl.assert_called_once_with("Pat", "Lee", domain=_DOMAIN, company="Elm Court Care Center", operator_name="Elm Holdings", title="Administrator")


def test_an_email_at_another_domain_is_held_not_written():
    hs = _hs()

    out, _ = _run("find_email", hs, fe=_fe("pat.lee@mailbox.example.net"))

    assert out.outcome == "held" and out.wrote == [] and hs.update_contact.call_count == 0
    assert "pat.lee@mailbox.example.net" not in out.message
    assert "pat.lee@mailbox.example.net" in _note(hs)


def test_an_email_that_checks_as_invalid_is_not_written():
    hs = _hs()

    out, v = _run("find_email", hs, fe=_fe(_ON_DOMAIN), ev=dict(_INVALID))

    assert out.outcome == "not_written" and "invalid" in out.message and hs.update_contact.call_count == 0
    assert _ON_DOMAIN not in out.message


def test_a_held_fullenrich_email_still_lets_seamless_try_and_a_good_one_wins():
    hs = _hs()

    out, v = _run("find_email", hs, fe=_fe("pat.lee@mailbox.example.net"), sl=_sl(email=_ON_DOMAIN))

    assert out.outcome == "found" and _written(hs)["email"] == _ON_DOMAIN
    v.sl.assert_called_once()


def test_when_nothing_better_comes_the_held_email_is_what_is_reported():
    hs = _hs()

    out, _ = _run("find_email", hs, fe=_fe("pat.lee@mailbox.example.net"), sl=_sl(email=""))

    assert out.outcome == "held"


def test_a_person_who_seamless_shows_elsewhere_gets_nothing_used():
    hs = _hs()

    out, _ = _run("find_email", hs, fe=_fe(""), sl=_sl(moved=True, company="Other Care Co", email="pat@other.example.com"))

    assert out.outcome == "moved" and hs.update_contact.call_count == 0
    assert "another organization" in out.message and "Other Care Co" not in out.message
    assert "Other Care Co" in _note(hs)


def test_find_email_says_so_when_nobody_has_one():
    hs = _hs()

    out, _ = _run("find_email", hs, fe=_fe(""), sl=None)

    assert out.status == "done" and out.outcome == "not_found" and hs.update_contact.call_count == 0
    assert "No email found" in out.message


def test_find_email_does_nothing_for_a_contact_that_already_has_an_email():
    hs = _hs({"email": "pat.lee@example.org"})

    out, v = _run("find_email", hs)

    assert out.outcome == "skipped" and "Already has an email" in out.message
    v.fe.assert_not_called(); v.sl.assert_not_called(); v.ev.assert_not_called()
    assert hs.create_note_on_contact.call_count == 0


def test_an_email_added_while_the_run_was_working_is_never_replaced():
    hs = _hs(reads=[{**_CONTACT, "email": "added.by.someone@example.org"}])

    out, _ = _run("find_email", hs, fe=_fe(_ON_DOMAIN))

    assert out.outcome == "skipped" and "nothing was replaced" in out.message and hs.update_contact.call_count == 0


def test_a_contact_linked_to_no_company_is_not_looked_up():
    hs = _hs(company_ids=())

    out, v = _run("find_email", hs)

    assert out.outcome == "skipped" and "not linked to a company" in out.message
    v.fe.assert_not_called(); v.sl.assert_not_called()


def test_a_contact_without_a_name_is_not_looked_up():
    hs = _hs({"lastname": ""})

    out, v = _run("find_email", hs)

    assert out.outcome == "skipped" and "first and last name" in out.message
    v.fe.assert_not_called()


def test_hubspot_refusing_the_email_is_reported_not_raised():
    hs = _hs()
    hs.update_contact.side_effect = HubSpotError("PATCH failed: 409", status_code=409)

    out, _ = _run("find_email", hs, fe=_fe(_ON_DOMAIN))

    assert out.status == "done" and out.outcome == "not_written" and "would not save" in out.message
    assert out.wrote == []


def test_find_email_writes_only_the_email_and_its_check():
    hs = _hs()

    _run("find_email", hs, fe=_fe(_ON_DOMAIN))

    assert set(_written(hs)) == {"email", "email_verification_result", "email_verification_date"}


# ── Which company a contact is checked against ────────────────────────────────

def test_a_facility_is_preferred_over_a_corporation_the_contact_is_also_linked_to():
    hs = _hs(company_ids=("600", "500"))

    _, v = _run("find_email", hs, fe=_fe(""), sl=None)

    assert v.fe.call_args.kwargs["company_name"] == "Elm Court Care Center"


def test_the_operator_is_the_facilitys_parent():
    _, v = _run("find_mobile", _hs(), sl=_sl())

    assert v.sl.call_args.kwargs["operator_name"] == "Elm Holdings"


def test_a_facility_with_no_parent_has_no_operator():
    hs = _hs(company_ids=("700",))

    _, v = _run("find_mobile", hs, sl=_sl())

    assert v.sl.call_args.kwargs["operator_name"] == "" and v.sl.call_args.kwargs["company"] == "Birch Hill Center"


def test_a_corporation_is_its_own_operator():
    hs = _hs(company_ids=("600",))

    _, v = _run("find_mobile", hs, sl=_sl())

    assert v.sl.call_args.kwargs["operator_name"] == "Elm Holdings" and v.sl.call_args.kwargs["company"] == "Elm Holdings"


def test_a_company_with_no_website_is_asked_for_by_name_alone():
    hs = _hs(company_ids=("600",))

    _, v = _run("find_mobile", hs, sl=_sl())

    assert v.sl.call_args.kwargs["domain"] == ""


# ── Find mobile ───────────────────────────────────────────────────────────────

def test_find_mobile_adds_a_mobile_number_and_nothing_else():
    hs = _hs()

    out, v = _run("find_mobile", hs, sl=_sl(phone="405-555-0199", phone_type="mobile", phone_confidence=96.0))

    assert out.outcome == "found" and out.message == "Mobile number added." and out.wrote == ["mobilephone"]
    assert _written(hs) == {"mobilephone": "405-555-0199"}
    v.fe.assert_not_called(); v.ev.assert_not_called()


def test_a_number_that_is_not_a_mobile_is_reported_not_written():
    hs = _hs()

    out, _ = _run("find_mobile", hs, sl=_sl(phone="405-555-0150", phone_type="direct"))

    assert out.outcome == "not_written" and "direct" in out.message and "not a mobile" in out.message
    assert hs.update_contact.call_count == 0 and "405-555-0150" not in out.message
    assert "405-555-0150" in _note(hs)


def test_a_number_with_no_type_is_not_taken_for_a_mobile():
    hs = _hs()

    out, _ = _run("find_mobile", hs, sl=_sl(phone="405-555-0150", phone_type=""))

    assert out.outcome == "not_written" and hs.update_contact.call_count == 0


def test_a_number_seamless_will_not_stand_behind_is_explained():
    hs = _hs()

    out, _ = _run("find_mobile", hs, sl=_sl(phone="", phone_rejected="the facility's main line (a switchboard, not a direct dial)"))

    assert out.outcome == "not_found" and "main line" in out.message and hs.update_contact.call_count == 0


def test_find_mobile_says_so_when_seamless_has_nothing():
    out, _ = _run("find_mobile", _hs(), sl=None)

    assert out.outcome == "not_found" and "Seamless" in out.message


def test_find_mobile_uses_nothing_for_someone_who_has_moved():
    hs = _hs()

    out, _ = _run("find_mobile", hs, sl=_sl(moved=True, company="Other Care Co", phone="405-555-0199", phone_type="mobile"))

    assert out.outcome == "moved" and hs.update_contact.call_count == 0


def test_find_mobile_does_nothing_for_a_contact_that_already_has_one():
    hs = _hs({"mobilephone": "405-555-0101"})

    out, v = _run("find_mobile", hs, sl=_sl(phone="405-555-0199", phone_type="mobile"))

    assert out.outcome == "skipped" and v.sl.call_count == 0 and hs.update_contact.call_count == 0


def test_a_mobile_added_while_the_run_was_working_is_never_replaced():
    hs = _hs(reads=[{**_CONTACT, "mobilephone": "405-555-0101"}])

    out, _ = _run("find_mobile", hs, sl=_sl(phone="405-555-0199", phone_type="mobile"))

    assert out.outcome == "skipped" and hs.update_contact.call_count == 0


def test_the_regular_phone_is_never_touched():
    hs = _hs({"phone": "405-555-0100"})

    _run("find_mobile", hs, sl=_sl(phone="405-555-0199", phone_type="mobile"))

    assert "phone" not in _written(hs)


# ── Verify email ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("result, phrase", [
    ("ok", "good"), ("catch_all", "risky"), ("unknown", "unknown"), ("invalid", "invalid"), ("disposable", "throwaway"),
])
def test_verify_email_records_the_answer_and_says_it_in_words(result, phrase):
    hs = _hs({"email": "pat.lee@example.org"})

    out, v = _run("verify_email", hs, ev={"result": result, "status": "X", "quality": ""})

    assert out.outcome == "verified" and phrase in out.message
    assert out.wrote == ["email_verification_result", "email_verification_date"]
    assert _written(hs)["email_verification_result"] == result
    v.ev.assert_called_once_with("pat.lee@example.org")


def test_verify_email_never_changes_the_email_itself_even_when_it_is_invalid():
    hs = _hs({"email": "pat.lee@example.org"})

    _run("verify_email", hs, ev=dict(_INVALID))

    assert set(_written(hs)) == {"email_verification_result", "email_verification_date"}


@pytest.mark.parametrize("ev", [{"result": "error", "status": "ERROR"}, {"result": "skipped", "status": "UNVERIFIED"}, {}])
def test_verify_email_says_when_the_check_could_not_be_made(ev):
    hs = _hs({"email": "pat.lee@example.org"})

    out, _ = _run("verify_email", hs, ev=ev)

    assert out.status == "failed" and out.outcome == "error" and "Try again" in out.message
    assert hs.update_contact.call_count == 0


def test_verify_email_has_nothing_to_do_without_an_email():
    hs = _hs()

    out, v = _run("verify_email", hs)

    assert out.outcome == "skipped" and "No email" in out.message and v.ev.call_count == 0


def test_verify_email_does_not_need_a_company():
    hs = _hs({"email": "pat.lee@example.org"}, company_ids=())

    out, _ = _run("verify_email", hs)

    assert out.outcome == "verified"


# ── Every run ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("objective, kwargs", [
    ("find_email", {"fe": _fe(_ON_DOMAIN)}),
    ("find_mobile", {"sl": _sl(phone="405-555-0199", phone_type="mobile")}),
])
def test_a_run_never_sets_the_enrichment_date_that_gates_company_runs(objective, kwargs):
    hs = _hs()

    _run(objective, hs, **kwargs)

    assert "most_recent_enrichment_date" not in _written(hs)


def test_the_note_says_who_asked_and_what(  ):
    hs = _hs()

    _run("find_email", hs, fe=_fe(_ON_DOMAIN), by="rep@example.org")

    note = _note(hs)
    assert "HubSpot app" in note and "Find email" in note and "rep@example.org" in note and _ON_DOMAIN in note


def test_what_a_rep_is_called_cannot_inject_markup_into_the_note():
    hs = _hs()

    _run("find_email", hs, fe=_fe(_ON_DOMAIN), by="<script>alert(1)</script>")

    assert "<script>" not in _note(hs) and "&lt;script&gt;" in _note(hs)


def test_a_note_that_cannot_be_saved_does_not_undo_the_work():
    hs = _hs()
    hs.create_note_on_contact.side_effect = HubSpotError("note failed", status_code=500)

    out, _ = _run("find_email", hs, fe=_fe(_ON_DOMAIN))

    assert out.outcome == "found" and _written(hs)["email"] == _ON_DOMAIN


def test_an_unexpected_error_ends_the_run_as_failed_and_says_nothing_was_changed():
    hs = _hs()
    with patch.object(co, "enrich_contact_info", side_effect=RuntimeError("boom")), \
         patch.object(co, "seamless_lookup", return_value=None):
        out = co.run_objective("find_email", "42", hs=hs, requested_by="rep@example.org")

    assert out.status == "failed" and out.outcome == "error" and "Nothing was changed" in out.message
    assert hs.update_contact.call_count == 0 and "boom" not in out.message


def test_hubspot_failing_to_read_the_contact_ends_the_run_as_failed():
    hs = _hs()
    hs.get_contact_for_app.side_effect = HubSpotError("GET failed: 500", status_code=500)

    out, _ = _run("find_email", hs)

    assert out.status == "failed" and out.outcome == "error"


def test_an_objective_nobody_defined_is_refused():
    with pytest.raises(ValueError):
        co.run_objective("delete_contact", "42", hs=_hs(), requested_by="rep@example.org")
