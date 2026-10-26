"""
A reused contact keeps a title that already says what the roster says.

When the run reuses a HubSpot contact it used to overwrite the job title with the found title.
A contact titled "Administrator/Executive Director" was cut down to "Administrator" because the
state roster only has one word for the role. Information was lost for no gain.

The rule: leave the existing title alone when it already contains the found title as one of its
own parts ("Administrator/Executive Director" contains "Administrator" and "Executive Director").
A title that merely mentions the words does not count: "Assistant Administrator" and
"Co-Administrator" are different jobs, so the found title still replaces them.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_keep_existing_title.py" -v
"""

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor as ep

_OK = {"result": "ok", "status": "VERIFIED", "quality": "good"}
_NO_FE = {"email": "", "email_status": "", "personal_email": "", "phone": ""}


# ── The comparison on its own ─────────────────────────────────────────────────

@pytest.mark.parametrize("existing, found", [
    ("Administrator/Executive Director", "Administrator"),
    ("Administrator/Executive Director", "Executive Director"),
    ("Administrator / Executive Director", "Executive Director"),
    ("Administrator & Executive Director", "Administrator"),
    ("Administrator and Executive Director", "Executive Director"),
    ("Administrator, Executive Director", "Administrator"),
    ("Administrator; Executive Director", "Executive Director"),
    ("Executive Director - Elm Court", "Executive Director"),
    ("Executive Director- Elm Court", "Executive Director"),
    ("Executive Director – Elm Court", "Executive Director"),
    ("Executive Director — Elm Court", "Executive Director"),
    ("Administrator | Elm Court", "Administrator"),
    ("administrator/executive director", "Administrator"),
    ("  Administrator/Executive Director ", "Executive  Director"),
    ("Administrator", "Administrator"),
])
def test_a_title_that_already_has_the_found_title_as_one_of_its_parts_covers_it(existing, found):
    assert ep._existing_title_covers(existing, found)


@pytest.mark.parametrize("existing, found", [
    ("Assistant Administrator", "Administrator"),
    ("Associate Administrator", "Administrator"),
    ("Co-Administrator", "Administrator"),
    ("Interim Administrator", "Administrator"),
    ("Former Administrator", "Administrator"),
    ("Administrator in Training", "Administrator"),
    ("Director of Nursing", "Administrator"),
    ("Administrator", "Executive Director"),
    ("Administrators Association Liaison", "Administrator"),
    ("", "Administrator"),
    ("   ", "Administrator"),
    ("Administrator", ""),
    ("Administrator", "   "),
    ("", ""),
    ("   ", "   "),
    ("Administrator /", ""),
])
def test_a_title_that_only_mentions_the_words_or_says_something_else_does_not(existing, found):
    assert not ep._existing_title_covers(existing, found)


# ── In the run ────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _vendors():
    with patch.object(ep, "enrich_contact_info", return_value=dict(_NO_FE)), \
         patch.object(ep, "verify_email_millionverifier", return_value=dict(_OK)), \
         patch.object(ep, "seamless_lookup", return_value=None):
        yield


def _reuse(existing_title, found_title="Administrator", *, dh_executive_id=""):
    hs = MagicMock()
    hs.get_contact_properties.return_value = {
        "jobtitle": existing_title, "email": "pat.lee@example.org", "phone": "405-555-0100",
    }
    ep._enrich_and_associate_existing_contact(
        hs=hs, matched_contact_id="42", matched_detail="matched by name", found_name="Pat Lee",
        found_title=found_title, found_email="", found_phone="", found_linkedin="",
        facility_name="Elm Court", facility_domain="example.org", email_validation={}, email_to_use="",
        data_freshness_value="", research_reasoning="", source_path="state_association_bq_table", _base={},
        operator_name="Elm Holdings", dh_executive_id=dh_executive_id,
    )
    return hs


def _written(hs) -> dict:
    merged: dict = {}
    for call in hs.update_contact.call_args_list:
        merged.update(call.args[1])
    return merged


def test_a_reused_contact_titled_with_two_roles_keeps_both_when_the_roster_names_one():
    hs = _reuse("Administrator/Executive Director", "Administrator")

    assert "jobtitle" not in _written(hs)


def test_the_other_role_in_the_title_is_covered_too():
    hs = _reuse("Administrator/Executive Director", "Executive Director")

    assert "jobtitle" not in _written(hs)


def test_a_title_that_is_a_different_job_is_still_replaced():
    hs = _reuse("Assistant Administrator", "Administrator")

    assert _written(hs)["jobtitle"] == "Administrator"


def test_a_title_with_a_hyphen_inside_a_word_is_not_split_into_two_roles():
    hs = _reuse("Co-Administrator", "Administrator")

    assert _written(hs)["jobtitle"] == "Administrator"


def test_a_contact_with_no_title_is_given_the_found_title():
    hs = _reuse("", "Administrator")

    assert _written(hs)["jobtitle"] == "Administrator"


def test_an_unrelated_title_is_replaced_as_before():
    hs = _reuse("Director of Nursing", "Administrator")

    assert _written(hs)["jobtitle"] == "Administrator"


def test_no_found_title_changes_nothing():
    hs = _reuse("Administrator/Executive Director", "")

    assert "jobtitle" not in _written(hs)


def test_keeping_the_title_does_not_stop_the_definitive_id_being_saved():
    hs = _reuse("Administrator/Executive Director", "Administrator", dh_executive_id="12345")

    written = _written(hs)
    assert "jobtitle" not in written
    assert written["definitive_executive_id"] == "12345"


def test_the_log_says_the_title_was_kept_and_why(caplog):
    with caplog.at_level(logging.INFO):
        _reuse("Administrator/Executive Director", "Administrator")

    assert any("keeping" in r.getMessage().lower() and "Administrator/Executive Director" in r.getMessage()
               for r in caplog.records)
