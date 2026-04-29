#!/usr/bin/env python3
"""
Unit tests for the state association guide helper functions added to
state_association_matcher.py.

Tests cover:
  - _load_association_guide()  — YAML parsing, caching, error handling
  - _normalize_state_name()    — abbreviations, full names, edge cases
  - _lookup_guide_sources()    — tier classification, type matching
  - _format_sources_for_prompt() — content of each tier's source block
  - _build_facility_research_prompt() — return type, tier, prompt structure
  - _build_contact_research_prompt()  — tier-aware association source line

Run with:
  python3.12 -m pytest test_guide_helpers.py -v
"""

import sys
import os
import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# ── Module bootstrap ──────────────────────────────────────────────────────────
# `utils.bigquery_client` is not importable outside the project environment;
# patch it at the sys.modules level before the module under test is imported.

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_MOCK_UTILS = MagicMock()
_MOCK_BQ = MagicMock()

with patch.dict(
    "sys.modules",
    {
        "utils": _MOCK_UTILS,
        "utils.bigquery_client": _MOCK_UTILS,
    },
):
    import state_association_matcher as m


# ── Fixtures ──────────────────────────────────────────────────────────────────

MINIMAL_GUIDE = textwrap.dedent("""\
    # State Senior Care Association Directory: Agent Reference

    ## States With Publicly Accessible Contact Data

    ```yaml
    state: Washington
    sources:
      - type: SNF/ALF
        association_name: Washington Health Care Association (WHCA)
        association_url: https://www.whca.org
        directory_url: https://www.whca.org/facility-finder/
        login_required: false
        contacts_available:
          - executive_director_name
          - executive_director_phone
        url_pattern: https://www.whca.org/facility-finder/{slug}/
        access_notes: Individual facility pages render via JavaScript.
        verified: true
    ```

    ## States With Login-Required Directories

    ```yaml
    state: Florida
    sources:
      - type: SNF
        association_name: Florida Health Care Association (FHCA)
        association_url: https://www.fhca.org
        directory_url: https://www.fhcadirectory.org/directory/
        login_required: true
        contacts_available:
          - administrator_name
          - administrator_email
        url_pattern: https://www.fhcadirectory.org/directory/{slug}/
        access_notes: Login required. Pre-authenticated cookies bypass the gate.
        verified: true
      - type: ALF
        association_name: Florida Assisted Living Association (FALA)
        association_url: https://www.fala.org
        directory_url: https://www.fala.org/facility-members
        login_required: false
        contacts_available: []
        url_pattern: null
        access_notes: Lists facility info only.
        verified: true
    ```

    ## States With No Named Contacts

    ```yaml
    state: Texas
    sources:
      - type: SNF
        association_name: Texas Health Care Association (THCA)
        association_url: https://txhca.org
        directory_url: https://txhca.org/choose-the-right-facility/
        login_required: false
        contacts_available: []
        url_pattern: null
        access_notes: Lists facility info only.
        verified: true
      - type: ALF
        association_name: Texas Assisted Living Association (TALA)
        association_url: https://tala.org
        directory_url: https://tala.org/member-list/
        login_required: false
        contacts_available: []
        url_pattern: null
        access_notes: Lists facility info only.
        verified: true
    ```

    ## Universal Fallback Sources

    ```yaml
    fallback_sources:
      - name: Medicare Care Compare
        url: https://www.medicare.gov/care-compare/
        facility_types:
          - SNF
        contacts_available:
          - administrator_name
        login_required: false
        access_notes: Covers all Medicare/Medicaid-certified SNFs nationwide.
        verified: true
      - name: State Licensing Databases
        url: varies_by_state
        facility_types:
          - SNF
          - ALF
        contacts_available:
          - administrator_name
        login_required: false
        access_notes: Most state health departments publish downloadable lists.
        verified: true
    ```
""")

GUIDE_WITH_YAML_SEPARATOR = textwrap.dedent("""\
    ```yaml
    state: Wyoming
    sources:
      - type: SNF/ALF
        association_name: Wyoming Long Term Care Association (WYLTCA)
        association_url: https://wyltc.com
        directory_url: https://www.wyltc.com/finding-a-nursing-home
        login_required: false
        contacts_available: []
        url_pattern: null
        access_notes: Lists facility info only.
        verified: true
    ---
    ```
""")


@pytest.fixture(autouse=True)
def reset_guide_cache():
    """Clear the module-level guide cache before every test."""
    m._GUIDE_CACHE = None
    yield
    m._GUIDE_CACHE = None


@pytest.fixture
def guide_file(tmp_path) -> Path:
    """Write the minimal test guide to a temp file and return its path."""
    p = tmp_path / "state_association_agent_guide.md"
    p.write_text(MINIMAL_GUIDE)
    return p


# ── _load_association_guide ───────────────────────────────────────────────────

class TestLoadAssociationGuide:

    def test_loads_state_entries(self, guide_file):
        entries, fbs = m._load_association_guide(str(guide_file))
        state_names = [e["state"] for e in entries]
        assert "Washington" in state_names
        assert "Florida" in state_names
        assert "Texas" in state_names

    def test_loads_fallback_sources(self, guide_file):
        entries, fbs = m._load_association_guide(str(guide_file))
        assert len(fbs) == 2
        fallback_names = [f["name"] for f in fbs]
        assert "Medicare Care Compare" in fallback_names

    def test_state_entry_structure(self, guide_file):
        entries, _ = m._load_association_guide(str(guide_file))
        wa = next(e for e in entries if e["state"] == "Washington")
        assert isinstance(wa["sources"], list)
        src = wa["sources"][0]
        assert src["directory_url"] == "https://www.whca.org/facility-finder/"
        assert src["login_required"] is False
        assert "executive_director_name" in src["contacts_available"]

    def test_returns_empty_on_missing_file(self):
        entries, fbs = m._load_association_guide("/nonexistent/path/guide.md")
        assert entries == []
        assert fbs == []

    def test_caches_result_on_second_call(self, guide_file):
        first = m._load_association_guide(str(guide_file))
        second = m._load_association_guide(str(guide_file))
        assert first is second  # Same object — returned from cache

    def test_handles_yaml_separator_inside_block(self, tmp_path):
        p = tmp_path / "guide.md"
        p.write_text(GUIDE_WITH_YAML_SEPARATOR)
        entries, _ = m._load_association_guide(str(p))
        assert any(e["state"] == "Wyoming" for e in entries)

    def test_partial_parse_error_skips_bad_block(self, tmp_path):
        content = "```yaml\nnot: valid: yaml: [\n```\n" + MINIMAL_GUIDE
        p = tmp_path / "guide.md"
        p.write_text(content)
        entries, _ = m._load_association_guide(str(p))
        # Valid blocks still parsed even if one is malformed
        assert len(entries) >= 3


# ── _normalize_state_name ─────────────────────────────────────────────────────

class TestNormalizeStateName:

    @pytest.mark.parametrize("abbrev,expected", [
        ("WA", "Washington"),
        ("wa", "Washington"),
        ("FL", "Florida"),
        ("TX", "Texas"),
        ("NY", "New York"),
        ("NC", "North Carolina"),
        ("ND", "North Dakota"),
    ])
    def test_two_letter_abbreviations(self, abbrev, expected):
        assert m._normalize_state_name(abbrev) == expected

    @pytest.mark.parametrize("full_name,expected", [
        ("Washington", "Washington"),
        ("washington", "Washington"),
        ("WASHINGTON", "Washington"),
        ("North Carolina", "North Carolina"),
    ])
    def test_full_names_normalized(self, full_name, expected):
        assert m._normalize_state_name(full_name) == expected

    def test_strips_whitespace(self):
        assert m._normalize_state_name("  WA  ") == "Washington"
        assert m._normalize_state_name("  Washington  ") == "Washington"

    def test_empty_string_returns_empty(self):
        assert m._normalize_state_name("") == ""

    def test_none_returns_empty(self):
        assert m._normalize_state_name(None) == ""

    def test_unknown_abbreviation_returns_as_is(self):
        # Unknown 2-char codes are returned unchanged (title-cased)
        result = m._normalize_state_name("ZZ")
        assert result == "Zz"


# ── _lookup_guide_sources ─────────────────────────────────────────────────────

class TestLookupGuideSources:

    @pytest.fixture(autouse=True)
    def use_minimal_guide(self, guide_file):
        """Point guide helpers at the temp file for all tests in this class."""
        with patch.object(m, "_GUIDE_PATH", str(guide_file)):
            yield

    @pytest.mark.parametrize("state,ftype,expected_tier", [
        ("Washington", "SNF",  "1"),   # SNF/ALF combined, contacts public
        ("Washington", "ALF",  "1"),   # Combined source also covers ALF
        ("WA",         "SNF",  "1"),   # Abbreviation resolved
        ("Florida",    "SNF",  "2"),   # Login required for SNF contacts
        ("Florida",    "ALF",  "3"),   # ALF source has empty contacts
        ("Texas",      "SNF",  "3"),   # No contacts in any public directory
        ("Texas",      "ALF",  "3"),
        ("TX",         "SNF",  "3"),   # Abbreviation resolved
    ])
    def test_tier_classification(self, state, ftype, expected_tier):
        tier, _, _ = m._lookup_guide_sources(state, ftype)
        assert tier == expected_tier, f"{state} {ftype}: expected tier {expected_tier}, got {tier}"

    def test_returns_fallback_sources(self, guide_file):
        _, _, fbs = m._lookup_guide_sources("Washington", "SNF")
        assert len(fbs) > 0

    def test_unknown_state_returns_tier_3(self):
        tier, srcs, _ = m._lookup_guide_sources("Narnia", "SNF")
        assert tier == "3"
        assert srcs == []

    def test_empty_state_returns_tier_3(self):
        tier, srcs, _ = m._lookup_guide_sources("", "SNF")
        assert tier == "3"

    def test_empty_facility_type_uses_all_sources(self):
        # With no type filter, Washington (SNF/ALF combined) should still resolve to Tier 1
        tier, srcs, _ = m._lookup_guide_sources("Washington", "")
        assert tier == "1"
        assert len(srcs) > 0

    @pytest.mark.parametrize("ccrc_type", ["CCRC", "ILF"])
    def test_ccrc_ilf_treated_as_alf(self, ccrc_type):
        # CCRC/ILF map to ALF/IL for matching; Washington SNF/ALF source should still match
        tier, srcs, _ = m._lookup_guide_sources("Washington", ccrc_type)
        assert tier == "1"

    def test_guide_missing_falls_back_to_tier_3(self):
        # Simulate the guide loader returning nothing (file missing / parse error)
        with patch.object(m, "_load_association_guide", return_value=([], [])):
            tier, srcs, fbs = m._lookup_guide_sources("Washington", "SNF")
        assert tier == "3"
        assert srcs == []
        assert fbs == []


# ── _format_sources_for_prompt ────────────────────────────────────────────────

class TestFormatSourcesForPrompt:

    @pytest.fixture
    def wa_tier1_data(self, guide_file):
        with patch.object(m, "_GUIDE_PATH", str(guide_file)):
            tier, srcs, fbs = m._lookup_guide_sources("Washington", "SNF")
        return tier, srcs, fbs

    @pytest.fixture
    def fl_tier2_data(self, guide_file):
        with patch.object(m, "_GUIDE_PATH", str(guide_file)):
            tier, srcs, fbs = m._lookup_guide_sources("Florida", "SNF")
        return tier, srcs, fbs

    @pytest.fixture
    def tx_tier3_data(self, guide_file):
        with patch.object(m, "_GUIDE_PATH", str(guide_file)):
            tier, srcs, fbs = m._lookup_guide_sources("Texas", "SNF")
        return tier, srcs, fbs

    # Tier 1 assertions
    def test_tier1_contains_association_name(self, wa_tier1_data):
        tier, srcs, fbs = wa_tier1_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Washington",
                                            facility_name="Brookdale Silver Lake", city="Everett")
        assert "Washington Health Care Association" in text

    def test_tier1_contains_directory_url(self, wa_tier1_data):
        tier, srcs, fbs = wa_tier1_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Washington",
                                            facility_name="Brookdale Silver Lake", city="Everett")
        assert "https://www.whca.org/facility-finder/" in text

    def test_tier1_contains_url_pattern_hint(self, wa_tier1_data):
        tier, srcs, fbs = wa_tier1_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Washington",
                                            facility_name="Brookdale Silver Lake", city="Everett")
        assert "{slug}" in text

    def test_tier1_contains_contacts_available(self, wa_tier1_data):
        tier, srcs, fbs = wa_tier1_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Washington",
                                            facility_name="Brookdale Silver Lake", city="Everett")
        assert "executive_director_name" in text

    def test_tier1_includes_cms_as_secondary(self, wa_tier1_data):
        tier, srcs, fbs = wa_tier1_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Washington",
                                            facility_name="Brookdale Silver Lake", city="Everett")
        assert "care-compare" in text

    # Tier 2 assertions
    def test_tier2_notes_login_barrier(self, fl_tier2_data):
        tier, srcs, fbs = fl_tier2_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Florida",
                                            facility_name="Sunrise of Naples", city="Naples")
        assert "login" in text.lower() or "Login" in text

    def test_tier2_includes_cms(self, fl_tier2_data):
        tier, srcs, fbs = fl_tier2_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Florida",
                                            facility_name="Sunrise of Naples", city="Naples")
        assert "care-compare" in text

    def test_tier2_does_not_list_association_url_as_direct_source(self, fl_tier2_data):
        tier, srcs, fbs = fl_tier2_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Florida",
                                            facility_name="Sunrise of Naples", city="Naples")
        # Should mention the login barrier, NOT present it as a direct-access URL to visit
        assert "fhcadirectory.org" not in text.replace("login", "")  # URL may appear in note, not as instruction

    # Tier 3 assertions
    def test_tier3_leads_with_cms(self, tx_tier3_data):
        tier, srcs, fbs = tx_tier3_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Texas",
                                            facility_name="Sunrise of Dallas", city="Dallas")
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        assert "care-compare" in lines[0].lower()

    def test_tier3_includes_state_doh_hint(self, tx_tier3_data):
        tier, srcs, fbs = tx_tier3_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Texas",
                                            facility_name="Sunrise of Dallas", city="Dallas")
        assert "Texas" in text
        assert "licensing" in text.lower() or "department" in text.lower()

    def test_tier3_includes_operator_website_hint(self, tx_tier3_data):
        tier, srcs, fbs = tx_tier3_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Texas",
                                            facility_name="Sunrise of Dallas", city="Dallas")
        assert "operator" in text.lower() or "corporate" in text.lower()

    def test_tier3_does_not_mention_thca(self, tx_tier3_data):
        """Association name should NOT appear as a recommended source in Tier 3."""
        tier, srcs, fbs = tx_tier3_data
        text = m._format_sources_for_prompt(tier, srcs, fbs, state="Texas",
                                            facility_name="Sunrise of Dallas", city="Dallas")
        # "Texas Health Care Association" should not be suggested as a source to check
        assert "Texas Health Care Association" not in text
        assert "txhca.org" not in text


# ── _build_facility_research_prompt ──────────────────────────────────────────

class TestBuildFacilityResearchPrompt:

    @pytest.fixture(autouse=True)
    def use_real_guide(self):
        """Use the real guide file for integration-style prompt tests."""
        yield

    def test_returns_tuple_of_prompt_and_tier(self):
        result = m._build_facility_research_prompt(
            "Sunrise of Dallas", "Dallas", "TX", "SNF", "Sunrise Senior Living"
        )
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_prompt_is_string(self):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Sunrise of Dallas", "Dallas", "TX", "SNF", "Sunrise Senior Living"
        )
        assert isinstance(prompt, str)
        assert len(prompt) > 100

    def test_tier_is_valid_value(self):
        for state, ftype in [("WA", "SNF"), ("TX", "SNF"), ("FL", "SNF")]:
            _, tier, _domain = m._build_facility_research_prompt(
                "Test Facility", "Test City", state, ftype, "Test Corp"
            )
            assert tier in ("1", "2", "3")

    def test_wa_snf_yields_tier_1(self):
        _, tier, _domain = m._build_facility_research_prompt(
            "Brookdale Silver Lake", "Everett", "WA", "SNF", "Brookdale"
        )
        assert tier == "1"

    def test_wa_snf_prompt_contains_whca_url(self):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Brookdale Silver Lake", "Everett", "WA", "SNF", "Brookdale"
        )
        assert "whca.org" in prompt

    def test_wa_snf_prompt_contains_url_pattern(self):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Brookdale Silver Lake", "Everett", "WA", "SNF", "Brookdale"
        )
        assert "{slug}" in prompt

    def test_tx_snf_yields_tier_3(self):
        _, tier, _domain = m._build_facility_research_prompt(
            "Sunrise of Dallas", "Dallas", "TX", "SNF", "Sunrise Senior Living"
        )
        assert tier == "3"

    def test_fl_snf_yields_tier_2(self):
        _, tier, _domain = m._build_facility_research_prompt(
            "Sunrise of Naples", "Naples", "FL", "SNF", "Sunrise Senior Living"
        )
        assert tier == "2"

    def test_prompt_preserves_facility_details(self):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Brookdale Silver Lake", "Everett", "WA", "SNF", "Brookdale"
        )
        assert "Brookdale Silver Lake" in prompt
        assert "Everett" in prompt
        assert "Brookdale" in prompt

    def test_prompt_contains_json_output_block(self):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Test Facility", "Test City", "TX", "SNF", "Test Corp"
        )
        assert "found_name" in prompt
        assert "found_title" in prompt
        assert "confidence" in prompt

    def test_prompt_contains_report_back_section(self):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Test Facility", "Test City", "TX", "SNF", "Test Corp"
        )
        assert "For each source you check" in prompt

    @pytest.mark.parametrize("ftype,expected_desc", [
        ("SNF",  "skilled nursing facility"),
        ("ALF",  "assisted living facility"),
        ("ILF",  "independent living facility"),
        ("CCRC", "continuing care retirement community"),
    ])
    def test_facility_type_description(self, ftype, expected_desc):
        prompt, _, _domain = m._build_facility_research_prompt(
            "Test Facility", "City", "TX", ftype, "Corp"
        )
        assert expected_desc in prompt


# ── _build_contact_research_prompt ────────────────────────────────────────────

class TestBuildContactResearchPrompt:

    def test_returns_string(self):
        result = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "WA", "SNF"
        )
        assert isinstance(result, str)

    def test_tier1_state_includes_specific_url(self):
        # Washington is Tier 1 — WHCA URL should appear in the source block
        prompt = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "WA", "SNF"
        )
        assert "whca.org" in prompt

    def test_tier3_state_uses_generic_language(self):
        # Texas is Tier 3 — should use generic fallback language
        prompt = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "TX", "SNF"
        )
        assert "health care association" in prompt.lower()

    def test_prompt_contains_contact_name(self):
        prompt = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "TX", "SNF"
        )
        assert "Jane Smith" in prompt

    def test_prompt_contains_report_back_section(self):
        prompt = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "TX", "SNF"
        )
        assert "For each source you check" in prompt

    def test_prompt_contains_cms_fallback(self):
        prompt = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "TX", "SNF"
        )
        assert "care-compare" in prompt or "medicare.gov" in prompt

    def test_empty_state_does_not_raise(self):
        prompt = m._build_contact_research_prompt(
            "Jane Smith", "Administrator", "Brookdale", "", "SNF"
        )
        assert isinstance(prompt, str)
        assert "Jane Smith" in prompt
