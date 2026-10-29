"""
Find people by job title from the company card, then add the ones a rep chooses.

Two runs, both queued like the contact card's and done by the same worker (webhook_server._execute_app_run):

  find_by_title   FullEnrich people search for the rep's titles at this company. A preview: nothing is written to
                  HubSpot. The people found are kept for two days (app_run_store.save_candidates) so the rep can choose
                  among them; the run record keeps only a count in its message.
  add_candidate   One chosen person goes through the step a company run uses for a person it found,
                  enrichment_processor._process_found_contact: the identity gate on any vendor email, MillionVerifier,
                  an existing HubSpot contact reused only when contact_match_guard says it is safe, otherwise a new
                  contact, and where it is linked decided by the same title routing (a corporate title goes to the
                  operator). So the rules are the company runs' rules, not new ones.

Custom titles are searched as the rep wrote them. The built-in title lists keep their own seniority filters for a
corporation; any other title is searched without them, because they would silently drop a manager the rep asked for.
Messages carry no names, emails or phone numbers, like every run message.
"""

import hashlib
import html
import logging
import re
from typing import Optional

import config
from contact_objectives import Outcome, company_kind
import run_trail
from fullenrich_client import search_facility_contacts, search_unavailable_reason
from hubspot_client import HubSpotError

logger = logging.getLogger(__name__)

MAX_TITLES = 5
MAX_TITLE_LENGTH = 80
# Contacts of the company read to mark a person who is already one. A company has a handful; this is a safety limit.
_LINKED_READ = 100
_NOTHING_CHANGED = "Something went wrong. Nothing was changed. Try again."
SEARCH_GONE = "Those search results are no longer available. Search again."
NEEDS_COMPANY_TYPE = "Set this company's Company Type to Facility or Corporation first, then search again."

FIND_OBJECTIVE = "find_by_title"
ADD_OBJECTIVE = "add_candidate"


# ── The titles ────────────────────────────────────────────────────────────────

def clean_titles(raw) -> "tuple[list[str], Optional[str]]":
    """The titles a rep sent as (titles, None), or ([], what is wrong). Trimmed, squeezed, no repeats, in order."""
    titles, seen = [], set()
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, str) or any((ord(c) < 32 and not c.isspace()) or c == "\x7f" for c in item):
            return [], "Job titles must be plain text."
        text = re.sub(r"\s+", " ", item).strip()
        if not text:
            continue
        if len(text) > MAX_TITLE_LENGTH:
            return [], f"A job title can be at most {MAX_TITLE_LENGTH} characters."
        if text.lower() not in seen:
            seen.add(text.lower())
            titles.append(text)
    if not titles:
        return [], "Choose at least one job title."
    if len(titles) > MAX_TITLES:
        return [], f"Choose at most {MAX_TITLES} job titles."
    return titles, None


def presets_for(kind: str) -> list[str]:
    """The titles the company runs look for, as quick picks: the corporate list on a corporation, else the facility's."""
    return list(config.W2_CORPORATE_TITLES if kind == "corporation" else config.FACILITY_LEADERSHIP_TITLES)


def titles_key(titles: list[str]) -> str:
    """The same titles in any order or case are the same search."""
    return "|".join(sorted(t.lower() for t in titles))


def candidate_id(person: dict) -> str:
    """A short stable id for a person a search found, so a rep's choice names the person, not a position in a list."""
    basis = f"{(person.get('full_name') or '').strip().lower()}|{(person.get('linkedin_url') or '').strip().lower()}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]


# ── Finding people ────────────────────────────────────────────────────────────

def _same_person(found_name: str, firstname: str, lastname: str) -> bool:
    parts = (found_name or "").strip().split()
    if not parts:
        return False
    return parts[0].lower() == (firstname or "").strip().lower() and " ".join(parts[1:]).lower() == (lastname or "").strip().lower()


def find_candidates(hs, company_id: str, props: dict, kind: str, titles: list[str]) -> list[dict]:
    """The people FullEnrich has holding any of *titles* at the company, each marked with the contact it already is here."""
    name, website = props.get("name") or "", props.get("website") or ""
    if kind == "corporation":
        # A corporation's own title list keeps the seniority filters (and no location guard, as in a company run); any
        # other title is searched plainly. City and state are left out: an operator's people are not at one address.
        own_titles = {t.lower() for t in config.W2_CORPORATE_TITLES}
        own = [t for t in titles if t.lower() in own_titles]
        other = [t for t in titles if t.lower() not in own_titles]
        base = {"company_name": name, "city": "", "state": "", "domain": website}
        searches = ([{**base, "titles": own, "corporate_mode": True}] if own else []) + ([{**base, "titles": other, "corporate_mode": False}] if other else [])
    else:
        searches = [{"company_name": name, "city": props.get("city") or "", "state": props.get("state") or "", "titles": titles, "domain": website}]

    people, seen = [], set()
    for search in searches:
        for person in search_facility_contacts(**search):
            key = (person.get("full_name") or "").strip().lower()
            if key and key not in seen:
                seen.add(key)
                people.append(person)
    if not people:
        return []

    here = hs.get_associated_contacts(company_id, limit=_LINKED_READ)
    found = []
    for person in people:
        linked = next((c["id"] for c in here if _same_person(person["full_name"], c.get("firstname"), c.get("lastname"))), "")
        found.append({
            "candidate_id": candidate_id(person),
            "full_name": person["full_name"],
            "title": person.get("title") or "",
            "linkedin_url": person.get("linkedin_url") or "",
            "updated_at": person.get("updated_at") or person.get("current_start") or "",
            "linked_contact_id": linked,
        })
    return found


def run_find_by_title(run: dict, *, hs, store, now) -> Outcome:
    """Search for the run's titles at its company and keep what is found for the rep to choose from. Never raises."""
    trouble = 0
    try:
        reason = search_unavailable_reason()
        if reason:
            return Outcome("failed", "error", reason)
        company_id = run["company_id"]
        props = hs.get_company_properties(company_id)
        titles = run.get("titles") or []
        run_trail.note("service", f"Searching FullEnrich for {len(titles)} title{'' if len(titles) == 1 else 's'} at {props.get('name') or 'the company'}: {', '.join(titles)}.")
        found = find_candidates(hs, company_id, props, _kind(props), titles)
        trouble = run_trail.failures("fullenrich")
        if not found and trouble:
            # A search that could not be done is not a finding that no one is there, and nothing is kept for the rep to choose from.
            return Outcome("failed", "error", "FullEnrich could not be searched, so this is not a finding that no one is there: see What happened. Try again later.")
        store.save_candidates(run["run_id"], company_id, found, now)
    except Exception:
        logger.exception("The title search %s failed", run.get("run_id"))
        return Outcome("failed", "error", _NOTHING_CHANGED)
    if not found:
        return Outcome("done", "not_found", "No one found for these titles at this company.")
    count = f"{len(found)} person" if len(found) == 1 else f"{len(found)} people"
    partial = " Part of the search could not be done: see What happened." if trouble else ""
    return Outcome("done", "found", f"Found {count}. Choose who to add.{partial}")


def _kind(props: dict) -> str:
    return company_kind(props.get("hierarchy_type", ""))


# ── Adding a person ───────────────────────────────────────────────────────────

def _name_of(hs, company_id: str) -> str:
    try:
        return (hs.get_company_properties(company_id).get("name") or "").strip() or "the company"
    except HubSpotError:
        return "the company"


def _note(hs, contact_id: str, requested_by: str, titles: list[str]) -> None:
    searched = f"Found in a FullEnrich search for: {html.escape(', '.join(titles), quote=False)}.<br>" if titles else ""
    body = f"<b>HubSpot app · Add by job title</b> (asked by {html.escape(requested_by, quote=False)})<br>{searched}"
    try:
        hs.create_note_on_contact(contact_id, body)
    except HubSpotError as exc:
        logger.warning("Could not save the note for the add on contact %s: %s", contact_id, exc)


def run_add_candidate(run: dict, *, hs, store) -> Outcome:
    """Add one person a title search found, the way a company run would add a person it found. Never raises."""
    try:
        kept = store.get_candidates(run["source_run_id"])
        person = None
        if kept and kept.get("company_id") == run["company_id"]:
            person = next((c for c in kept.get("candidates", []) if c.get("candidate_id") == run["candidate_id"]), None)
        if person is None:
            return Outcome("failed", "error", SEARCH_GONE)

        from enrichment_processor import _association_target, _process_found_contact

        company_id = run["company_id"]
        props = hs.get_company_properties(company_id)
        name = props.get("name") or ""
        if _kind(props) == "corporation":
            parent_id, operator, target = "", name, company_id
        else:
            parent_id = (props.get("hs_parent_company_id") or "").strip()
            operator = _name_of(hs, parent_id) if parent_id else name
            target = _association_target(person["title"], company_id, parent_id or company_id)
        action = _process_found_contact(
            hs=hs, found_name=person["full_name"], found_title=person["title"], found_email="", found_phone="",
            confidence="low_fullenrich", pre_populated_contact_ids="", target_company_id=target, facility_name=name,
            research_findings="", source_tier="", found_linkedin=person.get("linkedin_url", ""),
            facility_domain=props.get("website") or "", source_path="fullenrich_people_search", fe_people_search_ran=False,
            fe_updated_at=person.get("updated_at", ""), operator_name=operator,
            related_company_ids=tuple(x for x in (company_id, parent_id) if x),
        )
        outcome = _outcome_of(action, _name_of(hs, target))
        run_trail.note("hubspot", outcome.message, {"found": "ok", "not_found": "empty"}.get(outcome.outcome, "failed" if outcome.status == "failed" else "info"))
        if outcome.contact_id:
            started = store.get_run(run["source_run_id"]) or {}
            requested_by = run.get("requested_by") or f"HubSpot user {run.get('requested_by_id', '')}"
            _note(hs, outcome.contact_id, requested_by, started.get("titles") or [])
        return outcome
    except Exception:
        logger.exception("The add %s failed", run.get("run_id"))
        return Outcome("failed", "error", _NOTHING_CHANGED)


def _outcome_of(action, target_name: str) -> Outcome:
    emailed = " An email was added." if action.found_email and action.email_validation_status != "INVALID" else ""
    if action.action == "created_new":
        return Outcome("done", "found", f"Created a new contact and linked it to {target_name}.{emailed}", ["contact"], contact_id=action.contact_id)
    if action.action == "associated_existing":
        return Outcome("done", "found", f"Linked an existing contact to {target_name}.{emailed}", ["contact"], contact_id=action.contact_id)
    if action.action == "no_contact_found":
        return Outcome("done", "not_found", "Nothing was added for this person.")
    return Outcome("failed", "error", "Something went wrong adding this person. Check HubSpot before trying again.")
