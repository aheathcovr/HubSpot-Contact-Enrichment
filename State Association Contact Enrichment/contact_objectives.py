"""
The three things a rep can ask of one contact from the HubSpot app: Find email, Find mobile, Verify email.

Each reuses what a company run already does: FullEnrich and Seamless to look, MillionVerifier (and BounceBan, behind
it) to check, and the identity gate to hold an address that is not at the facility's or its operator's domain. The
rules are the company runs' rules:

  * fill blanks only: Find email is not offered to a contact with an email, Find mobile to one with a mobile, and the
    contact is read again right before a write so a value added in the meantime is never replaced;
  * never delete, and no invalid email is written;
  * a mobile is taken only if Seamless says it is one (and Seamless's own screening applies: not do-not-call, not a
    facility switchboard, North American, confident enough);
  * nothing is used for a person Seamless shows at another organisation;
  * the only things written are email, mobilephone, email_verification_result and email_verification_date, plus a note
    on the contact saying who asked and what happened. A run does NOT set most_recent_enrichment_date: that stamp
    belongs to company runs and gates them.

The outcome returned for a run carries a short message with no email address or phone number in it, because the run
record that stores it is kept to ids and statuses. The values go in the note on the HubSpot contact.
"""

import html
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from email_verifier import verify_email_millionverifier
from fullenrich_client import enrich_contact_info
from hubspot_client import HubSpotError
from identity_gate import bare_domain
from seamless_client import lookup_person as seamless_lookup

logger = logging.getLogger(__name__)

FACILITY_TYPE = "Individual Facility / Child"
# Companies read to find the one a contact is checked against. A contact is linked to one or two.
_MAX_COMPANIES = 5

# id, label, what it spends. The order is the order the card shows them in.
OBJECTIVES = (
    ("find_email", "Find email", "up to 2 lookups and 1 check"),
    ("find_mobile", "Find mobile", "1 lookup"),
    ("verify_email", "Verify email", "1 check"),
)
_LABELS = {oid: label for oid, label, _ in OBJECTIVES}

_NEEDS_A_NAME = "Needs a first and last name."


@dataclass
class Outcome:
    """How a run ended. status is done or failed; outcome is the word the card shows; wrote names the properties."""
    status: str
    outcome: str
    message: str
    wrote: list = field(default_factory=list)


@dataclass
class _Company:
    name: str
    domain: str
    operator: str


# ── What is open to a rep ─────────────────────────────────────────────────────

def _has_name(contact: dict) -> bool:
    return bool(contact.get("firstname") and contact.get("lastname"))


def _availability(objective: str, contact: dict) -> Optional[str]:
    """None when the objective is open to this contact, else the reason it is not."""
    if objective == "find_email":
        if contact.get("email"):
            return "Already has an email. Use Verify email."
        return None if _has_name(contact) else _NEEDS_A_NAME
    if objective == "find_mobile":
        if contact.get("mobilephone"):
            return "Already has a mobile number."
        return None if _has_name(contact) else _NEEDS_A_NAME
    return None if contact.get("email") else "No email to check."


def objectives_for(contact: dict) -> list[dict]:
    """The three objectives as the card shows them, each open or closed with the reason."""
    out = []
    for oid, label, spends in OBJECTIVES:
        reason = _availability(oid, contact)
        out.append({"id": oid, "label": label, "available": reason is None, "reason": reason, "spends": spends})
    return out


def _day(raw: str) -> Optional[str]:
    text = (raw or "").strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", text)
    return m.group(1) if m else None


def contact_summary(contact: dict) -> dict:
    """What the card shows about the contact."""
    name = " ".join(p for p in (contact.get("firstname"), contact.get("lastname")) if p) or contact.get("email") or "No name"
    return {
        "name": name,
        "title": contact.get("jobtitle") or None,
        "email": contact.get("email") or None,
        "mobile": contact.get("mobilephone") or None,
        "emailCheck": contact.get("email_verification_result") or None,
        "emailCheckedOn": _day(contact.get("email_verification_date", "")),
    }


# ── Pieces shared by the runs ─────────────────────────────────────────────────

def _belongs(email: str, domain: str, facility_name: str, operator_name: str) -> bool:
    # Imported when first needed: the processor is large, and the service starts without it.
    from enrichment_processor import _belongs_to_operator
    return _belongs_to_operator(email, domain, facility_name, operator_name)


def _verification_props(ev: dict) -> dict:
    from enrichment_processor import _verification_properties
    return _verification_properties(ev)


_CHECK_WORDS = {
    "ok": "good",
    "catch_all": "risky (the server accepts every address)",
    "unknown": "unknown (the server would not say)",
    "invalid": "invalid",
    "disposable": "a throwaway address",
}


def _phrase(result: str) -> str:
    return _CHECK_WORDS.get(result, result or "unchecked")


def _esc(text) -> str:
    return html.escape(str(text), quote=False)


def _company(hs, contact_id: str) -> Optional[_Company]:
    """The company a contact is checked against: a facility if it is linked to one, else the first it is linked to."""
    ids = list(hs.get_contact_company_ids(contact_id))[:_MAX_COMPANIES]
    if not ids:
        return None
    found = [(cid, hs.get_company_properties(cid)) for cid in ids]
    cid, props = next(((c, p) for c, p in found if p.get("hierarchy_type") == FACILITY_TYPE), found[0])
    name = (props.get("name") or "").strip()
    operator = name
    parent = (props.get("hs_parent_company_id") or "").strip()
    if props.get("hierarchy_type") == FACILITY_TYPE:
        operator = ""
        if parent:
            try:
                operator = (hs.get_company_properties(parent).get("name") or "").strip()
            except HubSpotError:
                operator = ""
    return _Company(name=name, domain=bare_domain(props.get("website") or ""), operator=operator)


def _write_blank(hs, contact_id: str, blank_field: str, props: dict, refused: str) -> Optional[Outcome]:
    """
    Save props on the contact, but only if *blank_field* is still empty: the contact is read again first, so a value
    someone added while the run was working is never replaced. None when it was saved; otherwise the outcome to give.
    """
    if hs.get_contact_for_app(contact_id).get(blank_field):
        return Outcome("done", "skipped", "Someone added one while this ran, so nothing was replaced.")
    try:
        hs.update_contact(contact_id, props)
    except HubSpotError as exc:
        logger.warning("HubSpot would not save %s on contact %s: %s", list(props), contact_id, exc)
        return Outcome("done", "not_written", refused)
    return None


def _note(hs, contact_id: str, objective: str, requested_by: str, lines: list[str]) -> None:
    body = f"<b>HubSpot app · {_LABELS[objective]}</b> (asked by {_esc(requested_by)})<br>" + "".join(lines)
    try:
        hs.create_note_on_contact(contact_id, body)
    except HubSpotError as exc:
        logger.warning("Could not save the note for the %s run on contact %s: %s", objective, contact_id, exc)


def _needs(contact: dict, hs, contact_id: str) -> "tuple[Optional[Outcome], Optional[_Company]]":
    """For a run that looks someone up: a skip if the contact cannot be looked up, else the company to check against."""
    if not _has_name(contact):
        return Outcome("done", "skipped", _NEEDS_A_NAME), None
    company = _company(hs, contact_id)
    if company is None:
        return Outcome("done", "skipped", "This contact is not linked to a company, so there is nobody to check an answer against."), None
    return None, company


# ── The runs ──────────────────────────────────────────────────────────────────

def _find_email(hs, contact_id: str, contact: dict, requested_by: str) -> Outcome:
    if contact.get("email"):
        return Outcome("done", "skipped", "Already has an email. Nothing was looked up.")
    skip, company = _needs(contact, hs, contact_id)
    if skip:
        return skip
    first, last = contact["firstname"], contact["lastname"]
    lines: list[str] = []
    held = invalid = moved = False

    def consider(source: str, email: str) -> Optional[Outcome]:
        """An address a vendor found: held if off-domain, dropped if invalid, otherwise written."""
        nonlocal held, invalid
        if not email:
            return None
        if not _belongs(email, company.domain, company.name, company.operator):
            held = True
            lines.append(f"<b>Held, not added:</b> {source} found {_esc(email)}, which is not at this facility's or its operator's domain.<br>")
            return None
        ev = verify_email_millionverifier(email)
        if ev.get("status") == "INVALID":
            invalid = True
            lines.append(f"<b>Not added:</b> {source} found {_esc(email)} but MillionVerifier says it is invalid.<br>")
            return None
        props = {"email": email, **_verification_props(ev)}
        refused = _write_blank(hs, contact_id, "email", props, "HubSpot would not save it. Another contact may already have this address.")
        if refused is not None:
            lines.append(f"<b>Not added:</b> {source} found {_esc(email)} but HubSpot would not save it.<br>")
            return refused
        lines.append(f"<b>Email added:</b> {_esc(email)} (from {source}; MillionVerifier says {_esc(_phrase(ev.get('result', '')))}).<br>")
        checked = f"MillionVerifier says it is {_phrase(ev.get('result', ''))}." if props.keys() - {"email"} else "It could not be checked."
        return Outcome("done", "found", f"Email added. {checked}", list(props))

    fe = enrich_contact_info(first, last, company_name=company.name, domain=company.domain, linkedin_url=contact.get("hs_linkedin_url", "")) or {}
    result = consider("FullEnrich", (fe.get("email") or "").strip())
    if result is None:
        sl = seamless_lookup(first, last, domain=company.domain, company=company.name, operator_name=company.operator, title=contact.get("jobtitle", ""))
        if sl and sl.get("moved"):
            moved = True
            lines.append(f"<b>Seamless:</b> shows this person at {_esc(sl.get('company') or 'another organization')}. Nothing was used.<br>")
        elif sl:
            result = consider("Seamless", (sl.get("email") or "").strip())
    if result is None:
        if held:
            result = Outcome("done", "held", "Found an email at a different domain (not the facility's or its operator's), so it was not added. The note on this contact has it.")
        elif invalid:
            result = Outcome("done", "not_written", "Found an email, but it checked as invalid, so it was not added.")
        elif moved:
            result = Outcome("done", "moved", "Seamless shows this person at another organization, so nothing was used. The note on this contact says where.")
        else:
            lines.append("<b>No email found.</b> FullEnrich and Seamless had nothing usable.<br>")
            result = Outcome("done", "not_found", "No email found. FullEnrich and Seamless had nothing usable.")
    if result.outcome != "skipped":
        _note(hs, contact_id, "find_email", requested_by, lines)
    return result


def _find_mobile(hs, contact_id: str, contact: dict, requested_by: str) -> Outcome:
    if contact.get("mobilephone"):
        return Outcome("done", "skipped", "Already has a mobile number. Nothing was looked up.")
    skip, company = _needs(contact, hs, contact_id)
    if skip:
        return skip
    sl = seamless_lookup(contact["firstname"], contact["lastname"], domain=company.domain, company=company.name,
                         operator_name=company.operator, title=contact.get("jobtitle", ""))
    lines: list[str] = []
    if not sl:
        lines.append("<b>No number.</b> Seamless could not be asked, or had nothing for this person.<br>")
        result = Outcome("done", "not_found", "Seamless could not be asked, or had nothing for this person.")
    elif sl.get("moved"):
        lines.append(f"<b>Seamless:</b> shows this person at {_esc(sl.get('company') or 'another organization')}. Nothing was used.<br>")
        result = Outcome("done", "moved", "Seamless shows this person at another organization, so nothing was used. The note on this contact says where.")
    elif sl.get("phone"):
        kind = (sl.get("phone_type") or "").strip().lower()
        if kind == "mobile":
            refused = _write_blank(hs, contact_id, "mobilephone", {"mobilephone": sl["phone"]}, "HubSpot would not save the number.")
            if refused is not None:
                lines.append(f"<b>Not added:</b> Seamless found {_esc(sl['phone'])} but HubSpot would not save it.<br>")
                result = refused
            else:
                lines.append(f"<b>Mobile added:</b> {_esc(sl['phone'])} (from Seamless, confidence {_esc(sl.get('phone_confidence') or 'not given')}).<br>")
                result = Outcome("done", "found", "Mobile number added.", ["mobilephone"])
        else:
            lines.append(f"<b>Not added:</b> Seamless found {_esc(sl['phone'])}, a {_esc(kind or 'direct')} line, not a mobile.<br>")
            result = Outcome("done", "not_written", f"Seamless found a {kind or 'direct'} line, not a mobile, so nothing was added. The note on this contact has it.")
    elif sl.get("phone_rejected") and sl["phone_rejected"] != "no phone returned":
        lines.append(f"<b>No usable number:</b> {_esc(sl['phone_rejected'])}.<br>")
        result = Outcome("done", "not_found", f"Seamless had a number but it was not usable: {sl['phone_rejected']}.")
    else:
        lines.append("<b>No number.</b> Seamless had none for this person.<br>")
        result = Outcome("done", "not_found", "Seamless had no number for this person.")
    if result.outcome != "skipped":
        _note(hs, contact_id, "find_mobile", requested_by, lines)
    return result


def _verify_email(hs, contact_id: str, contact: dict, requested_by: str) -> Outcome:
    email = contact.get("email", "")
    if not email:
        return Outcome("done", "skipped", "No email to check.")
    ev = verify_email_millionverifier(email)
    props = _verification_props(ev)
    if not props:
        return Outcome("failed", "error", "The email could not be checked right now. Try again later.")
    try:
        hs.update_contact(contact_id, props)
    except HubSpotError as exc:
        logger.warning("HubSpot would not save the check on contact %s: %s", contact_id, exc)
        return Outcome("failed", "error", "The email was checked, but HubSpot would not save the answer. Try again later.")
    phrase = _phrase(ev.get("result", ""))
    _note(hs, contact_id, "verify_email", requested_by, [f"<b>Checked:</b> {_esc(email)} is {_esc(phrase)} (MillionVerifier: {_esc(ev.get('result', ''))}).<br>"])
    return Outcome("done", "verified", f"Checked: the email is {phrase}.", list(props))


_RUNS = {"find_email": _find_email, "find_mobile": _find_mobile, "verify_email": _verify_email}


def run_objective(objective: str, contact_id: str, *, hs, requested_by: str) -> Outcome:
    """
    Do one objective for one contact and say how it went. Never raises for anything that goes wrong while working:
    that ends the run as failed with a plain message and changes nothing. An objective nobody defined is a bug, and
    raises ValueError.
    """
    if objective not in _RUNS:
        raise ValueError(f"Unknown objective {objective!r}")
    try:
        contact = hs.get_contact_for_app(contact_id)
        return _RUNS[objective](hs, contact_id, contact, requested_by)
    except Exception:
        logger.exception("The %s run for contact %s failed", objective, contact_id)
        return Outcome("failed", "error", "Something went wrong. Nothing was changed. Try again.")
