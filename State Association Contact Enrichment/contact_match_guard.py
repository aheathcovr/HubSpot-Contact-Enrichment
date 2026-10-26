"""
When may a person the run just found be treated as a contact HubSpot already has?

A name is not an identity. On 2026-10-25 a run took a HubSpot contact on the strength of a matching
name alone; it belonged to a customer's account at another operator, and the run linked it to a
different facility and changed its title and email. This module is the rule that stops that, with
no HubSpot in it so every case can be tested on its own:

  - the same email is the same person;
  - a contact already linked to this facility or its operator is reused;
  - a contact linked to no company is reused (nobody else has a claim on it);
  - a contact linked ONLY to other companies is reused only when two independent kinds of evidence
    place the person at this facility;
  - otherwise the run makes a separate contact and says why on its note. Two records can be merged
    later by a person; a wrong overwrite cannot easily be undone.
"""

from dataclasses import dataclass

_LABELS = {
    "licensing_roster": "the state licensing roster",
    "definitive": "Definitive Healthcare",
    "vendor_search": "a vendor's people search",
    "web_research": "web research",
    "email_at_facility_domain": "an email at the facility's or operator's domain",
    "existing_email_at_facility_domain": "the existing contact's own email at the facility's or operator's domain",
}

REQUIRED_KINDS = 2


@dataclass(frozen=True)
class Decision:
    reuse: bool
    reason: str


def _kind_of_source(source_path: str) -> str:
    path = str(source_path or "")
    if path == "state_association_bq_table":
        return "licensing_roster"
    if path.startswith("definitive_healthcare"):
        return "definitive"
    if path in ("fullenrich", "fullenrich_people_search"):
        return "vendor_search"
    if path.startswith("sonar_pro"):
        return "web_research"
    return ""


def signals_for(
    *, source_path: str, dh_verified: bool, found_email_at_domain: bool, existing_email_at_domain: bool,
) -> set:
    """The distinct kinds of evidence that place the found person at this facility."""
    kinds = {_kind_of_source(source_path)}
    if dh_verified:
        kinds.add("definitive")
    if found_email_at_domain:
        kinds.add("email_at_facility_domain")
    if existing_email_at_domain:
        kinds.add("existing_email_at_facility_domain")
    kinds.discard("")
    return kinds


def describe(signals: set) -> str:
    return ", ".join(_LABELS.get(k, k) for k in sorted(signals)) or "nothing"


def decide(
    *, candidate_company_ids: set, related_company_ids: set, signals: set, same_email: bool = False,
) -> Decision:
    """Whether an existing contact may be used for the person just found."""
    if same_email:
        return Decision(True, "it has the same email address")
    candidate = {str(c) for c in candidate_company_ids}
    related = {str(r) for r in related_company_ids}
    if candidate & related:
        return Decision(True, "it is already linked to this facility or its operator")
    if not candidate:
        return Decision(True, "it is linked to no company")
    if len(signals) >= REQUIRED_KINDS:
        return Decision(True, f"{len(signals)} independent sources place the person here ({describe(signals)})")
    return Decision(
        False,
        f"it is linked only to other companies ({', '.join(sorted(candidate))}), and only {describe(signals)} "
        f"places the person at this facility; two independent sources are needed",
    )
