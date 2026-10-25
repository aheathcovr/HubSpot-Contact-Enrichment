"""
Is an address a vendor returned actually at the organisation we asked about?

Ported from the outbound repo's covr_utils/enrichment/policy.py, where these rules
were built from measured failures. Pure functions: no network, no clock.

MillionVerifier answers "is this mailbox real". It cannot answer "is this the
right person", and the two come apart badly. A vendor that matches company NAME
loosely returns a real, deliverable mailbox at a different company, and
verification passes it (15 of 29 "sendable" Seamless contacts on the outbound
repo's 5-campus sweep were strangers this way). FullEnrich returns the person's
CURRENT employer's address, which is a stranger's whenever they have since moved.
The domain is the only identity check available before the address is written.

Unknown is never a match: an empty domain waving an address through is exactly how
this class of error reaches a mailbox. Anything rejected should be HELD for a human,
never dropped, because an operator mailing from a parent's domain looks identical to
a stranger.
"""

import re


def _domain_of(email: object) -> str:
    text = "" if email is None else str(email)
    return text.split("@")[-1].strip().lower() if "@" in text else ""


_HOST_RE = re.compile(r"[a-z0-9.-]+")


def bare_domain(value: object) -> str:
    """
    The host name in a website or domain value, or "" when it is not one.

    HubSpot stores a company's `website` as a URL, and that is what the pipeline passes
    around as the "domain": "https://www.valleyview.org/about?x=1" is "valleyview.org".
    Scheme, user info, port, path, query, fragment, `www.` and case are all dropped. A
    value with no dotted host name in it ("https://", "N/A", "localhost") is "", so a
    caller can fall back to something else instead of sending garbage on.
    """
    text = "" if value is None else str(value).strip().lower()
    text = re.sub(r"^[a-z][a-z0-9+.-]*://", "", text)
    text = re.split(r"[/?#]", text, maxsplit=1)[0]
    text = text.rsplit("@", 1)[-1].split(":", 1)[0]
    text = text.removeprefix("www.").strip(".")
    return text if "." in text and _HOST_RE.fullmatch(text) else ""


def same_org_domain(email: object, domain: object) -> bool:
    """
    Is this address at the organisation's domain (the strict test)?

    Same organisation means the same host, or one a subdomain of the other. A
    shared PUBLIC suffix is not an organisation: two agencies under ms.gov are two
    different buyers, so `dor.ms.gov` and `msva.ms.gov` are not the same.
    """
    address = _domain_of(email)
    if not address:
        return False
    host = bare_domain(domain)
    if not host:
        return False
    return (address == host
            or address.endswith(f".{host}")
            or host.endswith(f".{address}"))


# Words that appear in almost every operator's name in this market. Matching on any
# of them would accept any address from any operator against any other.
_GENERIC_ORG_WORDS = frozenset({
    "health", "healthcare", "care", "senior", "seniors", "living", "community",
    "communities", "services", "service", "center", "centers", "centre", "group",
    "inc", "llc", "the", "of", "and", "management", "home", "homes", "nursing",
    "rehabilitation", "rehab", "village", "campus", "family", "system", "systems",
    "corporation", "company", "holdings", "partners", "associates", "enterprises",
})


def _distinctive_words(name: object) -> set[str]:
    """The words in an organisation's name that actually identify it."""
    cleaned = re.sub(r"\(.*?\)", " ", str(name or "").lower())
    words = re.sub(r"[^a-z0-9 ]", " ", cleaned).split()
    return {w for w in words if len(w) >= 3 and w not in _GENERIC_ORG_WORDS}


def same_org_or_affiliate(email: object, domain: object, org_name: object) -> bool:
    """
    Is this address at the organisation, allowing for its OWN other domain?

    The strict test is right when the organisation's website is also where it takes
    mail. Often it is not: an operator publishes at one host and mails from another
    (Warm Hearth Village's site is retire.org and its mail is warmhearth.org). So a
    second rung: the address host may instead carry a DISTINCTIVE word from the
    organisation's own name. Generic trade words are excluded, because "Senior
    Living" and "Health Care" appear in most names here and would match everything.

    This is a heuristic with a known false accept: "Trinity Healthcare Services"
    matches trinity-health.org, the national Catholic system. It narrows a review
    queue; it does not replace the review.
    """
    if same_org_domain(email, domain):
        return True
    host = _domain_of(email)
    if not host:
        return False
    # Public-sector and academic hosts are shared by thousands of unrelated
    # employers; a name fragment appearing in one means nothing.
    if host.endswith((".gov", ".mil", ".edu")):
        return False
    stem = re.sub(r"[^a-z0-9]", "", host.rsplit(".", 1)[0])
    return any(word in stem for word in _distinctive_words(org_name))


def names_share_a_distinctive_word(name: object, org_name: object) -> bool:
    """
    Do two organisation NAMES share a word that actually identifies one?

    The same idea as the address check, for when a vendor gives a company name rather
    than a domain. Generic trade words ("Health Care", "Senior Living") never count.
    """
    return bool(_distinctive_words(name) & _distinctive_words(org_name))
