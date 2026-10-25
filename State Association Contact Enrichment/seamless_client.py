"""
Seamless: phone numbers, and an email when FullEnrich finds none.

Single public function: lookup_person(first, last, domain=, company=, ...) -> dict | None

Ported from the outbound repo's covr_utils/enrichment/seamless.py and policy.py, which
were built around measured behaviour:

  * A phone rides in the SAME research credit that returns the address (a 96%-confidence
    mobile for one credit); FullEnrich charges ten for a phone. So Seamless is the phone
    source. FullEnrich is asked for email only.
  * Its emails are weaker. When Seamless has no observed pattern it composes one from the
    corporate pattern: 74% of a sample came back invalid. So the email is a fallback behind
    FullEnrich, and the caller still holds it to the same identity and verification gates
    as every vendor address.
  * A person is researched BY NAME against an exact company domain
    (`contacts: [{contactName, domain}]`). A search by name is matched strictly and ANDed
    with everything else, so it returns nothing for people demonstrably in the index.
  * Research is two-phase and asynchronous: submit, then poll. The credit is spent at
    submit, so a poll that does not finish is never resubmitted.
  * The person that comes back must be the right person (surname) AT the right
    organisation. A loose company match returns real, verified people at strangers, and
    someone who has moved returns their NEW employer's phone and address. A person who
    is no longer at the organisation yields no data, and `moved` says where they are.

Phones are held to what an SDR can actually dial: not do-not-call, not the facility's
main line (it repeats across every contact there), a North American number, and above
the confidence floor when Seamless scores it.

Never raises. SEAMLESS_API_KEY unset means Seamless is skipped and the service runs the
same without it.
"""

import logging
import os
import threading
import time

import requests

from identity_gate import (
    names_share_a_distinctive_word,
    same_org_domain,
    same_org_or_affiliate,
)

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://api.seamless.ai/api/client/v1"
_BASE_URL = _DEFAULT_BASE_URL
_RESEARCH = "/contacts/research"
_POLL = "/contacts/research/poll"

_MIN_CONFIDENCE = 70.0            # below this a Seamless score is not trusted
_MIN_CREDITS = 25                 # below this nothing more is spent (the balance is a daily allowance)
_POLL_INTERVAL_S = 5.0
_POLL_TIMEOUT_S = 90.0
_RATE_LIMIT_RETRIES = 3
_MAX_RATE_LIMIT_WAIT_S = 30.0
_STOP_FOR_S = 600
_ANSWER_TTL_S = 6 * 3600
_WARNING_INTERVAL_S = 600
# Seamless limits every endpoint to 60 requests a minute for the WHOLE organisation, which
# the outbound pipelines share, so this process paces itself well inside that.
_MIN_REQUEST_INTERVAL_S = 1.5

_TERMINAL = frozenset({
    "failed", "error", "missing", "notfound", "not_found", "not found", "nodata", "no_data",
    "cancelled", "canceled", "invalid",
})
_OUT_OF_CREDITS = "no license or credits available"

_session = requests.Session()
_sleep = time.sleep               # replaced in tests so waits are recorded, not slept

_lock = threading.Lock()
_answers: dict = {}               # (first, last, where) -> (monotonic time, answer)
_stopped_until = 0.0
_credits = None                   # last balance seen on a response header
_last_pace = 0.0
_last_low_credits_warning = None


def reset_state() -> None:
    """Forget remembered answers, any stop and the balance. For tests."""
    global _stopped_until, _credits, _last_pace, _last_low_credits_warning
    with _lock:
        _answers.clear()
        _stopped_until = 0.0
        _credits = None
        _last_pace = 0.0
        _last_low_credits_warning = None


# ── Policy: pure functions, no network ────────────────────────────────────────

def _pct(value) -> "float | None":
    """`"98%"` -> 98.0. Scores arrive as strings with a percent sign."""
    text = str(value or "").strip().rstrip("%")
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _name_matches(first: str, last: str, contact: dict) -> bool:
    """Same person? Surname must match; the given name may be a diminutive (Jen / Jennifer)."""
    got_last = str(contact.get("lastName") or "").strip().lower()
    want_last = (last or "").strip().lower()
    if not got_last or not want_last or got_last != want_last:
        return False
    got_first = str(contact.get("firstName") or "").strip().lower()
    want_first = (first or "").strip().lower()
    if not got_first or not want_first:
        return True
    return got_first.startswith(want_first[:3]) or want_first.startswith(got_first[:3])


def _same_organisation(contact: dict, domain: str, company: str, operator_name: str):
    """True / False / None (cannot tell): is the person's CURRENT employer the facility or its operator?"""
    row_domain = str(contact.get("companyDomain") or contact.get("domain") or "").strip().lower()
    row_company = str(contact.get("companyName") or contact.get("company") or "").strip()
    if not row_domain and not row_company:
        return None
    if row_domain:
        probe = f"x@{row_domain}"
        if same_org_domain(probe, domain) or same_org_or_affiliate(probe, domain, company):
            return True
        if operator_name and same_org_or_affiliate(probe, "", operator_name):
            return True
    if row_company:
        if names_share_a_distinctive_word(row_company, company):
            return True
        if operator_name and names_share_a_distinctive_word(row_company, operator_name):
            return True
    return False


_NANP_LEN = 10


def _is_nanp(digits: str) -> bool:
    """A North American number: area code and exchange both begin 2-9."""
    return len(digits) == _NANP_LEN and digits[0] in "23456789" and digits[3] in "23456789"


def _phone_parts(value) -> tuple[str, str]:
    """
    (national number, extension). Seamless glues the extension on with no separator, so
    3307020110129 is 330-702-0110 x129. Read from the FRONT: the first ten digits are a
    valid North American number far more often than the last ten, and reading from the
    end turned 330-702-0110 x129 into a number that belongs to somebody else.
    """
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    while len(digits) > _NANP_LEN and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) < _NANP_LEN or not _is_nanp(digits[:_NANP_LEN]):
        return digits, ""
    return digits[:_NANP_LEN], digits[_NANP_LEN:]


def _format_phone(national: str, ext: str) -> str:
    pretty = f"{national[:3]}-{national[3:6]}-{national[6:]}"
    return f"{pretty} x{ext}" if ext else pretty


def _choose_phone(contact: dict) -> dict:
    """The phone an SDR can dial, or the reason there is not one."""
    raw = contact.get("contactPhone1")
    out = {"phone": "", "phone_type": "", "phone_confidence": None, "phone_rejected": ""}
    if not raw:
        out["phone_rejected"] = "no phone returned"
        return out
    kind = str(contact.get("contactPhone1DataType") or "").strip().lower()
    confidence = _pct(contact.get("contactPhone1TotalAI"))
    out["phone_type"], out["phone_confidence"] = kind, confidence
    if contact.get("isDncAll") or contact.get("isDncSome") or contact.get("contactPhone1IsDnc"):
        out["phone_rejected"] = "flagged do-not-call"
    elif kind == "main":
        out["phone_rejected"] = "the facility's main line (a switchboard, not a direct dial)"
    else:
        national, ext = _phone_parts(raw)
        if not _is_nanp(national):
            out["phone_rejected"] = "not a US number"
        elif confidence is not None and confidence < _MIN_CONFIDENCE:
            out["phone_rejected"] = f"confidence {confidence:.0f}% below {_MIN_CONFIDENCE:.0f}%"
        else:
            out["phone"] = _format_phone(national, ext)
    return out


def _choose_email(contact: dict) -> dict:
    """The best-scoring acceptable address. The three are NOT ordered by quality."""
    candidates = []
    for i in (1, 2, 3):
        address = contact.get(f"email{i}")
        if address:
            candidates.append((_pct(contact.get(f"email{i}TotalAI")), str(address).strip(),
                               str(contact.get(f"email{i}EmailAI") or "").strip().lower()))
    if not candidates and contact.get("email"):
        candidates.append((_pct(contact.get("emailTotalAI")), str(contact["email"]).strip(), ""))
    out = {"email": "", "email_confidence": None, "email_rejected": ""}
    if not candidates:
        out["email_rejected"] = "no email returned"
        return out
    candidates.sort(key=lambda c: -(c[0] if c[0] is not None else 0.0))
    reasons = []
    for confidence, address, verdict in candidates:
        if "@" not in address:
            continue
        if verdict == "invalid":
            reasons.append(f"{address} marked invalid by Seamless")
        elif confidence is not None and confidence < _MIN_CONFIDENCE:
            reasons.append(f"confidence {confidence:.0f}% below {_MIN_CONFIDENCE:.0f}%")
        else:
            out["email"], out["email_confidence"] = address, confidence
            return out
    out["email_rejected"] = reasons[0] if reasons else "no usable address"
    return out


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _pace() -> None:
    """Keep this process well inside the organisation-wide rate limit."""
    global _last_pace
    with _lock:
        wait = _MIN_REQUEST_INTERVAL_S - (time.monotonic() - _last_pace)
        _last_pace = time.monotonic() + max(wait, 0.0)
    if wait > 0:
        _sleep(wait)


def _stop(reason: str, **fields) -> None:
    global _stopped_until
    with _lock:
        _stopped_until = time.monotonic() + _STOP_FOR_S
    logger.warning(
        "Seamless unavailable (%s): lookups are skipped for %d minutes", reason, _STOP_FOR_S // 60,
        extra={"json_fields": {"alert": "seamless_unavailable", "reason": reason, **fields}},
    )


def _read_credits(resp) -> None:
    global _credits
    raw = (getattr(resp, "headers", None) or {}).get("X-PublicAPI-Credits")
    try:
        if raw is not None:
            with _lock:
                _credits = int(float(raw))
    except (TypeError, ValueError):
        pass


def _reset_wait(resp) -> float:
    """Seconds until the rate-limit window reopens, from X-RateLimit-Reset (epoch seconds)."""
    try:
        reset = float((getattr(resp, "headers", None) or {}).get("X-RateLimit-Reset", 0))
    except (TypeError, ValueError):
        reset = 0.0
    wait = reset - time.time()
    return min(max(wait, 1.0), _MAX_RATE_LIMIT_WAIT_S) if reset else 5.0


def _request(method: str, path: str, key: str, **kw):
    """One API call. Returns the JSON body, or None after saying why. Never raises."""
    limited = 0
    while True:
        _pace()
        try:
            resp = _session.request(
                method, f"{_BASE_URL}{path}", headers={"Token": key, "Content-Type": "application/json"},
                timeout=60, **kw,
            )
        except Exception as exc:
            logger.warning("Seamless request to %s failed: %s", path, exc)
            return None
        _read_credits(resp)
        code = resp.status_code
        if code == 200:
            try:
                return resp.json()
            except Exception as exc:
                logger.warning("Seamless returned unreadable JSON from %s: %s", path, exc)
                return None
        if code in (401, 403):
            _stop(f"{code} from {path}: bad or unauthorised key", status=code)
            return None
        if code == 422 and "insufficientcredits" in (resp.text or "").lower():
            _stop("out of credits", status=code)
            return None
        if code == 429 and limited < _RATE_LIMIT_RETRIES:
            limited += 1       # not processed, so repeating a submit is safe
            _sleep(_reset_wait(resp))
            continue
        logger.warning("Seamless returned %d from %s: %s", code, path, (resp.text or "")[:200])
        return None


def _poll(request_ids: list, key: str):
    """The finished contact row for the research request, or None. Never resubmits."""
    pending = list(request_ids)
    followed: set = set()
    waited = 0.0
    while waited <= _POLL_TIMEOUT_S:
        data = _request("GET", _POLL, key, params={"requestIds": ",".join(pending)})
        if data is None:
            return None
        for row in list(data.get("data") or []):
            status = str(row.get("status") or "").strip().lower()
            if row.get("contact"):
                return row["contact"]
            if status == _OUT_OF_CREDITS:
                _stop("out of credits", status=status)
                return None
            if status in ("duplicate", "contact-already-researched"):
                # Researched before: the record lives under the request id this stub names.
                original = str(row.get("originalRequestId") or row.get("duplicateOf") or "")
                if original and original not in followed:
                    followed.add(original)
                    pending.append(original)
                    continue
                logger.info("Seamless already researched this person and names no request to read it from")
                return None
            if status in _TERMINAL:
                return None
        _sleep(_POLL_INTERVAL_S)
        waited += _POLL_INTERVAL_S
    logger.warning(
        "Seamless research %s was not finished after %.0fs. The credit is spent; it is not resubmitted.",
        ",".join(request_ids), _POLL_TIMEOUT_S,
    )
    return None


# ── Public ────────────────────────────────────────────────────────────────────

def lookup_person(first: str, last: str, *, domain: str = "", company: str = "",
                  operator_name: str = "", title: str = ""):
    """
    Research one named person and return what is safe to use, or None.

    The dict always carries `moved` (their current employer is NOT the facility or its
    operator: no data is used), `company`, `linkedin`, and for each of phone and email the
    value (or "") with its confidence and, when rejected, the reason.
    """
    global _last_low_credits_warning
    key = os.getenv("SEAMLESS_API_KEY", "").strip()
    first, last = (first or "").strip(), (last or "").strip()
    domain = (domain or "").strip().lower().removeprefix("www.").split("/")[0]
    if not key or not first or not last or not (domain or company):
        return None

    now = time.monotonic()
    where = domain or company.strip().lower()
    with _lock:
        if now < _stopped_until:
            return None
        cached = _answers.get((first.lower(), last.lower(), where))
        if cached and now - cached[0] < _ANSWER_TTL_S:
            return cached[1]
        credits = _credits
        warn = credits is not None and credits < _MIN_CREDITS and (
            _last_low_credits_warning is None or now - _last_low_credits_warning >= _WARNING_INTERVAL_S)
        if warn:
            _last_low_credits_warning = now
    if credits is not None and credits < _MIN_CREDITS:
        if warn:
            logger.warning(
                "Seamless has %d credits left (floor %d): lookups are skipped", credits, _MIN_CREDITS,
                extra={"json_fields": {"alert": "seamless_credits_low", "credits": credits}},
            )
        return None

    target = {"contactName": f"{first} {last}"}
    target.update({"domain": domain} if domain else {"companyName": company})
    submitted = _request("POST", _RESEARCH, key, json={"contacts": [target]})
    if submitted is None:
        return None
    ids = submitted.get("requestIds") or submitted.get("data") or []
    if not ids or not isinstance(ids, list):
        logger.warning("Seamless accepted the request but returned no request id")
        return None

    contact = _poll([str(i) for i in ids], key)
    if not contact or not _name_matches(first, last, contact):
        return None

    same_org = _same_organisation(contact, domain, company, operator_name)
    base = {
        "status": "done",
        "moved": same_org is False,
        "company": str(contact.get("companyName") or contact.get("company") or ""),
        "company_domain": str(contact.get("companyDomain") or contact.get("domain") or ""),
        "title": str(contact.get("title") or ""),
        "linkedin": str(contact.get("lIProfileUrl") or contact.get("liUrl") or ""),
    }
    if same_org is True:
        answer = {**base, **_choose_phone(contact), **_choose_email(contact)}
    else:
        # Moved, or the organisation cannot be confirmed: their phone and address may belong
        # to somebody else's company, so none of it is used.
        reason = "no longer at this organisation" if same_org is False else "organisation not confirmed"
        answer = {**base, "phone": "", "phone_type": "", "phone_confidence": None, "phone_rejected": reason,
                  "email": "", "email_confidence": None, "email_rejected": reason}
    with _lock:
        _answers[(first.lower(), last.lower(), where)] = (now, answer)
    return answer
