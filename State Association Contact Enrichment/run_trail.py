"""
The step-by-step trail of a HubSpot-app run: what was asked of which vendor and what came back.

A run used to end as "Nothing found" or "Done" and say nothing about how it got there. The vendor clients now call
note() at the points where a vendor answers (a search that returned 0 people, a credit balance, a result word, a
rejection reason), and while a run is being done (webhook_server._execute_app_run) those notes are stored on the run
record as it goes, so the card can show them live and afterwards.

  note("fullenrich", 'Searched "Administrator" by website: 3 people', "ok")

A step is {at, vendor, result, text}. The run record holds no email address, phone number or name, so a step must not
either: callers pass counts, statuses, titles searched and reasons, and clean() hides anything shaped like an address
or a number as a second line of defence. The values themselves stay in the note left on the HubSpot contact.

Outside a run (the webhook, a company workflow, a script) nothing is recording, and note() does nothing. Recording a
step can never break a run: a sink that fails is logged and skipped.

The recording belongs to the thread that started it (a context variable), so two runs in one process never mix. None of
the vendor clients start threads of their own.
"""

import contextvars
import logging
import re
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# The most steps one run keeps, and the longest a step's text may be. A run does a handful of things; a longer one is
# a mistake worth cutting rather than storing.
STEP_CAP = 30
TEXT_MAX = 160

VENDORS = ("fullenrich", "seamless", "millionverifier", "bounceban", "hubspot", "service")
RESULTS = ("ok", "empty", "failed", "skipped", "info")

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# A run of digits with the separators a phone number has, hidden when it holds 7 or more digits (a count, a score or a
# credit balance never does, and a balance with thousands separators is not one run of digits).
_NUMBER = re.compile(r"(?<![\w,])\+?\d[\d\s().-]{5,}\d(?![\w,])")

_NOTICE = "More steps happened than can be shown."


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _hide_number(match: "re.Match") -> str:
    return "[number hidden]" if sum(ch.isdigit() for ch in match.group()) >= 7 else match.group()


def clean(text: str) -> str:
    """One line of at most TEXT_MAX characters with anything shaped like an email address or a phone number hidden."""
    text = _EMAIL.sub("[email hidden]", str(text))
    text = _NUMBER.sub(_hide_number, text)
    text = " ".join(text.split())
    return text if len(text) <= TEXT_MAX else text[: TEXT_MAX - 1].rstrip() + "…"


class _Trail:
    def __init__(self, sink: Callable[[dict], None]):
        self.sink = sink
        self.kept = 0
        self.overflowed = False
        self.tally: Counter = Counter()      # (vendor, result) -> steps, counted even when a step is dropped for length

    def _send(self, step: dict) -> None:
        try:
            self.sink(step)
        except Exception as exc:
            logger.warning("Could not record a run step: %s", exc)

    def add(self, vendor: str, text: str, result: str) -> None:
        vendor = vendor if vendor in VENDORS else "service"
        result = result if result in RESULTS else "info"
        self.tally[(vendor, result)] += 1
        if self.kept >= STEP_CAP:
            if not self.overflowed:
                self.overflowed = True
                self._send({"at": _stamp(_utcnow()), "vendor": "service", "result": "info", "text": _NOTICE})
            return
        self.kept += 1
        self._send({"at": _stamp(_utcnow()), "vendor": vendor, "result": result, "text": clean(text)})


_active: contextvars.ContextVar = contextvars.ContextVar("run_trail_active", default=None)


@contextmanager
def recording(sink: Callable[[dict], None]):
    """While this is open, notes made in this thread go to *sink*, one dict per step, in the order they happened."""
    token = _active.set(_Trail(sink))
    try:
        yield
    finally:
        _active.reset(token)


def note(vendor: str, text: str, result: str = "info") -> None:
    """Record one step of the run being done in this thread; nothing when no run is being recorded."""
    trail = _active.get()
    if trail is not None:
        trail.add(vendor, text, result)


def count(result: str, vendor: Optional[str] = None) -> int:
    """How many steps of this run had this result (for one vendor, or all). Zero outside a run."""
    trail = _active.get()
    if trail is None:
        return 0
    return sum(n for (v, r), n in trail.tally.items() if r == result and (vendor is None or v == vendor))


def failures(vendor: Optional[str] = None) -> int:
    """How many steps of this run failed (for one vendor, or all). A lookup that failed is not one that found nothing."""
    return count("failed", vendor)


_HTTP_HINTS = {
    401: " (the key was rejected)", 403: " (the key was rejected)", 402: " (out of credits)", 429: " (rate limited)",
}


def http_words(code: int) -> str:
    """An HTTP status for a rep: "HTTP 402 (out of credits)"."""
    return f"HTTP {code}" + _HTTP_HINTS.get(code, " (the vendor had a problem)" if code >= 500 else "")


def why(exc: BaseException) -> str:
    """
    A short reason a call to a vendor failed, in words for a rep. It never carries the address that was called (a
    lookup's address can hold a name or an email) or the error's own text, only the status or the kind of failure.
    """
    code = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(code, int) and not isinstance(code, bool):
        return http_words(code)
    kind = type(exc).__name__
    if "Timeout" in kind:
        return "timed out"
    if "Connection" in kind:
        return "could not connect"
    return "an unexpected error"
