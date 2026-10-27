"""
The HubSpot app and this service agree on what a request for enrichment is.

The Enrichment card (hubspot-app/) asks for a run by setting Request to enrich on a company. It never talks to this
service: HubSpot reacts to the property change and calls /webhook. So the app only works while it sets the
property, and the value, that the webhook listens for, and while it describes the 30-day rule the same way the
processor applies it. These tests read the app function's constants and compare them with the service's own, so a
rename or a changed threshold on either side fails here instead of in front of a rep.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_hubspot_app_contract.py" -v
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_processor
import webhook_server

_HERE = os.path.dirname(os.path.abspath(__file__))
_FUNCTION = os.path.join(_HERE, "..", "hubspot-app", "src", "app", "functions", "enrichment.js")


def _js():
    with open(_FUNCTION, encoding="utf-8") as f:
        return f.read()


def _string(name):
    m = re.search(rf"^const {name} = '([^']*)';", _js(), re.M)
    assert m, f"enrichment.js has no string constant {name}"
    return m.group(1)


def _list(name):
    m = re.search(rf"^const {name} = \[([^\]]*)\];", _js(), re.M)
    assert m, f"enrichment.js has no list constant {name}"
    return re.findall(r"'([^']*)'", m.group(1))


def _number(name):
    m = re.search(rf"^const {name} = (\d+);", _js(), re.M)
    assert m, f"enrichment.js has no number constant {name}"
    return int(m.group(1))


def _processor_source():
    path = os.path.join(_HERE, "enrichment_processor.py")
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_the_app_sets_the_property_the_webhook_listens_for():
    assert _string("TRIGGER_PROPERTY") == webhook_server.TRIGGER_PROPERTY


def test_the_value_the_app_sets_is_one_the_webhook_accepts():
    assert _string("TRIGGER_VALUE").strip().lower() in webhook_server.TRIGGER_VALUES


def test_the_app_reads_a_request_as_true_for_the_same_values_as_the_webhook():
    assert set(_list("TRIGGER_VALUES")) == set(webhook_server.TRIGGER_VALUES)


def test_the_service_clears_the_request_when_it_finishes():
    """The card shows a run as waiting while the property is true, so the run must put it back to false."""
    source = _processor_source()
    assert f'"{webhook_server.TRIGGER_PROPERTY}": "false"' in source


def test_the_app_reads_the_date_the_service_stamps_when_a_run_finishes():
    source = _processor_source()
    last_run = _string("LAST_ENRICHED_PROPERTY")
    assert f'"{last_run}": str(HubSpotClient.epoch_ms_today_utc())' in source
    assert f'props.get("{last_run}")' in source


def test_the_app_uses_the_same_number_of_days_as_the_service_before_it_enriches_again():
    thresholds = {int(n) for n in re.findall(r"if days_since < (\d+):", _processor_source())}
    assert thresholds == {_number("REFRESH_DAYS")}


def test_the_app_tells_a_facility_from_a_corporation_by_the_values_the_service_uses():
    assert _string("FACILITY_TYPE") == enrichment_processor.CHILD_HIERARCHY_TYPE
    assert _string("CORPORATION_TYPE") == "Corporation / Operator"
    assert _string("CORPORATION_TYPE") in _processor_source()


# ── The contact card and the /app endpoints ───────────────────────────────────
#
# The contact card (hubspot-app/src/app/cards/contact-model.ts and ContactCheckView.tsx) calls /app/contact/status
# and /app/contact/run and reads what comes back. These read the card's source for the names it relies on and
# compare them with the service's, so a renamed objective, outcome or field fails here.

_CARDS = os.path.join(_HERE, "..", "hubspot-app", "src", "app", "cards")


def _card_source(name):
    with open(os.path.join(_CARDS, name), encoding="utf-8") as f:
        return f.read()


def _source(name):
    with open(os.path.join(_HERE, name), encoding="utf-8") as f:
        return f.read()


def test_the_contact_card_offers_the_objectives_the_service_runs():
    import contact_objectives

    declared = re.search(r"export type ObjectiveId = ([^;]+);", _card_source("contact-model.ts")).group(1)

    assert re.findall(r"'([^']+)'", declared) == [oid for oid, _, _ in contact_objectives.OBJECTIVES]


def test_the_contact_card_names_the_objectives_as_the_service_does():
    import contact_objectives

    block = re.search(r"OBJECTIVE_LABELS: Record<string, string> = \{([^}]+)\}", _card_source("contact-model.ts")).group(1)
    labels = dict(re.findall(r"(\w+): '([^']+)'", block))

    assert labels == {oid: label for oid, label, _ in contact_objectives.OBJECTIVES}


def test_the_contact_card_calls_routes_the_service_has():
    routes = {(m, r.path) for r in webhook_server.app.routes if hasattr(r, "methods") for m in r.methods}
    view = _card_source("ContactCheckView.tsx")

    assert ("GET", "/app/contact/status") in routes and ("POST", "/app/contact/run") in routes
    assert "'GET', `/contact/status?contactId=" in view and "'POST', '/contact/run'" in view
    assert re.search(r"SERVICE_URL = '[^']+/app';", _card_source("contact-model.ts"))


def test_the_contact_card_reads_the_run_fields_the_service_sends():
    import app_run_store
    from datetime import datetime, timezone

    sent = app_run_store.run_view({"run_id": "r", "contact_id": "1", "objective": "find_email", "status": "queued",
                                   "created_at": datetime(2026, 10, 26, tzinfo=timezone.utc)})
    declared = re.search(r"export interface RunView \{(.*?)\n\}", _card_source("contact-model.ts"), re.S).group(1)

    assert set(re.findall(r"^\s+(\w+)\??:", declared, re.M)) == set(sent)


def test_the_contact_card_knows_the_run_statuses_the_service_writes():
    declared = re.search(r"status: ([^;]+);", re.search(r"export interface RunView \{(.*?)\n\}", _card_source("contact-model.ts"), re.S).group(1)).group(1)
    card = set(re.findall(r"'([^']+)'", declared))
    written = set(re.findall(r'Outcome\("(\w+)"', _source("contact_objectives.py")))
    written |= set(re.findall(r'(?:status="|"status": ")(\w+)', _source("app_run_store.py")))

    assert card == {"queued", "running", "done", "failed"} and written <= card


def test_the_contact_card_names_every_outcome_the_service_can_report():
    outcomes = set(re.findall(r'Outcome\("\w+", "(\w+)"', _source("contact_objectives.py"))) - {"error"}
    cases = set(re.findall(r"case '(\w+)':", _card_source("contact-model.ts")))

    assert outcomes <= cases, f"the card does not name these outcomes: {sorted(outcomes - cases)}"
