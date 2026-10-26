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
