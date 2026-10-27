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


def _json(name):
    """One of the app function's config files (hubspot-app/src/app/functions/)."""
    import json

    with open(os.path.join(_HERE, "..", "hubspot-app", "src", "app", "functions", name), encoding="utf-8") as f:
        return json.load(f)


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
    assert re.search(r"SERVICE_FUNCTION = '([^']+)';", _card_source("contact-model.ts")).group(1) == _json("service-hsmeta.json")["uid"]


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


# ── The company card picks contacts ───────────────────────────────────────────
#
# The Enrichment card (company-model.ts, usePicker.ts, PickPanel.tsx) calls /app/company/status and /app/company/run.
# These compare what it declares with what the service sends and does.

def _interface(source, name):
    return re.search(rf"export interface {name} \{{(.*?)\n\}}", source, re.S).group(1)


def _keys(declared):
    return set(re.findall(r"^\s+(\w+)\??:", declared, re.M))


def _fakes(monkeypatch):
    """A company with one contact, a memory run store, and a queue that takes everything: what the two routes need."""
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    import app_run_store
    import app_request_auth

    hs = MagicMock()
    hs.get_company_properties.return_value = {"name": "Elm Court Care Center", "hierarchy_type": "Individual Facility / Child"}
    hs.get_contact_company_ids.return_value = ["500"]
    hs.get_contact_for_app.return_value = {"firstname": "Pat", "lastname": "Lee", "email": "", "mobilephone": ""}
    store = app_run_store.MemoryRunStore()
    monkeypatch.setattr(webhook_server, "_hubspot", lambda: hs)
    monkeypatch.setattr(webhook_server, "_run_store", lambda: store)
    monkeypatch.setattr(webhook_server, "_enqueue_app_run", lambda run_id: None)
    monkeypatch.setattr(webhook_server, "_utcnow", lambda: datetime(2026, 10, 26, 16, tzinfo=timezone.utc))
    return app_request_auth.Identity(user_id="77", email="rep@example.org", portal_id="1", app_id="2")


def test_the_company_card_offers_the_objectives_the_service_plans():
    import contact_objectives

    declared = re.search(r"id: ([^;]+);", _interface(_card_source("company-model.ts"), "BatchObjective")).group(1)

    assert re.findall(r"'([^']+)'", declared) == [oid for oid, _, _ in contact_objectives.BATCH_OBJECTIVES]


def test_the_company_card_calls_routes_the_service_has():
    routes = {(m, r.path) for r in webhook_server.app.routes if hasattr(r, "methods") for m in r.methods}

    assert ("GET", "/app/company/status") in routes and ("POST", "/app/company/run") in routes
    assert "`/company/status?companyId=${companyId}`" in _card_source("company-model.ts")
    assert "'POST', '/company/run'" in _card_source("usePicker.ts")


def test_the_company_card_reads_the_fields_of_a_company_status_the_service_sends(monkeypatch):
    who = _fakes(monkeypatch)

    _, sent = webhook_server._app_company_status("500", who)

    source = _card_source("company-model.ts")
    assert _keys(_interface(source, "CompanyStatus")) == set(sent)
    assert _keys(_interface(source, "CompanyInfo")) == set(sent["company"])
    assert _keys(_interface(source, "BatchObjective")) == set(sent["objectives"][0])
    assert re.findall(r"limits: \{ (\w+): number; (\w+):", _interface(source, "CompanyStatus"))[0] == tuple(sent["limits"])


def test_the_company_card_reads_the_fields_of_a_batch_the_service_sends(monkeypatch):
    who = _fakes(monkeypatch)

    status, sent = webhook_server._app_company_start("500", ["1"], "find_email", who)

    source = _card_source("company-model.ts")
    assert status == 202 and _keys(_interface(source, "BatchResult")) == set(sent)
    assert _keys(_interface(source, "BatchItem")) == set(sent["items"][0])


def test_the_company_card_knows_the_item_statuses_the_service_writes():
    declared = re.search(r"export type ItemStatus = ([^;]+);", _card_source("company-model.ts")).group(1)
    card = set(re.findall(r"'([^']+)'", declared))
    service = _source("webhook_server.py")
    written = set(re.findall(r'_item\(\w+, \w+, "(\w+)"', service)) | set(re.findall(r'update\(status="(\w+)"', service)) | set(re.findall(r'\["status"\] = "(\w+)"', service))
    written -= {"pending"}   # a placeholder while a request is planned; every one is started or failed before the answer goes out

    assert card == {"started", "reused", "skipped", "failed"} and written <= card


def test_the_company_card_limits_a_pick_as_the_service_does():
    declared = int(re.search(r"DEFAULT_MAX_CONTACTS = (\d+);", _card_source("usePicker.ts")).group(1))

    assert declared == webhook_server._APP_BATCH_MAX_CONTACTS


def test_the_company_card_counts_the_most_runs_enrich_can_start_for_a_contact_as_the_service_plans_them():
    import itertools

    import contact_objectives

    most = 0
    for named, email, checked, mobile in itertools.product([True, False], repeat=4):
        contact = {"firstname": "Pat" if named else "", "lastname": "Lee" if named else "", "email": "x@example.org" if email else "",
                   "email_verification_result": "ok" if checked else "", "mobilephone": "405-555-0101" if mobile else ""}
        most = max(most, len(contact_objectives.plan_for("enrich", contact)[0]))

    assert most == 2
    assert "objective === 'enrich' ? picked * 2 : picked" in _card_source("company-model.ts")


# ── The company card finds people by job title ────────────────────────────────

def _search_and_candidate(now):
    """A finished search with one person kept, in the run store the fakes installed; the search's id."""
    store = webhook_server._run_store()
    store.create_run({"run_id": "f" * 32, "contact_id": "", "objective": "find_by_title", "titles": ["Medical Director"], "titles_key": "medical director",
                      "requested_by_id": "77", "requested_by": "rep@example.org", "portal_id": "1", "company_id": "500", "status": "done", "created_at": now})
    store.save_candidates("f" * 32, "500", [{"candidate_id": "a" * 12, "full_name": "Pat Lee", "title": "Medical Director", "linkedin_url": "", "updated_at": "", "linked_contact_id": ""}], now)
    return "f" * 32


def test_the_company_card_calls_the_title_routes_the_service_has():
    routes = {(m, r.path) for r in webhook_server.app.routes if hasattr(r, "methods") for m in r.methods}

    assert {("POST", "/app/company/find"), ("GET", "/app/company/candidates"), ("POST", "/app/company/add")} <= routes
    hook = _card_source("useTitleSearch.ts")
    assert "'POST', '/company/find'" in hook and "'POST', '/company/add'" in hook
    assert "`/company/candidates?companyId=${companyId}&runId=${runId}`" in _card_source("company-model.ts")


def test_the_company_card_names_the_runs_the_title_search_makes_as_the_service_does():
    import company_titles

    labels = dict(re.findall(r"(\w+): '([^']+)'", re.search(r"COMPANY_RUN_LABELS: Record<string, string> = \{([^}]+)\}", _card_source("contact-model.ts")).group(1)))

    assert set(labels) == {company_titles.FIND_OBJECTIVE, company_titles.ADD_OBJECTIVE}
    assert f"const FIND_OBJECTIVE = '{company_titles.FIND_OBJECTIVE}';" in _card_source("useTitleSearch.ts")


def test_the_company_card_reads_the_fields_of_a_find_the_service_sends(monkeypatch):
    who = _fakes(monkeypatch)

    status, sent = webhook_server._app_find_start("500", ["Medical Director"], who)

    assert status == 202 and _keys(_interface(_card_source("company-model.ts"), "FindResult")) == set(sent)


def test_the_company_card_reads_the_fields_of_the_people_a_search_found(monkeypatch):
    from datetime import datetime, timezone

    who = _fakes(monkeypatch)
    run_id = _search_and_candidate(datetime(2026, 10, 26, 16, tzinfo=timezone.utc))

    status, sent = webhook_server._app_candidates("500", run_id)

    source = _card_source("company-model.ts")
    assert status == 200 and _keys(_interface(source, "CandidateList")) == set(sent)
    assert _keys(_interface(source, "Candidate")) == set(sent["candidates"][0])


def test_the_company_card_reads_the_fields_of_an_add_the_service_sends(monkeypatch):
    from datetime import datetime, timezone

    who = _fakes(monkeypatch)
    run_id = _search_and_candidate(datetime(2026, 10, 26, 16, tzinfo=timezone.utc))

    status, sent = webhook_server._app_add_start("500", run_id, ["a" * 12], who)

    source = _card_source("company-model.ts")
    assert status == 202 and _keys(_interface(source, "AddResult")) == set(sent)
    assert _keys(_interface(source, "AddItem")) == set(sent["items"][0])


def test_the_company_card_brings_back_a_search_for_as_long_as_the_service_hands_the_same_one_back():
    declared = re.search(r"RESUME_WITHIN_MS = (\d+) \* (\d+) \* (\d+) \* (\d+);", _card_source("useTitleSearch.ts"))
    ms = 1
    for part in declared.groups():
        ms *= int(part)

    assert ms == webhook_server._APP_REUSE_WINDOW.total_seconds() * 1000


def test_the_company_card_knows_the_statuses_and_outcomes_the_title_search_reports():
    declared = re.search(r"status: ([^;]+);", re.search(r"export interface RunView \{(.*?)\n\}", _card_source("contact-model.ts"), re.S).group(1)).group(1)
    statuses = set(re.findall(r'Outcome\("(\w+)"', _source("company_titles.py")))
    outcomes = set(re.findall(r'Outcome\("\w+", "(\w+)"', _source("company_titles.py"))) - {"error"}
    cases = set(re.findall(r"case '(\w+)':", _card_source("contact-model.ts")))

    assert statuses <= set(re.findall(r"'([^']+)'", declared))
    assert outcomes <= cases, f"the card does not name these outcomes: {sorted(outcomes - cases)}"


# ── The cards reach the service through the service function ──────────────────
#
# A static-auth app has no client secret, so HubSpot signs nothing and a card cannot call the service itself. The cards
# call hubspot-app/src/app/functions/service.js, which says who is asking from HubSpot's context and signs the request
# with a secret it shares with the service. These tests tie the function to the service: the routes it forwards, the
# headers and secret it signs with, and (with Node, when it is installed) that a request the real function builds is
# accepted by the real check and cannot be altered.

_SERVICE_JS = os.path.join(_HERE, "..", "hubspot-app", "src", "app", "functions", "service.js")


def _service_js():
    with open(_SERVICE_JS, encoding="utf-8") as f:
        return f.read()


def _function_routes():
    return [(m, p, [k for k in re.findall(r"'([^']+)'", q)]) for m, p, q in
            re.findall(r"\{ method: '(\w+)', path: '([^']+)', query: \[([^\]]*)\] \}", _service_js())]


def test_the_service_function_forwards_exactly_the_routes_the_service_serves_to_the_app():
    forwarded = {(m, "/app" + p) for m, p, _ in _function_routes()}
    served = {(m, r.path) for r in webhook_server.app.routes if hasattr(r, "methods") and r.path.startswith("/app/") for m in r.methods}

    assert re.search(r"APP_PREFIX = '(/app)';", _service_js())
    # /app/run is the Cloud Tasks delivery of a queued run: it takes the queue's token, not a rep's request, and is not forwarded.
    assert forwarded == served - {("POST", "/app/run")}


def test_the_service_function_passes_the_query_keys_the_service_reads_and_none_that_say_who_is_asking():
    keys = {k for _, _, ks in _function_routes() for k in ks}
    read = set(re.findall(r'request\.query_params\.get\("(\w+)"', _source("webhook_server.py")))
    who = {"userId", "userEmail", "portalId", "appId"}

    assert keys == read - who and not keys & who


def test_the_cards_call_exactly_the_paths_the_service_function_forwards():
    sources = "".join(_card_source(n) for n in ("ContactCheckView.tsx", "usePicker.ts", "useTitleSearch.ts", "company-model.ts"))
    called = set(re.findall(r"/(?:contact|company)/(?:status|run|find|candidates|add)\b", sources))

    assert called == {p for _, p, _ in _function_routes()}


def test_the_service_function_signs_with_the_headers_and_secret_the_service_reads():
    import app_request_auth

    js = _service_js()
    assert re.search(r"SIGNATURE_HEADER = '([^']+)';", js).group(1) == app_request_auth.SIGNATURE_HEADER
    assert re.search(r"TIMESTAMP_HEADER = '([^']+)';", js).group(1) == app_request_auth.TIMESTAMP_HEADER
    secret = _json("service-hsmeta.json")["config"]["secretKeys"]
    assert secret == ["APP_REQUEST_SECRET"] and f'os.getenv("{secret[0]}"' in _source("webhook_server.py")
    assert "process.env.APP_REQUEST_SECRET" in js


def test_the_service_function_calls_the_service_over_https_without_a_trailing_slash():
    base = re.search(r"SERVICE_BASE = '([^']+)';", _service_js()).group(1)

    assert base.startswith("https://") and not base.endswith("/") and "?" not in base


def _real_call(request_js, who_js='{ userId: "77", email: "o\'neil+rep@example.org", portalId: "1234" }'):
    """What the real function builds for a request, run by Node: {method, url, body, headers}."""
    import json
    import shutil
    import subprocess

    import pytest

    node = shutil.which("node")
    if not node:
        pytest.skip("Node is not installed, so the real function cannot be run here")
    script = f"const fn = require({json.dumps(os.path.abspath(_SERVICE_JS))}); " \
             f"const call = fn._internals.buildCall({who_js}, {request_js}, 'test-app-secret', 1700000000000); " \
             "process.stdout.write(JSON.stringify(call));"
    out = subprocess.run([node, "-e", script], capture_output=True, text=True, check=True, timeout=30)
    return json.loads(out.stdout)


def _check(call, *, secret="test-app-secret"):
    import app_request_auth as auth

    return auth.authenticate(
        secret=secret, method=call["method"], url=call["url"], body=call["body"].encode("utf-8"),
        signature=call["headers"][auth.SIGNATURE_HEADER], timestamp=call["headers"][auth.TIMESTAMP_HEADER], now_ms=1700000000000,
    )


_POST = "{ ok: true, method: 'POST', path: '/contact/run', query: [], body: { contactId: '42', objective: 'find_email' } }"
_GET = "{ ok: true, method: 'GET', path: '/company/candidates', query: [['companyId', '500'], ['runId', 'f'.repeat(32)]], body: undefined }"


def test_a_request_the_real_function_builds_is_accepted_by_the_real_check_and_says_who_asked():
    for request in (_POST, _GET):
        who = _check(_real_call(request))

        assert (who.user_id, who.email, who.portal_id) == ("77", "o'neil+rep@example.org", "1234")


def test_a_request_the_real_function_builds_is_refused_once_who_asked_or_what_was_asked_is_changed():
    import app_request_auth as auth
    import pytest

    call = _real_call(_POST)
    for altered in (
        {**call, "url": call["url"].replace("userId=77", "userId=1")},
        {**call, "url": call["url"].replace("/contact/run", "/company/run")},
        {**call, "body": call["body"].replace("find_email", "verify_email")},
        {**call, "method": "GET"},
    ):
        with pytest.raises(auth.AuthError) as refused:
            _check(altered)
        assert refused.value.status_code == 401
    with pytest.raises(auth.AuthError):
        _check(call, secret="another-secret")
