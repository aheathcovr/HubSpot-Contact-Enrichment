# Tech Debt Audit — HubSpot Contact Enrichment — 2026-04-29

## Summary

| Severity | Count |
|----------|-------|
| Critical | 4 |
| High | 5 |
| Medium | 4 |
| Low | 3 |

**Test suite:** 64 passing, 16 failing, 1 file collection error (import failure), 1 file collection error (env/numpy issue)

Stack: Python 3.11 · FastAPI · Cloud Run · Cloud Tasks · BigQuery · OpenRouter / Perplexity Sonar

---

## Top 3 to address first

1. **Fix the broken tests** (Critical #1 and #2) — 2 import fixes + 16 test tuple-unpacking updates. Fast to do, restores the test suite that's meant to protect against regressions.
2. **Replace `print()` with `logger.*` in `state_association_matcher.py`** (Critical #4) — 88 `print()` calls in the most complex module make the Cloud Run logs almost useless for debugging production failures.
3. **CloudTasksClient singleton** (High #1) — one-line move turns a gRPC-connection-per-webhook into a reused client with no logic changes.

---

## Critical

### 1. `test_diagnostic.py` fails collection — `RATE_LIMIT_SECONDS` removed
**Location:** [`test_diagnostic.py:42`](State%20Association%20Contact%20Enrichment/test_diagnostic.py#L42)
**What:** `RATE_LIMIT_SECONDS` was renamed to `W1_RATE_LIMIT_SECONDS` / `W2_RATE_LIMIT_SECONDS` in the refactor but `test_diagnostic.py` still imports the old name. The entire file errors at collection time — none of its tests run.
**Why it matters:** The collection error blocks pytest from running any tests in the file. These tests cover the research pipeline.
**Fix:** Either add `RATE_LIMIT_SECONDS = W1_RATE_LIMIT_SECONDS` as an alias in `state_association/llm_client.py`, or update the import in `test_diagnostic.py:42` to use `W1_RATE_LIMIT_SECONDS`. Also remove the inline `time.sleep(RATE_LIMIT_SECONDS)` at line 356 — sleep in a test is a smell; that's testing the rate-limit implementation, not behavior.

---

### 2. 16 tests failing — `_build_facility_research_prompt` return-value mismatch
**Location:** [`test_guide_helpers.py:451–523`](State%20Association%20Contact%20Enrichment/test_guide_helpers.py#L451)
**What:** `_build_facility_research_prompt` now returns a 3-tuple `(prompt, tier, domain_filter)`. All 16 `TestBuildFacilityResearchPrompt` tests still unpack 2 values, so they all raise `ValueError: too many values to unpack`.
**Why it matters:** These tests cover the prompt-building logic for every US state / facility type combination — the core of the W1 research pipeline. With 16 failures, the test suite gives a false picture of health.
**Fix:** Update each unpacking call in `test_guide_helpers.py` from `prompt, tier = _build_facility_research_prompt(...)` to `prompt, tier, _ = _build_facility_research_prompt(...)` (or use the domain filter if the test cares about it). Also fix `test_prompt_contains_report_back_section` for the contact prompt — "Report back:" is no longer in the rendered output.

---

### 3. `test_state_assoc.py` fails collection — numpy double-import
**Location:** [`test_state_assoc.py:17`](State%20Association%20Contact%20Enrichment/test_state_assoc.py#L17)
**What:** `pandas` raises `ImportError: Unable to import required dependencies: numpy: cannot load module more than once per process`. This is a pytest isolation bug — a prior test or conftest imports numpy in a way that conflicts with pandas' import path. All 256 lines of integration tests in this file are unreachable.
**Why it matters:** `test_state_assoc.py` covers the BQ state-association fast path (W1's primary data source). No test coverage runs on this path.
**Fix:** Add a `conftest.py` in the `State Association Contact Enrichment/` directory that imports `pandas` once at session scope before any test modules collect. Alternatively, run tests with `--import-mode=importlib` to isolate module state between files.

---

### 4. `state_association_matcher.py` uses `print()` everywhere — invisible on Cloud Run
**Location:** [`state_association_matcher.py`](State%20Association%20Contact%20Enrichment/state_association_matcher.py) — 88 `print()` calls, 2 `logger.*` calls
**What:** The most complex module in the codebase uses `print()` almost exclusively for all diagnostic output. `enrichment_processor.py`, `webhook_server.py`, and `hubspot_client.py` all use structured `logger.*`. But the matcher — which does the actual research work and BQ queries — prints raw text to stdout. Cloud Run routes stdout to Cloud Logging, but without severity, logger name, or JSON structure, these messages don't participate in log-based alerting, severity filtering, or correlation.
**Why it matters:** When an enrichment fails silently in production, the structured logs show the error in `enrichment_processor.py` but the entire matcher's diagnostic trail (guide loading, BQ query results, Sonar Pro call decisions) is invisible in the Cloud Logging structured view. Debugging production failures requires reading raw stdout lines rather than querying `jsonPayload.severity`.
**Fix:** Add `logger = logging.getLogger(__name__)` at module level and replace `print(...)` with `logger.info(...)`, `logger.warning(...)`, `logger.error(...)` as appropriate. The `_CloudLoggingFormatter` in `webhook_server.py` is already active — `state_association_matcher.py` just needs to use `logging`.

---

## High

### 1. `CloudTasksClient` recreated on every webhook request
**Location:** [`webhook_server.py:292`](State%20Association%20Contact%20Enrichment/webhook_server.py#L292) inside `_enqueue_enrichment()`
**What:** `tasks_v2.CloudTasksClient()` is instantiated inside the function body. Each call to `/webhook` that needs to enqueue a task creates a new gRPC client, establishes a new TLS connection, and performs service discovery.
**Why it matters:** gRPC client creation is expensive (~100–300 ms). Under any burst of webhooks, this adds latency to every enqueue call and creates unnecessary connection churn.
**Fix:** Move the client to module level or use the `lifespan` hook to initialize it once:
```python
_tasks_client: tasks_v2.CloudTasksClient | None = None

def _get_tasks_client() -> tasks_v2.CloudTasksClient:
    global _tasks_client
    if _tasks_client is None:
        from google.cloud import tasks_v2
        _tasks_client = tasks_v2.CloudTasksClient()
    return _tasks_client
```

---

### 2. BigQuery client recreated on every enrichment run
**Location:** [`enrichment_processor.py:1890–1892`](State%20Association%20Contact%20Enrichment/enrichment_processor.py#L1890) inside `run_enrichment()`
**What:** `init_bigquery_client()` is called at the start of every enrichment job, creating a new gRPC connection to BigQuery each time. The `lookup_hubspot_contact_enhanced()` in `state_association_matcher.py` also initialises its own BQ client via the same pattern.
**Why it matters:** With `--concurrency=1` and `--max-instances=5`, each Cloud Run instance handles one enrichment at a time. The BQ client could safely be a module-level singleton per instance, reusing the connection pool.
**Fix:** Move `bq_client` to module scope in both `enrichment_processor.py` and `state_association_matcher.py`, initialised lazily on first use.

---

### 3. God files — two modules over 2,000 lines each
**Location:** 
- [`enrichment_processor.py`](State%20Association%20Contact%20Enrichment/enrichment_processor.py) — 2,138 lines; largest function is `_build_note()` at 379 lines, `run_enrichment()` at 306 lines
- [`state_association_matcher.py`](State%20Association%20Contact%20Enrichment/state_association_matcher.py) — 2,369 lines; `_research_one_contact()` at 224 lines, `lookup_hubspot_contact_enhanced()` at 165 lines

**What:** Both files mix concerns: BQ query logic, prompt construction, HubSpot API calls, note formatting, and workflow orchestration all live in the same module. `_build_note()` alone is 379 lines.
**Why it matters:** Any change to one concern risks breaking another. Onboarding is difficult. The functions are too long to understand without scrolling.
**Fix:** Extract the note-building logic from `enrichment_processor.py` into `note_builder.py`. Extract BQ query functions from `state_association_matcher.py` into `bq_queries.py`. This is a pure refactor with no behavior change.

---

### 4. Major version drift on critical dependencies
**Location:** [`requirements.txt`](requirements.txt)
**What:**
| Package | Pinned | Latest | Gap |
|---------|--------|--------|-----|
| `pyarrow` | 18.1.0 | 24.0.0 | 6 major |
| `uvicorn` | 0.32.1 | 0.46.0 | significant |
| `fastapi` | 0.115.6 | 0.136.1 | significant |
| `starlette` | 0.41.3 | 1.0.0 | 1 major |
| `pandas` | 2.2.3 | 3.0.2 | 1 major |
| `google-cloud-bigquery` | 3.27.0 | 3.41.0 | 14 minor |
| `google-auth` | 2.37.0 | 2.49.2 | 12 minor |

**Why it matters:** `starlette` 1.0.0 has breaking changes from 0.x. `pandas` 3.0 drops several deprecated APIs. `pyarrow` 24.0 has significant changes. Staying this far behind makes future upgrades painful (big-bang migrations) and may leave known CVEs unaddressed.
**Fix:** Pin to the latest of each major version. Test `pandas` and `starlette` upgrades separately — those are the most likely to have breaking changes.

---

### 5. `sys.exit(1)` at import time if BigQuery unavailable
**Location:** [`state_association_matcher.py:111–113`](State%20Association%20Contact%20Enrichment/state_association_matcher.py#L111)
**What:** If `google-cloud-bigquery` cannot be imported, the module calls `sys.exit(1)` at module level. This means any process that imports `state_association_matcher` (including the webhook server) exits immediately rather than raising a clean error.
**Why it matters:** On Cloud Run, this would appear as a silent crash with no structured log. The lifespan hook in `webhook_server.py` would never run, so no meaningful error would be emitted to Cloud Logging.
**Fix:** Raise `ImportError` or `RuntimeError` instead of `sys.exit(1)`, and let `uvicorn` catch and log the startup failure.

---

## Medium

### 1. `print()` calls mixed with `logger.*` in `fullenrich_client.py` and `state_association/llm_client.py`
**Location:** `fullenrich_client.py`, `state_association/llm_client.py`
**What:** `fullenrich_client.py` has no `print()` calls (good), but `state_association/llm_client.py` also uses `print()` in a few places for error output. Inconsistent with the rest of the codebase.
**Fix:** Same as Critical #4 — use `logger.*`.

---

### 2. No `conftest.py` or `pytest.ini` — test suite has no shared setup
**Location:** [`State Association Contact Enrichment/`](State%20Association%20Contact%20Enrichment/)
**What:** There's no `conftest.py` or `pytest.ini`. Tests rely on being run from the right directory with the right `PYTHONPATH`. The `numpy` double-import error in `test_state_assoc.py` is a symptom of this.
**Fix:** Add a `conftest.py` that imports shared fixtures, sets `sys.path`, and imports `pandas` once. Add `pytest.ini` or a `[tool.pytest.ini_options]` section in `pyproject.toml` with `testpaths` set.

---

### 3. No requirements split between production and development
**Location:** [`requirements.txt`](requirements.txt)
**What:** `pytest`, `python-dotenv` are development-only dependencies but are installed in the production image (because there's one `requirements.txt`). `python-dotenv` loads `.env` files — on Cloud Run this is harmless but it adds noise. `pytest` adds ~15 MB to the image.
**Fix:** Split into `requirements.txt` (production only) and `requirements-dev.txt` (includes prod + dev tools). The Dockerfile only installs `requirements.txt`.

---

### 4. Hardcoded GCP project ID fallback in 6 places
**Location:** `setup_cloud_tasks.sh:16`, `enrichment_processor.py:64`, `webhook_server.py:281`, `state_association_matcher.py:137`
**What:** `gen-lang-client-0844868008` is hardcoded as the `os.getenv(..., "gen-lang-client-0844868008")` default. Since Cloud Build now injects `GCP_PROJECT_ID` and `BQ_PROJECT_ID`, the fallback is never used in production — but it leaks your GCP project ID in source code and silently uses the wrong project if the env var is ever missing.
**Fix:** Change the defaults to `""` and add a startup assertion that fails loudly if the env var is missing, rather than silently billing against a hardcoded project.

---

## Low

### 1. `.gitignore` is a Node.js template
**Location:** [`.gitignore`](.gitignore)
**What:** The `.gitignore` is a Node.js boilerplate. It's missing Python-specific entries: `.mypy_cache/`, `.coverage`, `htmlcov/`, `*.egg-info/`, `dist/`, `.tox/`. It also doesn't ignore `context/` (the audit report directory just created).
**Fix:** Replace with a Python-appropriate `.gitignore` (GitHub's Python template is a good starting point).

---

### 2. `run_brookdale_workflow1.py` committed alongside production code
**Location:** [`State Association Contact Enrichment/run_brookdale_workflow1.py`](State%20Association%20Contact%20Enrichment/run_brookdale_workflow1.py)
**What:** A one-off operational script for a specific client run is tracked in `main` alongside the service code. It's now excluded from the Docker image by `.dockerignore`, but it adds confusion about what's "real" code.
**Fix:** Move to a `scripts/` directory at the repo root, or delete if it's no longer needed.

---

### 3. `BIGQUERY_AVAILABLE` flag is dead code
**Location:** [`state_association_matcher.py:108–113`](State%20Association%20Contact%20Enrichment/state_association_matcher.py#L108)
**What:** The code sets `BIGQUERY_AVAILABLE = True` on success and `BIGQUERY_AVAILABLE = False` + `sys.exit(1)` on failure. The `sys.exit` means `BIGQUERY_AVAILABLE = False` can never be observed by a caller — the process is dead. The flag provides no actual graceful degradation.
**Fix:** Remove the flag entirely. Raise `ImportError` instead of `sys.exit`, and let callers handle the missing dependency.
