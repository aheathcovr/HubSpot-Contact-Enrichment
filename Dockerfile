# ── Build stage: gcc needed only to compile C extensions ─────────────────────
FROM python:3.11-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage: no build tools in the final image ─────────────────────────
FROM python:3.11-slim

RUN useradd -m -u 1000 appuser

# Install compiled packages from the builder stage
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy application source (.dockerignore excludes test files, caches, CSVs)
COPY ["State Association Contact Enrichment/", "./"]

# Guide lives one level above the app package in-container:
#   state_association_matcher.py → os.path.join(dirname(__file__), "..", "..") points here
COPY state_association_agent_guide.md /state_association_agent_guide.md

# Cache directory owned by appuser before privilege drop
RUN mkdir -p /app/.llm_cache && chown appuser /app/.llm_cache

USER appuser

ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8080

# Single worker required: several module-level globals are shared per-process
# by design — the BigQuery client cache (enrichment_processor._get_bigquery_client),
# the OpenRouter rate-limit semaphores (state_association/llm_client.py), and the
# pooled HTTP sessions (hubspot_client, fullenrich_client, email_verifier). Multiple
# workers would each get their own copies, defeating the semaphores' whole purpose
# of capping concurrent calls process-wide. Concurrent requests within this one
# worker are handled via asyncio + run_in_threadpool (see webhook_server.handle_enrich),
# not via extra worker processes.
CMD ["uvicorn", "webhook_server:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--proxy-headers", "--forwarded-allow-ips=*"]
