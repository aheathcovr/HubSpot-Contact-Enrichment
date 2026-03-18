FROM python:3.11-slim

# gcc is needed to compile some google-cloud packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 appuser

WORKDIR /app

# Install dependencies first (layer-cached separately from source)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source (JSON array syntax required for paths with spaces)
COPY ["State Association Contact Enrichment/", "./"]

# The guide file is referenced via a relative path in state_association_matcher.py:
#   _GUIDE_PATH = os.path.join(os.path.dirname(__file__), "..", "state_association_agent_guide.md")
# With __file__ = /app/state_association_matcher.py, dirname = /app, .. = /
# So the guide must live at /state_association_agent_guide.md inside the container.
COPY state_association_agent_guide.md /state_association_agent_guide.md

# Create cache directory and set ownership before dropping privileges
RUN mkdir -p /app/.llm_cache && chown appuser /app/.llm_cache

# Drop root privileges before starting the server
USER appuser

ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# Single worker required — FastAPI BackgroundTasks are per-process
CMD ["uvicorn", "webhook_server:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--proxy-headers", "--forwarded-allow-ips=*"]
