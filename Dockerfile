FROM python:3.11-slim

# gcc is needed to compile some google-cloud packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

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

ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8080

# Single worker required — FastAPI BackgroundTasks are per-process
CMD ["uvicorn", "webhook_server:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
