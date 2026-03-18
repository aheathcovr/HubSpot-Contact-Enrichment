"""
LLM response cache — two-tier: GCS (production) and disk (local dev).

All calls to OpenRouter use temperature=0, so responses are deterministic
and safe to cache indefinitely. Delete cache entries to force fresh lookups.

GCS is used when the GCS_CACHE_BUCKET environment variable is set.
Disk cache (.llm_cache/ next to state_association_matcher.py) is the fallback.
"""

import json
import logging
import os
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GCS_CACHE_BUCKET: str = os.getenv("GCS_CACHE_BUCKET", "")

# Disk cache lives alongside state_association_matcher.py (one level up from this package)
_LLM_CACHE_DIR: Path = Path(__file__).parent.parent / ".llm_cache"

# GCS client — lazily initialized on first use, shared across all threads.
_gcs_client_lock = threading.Lock()
_gcs_client_instance = None  # google.cloud.storage.Client | False (unavailable)


def _get_gcs_client():
    """Return a shared GCS client, or False if GCS is unavailable/unconfigured."""
    global _gcs_client_instance
    if _gcs_client_instance is None:
        with _gcs_client_lock:
            if _gcs_client_instance is None:
                try:
                    from google.cloud import storage as _gcs
                    _gcs_client_instance = _gcs.Client()
                except Exception as exc:
                    logger.warning("GCS client unavailable (%s) — using disk cache", exc)
                    _gcs_client_instance = False
    return _gcs_client_instance


def gcs_get(key: str) -> Optional[str]:
    """Return cached LLM response from GCS, or None on miss/error."""
    client = _get_gcs_client()
    if not client:
        return None
    try:
        blob = client.bucket(GCS_CACHE_BUCKET).blob(f"llm_cache/{key}.json")
        if blob.exists():
            logger.info("GCS LLM cache hit (key=%s)", key)
            return json.loads(blob.download_as_text())["response"]
    except Exception as exc:
        logger.warning("GCS cache get failed (key=%s): %s", key, exc)
    return None


def gcs_set(key: str, response: str) -> None:
    """Write LLM response to GCS cache. Fails silently."""
    client = _get_gcs_client()
    if not client:
        return
    try:
        blob = client.bucket(GCS_CACHE_BUCKET).blob(f"llm_cache/{key}.json")
        blob.upload_from_string(
            json.dumps({"response": response}),
            content_type="application/json",
        )
    except Exception as exc:
        logger.warning("GCS cache set failed (key=%s): %s", key, exc)


def disk_get(key: str) -> Optional[str]:
    """Return cached LLM response from disk, or None on miss."""
    cache_file = _LLM_CACHE_DIR / f"{key}.json"
    if cache_file.exists():
        try:
            logger.info("Disk LLM cache hit (key=%s)", key)
            return json.loads(cache_file.read_text())["response"]
        except Exception as exc:
            logger.warning("Disk cache read failed (key=%s): %s", key, exc)
    return None


def disk_set(key: str, response: str) -> None:
    """Write LLM response to disk cache. Creates directory if needed."""
    try:
        _LLM_CACHE_DIR.mkdir(exist_ok=True)
        (_LLM_CACHE_DIR / f"{key}.json").write_text(json.dumps({"response": response}))
    except Exception as exc:
        logger.warning("Disk cache write failed (key=%s): %s", key, exc)


def cache_get(key: str) -> Optional[str]:
    """Return cached LLM response from GCS (production) or disk (dev), or None."""
    if GCS_CACHE_BUCKET:
        return gcs_get(key)
    return disk_get(key)


def cache_set(key: str, response: str) -> None:
    """Write LLM response to GCS (production) or disk (dev)."""
    if GCS_CACHE_BUCKET:
        gcs_set(key, response)
    else:
        disk_set(key, response)
