"""
state_association package — focused sub-modules for the enrichment research engine.

Modules:
  cache      — Two-tier LLM response caching (GCS + disk)
  llm_client — OpenRouter/Sonar Pro client, parse_found_name, normalize_phone
"""

from state_association.cache import (
    GCS_CACHE_BUCKET,
    cache_get,
    cache_set,
    gcs_get,
    gcs_set,
    disk_get,
    disk_set,
)

from state_association.llm_client import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    OPENROUTER_MODEL,
    OPENROUTER_MODEL_TIER23,
    RATE_LIMIT_SECONDS,
    RESEARCH_SYSTEM_PROMPT,
    _CONTACT_JSON_SCHEMA,
    _MAX_WORKERS,
    _OPENROUTER_SEMAPHORE,
    call_openrouter,
    normalize_phone,
    parse_found_name,
)

__all__ = [
    # cache
    "GCS_CACHE_BUCKET",
    "cache_get",
    "cache_set",
    "gcs_get",
    "gcs_set",
    "disk_get",
    "disk_set",
    # llm_client
    "OPENROUTER_API_KEY",
    "OPENROUTER_BASE_URL",
    "OPENROUTER_MODEL",
    "OPENROUTER_MODEL_TIER23",
    "RATE_LIMIT_SECONDS",
    "RESEARCH_SYSTEM_PROMPT",
    "_CONTACT_JSON_SCHEMA",
    "_MAX_WORKERS",
    "_OPENROUTER_SEMAPHORE",
    "call_openrouter",
    "normalize_phone",
    "parse_found_name",
]
