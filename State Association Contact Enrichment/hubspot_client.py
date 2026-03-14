"""
HubSpot REST API client.

Wraps the CRM v3/v4 endpoints needed for the enrichment workflow:
  - Read company properties
  - Update company properties (revops_enrichment_date, request_to_enrich)
  - Create contacts
  - Search contacts by email
  - Associate contacts with companies
  - Create notes and associate them with companies

All methods raise HubSpotError on unrecoverable API failures.
Transient errors (429, 5xx) are retried automatically via urllib3.
"""

import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

HUBSPOT_BASE_URL = "https://api.hubapi.com"


class HubSpotError(Exception):
    """Raised when a HubSpot API call fails after retries."""

    def __init__(self, message: str, status_code: int = 0, response_text: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


def _build_session() -> requests.Session:
    """Return a requests.Session with retry logic pre-configured."""
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class HubSpotClient:
    """Thin wrapper around the HubSpot CRM REST API."""

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key or os.environ["HUBSPOT_API_KEY"]
        self._session = _build_session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            }
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{HUBSPOT_BASE_URL}{path}"
        resp = self._session.get(url, params=params, timeout=30)
        if not resp.ok:
            raise HubSpotError(
                f"GET {path} failed: {resp.status_code}",
                status_code=resp.status_code,
                response_text=resp.text,
            )
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        url = f"{HUBSPOT_BASE_URL}{path}"
        resp = self._session.post(url, json=body, timeout=30)
        if not resp.ok:
            raise HubSpotError(
                f"POST {path} failed: {resp.status_code}",
                status_code=resp.status_code,
                response_text=resp.text,
            )
        return resp.json()

    def _patch(self, path: str, body: dict) -> dict:
        url = f"{HUBSPOT_BASE_URL}{path}"
        resp = self._session.patch(url, json=body, timeout=30)
        if not resp.ok:
            raise HubSpotError(
                f"PATCH {path} failed: {resp.status_code}",
                status_code=resp.status_code,
                response_text=resp.text,
            )
        return resp.json()

    def _put(self, path: str) -> None:
        """PUT with no request body (used for default associations)."""
        url = f"{HUBSPOT_BASE_URL}{path}"
        resp = self._session.put(url, timeout=30)
        # 200 or 204 are success; 409 means already associated — both are fine
        if not resp.ok and resp.status_code != 409:
            raise HubSpotError(
                f"PUT {path} failed: {resp.status_code}",
                status_code=resp.status_code,
                response_text=resp.text,
            )

    # ── Company ───────────────────────────────────────────────────────────────

    def get_company_properties(self, company_id: str) -> dict:
        """
        Fetch key properties for a company record.

        Returns the `properties` dict (values are strings or None).
        """
        props = ",".join([
            "name",
            "facility_type",
            "hierarchy_type",
            "revops_enrichment_date",
            "request_to_enrich",
            "definitive_healthcare_id",
            "city",
            "state",
            "hs_parent_company_id",
        ])
        data = self._get(
            f"/crm/v3/objects/companies/{company_id}",
            params={"properties": props},
        )
        return data.get("properties", {})

    def update_company(self, company_id: str, properties: dict) -> None:
        """Update one or more properties on a company record."""
        self._patch(
            f"/crm/v3/objects/companies/{company_id}",
            body={"properties": properties},
        )

    # ── Contacts ──────────────────────────────────────────────────────────────

    def create_contact(self, properties: dict) -> str:
        """
        Create a new contact and return the new contact ID.

        If the email already exists (409), extracts and returns the existing
        contact ID from HubSpot's error message instead of raising.
        """
        url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts"
        resp = self._session.post(url, json={"properties": properties}, timeout=30)

        if resp.status_code == 409:
            # Contact already exists — parse the existing ID from the error body
            body = resp.json()
            message = body.get("message", "")
            match = re.search(r"Existing ID:\s*(\d+)", message)
            if match:
                existing_id = match.group(1)
                logger.info(f"Contact already exists (409), using existing ID={existing_id}")
                return existing_id
            # If we can't parse the ID fall through to the error
            raise HubSpotError(
                f"Contact creation 409 but could not parse existing ID: {message}",
                status_code=409,
                response_text=resp.text,
            )

        if not resp.ok:
            raise HubSpotError(
                f"POST /crm/v3/objects/contacts failed: {resp.status_code}",
                status_code=resp.status_code,
                response_text=resp.text,
            )

        return str(resp.json()["id"])

    def search_contacts_by_email(self, email: str) -> list[dict]:
        """
        Search HubSpot contacts by exact email match.

        Returns a list of contact dicts with keys: id, firstname, lastname,
        email, associatedcompanyid. Empty list if not found.
        """
        if not email or "@" not in email:
            return []

        body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": email.strip().lower(),
                        }
                    ]
                }
            ],
            "properties": ["firstname", "lastname", "email", "associatedcompanyid"],
            "limit": 5,
        }
        try:
            data = self._post("/crm/v3/objects/contacts/search", body)
            results = data.get("results", [])
            contacts = []
            for r in results:
                p = r.get("properties", {})
                contacts.append(
                    {
                        "id": r["id"],
                        "firstname": p.get("firstname", ""),
                        "lastname": p.get("lastname", ""),
                        "email": p.get("email", ""),
                        "associatedcompanyid": p.get("associatedcompanyid", ""),
                    }
                )
            return contacts
        except HubSpotError as exc:
            logger.warning(f"Contact search by email failed: {exc}")
            return []

    # ── Associations ──────────────────────────────────────────────────────────

    def associate_contact_to_company(self, contact_id: str, company_id: str) -> None:
        """
        Add the default contact-to-company association.
        Silently succeeds if the association already exists (409).
        """
        self._put(
            f"/crm/v4/objects/contacts/{contact_id}/associations/default/companies/{company_id}"
        )
        logger.info(f"Associated contact {contact_id} → company {company_id}")

    # ── Notes ─────────────────────────────────────────────────────────────────

    def create_note_on_company(self, company_id: str, note_body: str) -> str:
        """
        Create a note (engagement) associated with a company record.

        Uses the Engagements v1 API, which is covered by the `timeline` scope.
        The CRM v3 notes endpoint requires `crm.objects.notes.write` which is
        not available as a Private App scope — Engagements v1 is the correct path.

        Returns the new engagement ID.
        """
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        payload = {
            "engagement": {
                "active": True,
                "type": "NOTE",
                "timestamp": now_ms,
            },
            "associations": {
                "companyIds": [int(company_id)],
            },
            "metadata": {
                "body": note_body,
            },
        }
        data = self._post("/engagements/v1/engagements", payload)
        engagement_id = str(data["engagement"]["id"])
        logger.info(f"Created note (engagement {engagement_id}) on company {company_id}")
        return engagement_id

    # ── Date helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def epoch_ms_today_utc() -> int:
        """
        Return today's date at UTC midnight as epoch milliseconds.

        HubSpot date picker properties store/receive values in this format.
        Example: 2026-03-13 → 1741824000000
        """
        today = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return int(today.timestamp() * 1000)
