"""
BigQuery client factory.

On Cloud Run the attached service account provides credentials automatically
via Application Default Credentials (ADC). For local development, set
GOOGLE_APPLICATION_CREDENTIALS to the path of a service account key file.
"""

import os
from typing import Tuple

from google.cloud import bigquery


def init_bigquery_client(
    project_id: str,
    location: str = "US",
) -> Tuple[bigquery.Client, str]:
    """
    Return a BigQuery client and a string describing the auth method used.

    Auth priority:
      1. GOOGLE_APPLICATION_CREDENTIALS env var → service account key file
      2. Application Default Credentials (ADC) — automatic on Cloud Run

    Args:
        project_id: GCP project ID to bill queries against.
        location:   Default query location (e.g. "US").

    Returns:
        (client, auth_method) where auth_method is one of:
          "service_account_file" | "application_default_credentials"

    Raises:
        RuntimeError: if no credentials can be found.
    """
    key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    if key_file and os.path.isfile(key_file):
        try:
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(
                key_file,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(
                project=project_id,
                credentials=credentials,
                location=location,
            )
            return client, "service_account_file"
        except Exception as exc:
            raise RuntimeError(
                f"Failed to load service account from {key_file}: {exc}"
            ) from exc

    # Fall back to ADC (works automatically on Cloud Run)
    try:
        import google.auth

        credentials, detected_project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(
            project=project_id,
            credentials=credentials,
            location=location,
        )
        return client, "application_default_credentials"
    except Exception as exc:
        raise RuntimeError(
            "No GCP credentials found. On Cloud Run, ensure the service account has "
            "BigQuery roles. Locally, set GOOGLE_APPLICATION_CREDENTIALS to a service "
            f"account key file path. Original error: {exc}"
        ) from exc
