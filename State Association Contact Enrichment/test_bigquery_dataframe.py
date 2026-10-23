"""
The BigQuery-to-pandas path, exercised offline.

state_association_matcher calls `client.query(...).to_dataframe()` at five
call sites. That call goes through google-cloud-bigquery, pyarrow, db-dtypes and
pandas, so an upgrade to any of them can change what the matcher receives (dtype
changes, a missing optional dependency) without any matcher test noticing,
because every other test fakes the BigQuery layer away.

This builds a real `RowIterator` from a canned first page (no network, no
credentials) and converts it exactly as the service does, with the column types
the Definitive Healthcare tables use: strings, integers with NULLs, and dates.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_bigquery_dataframe.py" -v
"""

import datetime
from unittest.mock import create_autospec

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

_SCHEMA = [
    bigquery.SchemaField("EXECUTIVE_NAME", "STRING"),
    bigquery.SchemaField("hospital_id", "INT64"),
    bigquery.SchemaField("LAST_UPDATE", "DATE"),
]


def _result(rows: list[list]) -> RowIterator:
    page = {
        "totalRows": str(len(rows)),
        "rows": [{"f": [{"v": None if v is None else str(v)} for v in row]} for row in rows],
    }
    client = create_autospec(bigquery.Client, instance=True)
    client.project = "test-project"     # an instance attribute, so autospec cannot know it
    return RowIterator(
        client=client,
        api_request=None,
        path=None,
        schema=_SCHEMA,
        first_page_response=page,
    )


def test_query_result_converts_to_a_dataframe_with_usable_types():
    result = _result([
        ["Jane Doe", 561557, datetime.date(2026, 3, 1)],
        ["John Roe", None, None],
    ])

    df = result.to_dataframe(create_bqstorage_client=False)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["EXECUTIVE_NAME", "hospital_id", "LAST_UPDATE"]
    assert df["EXECUTIVE_NAME"].tolist() == ["Jane Doe", "John Roe"]
    assert df["hospital_id"].iloc[0] == 561557
    assert pd.isna(df["hospital_id"].iloc[1])
    assert df["LAST_UPDATE"].iloc[0] == datetime.date(2026, 3, 1)
    assert pd.isna(df["LAST_UPDATE"].iloc[1])


def test_empty_query_result_converts_to_an_empty_dataframe_with_its_columns():
    df = _result([]).to_dataframe(create_bqstorage_client=False)

    assert df.empty
    assert list(df.columns) == ["EXECUTIVE_NAME", "hospital_id", "LAST_UPDATE"]
