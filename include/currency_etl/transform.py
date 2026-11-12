"""Pure-Python helpers for the currency ETL DAG (kept free of Airflow imports so they are unit-testable)."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Iterable

import requests

SOURCE_NAME = "Frankfurter API"
DEFAULT_API_URL = "https://api.frankfurter.dev/v2/rates"
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Field order of the target document schema
SCHEMA_FIELDS = (
    "base_currency",
    "target_currency",
    "exchange_rate",
    "rate_date",
    "source",
    "retrieved_at",
)


def utc_now_iso() -> str:
    """Timestamp in the format required by the assignment, e.g. 2026-11-05T07:15:00 (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def fetch_rates(
    base: str,
    quotes: Iterable[str],
    rate_date: str | None = None,
    api_url: str = DEFAULT_API_URL,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Call the Frankfurter v2 API.

    Example: GET https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY
    Response: [{"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1139}, ...]
    """
    params: dict[str, str] = {"base": base.upper(), "quotes": ",".join(q.upper() for q in quotes)}
    if rate_date:
        params["date"] = rate_date

    response = requests.get(api_url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Unexpected/empty response from Frankfurter API: {payload!r}")
    return payload


def _validate_currency(code: Any, field: str) -> str:
    if not isinstance(code, str) or not _CURRENCY_RE.match(code.strip().upper()):
        raise ValueError(f"Invalid currency code in '{field}': {code!r}")
    return code.strip().upper()


def transform_rates(raw_records: list[dict[str, Any]], retrieved_at: str) -> list[dict[str, Any]]:
    """Map raw API records to the target document schema.

    {
      "base_currency": "EUR", "target_currency": "USD", "exchange_rate": 1.15,
      "rate_date": "2026-11-04", "source": "Frankfurter API", "retrieved_at": "2026-11-05T07:15:00"
    }
    Invalid records (bad currency, non-positive rate, bad date) are skipped.
    """
    transformed: list[dict[str, Any]] = []
    for record in raw_records:
        try:
            base = _validate_currency(record.get("base"), "base")
            target = _validate_currency(record.get("quote"), "quote")
            rate = float(record["rate"])
            if rate <= 0:
                raise ValueError(f"Non-positive rate: {rate}")
            rate_date = date.fromisoformat(str(record["date"])).isoformat()
        except (KeyError, TypeError, ValueError) as exc:
            print(f"Skipping invalid record {record!r}: {exc}")
            continue

        if base == target:
            continue

        transformed.append(
            {
                "base_currency": base,
                "target_currency": target,
                "exchange_rate": round(rate, 6),
                "rate_date": rate_date,
                "source": SOURCE_NAME,
                "retrieved_at": retrieved_at,
            }
        )

    transformed.sort(key=lambda d: (d["rate_date"], d["target_currency"]))
    return transformed


def order_fields(document: dict[str, Any]) -> dict[str, Any]:
    """Return the document with keys in SCHEMA_FIELDS order.

    Airflow 3 keeps XComs in a Postgres JSONB column, which does not preserve key order,
    so the load task re-orders each document before writing it to MongoDB.
    """
    return {field: document[field] for field in SCHEMA_FIELDS}
