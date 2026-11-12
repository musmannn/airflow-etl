"""Unit tests for the transformation logic (no Airflow / network needed)."""

from include.currency_etl.transform import SOURCE_NAME, transform_rates

RAW = [
    {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.1226},
    {"date": "2026-11-04", "base": "EUR", "quote": "GBP", "rate": 0.83412},
    {"date": "2026-11-04", "base": "EUR", "quote": "JPY", "rate": 177.69},
]


def test_schema_matches_assignment():
    docs = transform_rates(RAW, retrieved_at="2026-11-05T07:15:00")
    usd = next(d for d in docs if d["target_currency"] == "USD")
    assert usd == {
        "base_currency": "EUR",
        "target_currency": "USD",
        "exchange_rate": 1.1226,
        "rate_date": "2026-11-04",
        "source": SOURCE_NAME,
        "retrieved_at": "2026-11-05T07:15:00",
    }
    assert len(docs) == 3


def test_invalid_records_are_skipped():
    bad = RAW + [
        {"date": "2026-11-04", "base": "EUR", "quote": "XX", "rate": 1.0},
        {"date": "2026-11-04", "base": "EUR", "quote": "CHF", "rate": -1},
        {"date": "not-a-date", "base": "EUR", "quote": "CAD", "rate": 1.5},
        {"base": "EUR", "quote": "AUD"},
    ]
    assert len(transform_rates(bad, retrieved_at="2026-11-05T07:15:00")) == 3


def test_order_fields_restores_schema_order():
    from include.currency_etl.transform import SCHEMA_FIELDS, order_fields

    scrambled = {"source": "Frankfurter API", "rate_date": "2026-11-04", "retrieved_at": "2026-11-05T07:15:00",
                 "base_currency": "EUR", "exchange_rate": 1.15, "target_currency": "USD"}
    assert tuple(order_fields(scrambled)) == SCHEMA_FIELDS
