# Airflow ETL Pipeline

MLOps Task 3 - 24L-0672

An Airflow pipeline made with Astro CLI. It gets daily currency exchange rates from the
[Frankfurter API](https://frankfurter.dev/), transforms them and saves them in MongoDB.
Mongo Express is used to see the data.

## Tasks

1. `extract_exchange_rates` - gets the rates from the API
2. `transform_exchange_rates` - changes them into the format below
3. `load_exchange_rates_to_mongodb` - saves them in MongoDB

The tasks pass data using named XComs (`raw_exchange_rates`, `transformed_exchange_rates` and `load_summary`).

Example of a transformed record:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.15,
  "rate_date": "2026-11-04",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-05T07:15:00"
}
```

## How to run

```bash
cp .env.example .env
astro dev start
```

Then open:

- Airflow: http://localhost:8080
- Mongo Express: http://localhost:8081 (admin / admin123)

Turn on the `currency_exchange_etl` DAG in Airflow and trigger it.

To run the tests:

```bash
astro dev pytest
```
