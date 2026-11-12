# Currency Exchange Rate ETL — Apache Airflow (Astro CLI) + MongoDB

MLOps Task 3 · **24L-0672**

A daily Airflow pipeline, built with the **Astro CLI**, that pulls currency exchange rates from the
[Frankfurter API](https://frankfurter.dev/), transforms them into a clean document schema and loads
them into **MongoDB**. **Mongo Express** runs as a separate Docker service for browsing the data.

```
Frankfurter API ──► extract_exchange_rates ──► transform_exchange_rates ──► load_exchange_rates_to_mongodb ──► MongoDB ◄── Mongo Express
                     XCom key:                  XCom key:                     XCom key:
                     raw_exchange_rates         transformed_exchange_rates    load_summary
```

## Project layout

| Path | Purpose |
|---|---|
| `dags/currency_exchange_etl.py` | The DAG: 3 tasks wired with **named XComs** |
| `include/currency_etl/transform.py` | API client + transformation logic (pure Python, unit-tested) |
| `Dockerfile` | Astro Runtime 3.3 (Airflow 3) image + OCI metadata `LABEL`s |
| `docker-compose.override.yml` | MongoDB + Mongo Express services, custom network, persistent volumes |
| `docker/mongodb/init-mongo.js` | Init script (mounted into MongoDB) creating `currency_db.exchange_rates` + unique index |
| `tests/` | DAG integrity + transformation unit tests |
| `.astro/config.yaml` | Astro project config (local Postgres moved to port 5433) |
| `.env.example` | Airflow connection `mongo_default` and pipeline settings |

## Transformed document schema

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

Documents are **upserted** on `(base_currency, target_currency, rate_date)` (unique index), so re-running a day never duplicates data.

## Docker configuration

* **Metadata** – the Airflow image built by Astro carries OCI `LABEL`s (`maintainer`, `org.opencontainers.image.*`, `com.mlops.*`) from the `Dockerfile`;
  the MongoDB and Mongo Express services carry the same metadata as compose `labels` (Astro CLI does not build extra images from the override file,
  so these use the official `mongo:7.0` / `mongo-express:1.0.2` images). The network and volumes are labelled too. Check with
  `docker inspect --format '{{json .Config.Labels}}' <container>`.
* **Networking** – a dedicated bridge network `currency_etl_net`; MongoDB, Mongo Express and the Airflow
  scheduler / dag-processor / api-server / triggerer all join it, so Airflow reaches the DB at `mongodb:27017`.
* **Persistent storage** – named volumes `currency_etl_mongo_data` (`/data/db`) and `currency_etl_mongo_config` (`/data/configdb`) survive `astro dev stop` / `astro dev restart`.

## Run it

```bash
# 1. install Astro CLI (Linux)
curl -sSL install.astronomer.io | sudo bash -s

# 2. configure
cp .env.example .env

# 3. start Airflow + MongoDB + Mongo Express
astro dev start

# 4. trigger the DAG (or use the UI)
astro dev run dags unpause currency_exchange_etl
astro dev run dags trigger currency_exchange_etl

# 5. tests
astro dev pytest
```

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 (or the `*.localhost:6563` URL printed by `astro dev start`) | — (local dev) |
| Mongo Express | http://localhost:8081 | `admin` / `admin123` |
| MongoDB | `mongodb://mongo_admin:mongo_pass_2026@localhost:27017/?authSource=admin` | |

Query the data from the shell:

```bash
docker exec -it $(docker ps -qf name=mongodb) mongosh -u mongo_admin -p mongo_pass_2026 \
  --authenticationDatabase admin currency_db --eval 'db.exchange_rates.find({}, {_id:0})'
```

DAG params (Trigger DAG w/ config): `base_currency` (default `EUR`), `quote_currencies` (default `["USD","GBP","JPY"]`), `rate_date` (optional `YYYY-MM-DD`, empty = latest).

> Note: Airflow 3 stores XComs in a Postgres JSONB column, which does not keep key order, so the load
> task re-orders each document into the schema's field order (`order_fields`) before writing to MongoDB.

Credentials here are local-development defaults only.
