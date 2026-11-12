"""
Currency Exchange Rate ETL  (Frankfurter API  ->  transform  ->  MongoDB)

    extract_exchange_rates  >>  transform_exchange_rates  >>  load_exchange_rates_to_mongodb

Data moves between tasks ONLY through *named* XComs (explicit keys):
    extract   --xcom key "raw_exchange_rates"         --> transform
    transform --xcom key "transformed_exchange_rates" --> load
    load      --xcom key "load_summary"               (final report of the run)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from include.currency_etl.transform import (
    DEFAULT_API_URL,
    fetch_rates,
    order_fields,
    transform_rates,
    utc_now_iso,
)

# ---- named XCom keys -------------------------------------------------------
XCOM_RAW = "raw_exchange_rates"
XCOM_TRANSFORMED = "transformed_exchange_rates"
XCOM_LOAD_SUMMARY = "load_summary"

MONGO_CONN_ID = "mongo_default"
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")
API_URL = os.getenv("FRANKFURTER_API_URL", DEFAULT_API_URL)

default_args = {
    "owner": "24L-0672",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API -> transform -> MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "currency", "mongodb", "frankfurter"],
    params={
        "base_currency": Param("EUR", type="string", minLength=3, maxLength=3),
        "quote_currencies": Param(["USD", "GBP", "JPY"], type="array"),
        "rate_date": Param(
            None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD. Empty = latest published rates.",
        ),
    },
    doc_md=__doc__,
)
def currency_exchange_etl():
    @task(task_id="extract_exchange_rates")
    def extract_exchange_rates(**context) -> None:
        params = context["params"]
        ti = context["ti"]

        raw = fetch_rates(
            base=params["base_currency"],
            quotes=params["quote_currencies"],
            rate_date=params.get("rate_date") or None,
            api_url=API_URL,
        )
        payload = {"retrieved_at": utc_now_iso(), "records": raw}

        print(f"Extracted {len(raw)} raw records from {API_URL}: {raw}")
        ti.xcom_push(key=XCOM_RAW, value=payload)

    @task(task_id="transform_exchange_rates")
    def transform_exchange_rates(**context) -> None:
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW)
        if not payload or not payload.get("records"):
            raise ValueError(f"No data found in XCom '{XCOM_RAW}'")

        documents = transform_rates(payload["records"], retrieved_at=payload["retrieved_at"])
        if not documents:
            raise ValueError("Transformation produced no valid documents")

        for doc in documents:
            print(doc)
        ti.xcom_push(key=XCOM_TRANSFORMED, value=documents)

    @task(task_id="load_exchange_rates_to_mongodb")
    def load_exchange_rates_to_mongodb(**context) -> None:
        from airflow.providers.mongo.hooks.mongo import MongoHook
        from pymongo import ASCENDING, ReplaceOne

        ti = context["ti"]
        documents = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED)
        if not documents:
            raise ValueError(f"No data found in XCom '{XCOM_TRANSFORMED}'")
        documents = [order_fields(d) for d in documents]

        client = MongoHook(mongo_conn_id=MONGO_CONN_ID).get_conn()
        collection = client[MONGO_DATABASE][MONGO_COLLECTION]
        collection.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True,
            name="uniq_pair_per_day",
        )

        # Upsert -> idempotent: re-running the same day updates instead of duplicating.
        operations = [
            ReplaceOne(
                {
                    "base_currency": d["base_currency"],
                    "target_currency": d["target_currency"],
                    "rate_date": d["rate_date"],
                },
                d,  # stored exactly in the required schema / field order
                upsert=True,
            )
            for d in documents
        ]
        result = collection.bulk_write(operations, ordered=False)

        summary = {
            "database": MONGO_DATABASE,
            "collection": MONGO_COLLECTION,
            "documents_received": len(documents),
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "matched": result.matched_count,
            "total_documents_in_collection": collection.count_documents({}),
        }
        print(f"MongoDB load summary: {summary}")
        ti.xcom_push(key=XCOM_LOAD_SUMMARY, value=summary)

    extract_exchange_rates() >> transform_exchange_rates() >> load_exchange_rates_to_mongodb()


currency_exchange_etl()
