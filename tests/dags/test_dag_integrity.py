"""DAG integrity tests (run with `astro dev pytest`)."""

from airflow.models import DagBag

DAG_ID = "currency_exchange_etl"


def _dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors():
    assert _dagbag().import_errors == {}


def test_dag_has_three_tasks_in_order():
    dag = _dagbag().dags.get(DAG_ID)
    assert dag is not None
    assert [t.task_id for t in dag.topological_sort()] == [
        "extract_exchange_rates",
        "transform_exchange_rates",
        "load_exchange_rates_to_mongodb",
    ]


def test_dag_is_daily_with_retries():
    dag = _dagbag().dags.get(DAG_ID)
    assert dag.schedule == "@daily" or str(dag.timetable.summary) == "@daily"
    for t in dag.tasks:
        assert t.retries == 2
