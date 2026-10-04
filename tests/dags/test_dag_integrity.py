"""DAG integrity checks. Run inside the Astro project with `astro dev pytest`."""

import os

import pytest
from airflow.models import DagBag

DAG_ID = "ad_analytics_daily"


@pytest.fixture(scope="module")
def dagbag():
    os.environ.setdefault("S3_BUCKET", "test-bucket")
    dags_dir = os.path.join(os.path.dirname(__file__), "..", "..", "dags")
    return DagBag(dag_folder=os.path.abspath(dags_dir))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_task_graph(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    order = ["wait_for_raw_partition", "spark_clean", "load.campaign_changes",
             "load.impressions_day", "dbt_source_freshness", "dbt_build", "publish_dbt_docs"]
    assert set(order) == set(dag.task_ids)
    for up, down in zip(order, order[1:]):
        assert down in dag.get_task(up).downstream_task_ids, f"{up} -> {down} missing"


def test_reliability_settings(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    assert dag.deadline, "deadline alerts (SLA replacement) must be configured"
    for t in dag.tasks:
        if t.task_id != "wait_for_raw_partition":
            assert t.retries >= 1, f"{t.task_id} has no retries"
            assert t.execution_timeout is not None, f"{t.task_id} has no timeout"
    assert dag.get_task("spark_clean").pool == "spark_local"
    assert dag.get_task("dbt_build").pool == "warehouse_writes"
