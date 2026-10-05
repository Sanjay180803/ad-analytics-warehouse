"""Ad analytics warehouse – daily batch pipeline (Airflow 3, Astro Runtime).

One DAG run = one day of Criteo impressions (the run's data interval):

    wait_for_raw_partition   S3 sensor (reschedule mode) on raw/.../dt=<ds>/_SUCCESS
    spark_clean              PySpark: dedupe, cast, quarantine, Parquet -> clean/.../dt=<ds>/
    load.campaign_changes    Snowflake: reload the small campaign change log
    load.impressions_day     Snowflake: delete dt=<ds> + COPY INTO (idempotent)
    dbt_source_freshness     fail fast if RAW didn't actually update
    dbt_build                dbt build for that day (microbatch facts, dims, marts, tests)
    publish_dbt_docs         dbt docs generate + upload to S3 (lineage site)

Reliability features
  * Retries: every task retries with exponential backoff; on_failure_callback alerts
    only once the last retry fails.
  * SLAs: Airflow 3 removed task SLAs; Deadline Alerts replace them. Two tiers
    (warn at 90 min, page at 3 h after the run is queued). Queued-time is used, not
    logical date, so backfilling January 2025 doesn't fire 30 instant "misses".
  * Per-task execution_timeout as a hard stop.
  * Backfills: every task is keyed on {{ ds }} and idempotent (partition overwrite in
    Spark, delete+COPY in Snowflake, microbatch in dbt), so any day can be rerun.
        astro dev run backfill create --dag-id ad_analytics_daily \
            --from-date 2025-01-01 --to-date 2025-01-31 --max-active-runs 4
  * Pools: Spark runs in local mode inside the worker, so only one Spark job at a
    time (pool spark_local=1); all Snowflake writes are serialized (warehouse_writes=1)
    so full-rebuild models never race. Sensors and Spark of other days still overlap.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, SyncCallback, TaskGroup, task
from airflow.timetables.interval import CronDataIntervalTimetable

from alerting import notify_deadline_missed, notify_task_failure

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE = f"{AIRFLOW_HOME}/include"
DBT_DIR = f"{INCLUDE}/dbt"
DBT_BIN = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"

BUCKET = os.getenv("S3_BUCKET", "change-me-ad-analytics")
RAW_ROOT = f"s3://{BUCKET}/raw/criteo"
CLEAN_ROOT = f"s3://{BUCKET}/clean/criteo"
ANCHOR = os.getenv("CRITEO_ANCHOR_DATE", "2025-01-01")

# dbt reads credentials from env (see include/dbt/profiles.yml); pass through what it needs
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "DBT_TARGET": os.getenv("DBT_TARGET", "prod"),
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PRIVATE_KEY_PATH": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", ""),
}

default_args = {
    "owner": "ad-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": notify_task_failure,
}

with DAG(
    dag_id="ad_analytics_daily",
    description="Criteo impressions: S3 -> Spark -> Snowflake -> dbt star schema",
    # Data-interval semantics: the run for 2025-01-05 covers [01-05, 01-06) and
    # starts after the day closes, so {{ ds }} is the day being processed.
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="UTC"),
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2025, 1, 31, 23, 59, tzinfo=timezone.utc),  # 30 days of data spill into a 31st calendar day
    catchup=False,  # history is loaded with an explicit backfill, not by the scheduler
    max_active_runs=4,
    default_args=default_args,
    template_searchpath=[INCLUDE],
    tags=["ads", "criteo", "snowflake", "dbt", "spark"],
    deadline=[
        DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(minutes=90),
            callback=SyncCallback(notify_deadline_missed, kwargs={"tier": "warn"}),
        ),
        DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(hours=3),
            callback=SyncCallback(notify_deadline_missed, kwargs={"tier": "page"}),
        ),
    ],
    doc_md=__doc__,
) as dag:

    wait_for_raw_partition = S3KeySensor(
        task_id="wait_for_raw_partition",
        bucket_key=f"{RAW_ROOT}/impressions/dt={{{{ ds }}}}/_SUCCESS",
        aws_conn_id="aws_default",
        mode="reschedule",          # frees the worker slot between checks
        poke_interval=300,
        timeout=6 * 3600,
        retries=0,                  # the sensor's own timeout is the retry policy
    )

    spark_clean = BashOperator(
        task_id="spark_clean",
        bash_command=(
            f"python {INCLUDE}/spark/clean_impressions.py "
            f"--raw-root {RAW_ROOT} --clean-root {CLEAN_ROOT} "
            f"--dt {{{{ ds }}}} --anchor-date {ANCHOR} --mode tuned "
            f"--metrics-out /tmp/spark_metrics/{{{{ ds }}}}.json"
        ),
        pool="spark_local",
        execution_timeout=timedelta(minutes=45),
    )

    with TaskGroup("load") as load:
        load_campaign_changes = SQLExecuteQueryOperator(
            task_id="campaign_changes",
            conn_id="snowflake_loader",
            sql="sql/load_campaign_changes.sql",
            split_statements=True,
            pool="warehouse_writes",
            execution_timeout=timedelta(minutes=10),
        )
        load_impressions_day = SQLExecuteQueryOperator(
            task_id="impressions_day",
            conn_id="snowflake_loader",
            sql="sql/load_impressions_day.sql",
            split_statements=True,
            pool="warehouse_writes",
            execution_timeout=timedelta(minutes=20),
        )
        load_campaign_changes >> load_impressions_day

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} source freshness --select source:criteo.impressions",
        env=DBT_ENV,
        append_env=True,
        pool="warehouse_writes",
        execution_timeout=timedelta(minutes=10),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # microbatch models build only this day; tables rebuild; all tests run after
        bash_command=(
            f"cd {DBT_DIR} && {DBT_BIN} build "
            "--event-time-start {{ ds }} --event-time-end {{ macros.ds_add(ds, 1) }}"
        ),
        env=DBT_ENV,
        append_env=True,
        pool="warehouse_writes",
        execution_timeout=timedelta(minutes=45),
    )

    # same pool as dbt_build: two dbt processes must not share target/ at once
    @task(retries=1, execution_timeout=timedelta(minutes=15), pool="warehouse_writes")
    def publish_dbt_docs(bucket: str) -> str:
        """Generate dbt docs (lineage + column docs) and publish them as a static site in S3."""
        import subprocess

        import boto3

        subprocess.run([DBT_BIN, "docs", "generate", "--static"], cwd=DBT_DIR,
                       env={**os.environ, **DBT_ENV}, check=True)
        key = "dbt-docs/index.html"
        boto3.client("s3").upload_file(f"{DBT_DIR}/target/static_index.html", bucket, key,
                                       ExtraArgs={"ContentType": "text/html"})
        return f"s3://{bucket}/{key}"

    (
        wait_for_raw_partition
        >> spark_clean
        >> load
        >> dbt_source_freshness
        >> dbt_build
        >> publish_dbt_docs(BUCKET)
    )
