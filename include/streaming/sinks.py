"""Warehouse sinks for the streaming job: Snowflake (production) and DuckDB (local/CI).

Both implement the same idempotent upsert, so a micro-batch that Spark replays
after a failure (foreachBatch is at-least-once) never creates duplicates:

    sink.upsert(table, pandas_df, keys=[...], update=False)   # insert-if-absent
    sink.upsert(table, pandas_df, keys=[...], update=True)    # insert-or-overwrite

Snowflake path: write_pandas into a session-temporary staging table, then one
MERGE into the target inside a transaction. Uses the repo's existing AIRFLOW_SVC
service user (key-pair auth) with the LOADER role, which owns RAW.

Why not the spark-snowflake connector? Micro-batches here are small (seconds of
events), the Python connector needs no JVM jar matched to the Spark/Scala
version, and MERGE gives exactly-once *effects* on top of at-least-once
delivery. For high-volume topics, Snowpipe Streaming via the Snowflake Kafka
connector is the better tool; see README.
"""
from __future__ import annotations

import os
import threading

import pandas as pd

# Column definitions shared by DuckDB DDL and the tests that check
# include/snowflake/03_streaming.sql. (name, duckdb type)
TABLES: dict[str, dict] = {
    "IMPRESSIONS_STREAM": {
        "keys": ["impression_id"],
        "columns": [
            ("impression_id", "VARCHAR"), ("event_ts", "TIMESTAMP"), ("user_id", "VARCHAR"),
            ("campaign_id", "BIGINT"), ("is_click", "BOOLEAN"), ("is_conversion_path", "BOOLEAN"),
            ("conversion_ts", "TIMESTAMP"), ("conversion_id", "BIGINT"), ("is_attributed", "BOOLEAN"),
            ("click_pos", "INTEGER"), ("click_nb", "INTEGER"), ("cost", "DECIMAL(18,8)"),
            ("cpo", "DECIMAL(18,6)"), ("time_since_last_click_s", "BIGINT"),
            *[(f"cat{i}", "VARCHAR") for i in range(1, 10)],
            ("dt", "DATE"), ("schema_version", "VARCHAR"), ("produced_at", "TIMESTAMP"),
            ("_kafka_topic", "VARCHAR"), ("_kafka_partition", "INTEGER"), ("_kafka_offset", "BIGINT"),
            ("_kafka_ts", "TIMESTAMP"), ("_ingested_at", "TIMESTAMP"),
        ],
    },
    "IMPRESSIONS_DLQ": {
        "keys": ["kafka_topic", "kafka_partition", "kafka_offset"],
        "columns": [
            ("kafka_topic", "VARCHAR"), ("kafka_partition", "INTEGER"), ("kafka_offset", "BIGINT"),
            ("kafka_ts", "TIMESTAMP"), ("kafka_key", "VARCHAR"), ("raw_value", "VARCHAR"),
            ("reject_reason", "VARCHAR"), ("schema_version", "VARCHAR"), ("_ingested_at", "TIMESTAMP"),
        ],
    },
    "CAMPAIGN_MINUTE_METRICS": {
        "keys": ["campaign_id", "window_start"],
        "columns": [
            ("campaign_id", "BIGINT"), ("window_start", "TIMESTAMP"), ("window_end", "TIMESTAMP"),
            ("impressions", "BIGINT"), ("clicks", "BIGINT"), ("conversion_path_impressions", "BIGINT"),
            ("spend", "DECIMAL(28,8)"), ("approx_users", "BIGINT"), ("_updated_at", "TIMESTAMP"),
        ],
    },
    "STREAM_BATCH_METRICS": {
        "keys": ["query_name", "batch_id"],
        "columns": [
            ("query_name", "VARCHAR"), ("batch_id", "BIGINT"), ("rows_in", "BIGINT"), ("rows_valid", "BIGINT"),
            ("rows_dlq", "BIGINT"), ("rows_duplicate_in_batch", "BIGINT"), ("min_event_ts", "TIMESTAMP"),
            ("max_event_ts", "TIMESTAMP"), ("latency_p50_s", "DOUBLE"), ("latency_p95_s", "DOUBLE"),
            ("dlq_rate", "DOUBLE"), ("_recorded_at", "TIMESTAMP"),
        ],
    },
    "STREAM_QUERY_PROGRESS": {
        "keys": ["query_id", "batch_id"],
        "columns": [
            ("query_id", "VARCHAR"), ("query_name", "VARCHAR"), ("batch_id", "BIGINT"),
            ("num_input_rows", "BIGINT"), ("input_rows_per_s", "DOUBLE"), ("processed_rows_per_s", "DOUBLE"),
            ("batch_duration_ms", "BIGINT"), ("rows_dropped_by_watermark", "BIGINT"),
            ("state_rows", "BIGINT"), ("watermark", "VARCHAR"), ("_recorded_at", "TIMESTAMP"),
        ],
    },
}


def columns(table: str) -> list[str]:
    return [c for c, _ in TABLES[table]["columns"]]


def merge_sql(target: str, staging: str, cols: list[str], keys: list[str], update: bool) -> str:
    """MERGE statement shared in spirit by both sinks (Snowflake executes this text)."""
    on = " and ".join(f"t.{k} = s.{k}" for k in keys)
    non_keys = [c for c in cols if c not in keys]
    parts = [f"merge into {target} t using {staging} s on {on}"]
    if update and non_keys:
        parts.append("when matched then update set " + ", ".join(f"{c} = s.{c}" for c in non_keys))
    parts.append(f"when not matched then insert ({', '.join(cols)}) values ({', '.join('s.' + c for c in cols)})")
    return "\n".join(parts)


class Sink:
    def upsert(self, table: str, df: pd.DataFrame, keys: list[str] | None = None, update: bool = False) -> int:
        raise NotImplementedError

    def close(self) -> None:
        pass


class DuckDBSink(Sink):
    """Local/CI stand-in for Snowflake with the same upsert semantics."""

    def __init__(self, path: str):
        import duckdb

        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._lock = threading.Lock()          # two streaming queries call in from different threads
        self._con = duckdb.connect(path)
        for t, spec in TABLES.items():
            cols = ", ".join(f"{c} {ty}" for c, ty in spec["columns"])
            self._con.execute(f"create table if not exists {t} ({cols}, primary key ({', '.join(spec['keys'])}))")

    def upsert(self, table, df, keys=None, update=False):
        if df.empty:
            return 0
        keys = keys or TABLES[table]["keys"]
        cols = columns(table)
        df = df.reindex(columns=cols)
        non_keys = [c for c in cols if c not in keys]
        action = ("do update set " + ", ".join(f"{c} = excluded.{c}" for c in non_keys)) if update else "do nothing"
        with self._lock:
            # DuckDB 2.x: every statement's result must be consumed before the next one
            cur = self._con.cursor()
            try:
                cur.register("_batch", df)
                before = cur.execute(f"select count(*) from {table}").fetchall()[0][0]
                cur.execute(f"insert into {table} ({', '.join(cols)}) select {', '.join(cols)} from _batch "
                            f"on conflict ({', '.join(keys)}) {action}").fetchall()
                after = cur.execute(f"select count(*) from {table}").fetchall()[0][0]
                cur.unregister("_batch")
            finally:
                cur.close()
        return after - before

    def query(self, sql: str) -> pd.DataFrame:
        with self._lock:
            cur = self._con.cursor()
            try:
                return cur.execute(sql).df()
            finally:
                cur.close()

    def close(self):
        self._con.close()


class SnowflakeSink(Sink):
    """Writes to RAW.CRITEO.* as LOADER. Connection settings come from the same
    environment variables the rest of the repo uses (.env.example)."""

    def __init__(self, connect=None, write_pandas=None):
        if connect is None:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas as _wp

            connect, write_pandas = snowflake.connector.connect, _wp
        self._write_pandas = write_pandas
        self._lock = threading.Lock()
        self._con = connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
            private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            role=os.getenv("SNOWFLAKE_STREAM_ROLE", "LOADER"),
            warehouse=os.getenv("SNOWFLAKE_STREAM_WAREHOUSE", "LOADING_WH"),
            database="RAW", schema="CRITEO", session_parameters={"QUERY_TAG": "streaming_loader", "TIMEZONE": "UTC"},
        )

    def upsert(self, table, df, keys=None, update=False):
        if df.empty:
            return 0
        keys = keys or TABLES[table]["keys"]
        cols = columns(table)
        df = df.reindex(columns=cols)
        df.columns = [c.upper() for c in df.columns]
        stg = f"{table}_STG"
        with self._lock:
            cur = self._con.cursor()
            try:
                cur.execute("begin")
                cur.execute(f"create temporary table if not exists {stg} like {table}")
                cur.execute(f"truncate table {stg}")
                self._write_pandas(self._con, df, stg, quote_identifiers=False, use_logical_type=True)
                cur.execute(merge_sql(table, stg, cols, keys, update))
                n = cur.rowcount or 0
                cur.execute("commit")
            except Exception:
                cur.execute("rollback")
                raise
            finally:
                cur.close()
        return n

    def close(self):
        self._con.close()


def make_sink(kind: str, duckdb_path: str | None = None) -> Sink:
    if kind == "snowflake":
        return SnowflakeSink()
    if kind == "duckdb":
        return DuckDBSink(duckdb_path or "data/stream_warehouse.duckdb")
    raise ValueError(kind)
