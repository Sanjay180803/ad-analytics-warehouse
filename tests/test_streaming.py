"""End-to-end tests of the streaming path without a broker or Snowflake.

The producer writes Kafka-shaped Parquet files (same columns as Spark's Kafka
source, one file per micro-batch); the streaming job reads them with the same
parsing, validation and sink code it uses against real Kafka, and writes to a
DuckDB stand-in for Snowflake with the same MERGE semantics. Expected outputs
are computed independently in pandas from what the producer sent.
The real-Kafka version of this test is tests/integration/test_kafka_e2e.py.
"""
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

import duckdb
import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "include", "streaming"))
sys.path.insert(0, os.path.join(ROOT, "include", "spark"))

import producer  # noqa: E402
import sinks  # noqa: E402

ANCHOR = datetime(2025, 1, 1, tzinfo=timezone.utc)
N_FILES = 4


def _run_stream(source_dir, duckdb_path, checkpoint, campaigns):
    import stream_impressions as si
    args = si.parser().parse_args([
        "--source-dir", str(source_dir), "--campaigns", str(campaigns), "--sink", "duckdb",
        "--duckdb-path", str(duckdb_path), "--checkpoint-root", str(checkpoint), "--available-now",
        "--shuffle-partitions", "4"])
    return si.run(args)


def _q(path, sql):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


@pytest.fixture(scope="module")
def streamed(tmp_path_factory):
    d = tmp_path_factory.mktemp("stream")
    scripts = os.path.join(ROOT, "include", "scripts")
    subprocess.run([sys.executable, f"{scripts}/generate_sample_data.py", "--rows", "20000",
                    "--out", str(d / "sample.tsv.gz")], check=True, capture_output=True)
    subprocess.run([sys.executable, f"{scripts}/land_raw_to_s3.py", "--src", str(d / "sample.tsv.gz"),
                    "--dest", str(d / "landing")], check=True, capture_output=True)
    records, stats = producer.build_stream(producer.read_rows(str(d / "sample.tsv.gz")), ANCHOR,
                                           fault_rate=0.02, dup_rate=0.02, late_rate=0.01, seed=11)
    producer.write_kafka_files(records, str(d / "kafka"), N_FILES, "ad.impressions.v1")
    campaigns = d / "landing" / "campaign_changes" / "campaign_changes.csv"
    queries = _run_stream(d / "kafka", d / "wh.duckdb", d / "ckpt", campaigns)
    return {"dir": d, "records": records, "stats": stats, "db": d / "wh.duckdb", "campaigns": campaigns,
            "queries": queries}


def _accepted_events(streamed) -> pd.DataFrame:
    """Events the consumer must accept, with their micro-batch number."""
    per_file = -(-len(streamed["records"]) // N_FILES)
    rows = [(i // per_file, r.impression_id, r.event, r.expect) for i, r in enumerate(streamed["records"])
            if not r.expect.startswith("dlq")]
    df = pd.DataFrame([{"batch": b, "id": i, "expect": x, "et": pd.Timestamp(e["event_ts"]).tz_localize(None),
                        "campaign_id": e["campaign_id"], "is_click": e["is_click"], "cost": e["cost"],
                        "conv": e["is_conversion_path"]} for b, i, e, x in rows])
    return df


# --------------------------------------------------------------------------- raw
def test_raw_table_has_every_accepted_impression_exactly_once(streamed):
    acc = _accepted_events(streamed)
    raw = _q(streamed["db"], "select impression_id from IMPRESSIONS_STREAM")
    assert raw.impression_id.is_unique
    assert set(raw.impression_id) == set(acc.id)              # incl. late events: raw never drops data
    late_ids = set(acc.loc[acc.expect == "late", "id"])
    assert late_ids and late_ids <= set(raw.impression_id)


def test_every_fault_lands_in_dlq_with_the_right_reason(streamed):
    dlq = _q(streamed["db"], "select raw_value, reject_reason from IMPRESSIONS_DLQ")
    expected = pd.Series([r.expect[4:] for r in streamed["records"] if r.expect.startswith("dlq")]).value_counts()
    got = dlq.reject_reason.value_counts()
    assert got.sort_index().to_dict() == expected.sort_index().to_dict()
    assert set(got.index) == set(producer.EXPECTED_REASON.values())   # all 7 rule types exercised


def test_no_dlq_event_reaches_the_raw_table(streamed):
    dlq_ids = {r.impression_id for r in streamed["records"] if r.expect.startswith("dlq")}
    ok_ids = {r.impression_id for r in streamed["records"] if not r.expect.startswith("dlq")}
    raw = set(_q(streamed["db"], "select impression_id from IMPRESSIONS_STREAM").impression_id)
    assert not (dlq_ids - ok_ids) & raw


def test_batch_and_stream_compute_the_same_impression_id(streamed):
    """The batch Spark job and the producer must key the same raw row identically."""
    from pyspark.sql import SparkSession, functions as F
    import clean_impressions as ci
    spark = SparkSession.builder.getOrCreate()
    day = "2025-01-03"
    raw = ci.read_raw(spark, str(streamed["dir"] / "landing"), day)
    epoch = int(ANCHOR.timestamp())
    spark_ids = {r.impression_id for r in ci.cast_and_validate(raw, epoch).filter(F.col("reject_reason").isNull())
                 .select("impression_id").collect()}
    prod_ids = {r.impression_id for r in streamed["records"]
                if not r.expect.startswith("dlq") and r.event["event_ts"].startswith(day)}
    assert prod_ids and prod_ids <= spark_ids


# --------------------------------------------------------------------- aggregates
def test_minute_aggregates_match_an_independent_recomputation(streamed):
    """Oracle for Spark's watermark semantics: a row in micro-batch b is dropped
    as late if its event time is older than the watermark in effect for batch
    b-1; windows are emitted (once) when window_end <= the final watermark."""
    acc = _accepted_events(streamed)
    camps = set(pd.read_csv(streamed["campaigns"]).campaign_id)
    acc = acc[acc.campaign_id.isin(camps)]
    nb = acc.batch.max() + 1
    mx = acc.groupby("batch").et.max().cummax()
    wm = [pd.Timestamp("1970-01-01")] + [mx[b] - pd.Timedelta(minutes=10) for b in range(nb)]
    late_thr = acc.batch.map(lambda b: wm[b - 1] if b >= 1 else wm[0])
    keep = acc[acc.et >= late_thr].drop_duplicates("id").assign(ws=lambda x: x.et.dt.floor("1min"))
    keep = keep[keep.ws + pd.Timedelta(minutes=1) <= wm[nb]]
    exp = keep.groupby(["campaign_id", "ws"]).agg(impressions=("id", "size"), clicks=("is_click", "sum"),
                                                  conv=("conv", "sum"), spend=("cost", "sum")).reset_index()
    got = _q(streamed["db"], "select campaign_id, window_start ws, impressions, clicks, "
                             "conversion_path_impressions conv, spend::double spend from CAMPAIGN_MINUTE_METRICS")
    m = got.merge(exp, on=["campaign_id", "ws"], how="outer", suffixes=("", "_exp"), indicator=True)
    assert (m._merge == "both").all(), m[m._merge != "both"].head()
    assert (m.impressions == m.impressions_exp).all() and (m.clicks == m.clicks_exp).all()
    assert (m.conv == m.conv_exp).all() and (m.spend - m.spend_exp).abs().max() < 1e-9
    # late rows were really dropped from the aggregates (and counted by the listener)
    dropped = _q(streamed["db"], "select sum(rows_dropped_by_watermark) n from STREAM_QUERY_PROGRESS "
                                 "where query_name = 'campaign_minutes'").n[0]
    assert dropped > 0


# -------------------------------------------------------------------- observability
def test_batch_metrics_account_for_every_record(streamed):
    m = _q(streamed["db"], "select * from STREAM_BATCH_METRICS where query_name = 'raw_impressions'")
    assert len(m) == N_FILES
    assert m.rows_in.sum() == len(streamed["records"])
    assert (m.rows_valid + m.rows_dlq == m.rows_in).all()
    assert m.latency_p50_s.notna().all() and (m.latency_p50_s >= 0).all()
    assert (m.latency_p95_s >= m.latency_p50_s).all()


# --------------------------------------------------------------- delivery semantics
def test_restart_from_checkpoint_processes_nothing_new(streamed):
    before = _q(streamed["db"], "select count(*) n from STREAM_BATCH_METRICS").n[0]
    _run_stream(streamed["dir"] / "kafka", streamed["db"], streamed["dir"] / "ckpt", streamed["campaigns"])
    after = _q(streamed["db"], "select count(*) n from STREAM_BATCH_METRICS").n[0]
    assert after == before                    # offsets were checkpointed: no batch re-ran


def test_full_replay_without_checkpoint_is_a_no_op_in_the_warehouse(streamed):
    """Simulates losing the checkpoint (worst case for at-least-once delivery):
    every event is re-delivered, and MERGE keeps the warehouse unchanged."""
    tables = ["IMPRESSIONS_STREAM", "IMPRESSIONS_DLQ", "CAMPAIGN_MINUTE_METRICS"]
    snap = lambda: {t: _q(streamed["db"], f"select count(*) n from {t}").n[0] for t in tables}  # noqa: E731
    agg = lambda: _q(streamed["db"], "select sum(impressions) s, sum(clicks) c from CAMPAIGN_MINUTE_METRICS")  # noqa: E731
    before, agg_before = snap(), agg()
    _run_stream(streamed["dir"] / "kafka", streamed["db"], streamed["dir"] / "ckpt_lost", streamed["campaigns"])
    assert snap() == before
    pd.testing.assert_frame_equal(agg(), agg_before)


# ------------------------------------------------------------------------ sinks
def test_merge_sql():
    sql = sinks.merge_sql("T", "S", ["a", "b", "c"], ["a"], update=True)
    assert "merge into T t using S s on t.a = s.a" in sql
    assert "when matched then update set b = s.b, c = s.c" in sql
    assert "when not matched then insert (a, b, c) values (s.a, s.b, s.c)" in sql
    assert "when matched" not in sinks.merge_sql("T", "S", ["a", "b"], ["a"], update=False)


class _FakeCursor:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on, self.rowcount = log, fail_on, 3

    def execute(self, sql):
        self.log.append(sql.split()[0].lower() if not sql.startswith("merge") else "merge")
        if self.fail_on and sql.startswith(self.fail_on):
            raise RuntimeError("boom")

    def close(self):
        pass


class _FakeCon:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on = log, fail_on

    def cursor(self):
        return _FakeCursor(self.log, self.fail_on)


def _snowflake_sink(log, fail_on=None, monkeypatch=None):
    for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"):
        monkeypatch.setenv(k, "x")
    return sinks.SnowflakeSink(connect=lambda **kw: _FakeCon(log, fail_on),
                               write_pandas=lambda con, df, table, **kw: log.append(f"write_pandas:{table}:{len(df)}"))


def test_snowflake_sink_stages_then_merges_in_one_transaction(monkeypatch):
    log = []
    sink = _snowflake_sink(log, monkeypatch=monkeypatch)
    df = pd.DataFrame({"impression_id": ["a", "b"]})
    assert sink.upsert("IMPRESSIONS_STREAM", df) == 3
    assert log == ["begin", "create", "truncate", "write_pandas:IMPRESSIONS_STREAM_STG:2", "merge", "commit"]


def test_snowflake_sink_rolls_back_on_failure(monkeypatch):
    log = []
    sink = _snowflake_sink(log, fail_on="merge", monkeypatch=monkeypatch)
    with pytest.raises(RuntimeError):
        sink.upsert("IMPRESSIONS_STREAM", pd.DataFrame({"impression_id": ["a"]}))
    assert log[-1] == "rollback" and "commit" not in log


def test_sink_tables_match_snowflake_ddl():
    ddl = open(os.path.join(ROOT, "include", "snowflake", "03_streaming.sql")).read().lower()
    for table, spec in sinks.TABLES.items():
        m = re.search(rf"create table if not exists {table.lower()} \((.*?)\n\)", ddl, flags=re.S)
        assert m, f"{table} missing from 03_streaming.sql"
        body = re.sub(r"--[^\n]*", "", m.group(1))
        cols = []
        for part in re.split(r",\s*(?![^()]*\))", body):
            tok = part.strip().split()
            if tok and tok[0] != "constraint":
                cols.append(tok[0])
        assert cols == sinks.columns(table), table
