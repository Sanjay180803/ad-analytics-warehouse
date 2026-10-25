"""Real Kafka end to end: producer -> broker -> Spark Structured Streaming -> warehouse.

Runs only when KAFKA_BOOTSTRAP is set (CI starts a KRaft broker as a service
container; locally: docker compose -f docker-compose.streaming.yml up -d).

    KAFKA_BOOTSTRAP=localhost:9092 pytest tests/integration -q
"""
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "include", "streaming"))
sys.path.insert(0, os.path.join(ROOT, "include", "spark"))

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="set KAFKA_BOOTSTRAP to run against a real broker")
ANCHOR = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _q(path, sql):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def _create_topic(name, partitions=6):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[name].result(30)
    for _ in range(30):                                  # wait for metadata to propagate
        if name in admin.list_topics(timeout=5).topics:
            return
        time.sleep(1)
    raise TimeoutError(name)


def _stream(topic, db, ckpt, campaigns):
    import stream_impressions as si
    return si.run(si.parser().parse_args([
        "--bootstrap", BOOTSTRAP, "--topic", topic, "--campaigns", str(campaigns), "--sink", "duckdb",
        "--duckdb-path", str(db), "--checkpoint-root", str(ckpt), "--available-now",
        "--max-offsets-per-trigger", "3000", "--shuffle-partitions", "4"]))


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    d = tmp_path_factory.mktemp("kafka")
    scripts = os.path.join(ROOT, "include", "scripts")
    subprocess.run([sys.executable, f"{scripts}/generate_sample_data.py", "--rows", "12000",
                    "--out", str(d / "sample.tsv.gz")], check=True, capture_output=True)
    subprocess.run([sys.executable, f"{scripts}/land_raw_to_s3.py", "--src", str(d / "sample.tsv.gz"),
                    "--dest", str(d / "landing")], check=True, capture_output=True)
    topic = f"ad.impressions.v1.it-{uuid.uuid4().hex[:8]}"
    _create_topic(topic)
    return {"dir": d, "topic": topic, "db": d / "wh.duckdb", "ckpt": d / "ckpt",
            "campaigns": d / "landing" / "campaign_changes" / "campaign_changes.csv"}


def test_kafka_to_warehouse_then_incremental(env):
    import producer

    rows = list(producer.read_rows(str(env["dir"] / "sample.tsv.gz")))
    first, second = rows[:7000], rows[7000:]

    # ---- round 1: produce, stream everything available ------------------------
    recs1, _ = producer.build_stream(first, ANCHOR, fault_rate=0.02, dup_rate=0.02, seed=1)
    producer.send_to_kafka(recs1, BOOTSTRAP, env["topic"])
    _stream(env["topic"], env["db"], env["ckpt"], env["campaigns"])

    ok1 = {r.impression_id for r in recs1 if not r.expect.startswith("dlq")}
    raw = _q(env["db"], "select impression_id, _kafka_topic, _kafka_partition from IMPRESSIONS_STREAM")
    assert raw.impression_id.is_unique and set(raw.impression_id) == ok1
    assert set(raw._kafka_topic) == {env["topic"]}
    assert raw._kafka_partition.nunique() > 1               # keyed by campaign across partitions
    dlq = _q(env["db"], "select reject_reason, count(*) n from IMPRESSIONS_DLQ group by 1")
    assert dlq.n.sum() == sum(r.expect.startswith("dlq") for r in recs1)
    batches1 = _q(env["db"], "select count(*) n from STREAM_BATCH_METRICS").n[0]
    assert batches1 >= 3                                    # maxOffsetsPerTrigger split the backlog

    # ---- round 2: new events only; the checkpoint resumes from committed offsets
    recs2, _ = producer.build_stream(second, ANCHOR, fault_rate=0.02, seed=2)
    producer.send_to_kafka(recs2, BOOTSTRAP, env["topic"])
    _stream(env["topic"], env["db"], env["ckpt"], env["campaigns"])

    ok2 = {r.impression_id for r in recs2 if not r.expect.startswith("dlq")}
    raw2 = _q(env["db"], "select impression_id from IMPRESSIONS_STREAM")
    assert set(raw2.impression_id) == ok1 | ok2
    m = _q(env["db"], "select sum(rows_in) n from STREAM_BATCH_METRICS where query_name = 'raw_impressions'")
    assert m.n[0] == len(recs1) + len(recs2)                # every Kafka record read exactly once
    agg = _q(env["db"], "select count(*) n, sum(impressions) s from CAMPAIGN_MINUTE_METRICS")
    assert agg.n[0] > 0 and agg.s[0] > 0
