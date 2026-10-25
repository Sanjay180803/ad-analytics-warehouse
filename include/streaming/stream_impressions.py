"""Spark Structured Streaming: Kafka -> contract validation -> Snowflake.

Two streaming queries read the topic, each with its own checkpoint:

  raw_impressions   stateless. Parses every event against the data contract,
                    splits valid rows from rejects, and per micro-batch
                    (foreachBatch) MERGEs valid rows into RAW.CRITEO.IMPRESSIONS_STREAM
                    keyed on impression_id and rejects into IMPRESSIONS_DLQ keyed
                    on (topic, partition, offset). Never drops data: duplicates
                    and late events are resolved by the idempotent MERGE.

  campaign_minutes  stateful. Valid events only, 10-minute event-time watermark,
                    dropDuplicatesWithinWatermark on impression_id, 1-minute
                    tumbling windows per campaign, append mode (each window is
                    emitted once, when final), MERGEd into CAMPAIGN_MINUTE_METRICS.
                    Late-data rule, precisely: Spark drops a row as late when its
                    event time is older than the watermark of the PREVIOUS
                    micro-batch (max event time seen so far minus 10 minutes, as
                    of the batch before). Rows between the previous and current
                    watermark are still accepted, and their windows cannot have
                    been emitted yet, so no window is ever emitted twice. Dropped
                    rows are counted in STREAM_QUERY_PROGRESS, and they still land
                    in the raw table, which dbt and the batch path treat as truth.

Delivery semantics: Kafka offsets are tracked in the checkpoint and foreachBatch
is at-least-once; MERGE on natural keys turns replays into no-ops, so the
warehouse sees each event's effect exactly once.

    # real Kafka + Snowflake
    python include/streaming/stream_impressions.py --bootstrap localhost:9092 \
        --sink snowflake --checkpoint-root s3://bucket/checkpoints/impressions_stream

    # local, no broker or Snowflake (Kafka-shaped files from producer.py --to-files)
    python include/streaming/stream_impressions.py --source-dir data/kafka_files \
        --sink duckdb --duckdb-path data/stream.duckdb --available-now
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "spark"))
from common import build_spark  # noqa: E402
from contract import Contract, load_contract  # noqa: E402
from sinks import Sink, columns, make_sink  # noqa: E402

KAFKA_SCHEMA = T.StructType([
    T.StructField("key", T.BinaryType()), T.StructField("value", T.BinaryType()),
    T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()), T.StructField("timestamp", T.TimestampType()),
    T.StructField("timestampType", T.IntegerType()),
])
CORRUPT = "_corrupt_record"


def kafka_package() -> str:
    import pyspark

    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


# --------------------------------------------------------------------------- #
# Sources
# --------------------------------------------------------------------------- #
def read_source(spark: SparkSession, a, contract: Contract) -> DataFrame:
    if a.bootstrap:
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", a.bootstrap)
                .option("subscribe", a.topic or contract.topic)
                .option("startingOffsets", a.starting_offsets)
                .option("maxOffsetsPerTrigger", a.max_offsets_per_trigger)
                .option("failOnDataLoss", "true")
                .load())
    # Kafka-shaped Parquet files: identical columns to the Kafka source, one file per micro-batch
    return spark.readStream.schema(KAFKA_SCHEMA).option("maxFilesPerTrigger", 1).parquet(a.source_dir)


def load_campaigns(spark: SparkSession, path: str | None) -> DataFrame | None:
    if not path:
        return None
    return (spark.read.option("header", "true").csv(path)
            .select(F.col("campaign_id").cast("bigint").alias("campaign_id")).distinct()
            .withColumn("_known_campaign", F.lit(True)))


# --------------------------------------------------------------------------- #
# Contract enforcement
# --------------------------------------------------------------------------- #
def parse_and_validate(kafka_df: DataFrame, contract: Contract, campaigns: DataFrame | None,
                       max_skew_s: int | None = None) -> DataFrame:
    """Parse JSON values against the contract and attach `reject_reason`
    (NULL = valid). Checks run in a fixed order, and the first failure wins:
      invalid_json -> type_mismatch -> unsupported_schema_version ->
      missing_required:<field> -> constraint:<field>:<rule> -> quality:<rule>
    """
    fields = [f.name for f in contract.fields]
    if max_skew_s is None:
        rule = next((r for r in contract.raw.get("quality_rules", []) if r["name"] == "not_from_future"), {})
        max_skew_s = int(rule.get("max_skew_seconds", 300))

    parsed = (kafka_df
              .withColumn("_raw", F.col("value").cast("string"))
              .withColumn("_j", F.from_json("_raw", contract.spark_schema(CORRUPT),
                                            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT}))
              .select("*", *[F.col(f"_j.{c}").alias(c) for c in fields + [CORRUPT]]).drop("_j"))

    all_null = F.lit(True)
    for c in fields:
        all_null = all_null & F.col(c).isNull()
    reason = F.when(F.col(CORRUPT).isNotNull() & all_null, F.lit("invalid_json"))
    reason = reason.when(F.col(CORRUPT).isNotNull(), F.lit("type_mismatch"))
    reason = reason.when(F.split(F.col("schema_version"), r"\.").getItem(0).cast("int") != F.lit(contract.major),
                         F.lit("unsupported_schema_version"))
    for c in contract.required:
        reason = reason.when(F.col(c).isNull(), F.lit(f"missing_required:{c}"))
    for f in contract.fields:
        if "minimum" in f.constraints:
            reason = reason.when(F.col(f.name) < F.lit(f.constraints["minimum"]), F.lit(f"constraint:{f.name}:minimum"))
        if "pattern" in f.constraints:
            reason = reason.when(~F.col(f.name).rlike(f.constraints["pattern"]), F.lit(f"constraint:{f.name}:pattern"))
    if campaigns is not None:
        parsed = parsed.join(F.broadcast(campaigns), "campaign_id", "left")
        reason = reason.when(F.col("_known_campaign").isNull(), F.lit("quality:campaign_known"))
    reason = reason.when(F.col("event_ts") > F.col("produced_at") + F.expr(f"INTERVAL {max_skew_s} SECONDS"),
                         F.lit("quality:not_from_future"))
    out = parsed.withColumn("reject_reason", reason)
    return out.drop("_known_campaign") if campaigns is not None else out


# --------------------------------------------------------------------------- #
# Micro-batch writers
# --------------------------------------------------------------------------- #
def _to_pandas(df: DataFrame, cols: list[str]) -> pd.DataFrame:
    pdf = df.select(*cols).toPandas()
    for c in pdf.columns:      # Spark hands back tz-naive UTC already (session TZ = UTC)
        if isinstance(pdf[c].dtype, pd.DatetimeTZDtype):
            pdf[c] = pdf[c].dt.tz_convert("UTC").dt.tz_localize(None)
    return pdf


def raw_writer(sink: Sink, query_name: str = "raw_impressions"):
    imp_cols, dlq_cols = columns("IMPRESSIONS_STREAM"), columns("IMPRESSIONS_DLQ")

    def write(batch: DataFrame, batch_id: int):
        batch = batch.persist()
        now = F.current_timestamp()
        valid = batch.filter(F.col("reject_reason").isNull())
        n_in, n_valid = batch.count(), valid.count()
        deduped = valid.dropDuplicates(["impression_id"])
        n_dedup = deduped.count()
        rows = deduped.select(
            *[c for c in imp_cols if not c.startswith("_") and c != "dt"],
            F.to_date("event_ts").alias("dt"),
            F.col("topic").alias("_kafka_topic"), F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"), F.col("timestamp").alias("_kafka_ts"),
            now.alias("_ingested_at"))
        imp = _to_pandas(rows, imp_cols)
        sink.upsert("IMPRESSIONS_STREAM", imp, update=False)

        rej = batch.filter(F.col("reject_reason").isNotNull()).select(
            F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("kafka_key"), F.col("_raw").alias("raw_value"),
            "reject_reason", "schema_version", now.alias("_ingested_at"))
        dlq = _to_pandas(rej, dlq_cols)
        sink.upsert("IMPRESSIONS_DLQ", dlq, update=False)

        # pipeline latency: Kafka broker append time -> row committed to the warehouse
        # (meaningful for live traffic and replays alike, unlike event_ts)
        lat = (imp["_ingested_at"] - imp["_kafka_ts"]).dt.total_seconds() if len(imp) else pd.Series(dtype=float)
        sink.upsert("STREAM_BATCH_METRICS", pd.DataFrame([{
            "query_name": query_name, "batch_id": batch_id, "rows_in": n_in, "rows_valid": n_valid,
            "rows_dlq": len(dlq), "rows_duplicate_in_batch": n_valid - n_dedup,
            "min_event_ts": imp["event_ts"].min() if len(imp) else None,
            "max_event_ts": imp["event_ts"].max() if len(imp) else None,
            "latency_p50_s": float(lat.quantile(0.5)) if len(lat) else None,
            "latency_p95_s": float(lat.quantile(0.95)) if len(lat) else None,
            "dlq_rate": (len(dlq) / n_in) if n_in else 0.0,
            "_recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }]), update=True)
        batch.unpersist()

    return write


def campaign_minutes(valid: DataFrame, watermark: str = "10 minutes", window: str = "1 minute") -> DataFrame:
    return (valid.withWatermark("event_ts", watermark)
            .dropDuplicatesWithinWatermark(["impression_id"])
            .groupBy(F.window("event_ts", window).alias("w"), "campaign_id")
            .agg(F.count(F.lit(1)).alias("impressions"),
                 F.sum(F.col("is_click").cast("long")).alias("clicks"),
                 F.sum(F.col("is_conversion_path").cast("long")).alias("conversion_path_impressions"),
                 F.sum("cost").cast("decimal(28,8)").alias("spend"),
                 F.approx_count_distinct("user_id").alias("approx_users"))
            .select("campaign_id", F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                    "impressions", "clicks", "conversion_path_impressions", "spend", "approx_users"))


def minutes_writer(sink: Sink):
    cols = columns("CAMPAIGN_MINUTE_METRICS")

    def write(batch: DataFrame, batch_id: int):
        pdf = _to_pandas(batch.withColumn("_updated_at", F.current_timestamp()), cols)
        sink.upsert("CAMPAIGN_MINUTE_METRICS", pdf, update=True)

    return write


class ProgressRecorder(StreamingQueryListener):
    """Writes every micro-batch's progress (throughput, duration, rows dropped by
    the watermark, state size) to STREAM_QUERY_PROGRESS for monitoring."""

    def __init__(self, sink: Sink):
        self.sink = sink

    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        ops = p.get("stateOperators") or []
        self.sink.upsert("STREAM_QUERY_PROGRESS", pd.DataFrame([{
            "query_id": p["id"], "query_name": p.get("name"), "batch_id": p["batchId"],
            "num_input_rows": p.get("numInputRows", 0),
            "input_rows_per_s": float(p.get("inputRowsPerSecond") or 0.0),
            "processed_rows_per_s": float(p.get("processedRowsPerSecond") or 0.0),
            "batch_duration_ms": (p.get("durationMs") or {}).get("triggerExecution"),
            "rows_dropped_by_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
            "state_rows": sum(o.get("numRowsTotal", 0) for o in ops),
            "watermark": (p.get("eventTime") or {}).get("watermark"),
            "_recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }]), update=True)


# --------------------------------------------------------------------------- #
def start(spark: SparkSession, a, sink: Sink, contract: Contract | None = None):
    contract = contract or load_contract()
    campaigns = load_campaigns(spark, a.campaigns)
    parsed = parse_and_validate(read_source(spark, a, contract), contract, campaigns)
    valid = parsed.filter(F.col("reject_reason").isNull())

    def trigger(w):
        return w.trigger(availableNow=True) if a.available_now else w.trigger(processingTime=f"{a.trigger_seconds} seconds")

    q_raw = trigger(parsed.writeStream.queryName("raw_impressions")
                    .option("checkpointLocation", f"{a.checkpoint_root}/raw_impressions")
                    .foreachBatch(raw_writer(sink))).start()
    q_min = trigger(campaign_minutes(valid, a.watermark).writeStream.queryName("campaign_minutes")
                    .outputMode("append")
                    .option("checkpointLocation", f"{a.checkpoint_root}/campaign_minutes")
                    .foreachBatch(minutes_writer(sink))).start()
    return [q_raw, q_min]


def run(a) -> list:
    """Start both queries; with --available-now, block until all data is processed."""
    conf = {"spark.sql.shuffle.partitions": str(a.shuffle_partitions),
            "spark.sql.streaming.stateStore.providerClass":
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"}
    if a.bootstrap:
        conf["spark.jars.packages"] = kafka_package()
    spark = build_spark("stream_impressions", uses_s3=a.checkpoint_root.startswith("s3"), conf=conf)
    sink = make_sink(a.sink, a.duckdb_path)
    listener = ProgressRecorder(sink)
    spark.streams.addListener(listener)
    queries = start(spark, a, sink)
    try:
        if a.available_now:
            for q in queries:
                q.awaitTermination()
            time.sleep(1.0)            # let the async listener record the final progress events
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.streams.removeListener(listener)
        sink.close()
    for q in queries:
        if q.exception():
            raise RuntimeError(f"{q.name} failed: {q.exception()}")
    return queries


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--bootstrap", help="Kafka bootstrap servers")
    src.add_argument("--source-dir", help="Kafka-shaped Parquet files (producer.py --to-files)")
    ap.add_argument("--topic", default=None, help="defaults to the contract's topic")
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=200_000)
    ap.add_argument("--campaigns", default="data/landing/campaign_changes/campaign_changes.csv",
                    help="campaign change log CSV used for the campaign_known rule ('' to disable)")
    ap.add_argument("--sink", choices=["snowflake", "duckdb"], default="duckdb")
    ap.add_argument("--duckdb-path", default="data/stream_warehouse.duckdb")
    ap.add_argument("--checkpoint-root", default="data/checkpoints/impressions_stream")
    ap.add_argument("--watermark", default="10 minutes")
    ap.add_argument("--trigger-seconds", type=int, default=10)
    ap.add_argument("--available-now", action="store_true", help="process everything available, then stop")
    ap.add_argument("--shuffle-partitions", type=int, default=8)
    return ap


if __name__ == "__main__":
    run(parser().parse_args())
