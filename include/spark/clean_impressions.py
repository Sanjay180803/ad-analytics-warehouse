"""Step 2 – clean raw Criteo impressions with PySpark and write partitioned Parquet.

What it does, per landed day (or all days with --all):
  1. Reads the gzipped TSV parts with an explicit all-string schema.
  2. Casts every column with try_cast; rows that fail a required cast go to
     quarantine with a reason instead of becoming silent nulls.
  3. Validates campaign IDs against the campaign list with a broadcast join.
  4. Builds a deterministic impression_id and deduplicates on it.
  5. Writes Parquet partitioned by dt, sized to ~target MB per file and sorted
     by campaign_id/event_ts for better compression and pruning.

Two modes so you can measure the tuning:
  --mode baseline  AQE off, broadcast joins off, 200 shuffle partitions, no cache,
                   no output sizing  (what you get when you don't think about it)
  --mode tuned     AQE on, explicit broadcast, shuffle partitions sized to input,
                   cached typed frame, one right-sized file per day bucket

Usage:
  python include/spark/clean_impressions.py --raw-root s3://bucket/raw/criteo \
      --clean-root s3://bucket/clean/criteo --dt 2025-01-05 --mode tuned
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    RAW_COLUMNS, RAW_SCHEMA, build_spark, input_size_bytes, target_partitions, timed,
    to_spark_path, write_json,
)

# Measured on the real Criteo file: gzip TSV -> snappy Parquet is roughly 1.1x.
# Re-measure after your first full run and update (see README, "Partition sizing").
GZ_TO_PARQUET_RATIO = float(os.getenv("GZ_TO_PARQUET_RATIO", "1.1"))
TARGET_FILE_MB = int(os.getenv("TARGET_FILE_MB", "128"))


def tc(col: str, typ: str):
    """try_cast that tolerates surrounding whitespace."""
    return F.expr(f"try_cast(trim(`{col}`) as {typ})")


def neg1_to_null(expr, typ: str):
    """Criteo uses -1 as 'not applicable'. Model that as NULL."""
    return F.when(expr == F.lit(-1).cast(typ), F.lit(None).cast(typ)).otherwise(expr)


def read_raw(spark, raw_root: str, dt: str | None) -> DataFrame:
    base = f"{raw_root}/impressions"
    path = f"{base}/dt={dt}/" if dt else f"{base}/dt=*/"
    return (
        spark.read.option("header", "true").option("sep", "\t").option("mode", "PERMISSIVE")
        .option("pathGlobFilter", "*.tsv.gz")
        .schema(RAW_SCHEMA).csv(path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("source_dt", F.regexp_extract("source_file", r"dt=(\d{4}-\d{2}-\d{2})", 1))
    )


def cast_and_validate(df: DataFrame, anchor_epoch: int) -> DataFrame:
    ts_s = tc("timestamp", "bigint")
    conv_ts_s = neg1_to_null(tc("conversion_timestamp", "bigint"), "bigint")
    typed = df.select(
        # deterministic surrogate key: hash of every raw field
        F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("")) for c in RAW_COLUMNS]), 256)
        .alias("impression_id"),
        F.timestamp_seconds(F.lit(anchor_epoch) + ts_s).alias("event_ts"),
        F.when(tc("uid", "bigint").isNotNull(), F.trim("uid")).alias("user_id"),
        tc("campaign", "bigint").alias("campaign_id"),
        (tc("click", "int") == 1).alias("is_click"),
        (tc("conversion", "int") == 1).alias("is_conversion_path"),
        F.timestamp_seconds(F.lit(anchor_epoch) + conv_ts_s).alias("conversion_ts"),
        neg1_to_null(tc("conversion_id", "bigint"), "bigint").alias("conversion_id"),
        (tc("attribution", "int") == 1).alias("is_attributed"),
        neg1_to_null(tc("click_pos", "int"), "int").alias("click_pos"),
        neg1_to_null(tc("click_nb", "int"), "int").alias("click_nb"),
        tc("cost", "decimal(18,8)").alias("cost"),
        neg1_to_null(tc("cpo", "decimal(18,6)"), "decimal(18,6)").alias("cpo"),
        neg1_to_null(tc("time_since_last_click", "bigint"), "bigint").alias("time_since_last_click_s"),
        *[F.trim(F.col(f"cat{i}")).alias(f"cat{i}") for i in range(1, 10)],
        "source_file", "source_dt",
    )
    reason = (
        F.when(F.col("event_ts").isNull(), "bad_timestamp")
        .when(F.col("user_id").isNull(), "bad_uid")
        .when(F.col("campaign_id").isNull(), "bad_campaign")
        .when(F.col("is_click").isNull() | F.col("is_conversion_path").isNull(), "bad_flag")
        .when(F.col("cost").isNull() | (F.col("cost") < 0), "bad_cost")
    )
    return typed.withColumn("reject_reason", reason).withColumn(
        "dt", F.coalesce(F.to_date("event_ts"), F.to_date("source_dt"))
    )


def run(args) -> dict:
    tuned = args.mode == "tuned"
    raw_root, clean_root = to_spark_path(args.raw_root), to_spark_path(args.clean_root)
    uses_s3 = raw_root.startswith("s3a://") or clean_root.startswith("s3a://")

    conf = {
        "spark.sql.adaptive.enabled": str(tuned).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(tuned).lower(),
        "spark.sql.autoBroadcastJoinThreshold": "10485760" if tuned else "-1",
        "spark.sql.shuffle.partitions": "200",
    }
    spark = build_spark(f"clean_impressions_{args.mode}", uses_s3, conf)
    metrics: dict = {"mode": args.mode, "dt": args.dt or "ALL", "timings_s": {}}
    t = metrics["timings_s"]

    anchor_epoch = int(datetime.strptime(args.anchor_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    with timed("total", t):
        in_path = f"{raw_root}/impressions" + (f"/dt={args.dt}" if args.dt else "")
        in_bytes = input_size_bytes(spark, in_path)
        metrics["input_bytes"] = in_bytes
        n_days = 1 if args.dt else 30
        files_per_day = target_partitions(in_bytes // n_days, GZ_TO_PARQUET_RATIO, TARGET_FILE_MB)
        metrics["files_per_day"] = files_per_day if tuned else "unmanaged"

        if tuned:
            # shuffle partitions sized to the data instead of the 200 default;
            # never fewer than the cores we have, so nothing sits idle.
            cores = spark.sparkContext.defaultParallelism
            sp = max(cores, target_partitions(in_bytes, 3.0, 128))  # ~3x expansion in memory
            spark.conf.set("spark.sql.shuffle.partitions", str(sp))
            metrics["shuffle_partitions"] = sp

        raw = read_raw(spark, raw_root, args.dt)
        typed = cast_and_validate(raw, anchor_epoch)
        if tuned:
            # read + parse the gzip once; both the clean and quarantine writes reuse it
            typed = typed.persist(StorageLevel.MEMORY_AND_DISK)

        # campaign reference list (tiny) for referential validation at the edge
        campaigns = (
            spark.read.option("header", "true").csv(f"{raw_root}/campaign_changes/campaign_changes.csv")
            .select(F.col("campaign_id").cast("bigint").alias("campaign_id")).distinct()
            .withColumn("_known", F.lit(True))
        )
        if tuned:
            campaigns = F.broadcast(campaigns)
        checked = typed.join(campaigns, "campaign_id", "left").withColumn(
            "reject_reason",
            F.when(F.col("reject_reason").isNull() & F.col("_known").isNull(), F.lit("unknown_campaign"))
            .otherwise(F.col("reject_reason")),
        ).drop("_known")

        good = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason", "source_dt")
        bad = checked.filter(F.col("reject_reason").isNotNull())

        deduped = good.dropDuplicates(["impression_id"])

        if tuned:
            # one shuffle that both groups rows by day and caps files per day,
            # then sort inside each file so Parquet min/max stats are tight.
            deduped = (
                deduped.withColumn("_bucket", F.pmod(F.xxhash64("impression_id"), F.lit(files_per_day)))
                .repartition("dt", "_bucket")
                .sortWithinPartitions("campaign_id", "event_ts")
                .drop("_bucket")
            )

        with timed("write_clean", t):
            (deduped.write.mode("overwrite").partitionBy("dt")
             .parquet(f"{clean_root}/impressions"))
        with timed("write_quarantine", t):
            (bad.coalesce(1).write.mode("overwrite").partitionBy("dt")
             .parquet(f"{clean_root}/quarantine"))

        # metrics read back from what was written (cheap: Parquet footers + small files)
        sel = (lambda p: p.filter(F.col("dt") == args.dt)) if args.dt else (lambda p: p)
        written = sel(spark.read.parquet(f"{clean_root}/impressions"))
        metrics["rows_in"] = typed.count()
        metrics["rows_quarantined"] = bad.count()
        metrics["rows_out"] = written.count()
        metrics["duplicates_removed"] = metrics["rows_in"] - metrics["rows_quarantined"] - metrics["rows_out"]
        metrics["files_out"] = len(written.inputFiles())

    if tuned:
        typed.unpersist()
    spark.stop()
    return metrics


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw-root", required=True)
    p.add_argument("--clean-root", required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dt")
    g.add_argument("--all", action="store_true")
    p.add_argument("--anchor-date", default=os.getenv("CRITEO_ANCHOR_DATE", "2025-01-01"))
    p.add_argument("--mode", choices=["baseline", "tuned"], default="tuned")
    p.add_argument("--metrics-out", default=None, help="write run metrics JSON here")
    args = p.parse_args()

    m = run(args)
    print(m)
    if args.metrics_out:
        write_json(args.metrics_out, m)
    if m["rows_out"] == 0:
        sys.exit(f"no clean rows written for dt={args.dt}; refusing to report success")


if __name__ == "__main__":
    main()
