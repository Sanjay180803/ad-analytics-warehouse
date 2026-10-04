"""Shared Spark helpers: session builder with S3A wiring, schema, and timing."""

from __future__ import annotations

import glob
import json
import math
import os
import re
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql import types as T

# Every raw column is read as a string first. Casting happens explicitly in the
# job so that a bad value becomes a quarantined row instead of a silent null.
RAW_COLUMNS = [
    "timestamp", "uid", "campaign", "conversion", "conversion_timestamp",
    "conversion_id", "attribution", "click", "click_pos", "click_nb", "cost",
    "cpo", "time_since_last_click",
    "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7", "cat8", "cat9",
]
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in RAW_COLUMNS])


def _bundled_hadoop_version() -> str:
    """hadoop-aws must match the Hadoop jars shipped inside pyspark exactly."""
    import pyspark

    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"))
    m = re.search(r"hadoop-client-api-(.+)\.jar$", jars[0]) if jars else None
    return m.group(1) if m else "3.4.1"


def build_spark(app_name: str, uses_s3: bool, conf: dict[str, str] | None = None) -> SparkSession:
    b = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.session.timeZone", "UTC")
        # write only the partitions present in the DataFrame; never wipe other days
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Snowflake reads INT64 micros timestamps cleanly; Spark's legacy INT96 is a trap
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if uses_s3:
        b = (
            b.config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{_bundled_hadoop_version()}")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )
    for k, v in (conf or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def to_spark_path(p: str) -> str:
    return "s3a://" + p[5:] if p.startswith("s3://") else p


def input_size_bytes(spark: SparkSession, path: str) -> int:
    """Total bytes under a path, via the Hadoop FS API (works for local and s3a)."""
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    if not fs.exists(p):
        return 0
    return int(fs.getContentSummary(p).getLength())


def target_partitions(bytes_on_disk: int, compression_ratio: float, target_mb: int, minimum: int = 1) -> int:
    """Number of output files so each lands near target_mb of Parquet.

    bytes_on_disk is gzip TSV; compression_ratio converts it to an estimate of the
    Parquet size (measured once on the real data and kept in config).
    """
    est = bytes_on_disk * compression_ratio
    return max(minimum, math.ceil(est / (target_mb * 1024 * 1024)))


@contextmanager
def timed(label: str, results: dict):
    t0 = time.perf_counter()
    yield
    results[label] = round(time.perf_counter() - t0, 2)
    print(f"[timing] {label}: {results[label]}s")


def write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
