"""Measure the Spark tuning: before/after runtimes you can put in your README.

Part A – cleaning job, baseline vs tuned (see clean_impressions.py for what differs).
Part B – the skewed join. Impressions are joined to a campaign x day attribute table
on (campaign_id, dt). Criteo campaigns are extremely skewed, so a plain sort-merge
join sends the hottest campaign-days to a handful of tasks. Four strategies:

  sort_merge      AQE off, broadcast off        -> straggler tasks on hot keys
  salted          AQE off, broadcast off, hot keys split into N salt buckets
  aqe_skew_join   AQE on with skew-join splitting, broadcast off
  broadcast       small side broadcast, no shuffle of impressions at all

At Criteo's size the attribute table is tiny, so broadcast should win. Salting is
what you reach for when the "small" side is too big to broadcast (millions of rows)
and AQE's automatic skew split is not enough or not available. Use --scale to blow
the impressions up and see where strategies diverge.

Usage:
  python include/spark/benchmark.py --raw-root data/landing --clean-root data/bench \
      --repeats 3 --scale 1 --out docs/benchmark_results.md
"""

from __future__ import annotations

import argparse
import os
import shutil
import statistics
import sys
import time
from types import SimpleNamespace

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import clean_impressions  # noqa: E402
from common import build_spark, to_spark_path, write_json  # noqa: E402

JOIN_STRATEGIES = ["sort_merge", "salted", "aqe_skew_join", "broadcast"]


def _rm(path: str) -> None:
    if not path.startswith(("s3://", "s3a://")) and os.path.exists(path):
        shutil.rmtree(path)


def bench_clean(raw_root: str, clean_root: str, anchor: str, repeats: int) -> dict:
    out = {}
    for mode in ("baseline", "tuned"):
        runs = []
        for _ in range(repeats):
            target = f"{clean_root}/clean_{mode}"
            _rm(target)
            m = clean_impressions.run(SimpleNamespace(
                raw_root=raw_root, clean_root=target, dt=None, all=True,
                anchor_date=anchor, mode=mode, metrics_out=None))
            runs.append(m)
        out[mode] = {
            "median_total_s": statistics.median(r["timings_s"]["total"] for r in runs),
            "runs_s": [r["timings_s"]["total"] for r in runs],
            "files_out": runs[-1]["files_out"],
            "rows_out": runs[-1]["rows_out"],
            "duplicates_removed": runs[-1]["duplicates_removed"],
            "rows_quarantined": runs[-1]["rows_quarantined"],
        }
    return out


def campaign_day_attributes(spark, raw_root: str, days: list[str]):
    """Explode the change log into one row per campaign per day (as-of attributes)."""
    ch = (spark.read.option("header", "true").csv(f"{raw_root}/campaign_changes/campaign_changes.csv")
          .select(F.col("campaign_id").cast("bigint").alias("campaign_id"),
                  F.to_timestamp("changed_at").alias("changed_at"), "budget_tier", "bid_strategy"))
    cal = spark.createDataFrame([(d,) for d in days], "d string").select(F.to_date("d").alias("dt"))
    w = Window.partitionBy("campaign_id", "dt").orderBy(F.col("changed_at").desc())
    return (ch.crossJoin(cal)
            .filter(F.to_date("changed_at") <= F.col("dt"))
            .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "changed_at"))


def run_join(spark, strategy: str, imps, attrs, salt_buckets: int, hot_keys) -> float:
    spark.conf.set("spark.sql.adaptive.enabled", str(strategy in ("aqe_skew_join", "broadcast")).lower())
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", str(strategy == "aqe_skew_join").lower())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    if strategy == "broadcast":
        joined = imps.join(F.broadcast(attrs), ["campaign_id", "dt"], "left")
    elif strategy == "salted":
        hot = F.broadcast(hot_keys.withColumn("_hot", F.lit(True)))
        imps_s = (imps.join(hot, "campaign_id", "left")
                  .withColumn("_salt", F.when(F.col("_hot"), F.pmod(F.xxhash64("impression_id"), F.lit(salt_buckets)))
                              .otherwise(F.lit(0)))
                  .drop("_hot"))
        attrs_s = (attrs.join(hot, "campaign_id", "left")
                   .withColumn("_salt", F.explode(F.when(F.col("_hot"), F.sequence(F.lit(0), F.lit(salt_buckets - 1)))
                                                  .otherwise(F.array(F.lit(0)))))
                   .drop("_hot"))
        joined = imps_s.join(attrs_s, ["campaign_id", "dt", "_salt"], "left").drop("_salt")
    else:  # sort_merge / aqe_skew_join
        joined = imps.join(attrs, ["campaign_id", "dt"], "left")

    # a per-row computation after the join so work lands on the (skewed) join tasks
    result = joined.withColumn("w", F.length(F.concat_ws("|", "impression_id", "budget_tier", "bid_strategy")))
    t0 = time.perf_counter()
    result.write.format("noop").mode("overwrite").save()
    return round(time.perf_counter() - t0, 2)


def bench_join(raw_root: str, clean_path: str, repeats: int, scale: int, salt_buckets: int) -> dict:
    spark = build_spark("benchmark_skew_join", clean_path.startswith("s3a://"),
                        {"spark.sql.shuffle.partitions": "64"})
    imps = spark.read.parquet(f"{clean_path}/impressions").select("impression_id", "campaign_id", "dt", "cost")
    if scale > 1:
        imps = (imps.withColumn("_r", F.explode(F.sequence(F.lit(1), F.lit(scale))))
                .withColumn("impression_id", F.concat_ws("-", "impression_id", "_r")).drop("_r"))
    imps = imps.cache()
    n = imps.count()
    days = [r.dt.isoformat() for r in imps.select("dt").distinct().orderBy("dt").collect()]
    attrs = campaign_day_attributes(spark, raw_root, days).cache()
    attrs.count()

    by_c = imps.groupBy("campaign_id").count()
    top = by_c.orderBy(F.desc("count")).limit(10).collect()
    avg = n / max(1, by_c.count())
    hot_keys = by_c.filter(F.col("count") > 5 * avg).select("campaign_id").cache()
    skew = {
        "impressions": n,
        "campaigns": by_c.count(),
        "top1_share": round(top[0]["count"] / n, 4),
        "top10_share": round(sum(r["count"] for r in top) / n, 4),
        "hot_campaigns_salted": hot_keys.count(),
        "salt_buckets": salt_buckets,
    }

    results = {}
    for s in JOIN_STRATEGIES:
        run_join(spark, s, imps, attrs, salt_buckets, hot_keys)  # warm-up, discarded
        times = [run_join(spark, s, imps, attrs, salt_buckets, hot_keys) for _ in range(repeats)]
        results[s] = {"median_s": statistics.median(times), "runs_s": times}
        print(f"[join] {s}: {results[s]}")
    spark.stop()
    return {"skew": skew, "strategies": results}


def to_markdown(clean: dict, join: dict, cores: int, scale: int) -> str:
    lines = ["# Spark tuning benchmark", "",
             f"Machine: local[*] with {cores} cores. Scale factor: {scale}x. "
             "Median of the listed runs; join timings exclude a warm-up run.", "",
             "## A. Cleaning job (all 30 days)", "",
             "| Mode | Median runtime (s) | Output files | Rows out | Duplicates removed | Quarantined |",
             "|---|---|---|---|---|---|"]
    for mode in ("baseline", "tuned"):
        c = clean[mode]
        lines.append(f"| {mode} | {c['median_total_s']} | {c['files_out']} | {c['rows_out']:,} | "
                     f"{c['duplicates_removed']:,} | {c['rows_quarantined']:,} |")
    b, tt = clean["baseline"]["median_total_s"], clean["tuned"]["median_total_s"]
    lines += ["", f"Speedup: **{b / tt:.2f}x**, files reduced "
              f"{clean['baseline']['files_out']} → {clean['tuned']['files_out']}.", "",
              "## B. Skewed join (impressions ⋈ campaign-day attributes)", ""]
    sk = join["skew"]
    lines += [f"Skew: top campaign = {sk['top1_share']:.1%} of {sk['impressions']:,} impressions; "
              f"top 10 = {sk['top10_share']:.1%}. {sk['hot_campaigns_salted']} hot campaigns salted into "
              f"{sk['salt_buckets']} buckets.", "",
              "| Strategy | Median (s) | Runs (s) |", "|---|---|---|"]
    for s in JOIN_STRATEGIES:
        r = join["strategies"][s]
        lines.append(f"| {s} | {r['median_s']} | {', '.join(map(str, r['runs_s']))} |")
    lines += ["", "Interpretation notes go here after you run it on the real data "
              "(which strategy won, and why; see README)."]
    return "\n".join(lines) + "\n"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw-root", required=True)
    p.add_argument("--clean-root", required=True, help="scratch location for benchmark outputs")
    p.add_argument("--anchor-date", default=os.getenv("CRITEO_ANCHOR_DATE", "2025-01-01"))
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--scale", type=int, default=1)
    p.add_argument("--salt-buckets", type=int, default=16)
    p.add_argument("--out", default="docs/benchmark_results.md")
    a = p.parse_args()

    raw, clean = to_spark_path(a.raw_root), to_spark_path(a.clean_root)
    clean_res = bench_clean(raw, clean, a.anchor_date, a.repeats)
    join_res = bench_join(raw, f"{clean}/clean_tuned", a.repeats, a.scale, a.salt_buckets)

    md = to_markdown(clean_res, join_res, os.cpu_count() or 0, a.scale)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w") as f:
        f.write(md)
    write_json(a.out.replace(".md", ".json"), {"clean": clean_res, "join": join_res})
    print(md)


if __name__ == "__main__":
    main()
