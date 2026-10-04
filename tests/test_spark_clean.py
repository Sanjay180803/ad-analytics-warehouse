"""End-to-end test of landing + Spark cleaning on a small synthetic Criteo file.

    pip install pyspark pandas numpy pytest
    pytest tests/test_spark_clean.py -q
"""

import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "include", "spark"))


@pytest.fixture(scope="module")
def landed(tmp_path_factory):
    d = tmp_path_factory.mktemp("criteo")
    src = d / "sample.tsv.gz"
    scripts = os.path.join(ROOT, "include", "scripts")
    subprocess.run([sys.executable, f"{scripts}/generate_sample_data.py", "--rows", "60000",
                    "--out", str(src)], check=True)
    subprocess.run([sys.executable, f"{scripts}/land_raw_to_s3.py", "--src", str(src),
                    "--dest", str(d / "landing")], check=True)
    return d


def test_landing_partitions_by_day(landed):
    days = sorted(p for p in os.listdir(landed / "landing" / "impressions") if p.startswith("dt="))
    assert len(days) == 30
    assert days[0] == "dt=2025-01-01" and days[-1] == "dt=2025-01-30"
    assert os.path.exists(landed / "landing" / "impressions" / days[0] / "_SUCCESS")


@pytest.mark.parametrize("mode", ["baseline", "tuned"])
def test_clean_one_day(landed, mode):
    import clean_impressions
    from pyspark.sql import SparkSession

    out = landed / f"clean_{mode}"
    m = clean_impressions.run(SimpleNamespace(
        raw_root=str(landed / "landing"), clean_root=str(out), dt="2025-01-03", all=False,
        anchor_date="2025-01-01", mode=mode, metrics_out=None))

    assert m["rows_out"] > 0
    assert m["rows_in"] == m["rows_out"] + m["rows_quarantined"] + m["duplicates_removed"]

    spark = SparkSession.builder.master("local[1]").getOrCreate()
    df = spark.read.parquet(str(out / "impressions"))
    assert df.select("dt").distinct().count() == 1
    assert df.count() == df.select("impression_id").distinct().count(), "duplicates survived"
    assert df.filter("user_id is null or campaign_id is null or event_ts is null").count() == 0
    # -1 sentinels became NULLs
    assert df.filter("conversion_id = -1 or cpo = -1").count() == 0
    if mode == "tuned":
        assert m["files_out"] == m["files_per_day"]
    spark.stop()


def test_rerun_is_idempotent(landed):
    import clean_impressions

    args = SimpleNamespace(raw_root=str(landed / "landing"), clean_root=str(landed / "idem"),
                           dt="2025-01-04", all=False, anchor_date="2025-01-01",
                           mode="tuned", metrics_out=None)
    first = clean_impressions.run(args)
    second = clean_impressions.run(args)
    assert first["rows_out"] == second["rows_out"]
    assert first["files_out"] == second["files_out"]
