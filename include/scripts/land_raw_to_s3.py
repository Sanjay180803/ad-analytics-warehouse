"""Step 1 – land the raw Criteo file in S3, partitioned by date.

The Criteo file has relative timestamps (seconds since the first impression), so we
anchor them to a real calendar date (CRITEO_ANCHOR_DATE, default 2025-01-01) to get
30 daily partitions. Values are otherwise landed exactly as received (strings,
tab-separated, gzipped). Cleaning is Spark's job, not the landing job's.

Layout produced:

    s3://<bucket>/raw/criteo/impressions/dt=2025-01-01/part-00000.tsv.gz
    s3://<bucket>/raw/criteo/impressions/dt=2025-01-01/_SUCCESS
    ...
    s3://<bucket>/raw/criteo/impressions/_rejected/part-00000.tsv.gz   (unparseable timestamp)
    s3://<bucket>/raw/criteo/campaign_changes/campaign_changes.csv

Why several gzip parts per day: gzip is not splittable, so one big .gz per day
means one Spark task per day no matter how many cores you have. Rotating parts at
~ROWS_PER_PART rows lets Spark read a day in parallel.

The campaign change log is simulated (Criteo campaigns have no attributes). It gives
dim_campaign real SCD Type 2 history to model; see README "Data caveats".

Usage:
    python include/scripts/land_raw_to_s3.py --src data/criteo_attribution_dataset.tsv.gz \
        --dest s3://my-bucket/raw/criteo
    # local dry run (writes to a folder instead of S3):
    python include/scripts/land_raw_to_s3.py --src data/sample.tsv.gz --dest data/landing
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import random
import shutil
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd

ROWS_PER_PART = 400_000  # ~60 MB uncompressed per part on the real file


def _anchor(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


class PartWriter:
    """Writes gzip parts per dt, rotating every ROWS_PER_PART rows."""

    def __init__(self, root: str, header: list[str]):
        self.root, self.header = root, header
        self.files: dict[str, tuple[gzip.GzipFile, int, int]] = {}

    def write(self, key: str, df: pd.DataFrame) -> None:
        while len(df):
            fh, part, n = self.files.get(key, (None, -1, ROWS_PER_PART))
            if fh is None or n >= ROWS_PER_PART:
                if fh is not None:
                    fh.close()
                part += 1
                d = os.path.join(self.root, key)
                os.makedirs(d, exist_ok=True)
                fh = gzip.open(os.path.join(d, f"part-{part:05d}.tsv.gz"), "wt", encoding="utf-8")
                fh.write("\t".join(self.header) + "\n")
                n = 0
            take = min(ROWS_PER_PART - n, len(df))
            df.iloc[:take].to_csv(fh, sep="\t", header=False, index=False)
            self.files[key] = (fh, part, n + take)
            df = df.iloc[take:]

    def close(self) -> list[str]:
        for fh, _, _ in self.files.values():
            fh.close()
        return sorted(self.files)


def simulate_campaign_changes(campaign_ids: list[str], anchor: datetime, days: int, seed: int) -> list[dict]:
    """Initial state for every campaign + 0..3 attribute changes during the 30 days."""
    verticals = ["retail", "travel", "fashion", "electronics", "home", "auto", "finance", "gaming"]
    tiers = ["low", "medium", "high", "enterprise"]
    strategies = ["manual_cpc", "target_cpa", "target_roas", "max_conversions"]
    out = []
    for cid in sorted(campaign_ids, key=lambda x: int(x)):
        rng = random.Random(f"{seed}-{cid}")
        state = {
            "campaign_id": cid,
            "advertiser_id": str(1000 + int(cid) % 97),
            "campaign_vertical": rng.choice(verticals),
            "budget_tier": rng.choice(tiers),
            "bid_strategy": rng.choice(strategies),
            "daily_budget_usd": rng.choice([50, 100, 250, 500, 1000, 5000]),
        }
        out.append({**state, "changed_at": anchor.isoformat(), "change_reason": "created"})
        change_days = sorted(rng.sample(range(1, days), k=rng.choice([0, 0, 1, 1, 2, 3])))
        for d in change_days:
            field = rng.choice(["budget_tier", "bid_strategy", "daily_budget_usd"])
            if field == "budget_tier":
                state["budget_tier"] = rng.choice([t for t in tiers if t != state["budget_tier"]])
            elif field == "bid_strategy":
                state["bid_strategy"] = rng.choice([s for s in strategies if s != state["bid_strategy"]])
            else:
                state["daily_budget_usd"] = int(state["daily_budget_usd"] * rng.choice([0.5, 1.5, 2]))
            ts = anchor + timedelta(days=d, seconds=rng.randint(0, 86_399))
            out.append({**state, "changed_at": ts.isoformat(), "change_reason": f"{field}_change"})
    return out


def upload_tree(local_root: str, dest: str) -> None:
    if not dest.startswith("s3://"):
        if os.path.abspath(local_root) != os.path.abspath(dest):
            shutil.copytree(local_root, dest, dirs_exist_ok=True)
        return
    import boto3

    bucket, _, prefix = dest[5:].partition("/")
    s3 = boto3.client("s3")
    # upload data first, _SUCCESS markers last, so a sensor never sees a half-written day
    files, markers = [], []
    for dirpath, _, names in os.walk(local_root):
        for n in names:
            (markers if n == "_SUCCESS" else files).append(os.path.join(dirpath, n))
    for path in sorted(files) + sorted(markers):
        key = f"{prefix.rstrip('/')}/{os.path.relpath(path, local_root)}".replace(os.sep, "/")
        s3.upload_file(path, bucket, key)
        print(f"uploaded s3://{bucket}/{key}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True)
    p.add_argument("--dest", required=True, help="s3://bucket/raw/criteo or a local folder")
    p.add_argument("--anchor-date", default=os.getenv("CRITEO_ANCHOR_DATE", "2025-01-01"))
    p.add_argument("--chunksize", type=int, default=1_000_000)
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args()

    anchor = _anchor(a.anchor_date)
    work = tempfile.mkdtemp(prefix="criteo_landing_") if a.dest.startswith("s3://") else a.dest
    imp_root = os.path.join(work, "impressions")
    writer = None
    campaigns: set[str] = set()
    total = rejected = 0

    reader = pd.read_csv(a.src, sep="\t", dtype=str, keep_default_na=False, chunksize=a.chunksize)
    for chunk in reader:
        if writer is None:
            writer = PartWriter(imp_root, list(chunk.columns))
        total += len(chunk)
        secs = pd.to_numeric(chunk["timestamp"], errors="coerce")
        dt = (pd.Timestamp(anchor) + pd.to_timedelta(secs, unit="s")).dt.strftime("%Y-%m-%d")
        bad = secs.isna()
        if bad.any():
            rejected += int(bad.sum())
            writer.write("_rejected", chunk[bad])
        good, dt = chunk[~bad], dt[~bad]
        campaigns.update(c for c in good["campaign"].unique() if c.isdigit())
        for day, part in good.groupby(dt, sort=True):
            writer.write(f"dt={day}", part)

    keys = writer.close() if writer else []
    for k in keys:
        if k.startswith("dt="):
            open(os.path.join(imp_root, k, "_SUCCESS"), "w").close()

    changes = simulate_campaign_changes(sorted(campaigns), anchor, 30, a.seed)
    cc_dir = os.path.join(work, "campaign_changes")
    os.makedirs(cc_dir, exist_ok=True)
    with open(os.path.join(cc_dir, "campaign_changes.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(changes[0].keys()))
        w.writeheader()
        w.writerows(changes)

    print(f"rows={total:,} rejected={rejected:,} days={sum(k.startswith('dt=') for k in keys)} "
          f"campaigns={len(campaigns)} campaign_change_rows={len(changes)}")
    upload_tree(work, a.dest)
    if a.dest.startswith("s3://"):
        shutil.rmtree(work)


if __name__ == "__main__":
    main()
