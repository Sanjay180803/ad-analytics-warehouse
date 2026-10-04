"""Generate a synthetic file in the exact Criteo Attribution format.

Use it to develop and test the pipeline without downloading the real 623 MB file.
It reproduces the properties that matter for the pipeline:

* tab-separated, gzipped, header row, sorted by `timestamp` (seconds from 0)
* heavily skewed campaign distribution (a few campaigns get most impressions),
  which is what makes salting worth demonstrating
* conversion timelines: several impressions share a conversion_id, exactly one
  of them may carry attribution=1
* injected exact-duplicate rows and malformed rows, so dedupe and quarantine
  logic actually has something to do

Usage:
    python include/scripts/generate_sample_data.py --rows 2000000 \
        --out data/criteo_attribution_dataset.tsv.gz
"""

from __future__ import annotations

import argparse
import gzip
import os

import numpy as np

COLUMNS = [
    "timestamp", "uid", "campaign", "conversion", "conversion_timestamp",
    "conversion_id", "attribution", "click", "click_pos", "click_nb", "cost",
    "cpo", "time_since_last_click",
    "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7", "cat8", "cat9",
]

THIRTY_DAYS = 30 * 24 * 3600


def generate(rows: int, n_campaigns: int, n_users: int, seed: int) -> list[list[str]]:
    rng = np.random.default_rng(seed)

    # Zipf-like campaign popularity: campaign 0 alone gets a big share.
    weights = 1.0 / np.arange(1, n_campaigns + 1) ** 1.3
    weights /= weights.sum()
    campaign_ids = rng.choice(np.arange(10_000, 10_000 + n_campaigns) * 7 + 3, size=n_campaigns, replace=False)
    campaigns = campaign_ids[rng.choice(n_campaigns, size=rows, p=weights)]

    ts = np.sort(rng.integers(0, THIRTY_DAYS, size=rows))
    uids = rng.integers(1, n_users, size=rows) * 13 + 5
    click = (rng.random(rows) < 0.36).astype(int)  # Criteo sample is click-heavy
    cost = np.round(rng.gamma(2.0, 0.0006, size=rows), 8)

    # Conversions: ~1% of impressions belong to a conversion timeline.
    conversion = (rng.random(rows) < 0.01).astype(int)
    conv_offset = rng.integers(60, 5 * 24 * 3600, size=rows)
    conv_ts = np.where(conversion == 1, ts + conv_offset, -1)
    # group converting impressions into ids of ~3 impressions each
    conv_id = np.full(rows, -1, dtype=np.int64)
    idx = np.flatnonzero(conversion)
    conv_id[idx] = 900_000 + idx // 3
    # one impression per timeline is attributed (the last clicked one in our toy version)
    attribution = np.zeros(rows, dtype=int)
    if len(idx):
        last_of_group = idx[np.r_[np.diff(conv_id[idx]) != 0, True]]
        attribution[last_of_group] = (rng.random(len(last_of_group)) < 0.6).astype(int)
        # conversions in a timeline share one conversion timestamp
        for start in range(0, len(idx), 3):
            grp = idx[start:start + 3]
            conv_ts[grp] = conv_ts[grp].max()
    click_pos = np.where(conversion == 1, rng.integers(0, 3, size=rows), -1)
    click_nb = np.where(conversion == 1, rng.integers(1, 4, size=rows), -1)
    cpo = np.where(attribution == 1, np.round(rng.gamma(2.0, 0.05, size=rows), 6), -1)
    tslc = np.where(rng.random(rows) < 0.4, rng.integers(1, 86_400, size=rows), -1)

    cats = [rng.integers(0, card, size=rows) * 101 + 17 for card in (6, 50, 300, 20, 1000, 40, 200, 10, 90)]

    out = []
    for i in range(rows):
        out.append([
            str(ts[i]), str(uids[i]), str(campaigns[i]), str(conversion[i]), str(conv_ts[i]),
            str(conv_id[i]), str(attribution[i]), str(click[i]), str(click_pos[i]), str(click_nb[i]),
            f"{cost[i]:.8f}", f"{cpo[i]:.6f}" if cpo[i] != -1 else "-1", str(tslc[i]),
            *[str(c[i]) for c in cats],
        ])

    # ~0.5% exact duplicates (same row emitted twice by an upstream retry)
    dup_idx = rng.choice(rows, size=max(1, rows // 200), replace=False)
    for i in dup_idx:
        out.insert(i, list(out[i]))
    # a handful of malformed rows that should land in quarantine
    for j in range(max(1, rows // 50_000)):
        bad = list(out[j * 7])
        bad[0] = "not_a_number" if j % 2 == 0 else bad[0]
        bad[2] = "" if j % 2 == 1 else bad[2]
        out.insert(j * 7 + 1, bad)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=500_000)
    p.add_argument("--campaigns", type=int, default=700)
    p.add_argument("--users", type=int, default=200_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/criteo_attribution_dataset.tsv.gz")
    a = p.parse_args()

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    rows = generate(a.rows, a.campaigns, a.users, a.seed)
    with gzip.open(a.out, "wt", encoding="utf-8") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(r) + "\n")
    print(f"wrote {len(rows):,} rows to {a.out}")


if __name__ == "__main__":
    main()
