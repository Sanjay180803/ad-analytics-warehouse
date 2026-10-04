# Spark tuning benchmark

Machine: local[*] with 14 cores. Scale factor: 1x. Median of the listed runs; join timings exclude a warm-up run.

## A. Cleaning job (all 30 days)

| Mode | Median runtime (s) | Output files | Rows out | Duplicates removed | Quarantined |
|---|---|---|---|---|---|
| baseline | 540.36 | 6200 | 16,468,027 | 0 | 0 |
| tuned | 344.18 | 31 | 16,468,027 | 0 | 0 |

Speedup: **1.57x**, files reduced 6200 → 31.

## B. Skewed join (impressions ⋈ campaign-day attributes)

Skew: top campaign = 2.7% of 16,468,027 impressions; top 10 = 18.6%. 23 hot campaigns salted into 16 buckets.

| Strategy | Median (s) | Runs (s) |
|---|---|---|
| sort_merge | 11.57 | 11.57 |
| salted | 13.73 | 13.73 |
| aqe_skew_join | 10.44 | 10.44 |
| broadcast | 3.94 | 3.94 |

Interpretation notes go here after you run it on the real data (which strategy won, and why; see README).
