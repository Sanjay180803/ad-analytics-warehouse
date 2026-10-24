# Spark tuning benchmark

Machine: local[4,4] (12 GB driver) on a 14-core Windows laptop. Scale factor: 1x. Single run per configuration; repeat with --repeats 3 or more before drawing firm conclusions. Join timings exclude a warm-up run.

## A. Cleaning job (all 31 days)

| Mode | Runtime (s) | Output files | Rows out | Duplicates removed | Quarantined |
|---|---|---|---|---|---|
| baseline | 540.36 | 6200 | 16,468,027 | 0 | 0 |
| tuned | 344.18 | 31 | 16,468,027 | 0 | 0 |

Speedup: **1.57x**, files reduced 6200 → 31.

## B. Skewed join (impressions ⋈ campaign-day attributes)

Skew: top campaign = 2.7% of 16,468,027 impressions; top 10 = 18.6%. 23 hot campaigns salted into 16 buckets.

| Strategy | Runtime (s) | Runs (s) |
|---|---|---|
| sort_merge | 11.57 | 11.57 |
| salted | 13.73 | 13.73 |
| aqe_skew_join | 10.44 | 10.44 |
| broadcast | 3.94 | 3.94 |

Interpretation: see "Spark tuning results" in the README.
