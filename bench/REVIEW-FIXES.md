# CRDB review fixes: S3 and F5

Follow-up branch `codex/ef-crdb-review-fixes` starts from integrated CRDB `0ef8863913314dbe1806fb361847019675ddeb68`. The reviewed findings are STANDARDS S3 (plot populations) and SPEC F5 (capacity signal propagation). Changes are limited to the Python report/parser, focused tests and documentation/evidence. Go scheduling and shared measurement coordination remain with the F2 owner.

## S3: preserve environment and repeat populations

The public `python -m crdb_bench.evaluation` CLI now retains the verified batch's `environment_hash` through report records, cell CSV, retry-layer CSV and per-cell evidence. Each plotted curve is identified by its existing scientific series, full environment hash and repeat. Points follow configured-load order while the x axis remains achieved throughput. Continuation cells in distinct environments never become one curve; individual repeats never become one connected trajectory.

Rendering uses the existing public `experiment_common.research_plot.plot_series` seam from integrated common. The adapter supplies native metric units and populations, groups the input records, and composes the returned SVG panels. It contains no axis, palette, legend or line renderer. This requires the integrated common package that provides `plot_series`; it adds no dependency.

The three separate `figure-7.8b-{metric}.svg` files accompany the composite `figure-7.8b.svg`. Null y values break lines through the common renderer. An entirely unavailable metric has `plots[metric].path=null` and a reason; its old panel file is removed if the same report directory is reused. JSON/CSV preserve null values, raw numerators, denominators and reasons. Calibration remains labelled observe-only and is not comparison-eligible.

## F5: propagate the observed client capacity limit

`parse_results` now emits the boolean `diagnostics.capacity_limited`. It is true if **any** parsed client has positive `capacity_wait_count` or positive `capacity_wait_ns`; existing aggregate raw counts and times remain available. A saturated second client therefore cannot be hidden by an unsaturated first client. The common selector already rejects this flag and applies its frozen extension/failure policy; no new selection or recovery policy is introduced. Capacity waiting remains excluded from execution latency, and SQL/KV ratios retain their original populations.

Coordination note for main/F2: the reviewed Go client at `0ef8863` counts waits from startup, including warmup. This Python fix neither edits `run.go` nor invents a new native schema. The F2 owner should retain `capacity_wait_count`/`capacity_wait_ns` and scope their values to the shared measurement interval. Legacy positive counters conservatively block automatic selection even if some waiting occurred during warmup; they are not relabelled as precisely measured-only counts. The regression fixtures have zero warmup. No measured-window correctness claim is made for the old Go counters.

## Verification

The public regressions first demonstrated the missing environment field and the selector accepting a capacity-limited plateau. The final focused suite ran **18 tests, all passing** against the integrated common checkout at `/home/lihan/Documents/code/cpi-ns/experiment-common`:

```sh
PYTHONPATH=bench:/absolute/path/to/integrated-common:bench/tests \
  bench/.venv/bin/python -m unittest test_review test_evaluation test_adapter -v
```

- The report CLI reads two hash-verified fixture batches containing eight cells: two environments, two repeats and two load points per curve. Each of the three panels exports four separate two-point curves. Fixtures are retained at `/tmp/crdb-review-s3-f5-populations`; sample output is [population-curves.svg](validation/review-s3-f5/population-curves.svg) and [population-cells.csv](validation/review-s3-f5/population-cells.csv).
- Public parsing plus common `select_workpoint` rejects a known plateau when either wait counter is positive; zero counters allow the same frozen rule to select load 20. A two-client fixture verifies aggregation when only the second client waits.
- A zero server-commit denominator remains null, with the raw 6/0 counters and a reason. Missing intermediate values break the plot, and re-exporting only unavailable CC ratios removes the stale panel.
- The existing native batch `/tmp/crdb-ticket11-native-2` was replayed through the public evaluation CLI into `/tmp/crdb-review-s3-f5-replay`. Its retry-layer evidence, layout and calibration are unchanged. The recorded environment and repeat now appear in exported artifacts. No database or cloud workload was started.

[Test output](validation/review-s3-f5/tests.log) and [replay/source proof](validation/review-s3-f5/proof.json) retain exact checks and hashes. Cached formatting and `git diff --check` pass. Mac remains untested. Existing historical results and source data were not rewritten.
