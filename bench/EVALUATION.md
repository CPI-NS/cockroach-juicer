# CRDB evaluation interface

The adapter provides layout placement, observe-only hold calibration, metric reconciliation and card exports. Repeated input draws remain intact. Native Mac operation requires separate validation.

## Configuring and inspecting the layout

Use these system fields with canonical workload names:

```json
{
  "topology": {
    "layout": {
      "hot_ranks": 16,
      "lease_nodes": ["n1", "n2", "n3"],
      "disable_rebalancing": true,
      "monitor_interval_seconds": 10
    },
    "metrics": {"require_retry_identity": false}
  },
  "juicer": {"enabled": false}
}
```

`topology.nodes` and `replicas` still define the cluster. Fixed placement requires every configured node to be a voting replica and every node to appear once in `lease_nodes`. With the three-node evaluation layout, set `replicas=3`. `hot_ranks` cannot exceed the accessed key space. A one-node local layout is supported with one replica/target. The helper validates explicit real node/store identities; it never assumes a role's array index equals its node or store ID.

After reset/load, the native helper computes the same coprime multiplier as current ycsbt and creates boundaries at each hot key and its successor. This gives each hot rank an isolated range even when keys are adjacent or draws repeat. It discovers node IDs through the configured SQL gateways and discovers their stores through the current status view. It waits, within the existing readiness deadline, for the table's own voters/replicas to converge. Unknown/duplicate stores, invalid ranges and SQL errors fail directly.

It then sets and verifies `kv.allocator.load_based_rebalancing='off'` and `kv.lease_queue.enabled=false`, keeping the normal replication queue active. Top ranks map round-robin over the explicitly ordered target list. Remaining ranges fill the least-loaded target so the **complete** table lease count also differs by at most one. Every transfer checks its returned range ID and `result='ok'`; actual initial raw boundaries, clipped logical table spans, replica roles, leases and SQL receipts are retained.

Administrative connections set session `allow_unsafe_internals=true`, required by this fork for catalog inspection. This is not a cluster override or a workload-session/concurrency-control change. `SET CLUSTER SETTING` executes as its own statement, as required by the database statement boundary.

A common-owned `watch-layout` observer verifies the full map during the workload. A final check runs after client completion. Both confirm table coverage, complete own voters, the configured hot-key/lease mapping and disabled automatic lease movement. The parser independently checks all collected maps and does not trust a `valid=true` field alone. Changes cause a normal failed cell and owned-process cleanup. Verification is by discrete snapshots; an unobserved move away and back is not excluded. Freeze the sampling interval and account for its SQL/CPU cost. Freeze the observer interval explicitly in the campaign configuration.

Public native operations are `layout-plan`, `layout`, `watch-layout`, and `layout-check`, taking the adapter-generated native `--config`. The normal user path remains common's `plan`/`run` lifecycle, which manages these operations and their output collection. Workers require only declared binaries and common's stdlib-Python executor, with no installed adapter package.

## Calibrating the hold limit

Set `juicer.enabled=true` and `juicer.hold_calibration=true` for a **separate baseline calibration stage**. This enables the existing `COCKROACH_JUICER_HOLD_CALIBRATION` observe-only mechanism: the interceptor observes nominated keys but does not enforce the rules, hold, delay or sort transactions. Adaptive wait and manual queues are rejected with calibration. Normal off/on cells retain `hold_calibration=false`.

The finalizer verifies the native calibration declaration on every server and retains the original Juicer log lines. The parser uses consecutive **observed native epoch-summary timestamps** to select complete epochs inside the intersection of the client measurement windows. Initialization, loading, partial boundary epochs and missing epoch boundaries are excluded. It verifies zero cumulative holds/queued/held markers in those epochs, rather than relying on the requested switch alone.

The source meter times first marker to commit/abort applied on that server/key. Its selected epoch histograms are merged across keys, epochs and nodes before quantiles are computed. Pooled lines are not added again, and per-epoch p99 values are not averaged. Histogram bucket midpoints and overflow semantics follow `juicer/hold_calibration.go`; p99 outside finite buckets cannot yield a recommendation. Unresolved/read-only spans and lost tracking are retained, not imputed. Individual span start timestamps are not present in the native histogram, so this is a **completion-epoch calibration population**, distinct from client execution-latency samples requiring both individual endpoints inside their window.

`hold-calibration.json` includes the histogram, span/commit/abort counts, unresolved/untracked counts, selected native epoch behavior, source references, window policy, p50/p99 in microseconds, and a ceiling-to-milliseconds `juicer.env.MAX_HOLD_MS` override. Lost tracking, actual holding or missing evidence invalidates calibration. A recommendation from a functional smoke is not the scientific workload's calibrated default; run the separate stage at its frozen selected workpoint.

## Explicit baseline stability (T-CR-00 / T-CR-01)

The card templates require a site binding for `measurement.stability`; their
literal `null` placeholder intentionally fails adapter validation. There is
no adopted scientific threshold. Bind every field before running:

| Field | Required binding / meaning |
| --- | --- |
| `criterion` | Explicitly choose `completion_bin_spread_v1` |
| `bin_seconds` | Positive whole-nanosecond bin width dividing the measurement window exactly |
| `min_bins` | Integer >= 2, no greater than the declared full-window bin count |
| `min_terminals_per_bin` | Positive integer, minimum sample count in each bin before judging p99 spread |
| `min_commit_rate_ratio` | Frozen lower bound in [0,1] for minimum / maximum bin commit rate |
| `max_p99_ratio` | Frozen upper bound >= 1 for maximum / minimum bin p99 |
| `stop_on_unstable_off` | Explicit `true` |

These are method bindings, not recommended numerical values. Omitting the
entire policy on a configuration reports `not_assessed`; it
does not establish stability. Partially supplied, null or unsupported policies
fail validation before execution. Thresholds remain in frozen configuration,
logical continuation identity and each stability decision.

The parser uses only native `measured` terminal events whose execution starts
and completes within that client's measurement window. It bins by completion
time relative to each process measurement start, preferring native monotonic
offsets minus the declared warmup; old raw records use native wall timestamps
relative to their own window start. Sources and clock modes are retained.
Cross-host wall-clock alignment is not inferred. Warmup and unfinished events
are excluded, including transactions that began inside but ended outside.

Each full bin retains committed/error terminal counts, exposure seconds,
commit rate, and nearest-rank p99 of the merged terminal event latencies.
Clients' p99 values are never averaged; per-process counts/windows remain
visible. P99 includes error terminals; rate counts commits only. Decision
ratios retain their raw numerators and denominators. A ratio strictly beyond
its frozen bound is `unstable`; equality passes. Insufficient samples or
zero quantile/rate denominators produce `insufficient`, never fabricated zero
latency. An observed commit-rate violation is unstable even when empty bins
make p99 unavailable. Passing this configured spread criterion is not proof
of stationarity, adequate client capacity, or an offered-rate plateau.
Capacity waiting remains a separate native diagnostic and selection gate.

Stability is independent of data integrity: an unstable or insufficient run
retains `validity.valid=true` when its execution/collection/measurement passes
those checks, and its attempt remains successful. Common stops before the
next cell when a policy-bound **off** is not stable; treatment does not trigger
this off-only stop. `summary.json` has status `stopped`, with structured reason
and `stability-stop.json`. No higher-rate cell, new pair, extension or followup
is launched after the stop. The existing AB/BA repeat order is preserved.
On continuation, common rechecks verified results across all linked batches
before any new cell/build; a prior unsafe off cannot be bypassed by supplying
only the newest continuation batch. Missing prior stability evidence under
a frozen policy is also insufficient. Threshold changes require a distinct
logical plan and do not silently override this boundary.

Selection still uses the existing explicitly frozen version-1 knee rules.
Any unstable/insufficient repeat excludes that load and all higher loads;
stable repeats cannot conceal the unsafe one. With sufficient safe evidence
the existing knee may be recorded, but stopped runs never launch followups.
Otherwise `selection.json` reports `insufficient_evidence` and a null selected
load; stopping is not treated as proof that the last stable point is a knee.

`diagnostics.stability` retains status, policy, bins, per-process counts,
sources, populations, ratio numerators/denominators and reasons. The public
`crdb_bench.evaluation` export adds `stability.json`, `stability-series.csv`
and a `stability_status` column in Figure 7.8b inputs; curve labels distinguish
the states. Unstable/insufficient measurements remain visible but are not
marked comparison-eligible. Original source hashes and raw files are retained.
See `stability-20261022/REPORT.md` for the stability method and its boundaries.

## Defined retry and error layers

`diagnostics.evaluation` preserves:

- Main concurrency-control restarts = txnaborted + writetooold + serializable + txnpush, per **server KV commit**.
- SQL automatic retries per server commit and all nine KV restart causes separately, including uncertainty, async-write failure, commit-deadline exceeded, exclusion violation and unknown.
- SQL retry total versus KV cause sum, their signed residual, and `identity_holds` in matching per-server scrape windows. `topology.metrics` requires all evaluation counters; `require_retry_identity=true` also rejects a nonzero/unavailable residual. False preserves the observation and reports disagreement without treating different instrumentation boundaries as database corruption.
- The restart histogram's `_count` is a count of transactions with restarts, not a restart-event count; `_sum` measures epochs and is also retained without replacing cause counts. These are not interchangeable denominators.
- Visible client 40001 attempts per client SQL attempt and terminal 40001 errors per completed logical transaction, with separate attempt/terminal SQLSTATE maps and explicit windows. Successful retries do not become terminal errors.
- Deadlocks per server commit and **failed server-side 1PC batch attempts / (successful + failed server-side 1PC batch attempts)**. This is not `1 - one_phase_commits / client_commits`; it does not count cross-range transactions that disabled 1PC before making a server-side attempt. Zero-attempt ratios are null with a reason.

SQL text-reason counters describe observed retry text, not preserved structured
KV causes. The same-node class sum is compared with its SQL retry total, while KV
reason counts remain separate. `sql_retry_cause_attribution` exposes complete
text-class populations with their own status and uncertainty. Read
[SQL-RETRY-TEXT-OBSERVATION.md](SQL-RETRY-TEXT-OBSERVATION.md) before interpretation.

All server numerators and denominators use each node's retained first/last successful scrape windows. They include server background and observation work; client attempts are a different population. Scrapes are not atomic across nodes and SQL totals are updated at transaction completion, so boundary residuals are observable rather than suppressed. No abort percentage sums these layers.

Repeated blind-write draws remain separate VALUES rows in the single UPSERT. Affected-row validation still expects the full input count. Read `IN` queries naturally count distinct rows without changing the input drawing policy. The wire test with five writes to the same key is retained.

## Card exports and examples

A standalone derived report uses verified common batches and collection hashes:

```sh
PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common" python3.14 \
  -m crdb_bench.evaluation --batch /absolute/launch/juicer-exp-runs/run-id/batch \
  --output /absolute/launch/juicer-exp-runs/run-id/evaluation-report
```

Multiple `--batch` values use common's continuation/deduplication checks; incompatible plans or conflicting successful repeats fail. Current parsing is recomputed from immutable collected files into the new report, leaving original batch results and raw data unchanged. Exports are `evaluation-report.json`, `retry-layers.csv`, `figure-7.8b.csv`, `figure-7.8b.svg`, `layout.json`, and `hold-calibration.json`. The three panels use achieved throughput versus p99, p50 and the correctly defined CC restart ratio. Conditions, Juicer settings, repeats and calibration stage remain distinct. These are per-cell input points, not the common layer's paired-block/min-max research estimator. Observe-only calibration points are visibly labelled and cannot masquerade as ordinary Juicer-on efficacy evidence.

`examples/evaluation-local.json` is a three-node, 20-key, three-second calibration recipe with explicit artifact placeholders. `examples/evaluation-site.json` provides independent server/client site bindings for either shared T-CR-00 or T-CR-01 card template. Replace its documentation-only addresses, artifacts and source/common revisions before execution; it does not provision resources. In plan-only form:

```sh
python -m crdb_bench plan --template /absolute/path/to/T-CR-00.yaml \
  --config bench/examples/evaluation-site.json --output /absolute/launch/juicer-exp-runs/run-id/crdb-card-plan
```

See [EVALUATION-COVERAGE.md](EVALUATION-COVERAGE.md) for capability boundaries.

## Source checks

Authoritative source interfaces:

| Operation / quantity | Authoritative source |
|---|---|
| Hot-rank scrambling and duplicate UPSERT inputs | `pkg/workload/ycsbt/ycsbt.go`; maintained helper `sqlclient/sql.go`. |
| Range/replica columns and per-key lookup | `pkg/sql/delegate/show_ranges.go`, `show_range_for_row.go`; implemented in `sqlclient/layout.go`. |
| Observe-only calibration switch | `pkg/rpc/juicer_spi.go`; gRPC fork `juicer/hold_calibration.go` and `queue_manager.go`. |
| Histogram bucket midpoint / finite overflow | shared gRPC `juicer/hold_calibration.go`; original key/epoch histogram rows are merged in `crdb_bench/evaluation_metrics.py`. |
| KV restart reasons and histogram population | `pkg/kv/kvclient/kvcoord/txn_metrics.go`, `txn_coord_sender.go`, `txn_interceptor_metric_recorder.go`. |
| SQL automatic-retry total at completion | `pkg/sql/conn_executor_exec.go`; SQL text-reason classes are separate from structured KV causes. |
| Server-side attempted 1PC success/failure | `pkg/kv/kvserver/metrics.go`; failed attempts divided by success plus failure. |

Native build identity includes the CRDB and shared gRPC revisions, toolchain
and binary hashes. Research exports retain environment/repeat identity; common
renders plots. Capacity-wait diagnostics reach working-point selection through
`diagnostics.capacity_limited`. Interpret counters with their native measurement
window and population; see [VALIDATION.md](VALIDATION.md).
