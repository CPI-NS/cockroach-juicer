# CRDB evaluation integration: ticket11

This extends the base adapter with maintained layout placement, observe-only hold calibration, cause-specific metric reconciliation and card-specific exports. It does not change database concurrency control, import a historical harness, merge repeated blind-write inputs, or create cloud resources. Native Mac validation remains deferred.

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

Administrative connections set session `allow_unsafe_internals=true`, required by this fork for catalog inspection. This is not a cluster override or a workload-session/concurrency-control change. `SET CLUSTER SETTING` executes as its own statement, avoiding the historical multi-statement transaction restriction.

A common-owned `watch-layout` observer verifies the full map during the workload. A final check runs after client completion. Both confirm table coverage, complete own voters, the configured hot-key/lease mapping and disabled automatic lease movement. The parser independently checks all collected maps and does not trust a `valid=true` field alone. Changes cause a normal failed cell and owned-process cleanup. Verification is by discrete snapshots; an unobserved move away and back is not excluded. Freeze the sampling interval and account for its SQL/CPU cost. The research site example uses ten seconds, consistent with the source harness; the small native smoke uses a shorter interval only for functional evidence.

Public native operations are `layout-plan`, `layout`, `watch-layout`, and `layout-check`, taking the adapter-generated native `--config`. The normal user path remains common's `plan`/`run` lifecycle, which manages these operations and their output collection. Workers require only declared binaries and common's stdlib-Python executor, with no installed adapter package.

## Calibrating the hold limit

Set `juicer.enabled=true` and `juicer.hold_calibration=true` for a **separate baseline calibration stage**. This enables the existing `COCKROACH_JUICER_HOLD_CALIBRATION` observe-only mechanism: the interceptor observes nominated keys but does not enforce the rules, hold, delay or sort transactions. Adaptive wait and manual queues are rejected with calibration. Normal off/on cells retain `hold_calibration=false`.

The finalizer verifies the native calibration declaration on every server and retains the original Juicer log lines. The parser uses consecutive **observed native epoch-summary timestamps** to select complete epochs inside the intersection of the client measurement windows. Initialization, loading, partial boundary epochs and missing epoch boundaries are excluded. It verifies zero cumulative holds/queued/held markers in those epochs, rather than relying on the requested switch alone.

The source meter times first marker to commit/abort applied on that server/key. Its selected epoch histograms are merged across keys, epochs and nodes before quantiles are computed. Pooled lines are not added again, and per-epoch p99 values are not averaged. Histogram bucket midpoints and overflow semantics follow `juicer/adaptive_wait.go`; p99 outside finite buckets cannot yield a recommendation. Unresolved/read-only spans and lost tracking are retained, not imputed. Individual span start timestamps are not present in the native histogram, so this is a **completion-epoch calibration population**, distinct from client execution-latency samples requiring both individual endpoints inside their window.

`hold-calibration.json` includes the histogram, span/commit/abort counts, unresolved/untracked counts, selected native epoch behavior, source references, window policy, p50/p99 in microseconds, and a ceiling-to-milliseconds `juicer.env.MAX_HOLD_MS` override. Lost tracking, actual holding or missing evidence invalidates calibration. A recommendation from a functional smoke is not the scientific workload's calibrated default; run the separate stage at its frozen selected workpoint.

## Defined retry and error layers

`diagnostics.evaluation` preserves:

- Main concurrency-control restarts = txnaborted + writetooold + serializable + txnpush, per **server KV commit**.
- SQL automatic retries per server commit and all nine KV restart causes separately, including uncertainty, async-write failure, commit-deadline exceeded, exclusion violation and unknown.
- SQL retry total versus KV cause sum, their signed residual, and `identity_holds` in matching per-server scrape windows. `topology.metrics` requires all evaluation counters; `require_retry_identity=true` also rejects a nonzero/unavailable residual. False preserves the observation and reports disagreement without treating different instrumentation boundaries as database corruption.
- The restart histogram's `_count` is a count of transactions with restarts, not a restart-event count; `_sum` measures epochs and is also retained without replacing cause counts. These are not interchangeable denominators.
- Visible client 40001 attempts per client SQL attempt and terminal 40001 errors per completed logical transaction, with separate attempt/terminal SQLSTATE maps and explicit windows. Successful retries do not become terminal errors.
- Deadlocks per server commit and **failed server-side 1PC batch attempts / (successful + failed server-side 1PC batch attempts)**. This is not `1 - one_phase_commits / client_commits`; it does not count cross-range transactions that disabled 1PC before making a server-side attempt. Zero-attempt ratios are null with a reason.

The source has no dedicated per-cause SQL automatic-retry counter. KV reason counts are not silently renamed SQL counts, even when the same-window totals reconcile. `sql_retry_cause_attribution` remains null with that specific reason; the real native evidence separately demonstrates the observed cause-sum identity. Exact per-SQL reason instrumentation would require a source capability outside this bench-only change.

All server numerators and denominators use each node's retained first/last successful scrape windows. They include server background and observation work; client attempts are a different population. Scrapes are not atomic across nodes and SQL totals are updated at transaction completion, so boundary residuals are observable rather than suppressed. No abort percentage sums these layers.

Repeated blind-write draws remain separate VALUES rows in the single UPSERT. Affected-row validation still expects the full input count. Read `IN` queries naturally count distinct rows without changing the input drawing policy. The wire test with five writes to the same key is retained.

## Card exports and examples

A standalone derived report uses verified common batches and collection hashes:

```sh
PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common" python3.14 \
  -m crdb_bench.evaluation --batch /absolute/path/to/batch \
  --output /absolute/path/to/evaluation-report
```

Multiple `--batch` values use common's continuation/deduplication checks; incompatible plans or conflicting successful repeats fail. Current parsing is recomputed from immutable collected files into the new report, leaving original batch results and raw data unchanged. Exports are `evaluation-report.json`, `retry-layers.csv`, `figure-7.8b.csv`, `figure-7.8b.svg`, `layout.json`, and `hold-calibration.json`. The three panels use achieved throughput versus p99, p50 and the correctly defined CC restart ratio. Conditions, Juicer settings, repeats and calibration stage remain distinct. These are per-cell input points, not the common layer's paired-block/min-max research estimator. Observe-only calibration points are visibly labelled and cannot masquerade as ordinary Juicer-on efficacy evidence.

`examples/evaluation-local.json` is a three-node, 20-key, three-second calibration recipe with explicit artifact placeholders. `examples/evaluation-site.json` provides independent server/client site bindings for either shared T-CR-00 or T-CR-01 card template. Replace its documentation-only addresses, artifacts and source/common revisions before execution; it does not provision resources. In plan-only form:

```sh
python -m crdb_bench plan --template /absolute/path/to/T-CR-00.yaml \
  --config bench/examples/evaluation-site.json --output /tmp/crdb-card-plan
```

See [EVALUATION-COVERAGE.md](EVALUATION-COVERAGE.md) for actual output paths and scope, and `validation/evaluation/` for retained functional artifacts.

## Source checks

The maintained code uses source semantics from the current fork rather than old-runner APIs:

| Operation / quantity | Authoritative source |
|---|---|
| Hot-rank scrambling and duplicate UPSERT inputs | `pkg/workload/ycsbt/ycsbt.go`; maintained helper `sqlclient/sql.go` (unchanged by ticket11). |
| Range/replica columns and per-key lookup | `pkg/sql/delegate/show_ranges.go`, `show_range_for_row.go`; historical layout SQL was audited, then implemented in maintained `sqlclient/layout.go`. |
| Observe-only calibration switch | `pkg/rpc/juicer_spi.go`; gRPC fork `juicer/hold_calibration.go` and `queue_manager.go`. |
| Histogram bucket midpoint / finite overflow | gRPC fork `juicer/adaptive_wait.go`; original key/epoch histogram rows are merged in `crdb_bench/evaluation_metrics.py`. |
| KV restart reasons and histogram population | `pkg/kv/kvclient/kvcoord/txn_metrics.go`, `txn_coord_sender.go`, `txn_interceptor_metric_recorder.go`. |
| SQL automatic-retry total at completion | `pkg/sql/conn_executor_exec.go`; no dedicated per-cause SQL retry counter exists here. |
| Server-side attempted 1PC success/failure | `pkg/kv/kvserver/metrics.go`; failed attempts divided by success plus failure. |

The cached binary's full source manifest is retained with the native proof. No historical Python runner or experiment directory is imported at runtime.


The S3/F5 follow-up preserves environment/repeat identities through the report CLI and uses common's public plotting renderer. Parsed capacity-wait evidence now reaches automatic selection through `diagnostics.capacity_limited`. See [REVIEW-FIXES.md](REVIEW-FIXES.md) for regression evidence and the F2 coordination note about legacy warmup counters.
