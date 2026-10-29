# CockroachDB experiment adapter

For separate cluster creation and reuse, see [MANUAL-AWS.md](MANUAL-AWS.md).

## Output configuration

Set the destination in the experiment configuration:

```yaml
output_root: juicer-exp-runs
run_id: example-001
```

`output_root` defaults to `juicer-exp-runs`, relative to the launch directory.
The run directory is `<output_root>/<run_id>/`. A standalone system can fall back
to `experiment_id` when `run_id` is omitted. Choose a new `run_id` for a new
execution. No environment variable is required; explicit `--output` overrides
this configuration. `status` and `stop` accept the same `--config`.
For a named cluster, also pass `--cluster <name>`.


## Python file entry

From this system's reviewed checkout root, use the pinned Python environment:

```sh
python bench/run.py --help
python bench/run.py plan --config /absolute/path/to/config.yaml --output /absolute/launch/juicer-exp-runs/run-id/plan
python bench/run.py run --config /absolute/path/to/config.yaml --output /absolute/launch/juicer-exp-runs/run-id/batch --detach
python bench/run.py status --output /absolute/launch/juicer-exp-runs/run-id/batch
python bench/run.py stop --output /absolute/launch/juicer-exp-runs/run-id/batch
python bench/run.py analyze --output /absolute/launch/juicer-exp-runs/run-id/batch
```

`bench/run.py` directly calls the existing `crdb_bench` CLI in this checkout.
All arguments, exit codes and cleanup behavior use the same implementation.
The module and installed console entries below remain supported. Keep the
checkout at the reviewed revision and follow the pinned installation instructions.

`crdb-bench` is an independent Python integration backed by the system-owned Go SQL client in `sqlclient/`. It uses `experiment-common==0.1.0` for configuration, matrix expansion, allocation, local/SSH execution, owned-process cleanup, continuation, collection, and plots. It operates independently of a juicer-project checkout.

## Run with existing dependencies

Install the pinned wheels with [packaging/README.md](packaging/README.md),
using existing Python 3.11+, uv and cached PyYAML. From this system checkout:

```sh
source bench/.venv/bin/activate
python -m crdb_bench validate --config bench/examples/local.json --output /absolute/launch/juicer-exp-runs/run-id/crdb-validate
python -m crdb_bench plan --config bench/examples/local.json --output /absolute/launch/juicer-exp-runs/run-id/crdb-plan
python -m crdb_bench build --config bench/examples/local.json --output /absolute/launch/juicer-exp-runs/run-id/crdb-build
python -m crdb_bench run --config bench/examples/local.json --output /absolute/launch/juicer-exp-runs/run-id/crdb-batch-1
python -m crdb_bench status --output /absolute/launch/juicer-exp-runs/run-id/crdb-batch-1
python -m crdb_bench analyze --output /absolute/launch/juicer-exp-runs/run-id/crdb-batch-1
```

Replace the example artifact paths and source revision with actual, audited values first. `--set key=value` changes public configuration fields, for example `--set workload.transaction_size=1`. The installed console command `crdb-bench` and the common CLI's `--adapter crdb_bench:CRDBAdapter` use the same implementation. The package also registers the `crdb` adapter entry point.

Ctrl-C or `python -m crdb_bench stop --output /absolute/launch/juicer-exp-runs/run-id/crdb-batch-1` stops new cells and cleans experiment-owned server/client process groups, including children. Other workloads are not selected by process name. Ordinary errors fail clearly and clean up; there is no adapter recovery framework. For a new batch that preserves valid completed cells:

```sh
python -m crdb_bench run --config bench/examples/local.json --output /absolute/launch/juicer-exp-runs/run-id/crdb-batch-2 --previous /absolute/launch/juicer-exp-runs/run-id/crdb-batch-1
python -m crdb_bench analyze --output /absolute/launch/juicer-exp-runs/run-id/crdb-joint --previous /absolute/launch/juicer-exp-runs/run-id/crdb-batch-1 --previous /absolute/launch/juicer-exp-runs/run-id/crdb-batch-2
```

The configuration and source/artifact identity must remain compatible. An interrupted cell is rerun in full. SVG plots, CSV, and JSON are written by common analysis; never average client quantiles.

## Public workload and SQL meaning

| Public field | SQL client interpretation |
|---|---|
| `name` | `ycsbt`; other workloads fail validation |
| `keys` | Accessed and loaded integer keys `0..keys-1`; supports the 10,000,000-key evaluation configuration |
| `transaction_size` | Number of draws in each logical transaction; five means five draws |
| `zipf` | Gray/YCSB Zipf theta in `[0,1)`; zero is uniform |
| `distinct_keys` | False permits repeated draws; true rejects impossible transaction sizes and samples distinct keys |
| `read_fraction` | Probability that a whole transaction is read-only, including fractional percentages |
| `read_mode` | Must be `per_transaction`; `per_operation` is rejected, not reinterpreted |
| `seed` | uint64 deterministic workload seed; process ID offsets independent client streams |
| `retries` | Maximum **client** retries after a visible SQLSTATE 40001, within the same slot/latency measurement |

Each transaction is one nonlocking `SELECT count(*) ... WHERE ycsb_key IN (...)` or one blind `UPSERT`. Reads verify the distinct row count, while the fast-path UPSERT must report the full input row count, including duplicates, matching `pkg/workload/ycsbt/ycsbt.go`. Repeated input draws are preserved even though SQL `IN` naturally visits a repeated key once. The optional system fields are `scramble` (default true, deterministic coprime permutation), `request_timeout_seconds`, `retry_backoff_seconds`, and `load_batch_rows`.

These are integer-key SQL workloads, not native KV operations. `sqlclient trace` consumes the **generated native JSON**, not the public workload config, and can inspect deterministic draws and regular/Poisson offsets without a database.

Closed concurrency is the sum of slots over client processes. Each slot owns exactly one preconnected SQL connection, round-robin across node URLs. URL count never multiplies the configured concurrency. Open `rate` is global **new transactions per second**; common allocates rates and inflight slots to processes. The client waits at capacity, keeps no pending request queue, drops no arrivals, and reanchors the next arrival interval at actual admission instead of catching up. Poisson intervals use an independent seeded stream. Per-process arrival streams have independent start times; the plan retains all allocations.

## Data preparation and topology

`topology.nodes` is an explicit list of `{id, host_id, rpc_port, sql_port, http_port, advertise_address?}`. Every node is a real foreground CRDB process with separate per-cell store/log paths. Node IDs and physical-host ports must be unique. Optional `replicas`, `splits`, `cache_size`, and `max_sql_memory` are validated or passed to their actual current flags. `splits` creates evenly spaced explicit range boundaries. Replica/lease assignment beyond CRDB's zone configuration is not claimed as pinned placement.

After all servers start, bounded readiness commands initialize the cluster, connect to each SQL gateway, recreate `usertable`, load deterministic values in SQL `generate_series` batches, set the replica count, split ranges, and verify the loaded row count. Each cell uses a fresh store under its owned workspace; `runtime.reuse_data=true` is rejected. No command drops data in an external cluster.

`juicer.enabled` becomes `COCKROACH_JUICER=true|false` on every server. Explicit `juicer.env` keys map to the audited `COCKROACH_JUICER_` suffixes. Unsupported keys fail. Finalization extracts real Juicer startup lines from each node's own log files and validates interception against the requested switch. Failed/missing treatment evidence invalidates the result.

`juicer.env.RULES: "readhold-ordered"` selects a timestamp-directed admission
candidate. It keeps `readhold`'s operation types and same-transaction exemption,
but a held plain read or write blocks another transaction's write head only
when its sorting HLC is strictly earlier (physical, then logical). Exact HLC
ties do not block; transaction IDs provide no ordering tie break. The sorting
key is batch-now (falling back to MinTimestamp), not the MVCC timestamp.
Hold membership, release events, cancellation and the `MAX_HOLD_MS` TTL remain
those of `readhold`. The native banner reports `rules=readhold-ordered`.
This is a local admission candidate without a global deadlock-freedom or
validated performance claim; native SQL and concurrency control are unchanged.

`juicer.env.RULES: "readhold-ordered-readpass"` additionally admits the earliest
queued `OpGet` marker ahead of a write head blocked by the enforcer. The read
uses the same enforcer, token, hold and release rules; the real write is
rechecked between reads. This includes point Refresh and nonlocking fragments
of mixed RPCs, which still wait for all keys. It requires `FIFO=false`,
`LATE_POLICY=queue`, `FIXED_WAIT_MS=0`, `AW=false`, calibration off,
`SKIP_QUEUEING=false` and `FILTER_DIAGNOSTIC=off`. Other combinations fail
startup validation. Existing rules are unchanged. Heap scans can add CPU cost;
this candidate has no established performance or global fairness guarantee.

`juicer.env.RULES: "readfirst-ordered-readpass"` removes only the held-write
blocking a write head clause from `readhold-ordered-readpass`. Only an earlier
cross-transaction plain `OpGet` hold blocks an `OpSet` head, using strict sorting
HLC (physical, then logical; no transaction-ID tie break). Writes and locking
reads hold nothing. Overtaking plain reads acquire real holds, but a read newer
than the waiting write cannot prolong that write's wait; an earlier read still
blocks it. Same-transaction exemption, response/arrival releases, unbound
admission, cancellation and `MAX_HOLD_MS` remain unchanged. This is not the
unordered `readfirst` relation or an applied-control release mode. It has the
same readpass startup restrictions above and leaves StageTiming off by default;
`JUICER_STAGE_TIMING=1` still overrides the diagnostic option. It changes no
shared library or native database protocol and promises no global fairness,
deadlock freedom or performance improvement.

`juicer.env.RULES: "readpass-guarded"` uses the same read-ahead path and
configuration restrictions, but either plain reads or writes wait behind an
earlier active write. Cross-transaction plain read/write pairs block only when
at least one operation is a write and the held sorting HLC is strictly earlier.
Read/read pairs, locking reads and exact HLC ties do not block. It reuses
`readhold` releases and TTL; performance and global deadlock freedom are unproven.

`juicer.env.RULES: "writes-readpass"` retains the exact `writes` dependency
relation, releases, TTL and unbound-admission behavior, adding only plain
`OpGet` admission ahead of a blocked write queue head through the same enforcer.
Reads create no holds; cross-transaction write/write blocking is independent
of sorting HLC. It requires `FIFO=false`, `LATE_POLICY=queue`, `FIXED_WAIT_MS=0`,
`AW=false`, calibration off, `SKIP_QUEUEING=false` and `FILTER_DIAGNOSTIC=off`.
The default relation is unchanged. This targeted admission candidate changes no
native SQL or database protocol and has no established performance or fairness
guarantee. Benchmark only after reviewed shared dependency integration and
native validation.

`juicer.env.RULES: "writes-applied-readpass"` keeps the same Set/Set
cross-transaction blocking, unholdable plain/locking reads and existing read-ahead
as `writes-readpass`. It enables applied-control observations and ends Set holds
on the SPI's observed finalized commit/abort or the admitted request's own failed
response (matched by marker token), with the existing cancellation, eviction and
TTL backstops. A successful PENDING/STAGING Set response or commit/abort arrival
alone does not release a hold. It has the same read-ahead startup restrictions.
The startup banner declares this release contract. Applied events do not certify
per-key physical intent removal: ranged resolutions and missing/late events can
leave holds until TTL. This is an explicit candidate, with no default or existing
mode change, native transaction-protocol change, or performance guarantee.

The adapter uses insecure SQL/RPC for isolated trusted experiment networks. It does not configure production authentication or install firewall rules.

## Admission stage timing diagnostic

`juicer.env.STAGE_TIMING: true` maps to `COCKROACH_JUICER_STAGE_TIMING=true`.
It defaults to false and does not enable Juicer by itself. With Juicer enabled,
the shared gRPC observer records unary and serial-stream admission stages and
logs cumulative `juicer-stage:` JSON at existing epochs and interceptor Stop.
It changes no database protocol, SQL workload, release/hold rules or policy
knobs. Keep it off for ordinary comparisons; timing instrumentation has cost.
Native integration requires the reviewed shared gRPC revision providing
`grpc.JuicerStageTiming(bool)`; do not build against an older dependency pin.

For a separate 30-second diagnostic, enable it through the maintained entry's
`--set juicer.env.STAGE_TIMING=true` with `juicer.enabled=true`, keeping the
reviewed workload and policy configuration fixed. The existing `evidence`
finalizer retains every log line containing `juicer` in each node's
`*.evidence.json` under `juicer_log_lines`. Common collects that file unchanged
under `collected/<host_id>/`; the result parser retains its path/hash without
turning stage snapshots into SQL latency or retry metrics. Decode JSON after
`juicer-stage:` and treat it as cumulative; do not sum snapshots or subtract
stage quantiles to infer a transaction critical path.

The helper scans the **first 64 MiB of each regular `.log` file**, not its tail;
symlinks are skipped. Budget the full process lifetime, including reset/load,
warmup, measurement and evidence finalization. Count every emitted epoch over
each interceptor's full lifetime, plus any Stop rows. For each log file require
`other_log_bytes + sum(row_count * max_row_bytes)
< 67,108,864`, using the actual maximum serialized snapshot size including its
logger prefix/newline and every interceptor writing there. A four-row synthetic
retention test or a 30-second measurement alone does not establish lifetime fit.
At execution, check each evidence `logs[].bytes_scanned` against the actual
source log file size, both below the cap, and decode every expected stage row as
complete JSON. Evidence finalization precedes server shutdown; do not depend on
the Stop row or claim full-window coverage from its absence.

## SSH and artifact provenance

`examples/ssh.json` illustrates explicit host roles. For an actual cluster, specify real addresses and ensure every advertised endpoint is reachable from clients and peers. Each remote host needs the common executor's Python and existing executables; no Python adapter package is required on workers. Build on control, deploy once, then run. Common performs all SSH and transfer operations.

Artifact strings refer to absolute paths already present on every participating worker. For managed deployment, use common's artifact mapping:

```json
{"path":"/control/artifacts/cockroach-short","deploy_to":"/workers/bin/cockroach-short","hosts":["s1","c1"],"sha256":"ACTUAL_64_HEX_SHA256"}
```

Both `cockroach` and `sql_client` must be available on server and client hosts: server finalization uses the helper client, and client provenance probes the database binary. `source_manifest` can use the same path/deploy mapping for an existing build manifest. The adapter records binary hashes, actual `cockroach version`, OS/architecture, declared source revision, and optional source-manifest bytes/hash on every host. Optional expected hashes fail before launch if mismatched. Common additionally records adapter/common source hashes and Git revisions. Source revision declarations alone are not an attestation of an arbitrary binary; use a verified build manifest and hashes.

Generated files, outputs, and store/log paths are relative to the logical cell workspace provided by common. It materializes the same absolute workspace path on each involved SSH host. Collection preserves workspace-relative files under `collected/<host_id>/`. Host `work_dir` is not substituted into individual CRDB paths: set `runtime.workspace_root` for the shared remote root if needed.

## Metrics and validity

Latency starts immediately before SQL execution and ends at the logical transaction's terminal result, including client retries/backoff and server work. Preconnection, key generation, admission/capacity waiting, and logging are outside latency. Both start and completion must fall inside that process's measurement window. Unfinished-at-cutoff requests are canceled and excluded; no drain extends the window. New event files retain monotonic offsets as well as wall timestamps. Attempts are counted separately when their own SQL attempt starts and completes inside the attempt window.

Only the first allocated client observes every server's `/_status/vars`, once per second. Raw HTTP payloads and timestamps stream to `.metrics.jsonl`; the summary retains first/last successful in-window scrapes per server. At least two successful scrapes per node are necessary, so very short or slow-scrape windows can fail validity. The three-second functional examples meet this requirement on the tested host. Scrape timestamps and per-server windows remain explicit; their denominator is server KV commits, not client terminal commits, and includes server background work.

Separate reported layers are:

- Concurrency-control restarts: `txn_restarts_txnaborted + txn_restarts_writetooold + txn_restarts_serializable + txn_restarts_txnpush`, divided by server `txn_commits`. It may exceed one.
- SQL invisible auto retries per server commit, uncertainty restarts per server commit, and raw restart reasons.
- Client SQL attempts, failed attempts, visible 40001s, client retries, and terminal errors, with their distinct windows.
- Deadlocks, 1PC commit share, and failed 1PC batches per attempted 1PC batch, where exposed. Failed 1PC batches are a separate counter definition, not added to aborts.

Unavailable optional counters are null with reasons. Missing required retry counters, malformed/incomplete samples, missing process output, wrong reset data, counter resets, and failed Juicer evidence invalidate a measurement. `metrics.aborts` is the server concurrency-control restart numerator; it must not be divided by the client `metrics.attempts`. Named ratios retain their own raw numerators and denominators in `diagnostics.retry_layers`. Program exit status and result validity are independent checks.

## Native builds and verification

See [BUILD.md](BUILD.md) for offline Linux and Apple Silicon recipes, [MIGRATION.md](MIGRATION.md) for module responsibilities, and [VALIDATION.md](VALIDATION.md) for actual evidence and platform limits.

## Evaluation layout and calibration

See [EVALUATION.md](EVALUATION.md) for fixed hot-rank range/lease placement, common-owned runtime verification, observe-only hold calibration, defined retry-layer exports and the Figure 7.8b input report. Actual native output paths are in [EVALUATION-COVERAGE.md](EVALUATION-COVERAGE.md).
