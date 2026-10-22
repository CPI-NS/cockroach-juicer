# CockroachDB experiment adapter

`crdb-bench` is an independent Python integration backed by the system-owned Go SQL client in `sqlclient/`. It uses `experiment-common==0.1.0` for configuration, matrix expansion, allocation, local/SSH execution, owned-process cleanup, continuation, collection, and plots. It never imports a historical run directory or requires a juicer-project checkout.

## Run with existing dependencies

Use Python 3.11+ with the common package and PyYAML already available. From this system checkout, the source-only equivalent is:

```sh
export PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common"
python3.12 -m crdb_bench validate --config bench/examples/local.json --output /tmp/crdb-validate
python3.12 -m crdb_bench plan --config bench/examples/local.json --output /tmp/crdb-plan
python3.12 -m crdb_bench build --config bench/examples/local.json --output /tmp/crdb-build
python3.12 -m crdb_bench run --config bench/examples/local.json --output /tmp/crdb-batch-1
python3.12 -m crdb_bench status --output /tmp/crdb-batch-1
python3.12 -m crdb_bench analyze --output /tmp/crdb-batch-1
```

Replace the example artifact paths and source revision with actual, audited values first. `--set key=value` changes public configuration fields, for example `--set workload.transaction_size=1`. The installed console command `crdb-bench` and the common CLI's `--adapter crdb_bench:CRDBAdapter` use the same implementation. The package also registers the `crdb` adapter entry point.

Ctrl-C or `python3.12 -m crdb_bench stop --output /tmp/crdb-batch-1` stops new cells and cleans experiment-owned server/client process groups, including children. Other workloads are not selected by process name. Ordinary errors fail clearly and clean up; there is no adapter recovery framework. For a new batch that preserves valid completed cells:

```sh
python3.12 -m crdb_bench run --config bench/examples/local.json --output /tmp/crdb-batch-2 --previous /tmp/crdb-batch-1
python3.12 -m crdb_bench analyze --output /tmp/crdb-joint --previous /tmp/crdb-batch-1 --previous /tmp/crdb-batch-2
```

The configuration and source/artifact identity must remain compatible. An interrupted cell is rerun in full. SVG plots, CSV, and JSON are written by common analysis; never average client quantiles.

## Public workload and SQL meaning

| Public field | SQL client interpretation |
|---|---|
| `name` | `ycsbt`; other workloads fail validation |
| `keys` | Accessed and loaded integer keys `0..keys-1`; supports the 10,000,000-key evaluation configuration |
| `transaction_size` | Number of draws in each logical transaction; five is represented as five, rather than the historical fixed ten |
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

The adapter uses insecure SQL/RPC for isolated trusted experiment networks. It does not configure production authentication or install firewall rules.

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

See [BUILD.md](BUILD.md) for offline Linux and Apple Silicon recipes, [MIGRATION.md](MIGRATION.md) for the source reuse audit, and [VALIDATION.md](VALIDATION.md) for actual evidence and platform limits.

## Evaluation layout and calibration

See [EVALUATION.md](EVALUATION.md) for fixed hot-rank range/lease placement, common-owned runtime verification, observe-only hold calibration, defined retry-layer exports and the Figure 7.8b input report. Actual native output paths are in [EVALUATION-COVERAGE.md](EVALUATION-COVERAGE.md).
