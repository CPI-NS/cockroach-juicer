# SQL client implementation responsibilities

The system-owned `bench/sqlclient/` module generates SQL transactions and records
client measurements. `crdb_bench` maps experiment configuration to SQL-client and
server commands and interprets system metrics. Experiment-common owns execution,
process cleanup, batching, continuation, collection and plotting.

| Component | Responsibility |
|---|---|
| `sqlclient/sql.go` | Parameterized pgx statements, SQLSTATE handling and row validation |
| `sqlclient/workload.go` | Key draws, transaction size and read/write semantics |
| `sqlclient/run.go` | Global allocation, open/closed-loop scheduling and measurement |
| `sqlclient/metrics.go` | Native metric acquisition |
| `crdb_bench/results.py` | Retry layers, latency distributions and merged results |
| `crdb_bench/evaluation_metrics.py` | Observe-only hold calibration and window checks |
| `experiment-common` | Generic lifecycle and analysis |

Use [README.md](README.md) for commands, [BUILD.md](BUILD.md) for pinned native
builds and [EVALUATION.md](EVALUATION.md) for evaluation-specific operations.
The client module's `go.mod` and `go.sum` define its pgx dependency. Offline
builds require the pinned dependency cache.
