# CRDB adapter interface

`crdb_bench:CRDBAdapter` implements the shared adapter contract. The standalone
entry is `python bench/run.py` or installed `python -m crdb_bench`. Public
configuration uses the canonical workload fields and `read_mode=per_transaction`.
Generated private SQL-client JSON uses the native field names.

The common package owns lifecycle, SSH, process cleanup, batching, continuation,
collection and plotting. Dictionary artifacts use control `path` for builds and
`deploy_to` on workers; a string declares a preinstalled artifact. The SQL helper
runs preparation, provenance and finalization as well as client traffic, so deploy
it to all participating hosts. Workers need the declared executable and stdlib
Python, not the adapter package.

Readiness initializes/probes the cluster and resets/loads data after servers
start. Finalizers validate node-level Juicer identity. `topology.layout`,
`topology.metrics` and `juicer.hold_calibration` configure layout and measurement
helpers. `python -m crdb_bench.evaluation` analyzes verified batches without a
second lifecycle runner.

Package versions and wheel identity are defined by `common-pin.json`, `uv.lock`
and the selected release artifacts. See [BUILD.md](BUILD.md),
[VALIDATION.md](VALIDATION.md) and [EVALUATION-COVERAGE.md](EVALUATION-COVERAGE.md).
