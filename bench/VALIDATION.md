# CRDB adapter verification

Use a pinned Python environment, the shared gRPC submodule and existing native
Go/Bazel dependencies. See [BUILD.md](BUILD.md) and the
[installation guide](packaging/README.md).

## Configuration and parser tests

```sh
PYTHONPATH="$PWD/bench:/absolute/path/to/experiment-common" \
  python -m unittest discover -s bench/tests -v
```

Select the relevant tests before running: opt-in integration tests may start
native processes. Check exact global slot/rate allocation, repeated and distinct
key draws, read modes, SQLSTATE handling, retry timing, capacity waiting, pooled
quantiles, measurement windows and missing-process rejection.

## Native checks

Compile the SQL client offline and run its unit/wire tests. Use CRDB's cached
Bazel environment for `//pkg/rpc:rpc_test` with `TestJuicer`, then build the
production `cockroach-short` target. Confirm Bazel and Go consume the same clean
shared gRPC pin. Evaluate native operation separately from parser fixtures.

For an authorized process test, use tiny data, short measurements, isolated
ports, external output directories and continuous progress observation. Verify
Juicer off/on identity and completion-window samples. Exercise Ctrl-C and public
stop; owned descendants must exit and unrelated processes must survive.

## Boundaries

Check standalone installed-package execution from a different directory and
verify wheel/lock hashes. Native macOS requires separate platform validation.
A local functional check does not establish cluster performance or completion
of the [evaluation capabilities](EVALUATION-COVERAGE.md).
