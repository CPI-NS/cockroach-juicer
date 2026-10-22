# Issue04 validation

Executed on Linux amd64 on 2026-10-21. New adapter/client code is under `bench/`; no historical runner was called. This is functional evidence, not a throughput/latency research comparison. No AWS resources were created and no dependency/tool was downloaded.

## Real SQL and exported artifacts

The final same-binary batch at `/tmp/crdb-issue04-final` completed two cells: closed-loop/Juicer-off and Poisson-open/Juicer-on. Each used 20 loaded/accessed keys, five input draws per transaction, repeats permitted, Zipf 0.99, a 50% read-only transaction mix, two SQL client processes and two global slots. Warmup was one second and measurement three seconds per cell. Commands were:

```sh
PYTHONPATH=bench:../common /home/lihan/anaconda3/bin/python -m crdb_bench run \
  --config /tmp/crdb-issue04-final-config.json --output /tmp/crdb-issue04-final
PYTHONPATH=bench:../common /home/lihan/anaconda3/bin/python -m crdb_bench analyze \
  --output /tmp/crdb-issue04-final
```

Both cells completed init/readiness/reset/load, real mixed SQL, finalization, owned-process cleanup and hash-verified collection. Both parsed as valid; Juicer-on had real interception startup evidence. A separate regular-open real CLI smoke completed at `/tmp/crdb-issue04-cli`; the earlier direct adapter smoke covered closed/off and Poisson-open/on. Those earlier smokes used 100 keys; they are not mixed into the final two-cell batch.

Retained reviewable artifacts are [validation/evidence.json](validation/evidence.json), [validation/results.csv](validation/results.csv), [validation/results.json](validation/results.json), and the three SVGs in `validation/`. Raw client events, HTTP metric payloads, source provenance, process logs, exits, and transfer manifests remain in the referenced local batch directories. SHA-256 and raw-result identities are retained. The few smoke points validate execution and plotting; they do not establish a knee, scientific scaling result or a Juicer speedup.

The reused database binary is the cached Juicer fork built from CRDB `1a2f996bd33530f96f022ed797842a760a43f0a0` and gRPC `2d0f58eb5e0719569e5fb39997298560723dbb10`. Its SHA-256 is `86bdaa026f8261af1b1d8408d97f4cfba7a8a238bd8e87a7218ff8005f8df0cb`; original provenance is `crdb-align/build/artifacts/crdb-1a2f996bd335-grpc-2d0f58eb5e07/SOURCE-MANIFEST.json`. The full manifest is copied into generated per-host provenance files. This binary predates the worktree's spec-banner commits. It proves the real SQL/adapter path against the recorded fork artifact, not a newly completed build of the current database HEAD.

## Ctrl-C and explicit stop

A real standalone CRDB batch at `/tmp/crdb-issue04-real-stop` was interrupted only after observing SQL client `PROGRESS dispatched=` output. Its driver exited **130**, batch status became `stopped`, and the first cell was `interrupted`; the second planned cell never started. Every recorded owned process was finished and its PID was inactive. Cleanup errors were empty. [validation/real-ctrl-c.json](validation/real-ctrl-c.json) records the actual PID/exit checks.

The two executable-boundary CLI tests additionally exercise Ctrl-C and explicit `stop` with server/client **child processes**, while an unrelated sentinel remains alive. Both stop new matrix cells, preserve an interrupted attempt/exits evidence and report no cleanup errors. Detailed fixture evidence is retained in `/tmp/crdb-issue04-stop-evidence/{ctrl-c,explicit-stop}`. These are orchestration tests, labelled separately from the real SQL smoke.

## Correctness and packaging checks

The final isolated run used existing CPython 3.14.8, with PyYAML 6.0.3 installed from cache into `bench/.venv`, and local common/adapter source packages. The 15-test suite reports **14 passed, one opt-in smoke skipped**; actual real-system smoke was executed separately as above. Its output is retained in [validation/tests-python314.log](validation/tests-python314.log).

Tests cover canonical input names/read-mode rejection, exact multi-URL slot/rate allocation, local/control versus deployed artifact paths, SSH command planning, deterministic 10M-key/five-operation traces, duplicate and distinct draws, current ycsbt scramble semantics, regular/Poisson intervals, actual pgx SQL wire behavior, visible 40001 retry timing, wait-cap diagnostics, no unfinished latency samples, merged quantiles, layered retry denominators above 100%, missing-process and failed-treatment evidence, Ctrl-C, explicit stop, and unchanged unrelated processes. The duplicate-blind-write wire test checks that all five repeated key inputs reach the SQL VALUES list without merging. The real mixed SQL cells also validate affected-row counts against the fork.

The prior full suite also passed with a race-instrumented Go client; the additional blind-write boundary was separately checked with race instrumentation. `go vet ./...` and offline `go build -mod=readonly` succeeded. Cached `crlfmt` and black formatted the new sources. `git diff --check` is part of handback verification.

`uv lock --offline` initially failed for absent PyYAML 6.0.1 metadata, and then for universal PyYAML 6.0.3 metadata selection. The coordinated 6.0.3 pin plus **exact metadata read from the cached upstream wheel** resolves offline without fabricating hashes or reducing Python/platform support. `bench/uv.lock` is generated by uv. The dependency-only frozen sync succeeds offline with existing Python 3.14.8. An adapter wheel built offline using existing setuptools 75.1.0/wheel 0.44.0 installed into that isolated environment, and its installed `crdb-bench validate` passed for two real configurations, with common supplied from its independent source checkout. This does not claim a complete fresh common build environment was installed. No global environment was changed.

## Limits and follow-up

- Native macOS arm64 build/execution is **untested and explicitly deferred**. Portable paths and native offline recipes are supplied; Linux does not prove Mac support.
- Real SSH execution was not run against an external inventory. SSH plans, worker destinations and multi-URL global allocation are tested; SSH lifecycle implementation is common's responsibility.
- This file records the base Issue04 run. Ticket11 now implements layout/lease placement, observe-only calibration and defined evaluation metrics; see [EVALUATION-COVERAGE.md](EVALUATION-COVERAGE.md) for its separate native evidence and source capability limits.
- RootQ57 remains preserved: every blind-write input draw is emitted; duplicates are not merged. The retained 1PC layer reports raw failed/successful 1PC batch counters and `failures / (failures + successes)`, separate from concurrency-control restarts, SQL retries and visible client errors.
