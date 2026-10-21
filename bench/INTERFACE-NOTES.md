# Issue04 integration handback

Import `crdb_bench:CRDBAdapter`; its standalone entry is `python -m crdb_bench`. It consumes canonical CONTRACT workload names and requires `read_mode=per_transaction`. Native SQL names appear only in generated private client JSON. The common package is the only lifecycle/SSH/cleanup/continuation executor.

`experiment_common.build.artifact_path` resolves all runtime and build paths. Dictionary artifacts use control `path` for builds/local and `deploy_to` on SSH workers; string shorthand is explicitly preinstalled. The client build command uses `host_id=local` for current control. Both executables are deployed to participating server/client hosts, because the Go helper performs preparation, provenance and finalization. Workers need no adapter installation.

`CellPlan.readiness` initializes the cluster, probes SQL and resets/loads after servers launch. Finalizers preserve and validate per-node Juicer startup evidence. Collection contains exact generated output paths. The SQL client owns only its bounded SQL request scheduler; common owns subprocess groups.

The package pins common 0.1.0 and uses the coordinated PyYAML 6.0.3 dependency via common. `uv.lock` was generated offline using exact dependency metadata from the cached upstream wheel. Isolated cached-dependency tests and offline adapter wheel/installed console validation passed. No current packaging blocker remains; a complete fresh common build environment was not installed by this worker.

See VALIDATION.md and validation/evidence.json for real closed/off and Poisson-open/on CLI smoke, regular-open evidence, real Ctrl-C exit130/PID cleanup, executable-fixture Ctrl-C/explicit-stop tests with grandchildren and unrelated sentinel, sample/retry/duplicate correctness tests, and exported CSV/JSON/SVGs. Source binary provenance records the older cached fork honestly. Mac is explicitly deferred/untested; external SSH inventory execution is untested.

Base Issue04 is ready for integration. CR-LAYOUT and the remaining calibration/defined evaluation metrics belong to coordinator ticket11 and are not claimed complete. There is no known source capability gap for preserving duplicate blind-write inputs or collecting the existing 1PC success/failure counters: both are retained with their separate numerator/denominator semantics. This branch does not change CC code or database algorithms.
