# CRDB baseline stability and dispatch boundary

T-CR-00/01 now have an explicit completion-time stability capability. CRDB
bench validates a frozen policy, computes time-series evidence from existing
native measured terminal timestamps and exports its decision. Common consumes
the decision at its existing dispatch, continuation and selection seams.
No database/client algorithm or Spanner source changed. Root README files
belong to the coordinator and are excluded from these commits.

## Scientific binding and implementation boundary

The cards require checking stability and stopping on unstable off, but supply
no adopted numerical definition. `measurement.stability` therefore requires
an explicit choice of `completion_bin_spread_v1` and every threshold: bin width,
minimum full bins, minimum terminal samples per bin, minimum min/max commit-rate
ratio and maximum max/min bin-p99 ratio. `stop_on_unstable_off` must be true.
Common T-CR-00/01 templates use a deliberately unbound null placeholder that
the adapter rejects until a site policy is supplied. Legacy/custom input
without the whole policy is `not_assessed`, never silently stable. The full
schema and population are in `bench/EVALUATION.md`.

This method is an implementation option that must be explicitly selected,
not a scientific default or an adopted stationarity test. All fixture numbers
below are synthetic test bindings. The campaign still needs a scientifically
reviewed method/threshold policy before it can run or claim baseline stability.

`stability.py` is a small CRDB-specific parser/statistic module. It uses only
validated native measured events with both execution endpoints inside their
client window. Completion bins align relative to each process measurement
start; native monotonic offsets subtract configured warmup, while old records
use native wall timestamps relative to their own window. Clock modes and
window/source references remain explicit, without assuming aligned host clocks.
Warmup and unfinished-at-end transactions do not enter this population.

Bins retain commits, error terminals, exposure, commit rate and merged-event
nearest-rank p99. Per-process counts remain separate. Quantiles are computed
after pooling events, never by averaging client quantiles. Both decision
ratios retain raw numerator and denominator. Strict bound violations are
unstable; equality passes. Inadequate samples or zero denominators are
insufficient, not measured zeros. A witnessed commit-rate violation remains
unstable even if an empty bin prevents a p99 judgment. This does not identify
an aggregate throughput plateau as stability or override client capacity
diagnostics; capacity-limited points retain the existing selection rejection.

CRDB `adapter.py` adds policy validation; `results.py` feeds native events into
the assessor; `evaluation.py` adds `stability.json`, `stability-series.csv`,
status in metric CSV and state-labelled curves. Unstable/insufficient metrics
remain visible but not comparison-eligible. Raw/source result hashes,
populations and policy are retained alongside the full original metrics.

Common adds a direct check after each successful cell and over verified linked
prior results before new dispatch/build. A policy-bound off that is not stable
ends the batch as stopped, with structured reason and `stability-stop.json`.
Its attempt remains successful and measurement valid; instability is not a
collection/parse corruption. No higher-rate cell, new pair, extension or
followup launches afterward. On treatment cannot trigger the off-only stop.
The existing 452f759 AB/BA repeat order is unchanged. Continuation follows
all batch links and cannot forget the earlier unsafe off; absent stability
evidence under a bound policy also blocks dispatch.

Selection retains the existing frozen version-1 knee rules. Any non-stable
repeat excludes that entire load and higher loads. A valid earlier knee can
be recorded without running followups after a stop. Otherwise selection
records `insufficient_evidence` and null selected load. No arbitrary last-good
or 80%-of-knee workpoint is introduced, and no extension retries an unsafe
boundary. The common patch is limited to `runner.py`, `analysis.py`, its focused
public lifecycle tests, the two CRDB template bindings and documentation.

## Actual checks and retained evidence

Working directory:
`/home/lihan/Documents/code/cpi-ns/experiment-framework-worktrees/integration-crdb`.
CRDB base at final checks: `68800a84316fd69ee7aa47ca272b0ac0dbc66f6e`.
Common base: `10e45e633b24f295586ab0be02b917a14707080c`, including 452f759.
Parent-selected model is `gpt-6.1-sol`, high effort; available tools do not
independently expose the actual backend identifier. No alternate identifier
is asserted. Existing Anaconda Python/pytest were used without installation.

The first public parser check failed as expected before implementation
(`KeyError: stability`, exit 1). The first implementation passed 17 focused
checks. After adding multi-client, export and extension-boundary checks, the
following affected regression command exited 0: **57 passed, 0 skipped**, in
15.85 seconds. This includes 15 new native-record parser/export/policy checks,
6 new public lifecycle/selection checks, and 36 existing regressions:

```sh
PYTHONPATH=/home/lihan/Documents/code/cpi-ns/experiment-common:bench \
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
/home/lihan/anaconda3/bin/python -m pytest \
  bench/tests/test_stability.py bench/tests/test_adapter.py \
  bench/tests/test_evaluation.py bench/tests/test_review.py \
  /home/lihan/Documents/code/cpi-ns/experiment-common/tests/test_stability_stop.py \
  /home/lihan/Documents/code/cpi-ns/experiment-common/tests/test_runner.py \
  /home/lihan/Documents/code/cpi-ns/experiment-common/tests/test_analysis.py \
  /home/lihan/Documents/code/cpi-ns/experiment-common/tests/test_config.py \
  -q -p no:cacheprovider --tb=short \
  --junitxml=bench/results/stability-20261022/affected.junit.xml
```

Native-record fixtures verify 0.5-second bins with one commit each produce
2 commits/s and p99=4 ms; a final 20 ms bin produces a p99 ratio of 5 and an
unstable decision while preserving measurement validity. Two client windows
with different wall-clock origins pool two events per bin: rate=4/s and
p99=8 ms, rather than averaging client p99s to 6 ms. Both native monotonic
offset/warmup and legacy timestamp modes are tested. The public evaluation
CLI retains the unstable series and all original file hashes remain unchanged.

Lifecycle checks launch only tiny owned Python executable fixtures, not CRDB
or SQL load. Counterbalanced execution at a safe rate is off/on/on/off; the
next unstable off stops before on/higher rates. Two linked continuations
launch no additional cells and retain the original boundary. Separate checks
verify unstable treatment does not stop dispatch; insufficient off cannot
trigger extensions/followups; an earlier stable knee remains selected using
existing rules; and an unstable extension cannot trigger the next extension.

`bench/results/stability-20261022/` retains `tests-red.log`, `tests-focused.log`,
`tests-affected.log`, both JUnit files and `execution.json` with actual commands,
exit codes, counts, model disclosure and tested source hashes from both repos.
`fixtures/` retains the passing parser export, linked stop/continuation,
stable-knee-after-stop and extension-stop cases. These are copies of exact
passing fixture outputs, with `fixture-retention.json` mapping original paths;
no workload or test was rerun to package them. No executable edits followed
the 57-check pass. `commit-handback.json` records the exact owned signed common
and CRDB commits plus signature verification.

## Limits

No real baseline campaign or threshold adoption is claimed. This bounded
capability does not diagnose a physical instability cause, assert statistical
stationarity, align unobserved host clocks, or invent a default safe workpoint.
Data integrity, time-series stability, client capacity and knee evidence remain
separate. No AWS operation, native workload, dependency installation, Skill
test, Spanner edit, push or main merge was performed.
