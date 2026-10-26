# CRDB baseline stability and dispatch boundary

T-CR-00/01 have an explicit completion-time stability capability. CRDB
bench validates a frozen policy, computes time-series evidence from existing
native measured terminal timestamps and exports its decision. Common consumes
the decision at its existing dispatch, continuation and selection seams.
Use the maintained adapter and common executor for this capability.

## Scientific binding and implementation boundary

The cards require checking stability and stopping on unstable off, but supply
no adopted numerical definition. `measurement.stability` therefore requires
an explicit choice of `completion_bin_spread_v1` and every threshold: bin width,
minimum full bins, minimum terminal samples per bin, minimum min/max commit-rate
ratio and maximum max/min bin-p99 ratio. `stop_on_unstable_off` must be true.
Common T-CR-00/01 templates use a deliberately unbound null placeholder that
the adapter rejects until a site policy is supplied. Input
without the whole policy is `not_assessed`, never silently stable. The full
schema and population are in `bench/EVALUATION.md`.

This method is an implementation option that must be explicitly selected,
not a scientific default or an adopted stationarity test. The campaign needs a scientifically reviewed method and threshold policy before
it can claim baseline stability.

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
boundary. The shared runner and analysis interfaces enforce this dispatch boundary.


See [the evaluation guide](../EVALUATION.md) for configuration and
[verification procedures](../VALIDATION.md) for the relevant checks.
