# SQL automatic retry text observation

This is **TEXT CLASSIFICATION**, not structured SQL causality. The native SQL
wrapper retains rendered text, not the original KV error detail. KV restart
metrics remain a separate observation with a different counting boundary.

## Native boundary and metrics

`pkg/sql/conn_fsm.go::prepareTxnForRetryWithRewind` increments one element of a
fixed nine-counter transaction array under the existing transaction lock,
immediately after the unchanged `autoRetryCounter` increment. Both count even
when `PrepareForRetry` fails. `txn_state.go::resetForNewSQLTxn` resets the array
with that total. `conn_executor_exec.go::recordTransactionFinish` exports every
class immediately after `TxnRetryCount`, before existing early returns, on both
commit and error terminals. Retry decisions, error payloads, rewind order,
protocols and terminal acceptance are unchanged.

Each fixed counter is named `sql.txn.auto_retry.text_reason.<reason>.count`,
exported as `sql_txn_auto_retry_text_reason_<reason>_count`. Reasons are
`txnaborted`, `writetooold`, `serializable`, `txnpush`,
`readwithinuncertainty`, `asyncwritefailure`, `commitdeadlineexceeded`,
`exclusionviolation`, and `unknown`. EngineMetrics registers the array through
the existing metric registry. Internal executors get the existing `.internal`
suffix and internal metadata; the bench attribution uses external SQL counters.
There are no dynamic labels, per-retry logs or retained keys/errors in this array.

The class sum equals its **own SQL retry total at a completed terminal export**.
The total and classes are separate counters; a concurrent Prometheus scrape can
observe an intermediate export. A sampled mismatch is reported, never repaired.
Unfinished transactions export neither their total nor their classes yet.

## Decoder evidence and limits

The decoder uses `errors.As` for exactly
`*kvpb.TransactionRetryWithProtoRefreshError`. It inspects only that wrapper's
`MsgRedactable.StripMarkers()` or legacy `Msg`. Conflicting populated fields,
multiple visible wrapper causes, secondary error attachments, incomplete or
unsupported text, and absent typed wrappers produce `unknown`. Joined error
branches are inspected; outer error messages are never searched for reasons.

Anchored native formats from `pkg/kv/kvpb/errors.go` support:

- `TransactionRetryError: retry txn (<native enum>...)`: write-too-old,
  serializable, async-write-failure and commit-deadline-exceeded enums.
- `TransactionAbortedError(<native abort enum>)`: txnaborted.
- Native WriteTooOld timestamp framing, uncertainty interval framing, push
  transaction framing, and exclusion key/timestamp framing: their matching class.

Optional transaction suffixes from `kvpb.Error` are accepted without searching
them for causes. Unsupported formats and multiline details remain unknown.

All constructor routes were inspected: `TxnCoordSender.handleRetryableErrLocked`
passes `redact.Sprint(pErr)`; `TxnCoordSender.GenerateForcedRetryableErr` passes
arbitrary text; savepoint validation passes `cannot ... savepoint after a
transaction restart`; `MockTransactionalSender.GenerateForcedRetryableErr` also
passes arbitrary text; callers can invoke the constructor directly. Constructor
behavior and KV/proto sources are unchanged. Tests render actual native error
constructors through `kvpb.NewError`, with/without transaction suffixes, and
cover modern/legacy redaction fields, outer wrappers and joined causes.

Ordinary forced, injected and savepoint text stays unknown. **A forced or direct
constructor can supply exact native-looking text and receive that matching text
class. It is indistinguishable here and is not proof of an original KV cause.**
The native lifecycle test exercises this through the mock sender's actual
forced-retry API. Classification cannot recover discarded structure.

## Bench attribution

`diagnostics.evaluation.sql_retry_cause_attribution` reports per-server counts,
fractions, status and reasons, plus a pooled population only when every configured
server is complete and there are no unexpected servers. Each numerator is the
reason counter delta; its denominator is `sql_txn_auto_retry_count` from the same
node and identical first/last scrape bytes and timestamps. Raw SHA-256 identities
and window endpoints are retained. Pooling sums counts and these SQL totals;
it does not average fractions or borrow KV commits/reasons.

The existing full `/_status/vars` capture is reused. When `.metrics.jsonl` is
present, intermediate successful scrapes are checked inside the selected summary
boundaries, and those boundaries must match the raw capture exactly. Summary-only
fixtures retain their two raw boundaries and identify that validation source.
Missing/duplicate/changed label series, missing configured servers, missing
uptime, observed uptime/counter resets, and malformed capture make attribution
unavailable. A complete but unequal SQL sum is invalid. Sampling cannot exclude
a reset and complete recovery between observed scrapes. No second monitor runs.

Unknown counts remain visible. A zero SQL total retains raw zeros and null
fractions. Old binaries without these counters are explicitly unavailable.
`sql_retry_cause_identity` remains the separately named legacy KV/SQL comparison;
legacy required counters, `require_retry_identity`, result validity and frozen
client transaction/attempt counters keep their existing semantics. New SQL text
attribution status does not silently impose a new baseline-validity policy.

## Focused verification

Maintained Bazel targets are `//pkg/sql:txn_retry_reason_test` (native decoder
and constructor tests) and `//pkg/sql:txn_retry_observation_test` (embeds the
actual `:sql` library, adds lifecycle and metric registration/exposure tests).
Use the offline flags in `bench/BUILD.md`, a dedicated output root, bounded
jobs/RAM, and explicitly empty remote cache/executor settings. These targets
avoid the enormous `//pkg/sql:sql_test` suite.

Bench tests are `bench/tests/test_sql_retry_observation.py`, alongside existing
adapter/evaluation/review/stability tests. Development receipts, commands,
source hashes, logs and JUnit evidence belong under
`build/sql-retry-observation/`. Unit/fixture acceptance is distinct from live
HTTP capture acceptance; a cached older binary cannot establish the latter.
