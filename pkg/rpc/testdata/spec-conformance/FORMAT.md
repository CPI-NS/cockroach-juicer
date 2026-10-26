# Juicer conformance scenario format

Format version: `1.0`. Specification version: `1.2`.

The CC, CRDB and Spanner runners read UTF-8 JSON scenarios using the same neutral
fields and comparison rules. Scenario descriptions are human-readable metadata;
keys and event types are machine-readable. Time fields ending in `_us` are integer
microseconds. Configuration durations are multiples of 1,000 us so that each
adapter can express them. Expected behavior must follow the specification;
unresolved cases use `pending_ambiguity` and link to `AMBIGUITIES.md`.

## 1. Scope and layers

A scenario models Juicer on one server: filtering, nomination, removal, per-key
queues, dependency holds, multikey admission, cancellation, banners and summaries.
Inputs supply network arrivals and storage responses; the suite does not simulate
the database's concurrency-control implementation.

| Layer | Interface under test |
|---|---|
| `core` | Filters, queues, enforcer and dispatcher with an injected clock |
| `adapter` | Real system interceptor/SPI/classifier with system message shapes |
| `banner` | Configuration declaration and epoch-summary fields |

## 2. File envelope

```json
{
  "format": "juicer-spec-conformance",
  "format_version": "1.0",
  "spec_version": "1.2",
  "file": "s05-rules",
  "title": "Dependency rules and applied control events",
  "clauses": ["§5"],
  "scenarios": []
}
```

Reject an unknown `format` or a `format_version` whose major version is not 1.

## 3. Scenario fields

| Field | Required | Meaning |
|---|---|---|
| `id` | Yes | `S<section>-<number>` or `P-<number>` |
| `name` | Yes | Human-readable name |
| `spec_refs` | Yes | Specification clauses supporting the expectations |
| `layer` | Yes | `core`, `adapter`, or `banner` |
| `applies_to` | Yes | Subset of `juicer-cc`, `crdb`, `spanner` |
| `not_applicable` | No | Reasons for excluded systems |
| `status` | Yes | `active`, `pending_caliber`, or `pending_ambiguity` |
| `blocked_by` | No | Ambiguity identifier for a pending case |
| `caliber` | No | Buffer-policy definition awaiting a decision |
| `purpose` | Yes | Test objective |
| `config` | Yes | Configuration; `{}` in a banner case selects native defaults |
| `config_variants` | No | Named overrides, each checked independently |
| `adapter_notes` | No | System-specific input mapping |
| `inputs` | Yes | Events in nondecreasing time order |
| `end_us` | Yes | Final clock advance and observation time |
| `checkpoint_times` | No | Intermediate snapshot times |
| `tolerance_us` | No | Timing tolerance, default 100 us |
| `expect` | Yes | Expected fields; may be null for pending cases |
| `expect_by_option` | No | Alternative expectations for unresolved decisions |
| `notes` | No | Explanatory metadata |

## 4. Configuration

### 4.1 Fields

| Field | Meaning | Default |
|---|---|---|
| `filter.mode` | `hotspot` filtering, or `fixed` managed-key queues | `hotspot` |
| `filter.managed_keys` | Fixed-mode managed keys | Explicit |
| `filter.K` | First-level candidate limit | 100 |
| `filter.S` | Second-level nominations per epoch | 1 |
| `filter.idle_epochs` | Consecutive reorder-free epochs before removal | 3 |
| `filter.epoch_us` | Declared epoch length; explicit `epoch_end` drives boundaries | 10000000 |
| `rules` | `conflict-table` or sorting-only `none` | `conflict-table` |
| `H_us` | Hold cap; explicit except in default-banner tests | Explicit |
| `buffer` | Fixed `W_us`, or adaptive `enabled` | Fixed, zero |
| `late_policy` | `queue` or nondefault `bypass` | `queue` |
| `queue_removal` | Stop intake, drain, delete; nomination may cancel draining | `drain` |
| `max_held` | Spanner-only concurrent held-message limit | Unset |
| `diag` | CRDB per-message diagnostics | false |

Disable autonomous epoch timers: only input `epoch_end` advances epochs. The
configured epoch length is used for declarations, not to schedule scenario inputs.
In fixed mode, CC creates each queue on its first message. A scenario must touch
all fixed managed keys before asserting queue counts.

### 4.2 Native mapping

| Field | CC | CRDB | Spanner |
|---|---|---|---|
| Fixed managed keys | `--manualQueueKeys` | `COCKROACH_JUICER_MANUAL_QUEUE_KEYS` | `--juicer_manual_queue`, `--juicer_manual_keys`; disable static passthrough |
| K / S | `--filterK`, `--filterS` | `COCKROACH_JUICER_FILTER_K`, `_FILTER_S` | `--juicer_filter_k`, `--juicer_filter_s` |
| Idle epochs | `--idleEpochs` | `COCKROACH_JUICER_IDLE_EPOCHS` | `--juicer_idle_epochs` |
| Epoch | `--epochSec` (seconds) | `COCKROACH_JUICER_EPOCH_MS` | `--juicer_epoch_ms` |
| Nomination | Epoch end | Epoch end | Epoch end |
| Observation | Candidate's current epoch | Candidate's current epoch | Same-epoch watch |
| Conflict table | `--juicerRules=conflict-table` | `COCKROACH_JUICER_RULES=conflict-table` | `--juicer_rules=conflict-table` |
| Sorting only | NCC or MVTO | Not applicable | Non-2PL store; outside current Spanner evaluation scope |
| Hold cap | `--maxHoldMs` | `COCKROACH_JUICER_MAX_HOLD_MS` | `--juicer_max_hold_us` |
| Fixed wait | `--waitPolicy=fixed --fixedWaitUs` | `COCKROACH_JUICER_FIXED_WAIT_MS` | `--juicer_fixed_wait_us` |
| Adaptive wait | `--waitPolicy=marginal` | `COCKROACH_JUICER_AW` | `--juicer_adaptive_wait` |
| Late handling | `--latePolicy` | `COCKROACH_JUICER_LATE_POLICY` | `--juicer_late_policy` |
| Removal | Drain | Drain | `--juicer_queue_removal=drain` |
| Held-message cap | Not applicable | Not applicable | `--juicer_max_held` |
| Diagnostics | Native diagnostic options | `COCKROACH_JUICER_DIAG` | Native diagnostic options |

Spanner abort-first delivery, orphan-marker cleanup and no-reply abort release
use their enabled defaults unless a scenario explicitly targets those features.

### 4.3 Variants

Each variant contains `name`, `override`, and optional `note`/`expect_startup`.
Apply field overrides to `config`; null omits an option and selects its native
default. Run the complete input sequence against the same expectation per variant.
An explicit startup expectation checks only `accept`/`reject`, without inputs.

## 5. Inputs

All inputs have `t_us` and `type`. Process equal-time inputs in file order.
Scenario inputs and timer expiry times must be separated by at least 500 us,
avoiding the unresolved simultaneous input/expiry ordering in A11.

### 5.1 Data arrival

```json
{"t_us": 1000, "type": "arrive", "msg": "b", "txn": 2, "ts": 200,
 "ops": [{"key": "k1", "access": "write"},
         {"key": "k2", "access": "read", "class": "request"}],
 "payload": "b-payload-12", "ts_by_system": {"juicer-cc": 100}}
```

`msg` is unique within the scenario. Positive `txn` identifies a transaction;
zero denotes an unbound identity where the system supports that concept.
`ts` is an integer or CRDB `[wall, logical]`; integer n means `[n, 0]`.
`ts_by_system` overrides a system's sorting key without changing the expectation.

Each `ops` item names a key and `access`: `read`, `write`, or `lock_read`.
Locking reads include GetForPut and count as writes in the conflict relation.
A multikey message's repeated accesses merge into one marker per key, choosing
write over locking read over read. This marker-footprint rule does not remove
transaction operations or repeated draws from the actual workload.

`class` is `control`, `request`, or default `any`. Writes/locking reads are
control-class. CC reads and Spanner RW reads are control-class; CRDB plain reads
and Spanner RO reads are request-class. `any` cases avoid release observations
whose expected outcome would depend on that distinction. `payload` defaults to
`msg` and must reach the handler unchanged.

Record reorders for observed keys. Enqueue markers only on accepting managed
queues, applying the declared late policy. Draining and unmanaged keys add no
queued marker. Deliver a message once all its queued markers are released;
messages without such markers pass directly.

| Abstract operation | CC | CRDB | Spanner |
|---|---|---|---|
| Message | Single-key RPC | Local BatchRequest with batch-header `Now` | RW prepare or RO commit at one shard |
| Control-class read | Get | Not applicable | RW read set |
| Request-class read | Not applicable | Nonlocking Get/point Scan/Refresh | RO read |
| Write | Set | Put/Delete/CPut/Increment | RW write set |
| Locking read | GetForUpdate/GetForPut | Locking Get/Scan | Not applicable |

Mark scenarios inapplicable when the native message shape cannot represent them.

### 5.2 Other events

| Type | Fields | Effect |
|---|---|---|
| `reply` | `msg`, `outcome`: `ok`/`failed` | Native handler response; release only the request's own matching hold tokens |
| `control_arrive` | `txn`, `kind`: commit/abort/prepare, `keys` | Observe control arrival without queueing it or releasing applied-event holds |
| `control_applied` | `txn`, commit/abort `kind`, `keys`, optional `via` | Storage completion on these keys; synthesize arrival if none was supplied |
| `cancel` | `msg` | Abandon an undelivered message; P-04 separately describes after-delivery cancellation |
| `level1_abort` | `txn`, `keys` | One first-level abort input for each transaction/key pair |
| `epoch_end` | None | Nominate, remove idle queues and reset epoch statistics |
| `advance` | None | Advance the injected clock and due timers |
| `arrive_non_txn` | `msg` | Adapter-layer nontransactional traffic |

Epochs start at zero; the i-th `epoch_end` ends epoch i-1. In core scenarios,
first-level input comes only from `level1_abort`; do not additionally count
failed replies or abort controls. Adapter scenarios use the SPI's actual abort
sources instead. A real-interceptor core driver may still report failed replies
through the native SPI: such cases must use fixed filtering or otherwise prove
that this cannot change expectations.

CRDB core drivers inject `control_arrive` directly. Adapter-level arrival without
application uses an EndTxn response whose transaction remains pending.

| `control_applied.via` | System | Required native observation |
|---|---|---|
| `commit_handler_returned`, `abort_handler_returned` | CC | Commit/Abort handler return |
| `endtxn_anchor` | CRDB | EndTxn handler return on anchor |
| `endtxn_lock_span` | CRDB | Local point key in EndTxn lock spans |
| `resolve_intent` | CRDB | ResolveIntent return, transaction ID from `IntentTxn` |
| `one_pc_reply` | CRDB | Write-batch response with committed transaction |
| `coordinator_final_reply` | Spanner | Final coordinator reply after commit/unlock |
| `participant_decision_applied` | Spanner | Decision applied by participant storage |
| `abort_delivered_ok` | Spanner | Abort/Wound delivered to storage with OK |
| `noreply_abort` | Spanner | Participant aborted in PREPARE_WAIT without reply |

### 5.3 Identities

Scenario keys are ASCII and their mappings preserve byte order for tie-breaking.
CRDB may prefix every key identically. Map positive scenario transaction numbers
deterministically to native identities, including CRDB UUIDs. Spanner treats zero
as a real ID, so unbound-identity scenarios exclude it.

## 6. Expected observations

Omitted expectation fields are not compared. Supplied fields are mandatory
unless explicitly described as optional.

| Field | Observation and comparison |
|---|---|
| `startup` | Exact `accept`/`reject` |
| `deliveries` | Complete set of `{msg,t_us,path}`; path queued/direct/not_intercepted; optional early delivery |
| `drops` | Complete set of `{msg,t_us}` |
| `releases` | Per-key ordered `{msg,t_us}` sequences; equal sorting keys have no ordering requirement |
| `holds` | Complete set by `(msg,key)`, with start and open/end reason/time; see 7.2 |
| `no_hold_for` | Message/key pairs that must never hold |
| `late` | Complete set of `{msg,key,handling}`; handling queued/bypass |
| `queue_events` | Created/drain_started/deleted/drain_canceled events; preserve per-key order |
| `epochs` | Listed per-key reorder counts; unlisted keys have zero or are unobserved |
| `checkpoints` | Listed managed/draining sets, queue lengths, held messages, live holds and counters |
| `counters` | Exact values or inclusive `{min,max}` intervals |
| `epoch_summaries` | Listed managed W/n/r, lifecycle, late, hold-end and conservation fields |
| `banner` | Nested fields and matchers below |
| `buffer_timers_armed` | Optional when instrumentation exposes it |

Hold endings are `event`, `ttl`, `cancel`, or `queue_deleted`. `open` means not
expired at the final time. Ignore expired but unswept entries in live-hold snapshots.
Equal-time deliveries on different messages and events on different keys have no
relative-order requirement.

### 6.1 Neutral counters

Count data `arrive` messages only. Subtract injected control traffic from native
intercepted/direct counters until A1 is resolved. Conservation is:

`intercepted = direct + queued_delivered + dropped + held_now`.

Also map `holds_created`, `hold_end.{event,ttl,cancel,queue_deleted}`,
`late_queued.{read,write,lock_read}`, `late_bypass.{...}`, `queues_now`,
`queues_max`, and Spanner `max_held_pass`. Avoid comparing queue counts during
draining until A8 is resolved.

### 6.2 Banner fields

Parse the native declaration into `spec_version`, `intercepted_types`,
`level1_abort_sources`, `K`, `S`, `epoch_us`, `idle_epochs`, `nomination`, `rules`,
`hold_end_events`, `H_us`, `buffer`, `late_policy`, `queue_removal`, `sort_key`,
`caller_abandon`, `hygiene`, `per_message_diag`, and `event_log`.
`sort_key` is `txn_timestamp` or CRDB `batch_clock`; `nomination` is `epoch_end`.
Hold-end events use `applied`, `own_reply`, `own_failure`, `ttl`. Spanner hygiene
covers abort-first delivery, orphan cleanup, no-reply abort release and max held.

At any nesting level, literals compare exactly; `{present:true}` checks presence;
`{min:n}` checks a lower bound; `{per_system:{...}}` selects a system value, with
null meaning the field is not printed; `{expected:v,compare:"warn"}` is advisory.

## 7. Comparison rules

### 7.1 Time

Injected-clock results are authoritative. Actual time lies in
`[expected, expected+tolerance_us]`, default tolerance 100 us. Event-driven outputs
should be exact; timer granularity accounts for the one-sided allowance.
Real-clock diagnostics are nonauthoritative: scale every time/duration by the
same integer f >= 1, allow f*100 us + 3 ms and identify the mode explicitly.

### 7.2 Hold endings

Reasons must match. Nonlazy endings use the timing rule above. A `lazy:true` TTL
may be swept after its expiry, or remain unswept at `end_us`; it must not be
reported early. Its counter may use an interval. Avoid own-release events arriving
between expiry and lazy sweep until A16 resolves event-versus-TTL attribution.

### 7.3 Invariants

| ID | Requirement |
|---|---|
| I1 | Every input message is delivered exactly once, dropped, or held; no invented message |
| I2 | Delivered payload equals input payload |
| I3 | Every hold corresponds to its released marker; no queued marker holds; at most one hold per message/key |
| I4 | Conservation holds at checkpoints and completion |

Check after every input if possible, and at least at checkpoints and completion.

### 7.4 Implementation freedom

Native names, log syntax, additional counters, histogram bins, epoch labels,
simultaneous cross-key order and lazy-sweep delay may differ. Do not compare
lifetime quantiles across implementations with different bucket definitions.
Spanner abort-first handling may order the terminal message and flushed requests
differently while preserving the required outcomes.

## 8. Results

Emit one result per scenario/variant/system: `pass`, `fail` with field differences,
`skip` with not_applicable/pending_caliber/pending_ambiguity/unsupported_layer,
or `error`. See README section 4 for JSONL output fields.

## 9. Versioning

The format version is 1.0. Additive fields require a minor version increment;
readers ignore unknown fields. Changed field semantics require a major version.
The exact suite identity is the SHA-256 of `SHA256SUMS`; do not embed that digest
inside a checksummed document. Scenario `spec_version` identifies its normative
specification. Update all copies and runner digest constants together.
