# Juicer specification conformance suite

This suite checks specification 1.2 through system-neutral scenarios for
Juicer-CC, CockroachDB and Spanner. Scenarios use format 1.0, injected time,
explicit arrivals/storage completion/cancellation, and observable expectations.
They test the Juicer interface rather than database performance.

## 1. Contents

| Path | Purpose |
|---|---|
| `FORMAT.md` | Input fields, native mappings, comparison rules and invariants |
| `AMBIGUITIES.md` | Pending specification choices and their test boundaries |
| `SHA256SUMS` | SHA-256 of every suite file except the manifest itself |
| `scenarios/s01-conservation.json` | Interception and conservation |
| `scenarios/s02-filter.json` | Candidate observation, nomination and idle removal |
| `scenarios/s03-ordering.json` | Marker order and dependency admission |
| `scenarios/s04-buffer.json` | Buffer deadlines and pending adaptive definitions |
| `scenarios/s05-rules.json` | Conflict relations and system applied events |
| `scenarios/s06-hold-cap.json` | Hold expiry and cross-key waits |
| `scenarios/s07-late.json` | Late markers and policy |
| `scenarios/s08-lifecycle.json` | Drain, deletion and renewed nomination |
| `scenarios/s09-multikey.json` | Whole-message admission and repeated accesses |
| `scenarios/s10-cancel.json` | Caller cancellation and no-reply abort |
| `scenarios/s11-banner.json` | Configuration declarations and epoch summaries |
| `scenarios/p-pending-ambiguity.json` | Alternative expectations awaiting decisions |

## 2. Coverage

The JSON files declare each scenario's system applicability, layer, status,
configuration variants and specification references. `active` cases run;
`pending_caliber` and `pending_ambiguity` cases are skipped with their reasons.
Pending alternatives do not count as established conformance.

The layers are `core`, `adapter` and `banner`. CC has single-key RPCs; CRDB
has multikey batches and physical/logical clocks; Spanner has shard-local
messages and participant/coordinator completion events. A system that cannot
represent a native shape reports not_applicable rather than simulating success.

The suite does not establish concurrent atomic delivery, read-only access to
concurrency-control state, race freedom, absence of disabled statistics objects,
calibrated H, time-weighted queue populations or workload no-dedup semantics.
Those need source review and the corresponding native tests or measurements.
Adaptive formula cases require an approved numerical definition.

## 3. Driver requirements

Use an injected clock and the production queue/filter/enforcer paths. Observe
native events; keep expected behavior out of the implementation driver. Preserve
message payload, identity, per-key order and whole-message admission.

- Core scenarios drive the core interfaces. Supply first-level abort input only
  through `level1_abort`, subject to FORMAT's real-interceptor exception.
- Adapter scenarios drive actual message classification/SPI and the indicated
  storage-applied completion path.
- Banner scenarios parse actual configuration declarations, including defaults.
- Mark unsupported layers as skips; do not substitute a reference model for the
  implementation under test.
- Use FORMAT's timing, lazy expiry, equal-key and cross-key ordering allowances.
  Check I1-I4 at every checkpoint and completion.

## 4. Integrity and result contract

The runner pins the SHA-256 of `SHA256SUMS`. Before executing any scenario,
verify that manifest digest, then every listed file. Any mismatch fails the
entire load. Reject unsupported format major versions.

For each applicable scenario and configuration variant: initialize a fresh
implementation, apply configuration, feed time-ordered inputs, pump due work,
advance to `end_us`, compare supplied expectations and check invariants.
Treat invalid setup as `error`; a field mismatch is `fail`.

Emit one JSON line per case with `suite_digest`, `format_version`, `spec_version`,
`system`, `repo`, `commit`, `clock_mode`, `scenario`, `variant`, `result`, `diffs`
and optional `skip_reason`. Differences identify `field`, `expected` and `actual`.
The final summary includes pass/fail/skip/error counts and the suite identity.
A real-clock diagnostic explicitly states its timing scale and is nonauthoritative.

## 5. Implementation entry points

- Shared Go core: `juicer/spectest` loads the suite; `juicer` supplies the
  deterministic core driver and native filter/queue tests.
- CC: the server's `TestSpecConformanceServer` verifies adapter integration;
  its pinned `juicer-grpc-go` submodule supplies the core suite and fixtures.
- CRDB: `pkg/rpc/juicer_conformance_test.go` exercises real BatchRequests,
  SPI applied effects and the server's configured interceptor. Go and Bazel
  consume its pinned shared gRPC submodule.
- Spanner: `src/store/juicer/tests/spec-conformance-runner.cc` uses the native
  engine, fake clock/timers and store/transport fixtures. Its own native tests
  cover integration properties not representable by the common schema.

Current performance evaluation runs Spanner 2PL only. Protocol-specific suite
applicability is a correctness-test property, not an expansion of that campaign.

## 6. Maintenance

Keep the scenario schema, applicability and expectations aligned with the
normative specification. Record underdetermined expectations in AMBIGUITIES;
activate them only after the corresponding decision. Documentation translation
must preserve all scenario inputs and expectations.

The workspace's specification directory holds the cross-system suite; the shared
Go repository and baseline test directories carry their required copies. Update
files, regenerate each manifest and update its runner digest in one coordinated
change. Preserve system-specific fixture differences where the loader requires
them. Verify every manifest and rerun the affected conformance entry.

To regenerate a manifest from its existing file inventory, hash each listed file
and retain its relative path/order. The suite identity is `sha256sum SHA256SUMS`.
Do not print that identity inside a checksummed file, which would create a
self-reference. Match both consumer gRPC pins when publishing shared Go changes.
