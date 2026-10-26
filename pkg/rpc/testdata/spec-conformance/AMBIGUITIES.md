# Specification questions and pending expectations

This register identifies behavior not uniquely determined by specification 1.2.
It describes the open choice and the scope that a conformance scenario can safely
assert. Recommendations are proposals, not approval to change implementation.
A14 is resolved; the other entries remain specification questions.

Pending scenarios are in `scenarios/p-pending-ambiguity.json`. They contain
`expect_by_option` and are skipped. To resolve one, update the specification,
select its expectation, mark it active and synchronize checksums and runner
identities. Active scenarios avoid outcomes that depend on unresolved choices.

## A1. Control messages in conservation counters (1.1, 1.7)

Does intercepted traffic include commit, abort, prepare, ResolveIntent and
coordinator decisions, or only sortable data messages? P-05 covers the choice.
Active comparisons normalize to data messages as FORMAT 6.1 specifies.
Recommendation: count all transactional messages entering Juicer, classify
controls as direct, and expose their count separately.

## A2. Control-message markers (5.1, 9.1)

The phrase "each touched managed key" does not explicitly exclude controls.
The system adapters treat controls as observed events, not queued data markers.
Recommendation: state explicitly that release-only controls create no markers.
The scenario language already models them as control events.

## A3. Candidate retention across epochs (2.1, 2.3)

The specification resets first-level counts but does not define the next epoch's
initial observation set. Choices are empty, the preceding top K plus current
candidates, or all candidates ever observed. The shared Go analyzer's ApplyDelta
adds candidates without removing them. P-01 distinguishes the alternatives.
Recommendation: retain the preceding top K for one epoch and retain nominated
keys until removal; stop observing other withdrawn candidates.

## A4. Arrivals before nomination within an epoch (2.2, 2.3)

"Starting this epoch" can mean retroactive observation from the epoch boundary
or observation starting at candidate admission. Active cases avoid dependence on
retroactivity. Recommendation: start at admission; retroactive reconstruction
requires per-key arrival history independent of K.

## A5. Observation while draining (2.3, 8.2)

Does observation stop when removal starts, or when the queue is deleted?
This controls whether a draining noncandidate can be renominated. P-02 covers
it; S08-03 explicitly supplies a candidate during the draining epoch.
Recommendation A5-a: observe until deletion so cancellation of draining remains
reachable independently of first-level candidacy.

## A6. Unbound identity and conflicting holds (3.5, 5.2)

"Must admit" an unbound head can mean unconditional admission, or merely that
identity binding is not an additional blocker. P-03 distinguishes them; S05-12
uses no conflicting hold. Spanner treats transaction zero as a real identity,
so these unbound cases apply only to CC/CRDB. Recommendation A6-a: respect buffer
and dependency conditions, then admit without registering a hold.

## A7. First-level ties (2.1)

The specification gives a second-level byte-order tie-break but no first-level
rule. Active cases avoid ties at the K boundary. Recommendation: use smaller
key byte order for equal abort counts, matching the second level.

## A8. Draining queues in queue counts (2.6, 8.1)

An existing draining queue consumes resources but is no longer accepting.
P-06 distinguishes inclusion in `queues_now`; active cases avoid disputed
snapshots. Recommendation A8-a: count every existing queue and report draining
queues as a separate subset.

## A9. Cancellation after delivery (1.3, 10.1)

The cancellation clause describes messages still held by Juicer; it does not
specify whether cancellation during handler execution removes admitted holds.
P-04 covers the case. Recommendation A9-a: retain those holds until their normal
release event or H. Delivery has already occurred and storage locks do not
vanish merely because the caller abandons the RPC.

## A10. Zero and sub-millisecond hold caps (4.3, 6.2)

The default cap is described as at least 1 ms, but the validity of smaller
configured values is unspecified. Active scenarios use H >= 7 ms.
Recommendation: reject H < 1 ms, including zero, rather than treating zero as
unbounded. Any accepted behavior needs an explicit normative decision.

## A11. Expiry boundary and equal-time inputs (6.1)

Does a hold stop blocking at age >= H or > H, and does expiry precede input at
the same time? Inputs stay at least 500 us away from expiry; comparisons use
100 us tolerance. Recommendation: expire at >= H and process expiry first.

## A12. Lazy-expiry lifetime and accounting (6.1, 6.4)

An idle hold may be swept only when a later head is evaluated. Should its
lifetime be H or its age when swept? Should an expired unswept entry count as
ended at scenario completion? FORMAT 7.2 permits lazy sweep and count intervals.
Recommendation: lifetime is min(release time, start+H)-start; separately report
expired unswept entries.

## A13. Epoch deltas versus cumulative counters (1.7, 11.1)

The specification explicitly makes conservation cumulative, but does not define
whether late-marker and hold-end distributions are cumulative or per epoch.
S11-03 avoids distinguishing them. Recommendation: per-epoch deltas for
distributions and late counts, cumulative conservation counters.

## A14. Banner specification version — resolved

Every implementation declares `spec_version` exactly `1.2`. S11-01 compares
that field strictly. Use the normative specification version without a suffix.

## A15. CRDB EndTxn keys outside the anchor range (table 5-C)

EndTxn may synchronously resolve keys in the anchor range while other local
ranges still have intents awaiting asynchronous ResolveIntent. The SPI can
report applied completion for point keys in the lock spans and in-flight writes
once the transaction is decided. Is that sufficient, or must each other range
wait for its own ResolveIntent? P-07 distinguishes the choices. S05-15 separates
anchor and remote-intent events to avoid the ambiguity.

Recommendation A15-b: EndTxn releases holds in the anchor range; other ranges
use ResolveIntent completion. Alternative A15-a explicitly permits decided-state
release and documents that conflicting requests resolve decided intents rather
than abort. This register does not authorize changing the SPI.

## A16. Own release after expiry but before sweep (6.1, 6.4)

When an own reply/applied event arrives after start+H but before lazy expiry
cleanup, should the reason be `event` or `ttl`? This affects lifetime and expiry
share. Active cases avoid the condition: their holds end before H or expire
while blocking a head. Recommendation: attribute to TTL at start+H with lifetime
H, even if accounting occurs later.

## A17. CRDB hashed queue-key tie-break (2.1, 2.4)

CRDB uses an FNV-64a int64 queue-key identity. Comparing its decimal spelling is
not raw-key byte order, so equal-count nominations can differ. Core scenario
S02-04 uses an order-preserving mapping and does not establish this adapter
property. Options: (a) use raw-key bytes for identity/order and update manual-key
configuration; (b) specify a CRDB exception ordering by queue-key hash.
User decision required. Option (a) also needs an adapter test whose raw and hash
orders differ.

## Pending buffer definitions

These are explicitly undecided specification parameters, represented by
`pending_caliber` rather than the ambiguity numbering:

- Sections 4.3/6.3: whether W <= W_max <= H is required at startup. S04-04
  encodes rejection for W > H and acceptance for W = H if that rule is chosen.
- Sections 4.1/4.6: definitions of L_a, r, F(W), candidate waits, thresholds,
  scale s and sample population. S04-05 asserts the intended epoch/key
  stability property but requires numerical inputs. Formula cases belong in
  `s04-formula.json` once their normative definition is approved.
