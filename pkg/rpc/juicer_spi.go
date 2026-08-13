// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/juicer"
)

// Juicer wires the fork's request-reordering layer (google.golang.org/grpc is
// replaced by github.com/CPI-NS/juicer-grpc-go, see go.mod) into the KV RPC
// server. It is entirely opt-in: with COCKROACH_JUICER unset (the default) no
// Juicer server options are appended and the gRPC server is byte-identical to
// upstream.
var (
	juicerEnabled          = envutil.EnvOrDefaultBool("COCKROACH_JUICER", false)
	juicerSkipQueueing     = envutil.EnvOrDefaultBool("COCKROACH_JUICER_SKIP_QUEUEING", false)
	juicerScaleFactor      = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_SCALE_FACTOR", 0)
	juicerMaxHoldMillis    = envutil.EnvOrDefaultInt("COCKROACH_JUICER_MAX_HOLD_MS", 25)
	juicerDebugLogs        = envutil.EnvOrDefaultBool("COCKROACH_JUICER_DEBUG", false)
	juicerUnaryBatchPaths  = []string{"/cockroach.roachpb.Internal/Batch", "/cockroach.roachpb.KVBatch/Batch"}
	juicerStreamBatchPaths = []string{"/cockroach.roachpb.Internal/BatchStream", "/cockroach.roachpb.KVBatch/BatchStream"}
)

// Measurement-arm knobs. Both select an alternative to the default sorting
// behaviour and both are off unless set, so a run that does not set them is
// the sorting arm exactly as before.
var (
	// juicerWaitStrategy names the fork's head wait strategy. "qlen-gated"
	// imposes the wait window only when the per-key queue holds at least
	// COCKROACH_JUICER_WAIT_MIN_QLEN markers; "fixed" imposes a constant
	// COCKROACH_JUICER_FIXED_WAIT_MS window measured from each message's
	// arrival, ignoring the damage feedback entirely; empty (the default) keeps
	// the contention-blind insertion-time strategy.
	juicerWaitStrategy = envutil.EnvOrDefaultString("COCKROACH_JUICER_WAIT_STRATEGY", "")
	juicerWaitMinQLen  = envutil.EnvOrDefaultInt("COCKROACH_JUICER_WAIT_MIN_QLEN", 2)
	// juicerFixedWaitMillis is the "fixed" strategy's window, in floating-point
	// milliseconds so a sweep can go below 1 ms. 0 makes "fixed" a zero-window
	// arm: pure sorting, no delay.
	juicerFixedWaitMillis = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_FIXED_WAIT_MS", 0)

	// juicerRandomDelay swaps the sorting queues for an injected random sleep
	// on nominated keys, drawn from the wait-window distribution Juicer
	// actually imposed (juicer.DefaultDelayQuantiles). Nothing is reordered
	// and nothing is mutually excluded: this is the control arm that
	// separates the effect of Juicer's ordering from the effect of its delay.
	juicerRandomDelay = envutil.EnvOrDefaultBool("COCKROACH_JUICER_RANDOM_DELAY", false)
	// juicerRandomDelaySeed makes the delay sequence reproducible; 0 seeds
	// from the clock.
	juicerRandomDelaySeed = envutil.EnvOrDefaultInt64("COCKROACH_JUICER_RANDOM_DELAY_SEED", 0)
	// juicerRandomDelayFixedMillis, when positive, replaces the quantile table
	// with a constant: every nominated operation sleeps exactly this many
	// (floating-point) milliseconds. The seed is then unused. 0 (the default)
	// keeps the empirical distribution.
	juicerRandomDelayFixedMillis = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_RANDOM_DELAY_FIXED_MS", 0)

	// juicerHoldUntilDone turns on the sorting queues' completion coupling:
	// a key admits its next message only after the previous one released on
	// that key has completed (its response was observed, success or failure).
	// Per message, independent of COCKROACH_JUICER_RULES; the two can be
	// combined. juicerMaxBusyMillis bounds one hold (the response can be
	// lost, or the released message can be waiting for its sibling markers
	// on other keys); 0 falls back to the fork's DefaultMaxBusy.
	juicerHoldUntilDone = envutil.EnvOrDefaultBool("COCKROACH_JUICER_HOLD_UNTIL_DONE", false)
	juicerMaxBusyMillis = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_MAX_BUSY_MS", 25)
)

// millisToDuration converts a floating-point millisecond knob to a Duration.
// Non-positive values yield 0, which every consumer reads as "not set".
func millisToDuration(ms float64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// juicerTxnKeys pins each transaction's sorting key for its lifetime; see
// juicerTxnKeyTable. It is package-level because the SPI is a stateless value
// type constructed per server, while the pin has to outlive individual calls.
// It is consulted only in juicerSortKeyPinnedRead mode (and as the fallback
// for a batch whose MinTimestamp is empty).
var juicerTxnKeys = newJuicerTxnKeyTable(juicerTxnKeyTTL, juicerTxnKeySweepInterval)

// juicerSortKeySource names where a transaction's sorting key comes from.
//
// Juicer sorts a key's operations by transaction. For that to mean anything the
// per-transaction sorting key must be one value, agreed on by every node that
// participates in the transaction and stable for the transaction's whole life.
// The two sources below differ in how much of that they achieve.
type juicerSortKeySource string

const (
	// juicerSortKeyMinTimestamp takes the key from the transaction header's
	// MinTimestamp: the gateway's clock reading when the transaction was
	// created (roachpb.MakeTransaction, pkg/roachpb/data.go). It is the right
	// answer to all three requirements at once, and it needs no state:
	//
	//   - It is assigned exactly once. The only writes after creation are
	//     Transaction.Update, which merges with Backward and so can never
	//     raise it, and TxnCoordSender.SetFixedTimestamp, which is rejected
	//     once the transaction has read or written and therefore cannot fire
	//     mid-flight.
	//   - It survives internal retries. Transaction.Restart, BumpEpoch and
	//     BumpReadTimestamp do not touch it, and kvpb.PrepareTransactionForRetry
	//     leaves it alone for every retry reason except TransactionAbortedError
	//     — which mints a whole new transaction, new UUID included, so the
	//     retry is a different transaction by every measure, not just this one.
	//   - Every node sees the same value. It rides in the embedded TxnMeta of
	//     Header.Txn, serialized into every BatchRequest; kvcoord clones the
	//     proto per range without modifying it, and the first server-side
	//     mutation (Store.Send's UpdateObservedTimestamp) runs downstream of
	//     this interceptor.
	//
	// CockroachDB already relies on exactly this property elsewhere:
	// kv.AdmissionHeaderForLockUpdateForTxn uses MinTimestamp.WallTime as a
	// transaction's stable FIFO ordering timestamp for admission control.
	juicerSortKeyMinTimestamp juicerSortKeySource = "min-timestamp"

	// juicerSortKeyBatchNow takes the key from the batch header's Now: the
	// gateway DistSender's clock reading, stamped once per client-level send
	// in initAndVerifyBatch (pkg/kv/kvclient/kvcoord/dist_sender.go) and
	// shared verbatim by every per-range sub-batch of that send.
	//
	// It changes the unit being sorted from "one transaction" to "one send
	// attempt". MinTimestamp is assigned once and survives every internal
	// retry, so late messages of a long-lived transaction sort as very old
	// and are mostly BYPASSED (their key is below the queue's released
	// watermark) — the sorting is thrown away exactly where it was supposed
	// to act, and the damage feedback measures transaction age instead of
	// arrival disorder (campaign 10: MaxDamage p50 4.6 s against a 0.169 ms
	// RTT). Now moves with each attempt, so the key-to-arrival gap shrinks
	// to one-way network latency and both defects close at once.
	//
	// Two knowingly accepted imperfections, from the 2026-08-13 audit:
	// DistSender-internal transport retries (NotLeaseHolder and friends)
	// re-send without re-stamping, so those arrivals still look as old as
	// their client-level send — but never as old as the transaction; and a
	// proxied sub-batch is re-stamped with the proxying node's clock, so it
	// can disagree with its siblings by up to the cluster's clock offset.
	// Cross-gateway ties within one HLC tick are broken arbitrarily by the
	// heap; unlike the MinTimestamp key, where every message of a
	// transaction ties by construction, such ties are vanishingly rare.
	juicerSortKeyBatchNow juicerSortKeySource = "batch-now"

	// juicerSortKeyPinnedRead is the campaign-7 behaviour: the key is the
	// ReadTimestamp of the first batch of the transaction *this node* happened
	// to see, held in juicerTxnKeyTable for the transaction's lifetime.
	//
	// It is retained only so campaign 7 can be reproduced. It solves the
	// mid-flight instability of a raw ReadTimestamp, but it solves it locally:
	// the pin is per node and first-sight, so a node that first sees the
	// transaction after a refresh pins a later key than one that saw it
	// earlier. Measured on the campaign-7 debug cells, 49.7% of multi-node
	// transactions (3,721 of 7,484) were pinned to different keys on different
	// nodes, which means the participants sorted them into inconsistent
	// positions. It also makes the key cover the whole retry chain, so a
	// transaction's apparent age — and with it the damage feedback — grows
	// without bound while it retries.
	juicerSortKeyPinnedRead juicerSortKeySource = "pinned-read-timestamp"
)

var juicerSortKeyEnv = envutil.EnvOrDefaultString(
	"COCKROACH_JUICER_SORT_KEY", string(juicerSortKeyMinTimestamp))

// juicerSortKeyMode is resolved once at startup rather than per batch: it is
// read on the RPC hot path, and an unrecognized value must not change the
// shipped default silently, so the resolved mode is logged at injection.
var juicerSortKeyMode = parseJuicerSortKeySource(juicerSortKeyEnv)

func parseJuicerSortKeySource(s string) juicerSortKeySource {
	switch juicerSortKeySource(strings.ToLower(strings.TrimSpace(s))) {
	case juicerSortKeyPinnedRead:
		return juicerSortKeyPinnedRead
	case juicerSortKeyBatchNow:
		return juicerSortKeyBatchNow
	default:
		return juicerSortKeyMinTimestamp
	}
}

// Sizes of the fork's two hot-key filters: K keys admitted by the abort tracker
// (Level 1), S of those tracked for reorder damage (Level 2).
//
// These MUST be passed explicitly. The fork's defaultServerOptions does not
// initialize juicerFilterK/juicerFilterS, so omitting the ServerOptions leaves
// both at Go's zero value, and AbortTracker.calculateAndSendDelta returns
// immediately when TopNFirstFilter <= 0. Level 1 then never promotes a key, never
// emits a TopKeysDelta, and no per-key queue is ever created — however many
// aborts are recorded. The defaults below mirror the fork's own
// juicer.DefaultAbortTrackerConfig() (1000/100), which the interceptor
// construction path does not consult.
var (
	juicerFilterK = envutil.EnvOrDefaultInt("COCKROACH_JUICER_FILTER_K", 1000)
	juicerFilterS = envutil.EnvOrDefaultInt("COCKROACH_JUICER_FILTER_S", 100)
)

// juicerRules names the strength of the dependency relation BuildRules hands to
// the enforcer. See BuildRules for what each one means and why the axis exists.
type juicerRules string

const (
	juicerRulesFull juicerRules = "full"
	juicerRulesSFU  juicerRules = "sfu"
	juicerRulesOff  juicerRules = "off"
)

var juicerRulesEnv = envutil.EnvOrDefaultString("COCKROACH_JUICER_RULES", string(juicerRulesFull))

// juicerRulesMode parses COCKROACH_JUICER_RULES, falling back to full on an
// unrecognized value. Falling back rather than failing is deliberate: this is a
// measurement knob, and a typo must not change the shipped default silently in
// one direction — the mode is logged once at injection so a fallback is visible
// in the node log.
func juicerRulesMode() juicerRules {
	switch juicerRules(strings.ToLower(strings.TrimSpace(juicerRulesEnv))) {
	case juicerRulesOff:
		return juicerRulesOff
	case juicerRulesSFU:
		return juicerRulesSFU
	default:
		return juicerRulesFull
	}
}

// Interception-hit counters: SplitMarker/IsSelfAbortedResponse are called
// only by the fork's interceptors, so non-zero deltas prove real traffic is
// flowing through Juicer (a single node short-circuits local ranges via the
// internal client adapter and shows zeros here).
var (
	juicerSplitCalls    atomic.Uint64
	juicerResponseCalls atomic.Uint64
	juicerReporterOnce  sync.Once
)

// kvFailureCounters buckets errored BatchResponses by kvpb error detail. Two
// independent instances exist and they must classify identically so their
// numbers can be compared, which is why the type is shared rather than
// duplicated:
//
//   - juicerFailures below, incremented from the fork's interceptor callback.
//     Reachable only when COCKROACH_JUICER=true, so it doubles as proof that
//     interception happened — and is therefore unusable as a baseline.
//   - kvBaselineFailures in kv_errlog.go, incremented from the KV server's batch
//     choke point regardless of mode. See that file for why both are needed.
//
// The question the buckets are shaped to answer: does CRDB return enough
// KV-visible conflict errors on this path for a Level-1 hot-key filter fed by
// aborts to have anything to work with, and does queue-based sorting change that
// rate?
//
// For juicerFailures, counts are call-granular (one Add per
// IsSelfAbortedResponse call carrying an error), not response-granular, matching
// juicerResponseCalls above. All fields are written from the RPC fast path and
// read only by a minute reporter.
//
// byType retains the raw kvpb.ErrorDetailType tally for everything that falls
// into "other", so a dominant unclassified error (NotLeaseHolder, RangeKeyMismatch,
// ...) can be named in the report rather than hidden behind a single number.
type kvFailureCounters struct {
	total             atomic.Uint64
	writeTooOld       atomic.Uint64
	retry             atomic.Uint64
	retrySerializable atomic.Uint64
	aborted           atomic.Uint64
	lockConflict      atomic.Uint64
	uncertainty       atomic.Uint64
	other             atomic.Uint64
	byType            [kvpb.NumErrors]atomic.Uint64
}

var juicerFailures kvFailureCounters

// kvFailureSnapshot is a plain-value copy of kvFailureCounters so the
// reporter can difference successive ticks without re-reading racing atomics.
type kvFailureSnapshot struct {
	total             uint64
	writeTooOld       uint64
	retry             uint64
	retrySerializable uint64
	aborted           uint64
	lockConflict      uint64
	uncertainty       uint64
	other             uint64
	byType            [kvpb.NumErrors]uint64
}

func (c *kvFailureCounters) snapshot() kvFailureSnapshot {
	s := kvFailureSnapshot{
		total:             c.total.Load(),
		writeTooOld:       c.writeTooOld.Load(),
		retry:             c.retry.Load(),
		retrySerializable: c.retrySerializable.Load(),
		aborted:           c.aborted.Load(),
		lockConflict:      c.lockConflict.Load(),
		uncertainty:       c.uncertainty.Load(),
		other:             c.other.Load(),
	}
	for i := range c.byType {
		s.byType[i] = c.byType[i].Load()
	}
	return s
}

// recordFailure buckets one errored BatchResponse. The kvpb.Error detail is the
// authoritative discriminator: br.Error.TransactionRestart() only says whether a
// restart is possible, and merges WriteTooOld with serializable retries.
func (c *kvFailureCounters) recordFailure(pErr *kvpb.Error) {
	c.total.Add(1)
	detail := pErr.GetDetail()
	switch d := detail.(type) {
	case *kvpb.WriteTooOldError:
		c.writeTooOld.Add(1)
	case *kvpb.TransactionRetryError:
		c.retry.Add(1)
		if d.Reason == kvpb.RETRY_SERIALIZABLE {
			c.retrySerializable.Add(1)
		}
	case *kvpb.TransactionAbortedError:
		c.aborted.Add(1)
	case *kvpb.LockConflictError, *kvpb.WriteIntentError:
		c.lockConflict.Add(1)
	case *kvpb.ReadWithinUncertaintyIntervalError:
		c.uncertainty.Add(1)
	default:
		c.other.Add(1)
		// A nil detail means the error carries no recognized kvpb detail (a
		// plain internal or communication error); there is no type to tally.
		if detail != nil {
			if t := int(detail.Type()); t >= 0 && t < len(c.byType) {
				c.byType[t].Add(1)
			}
		}
	}
}

// kvOtherBreakdown pre-formats the per-ErrorDetailType tally of the "other"
// bucket for the reporter. It is passed to the log call as a single %s argument
// because fmtsafe requires the format string itself to be a constant.
func kvOtherBreakdown(cur, prev kvFailureSnapshot) string {
	var parts []string
	for i := range cur.byType {
		if delta := cur.byType[i] - prev.byType[i]; delta > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", kvpb.ErrorDetailType(i), delta))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " other-detail[" + strings.Join(parts, " ") + "]"
}

// startJuicerHitReporter logs the resolved configuration once, then
// interception-hit deltas every minute.
func startJuicerHitReporter(ctx context.Context) {
	juicerReporterOnce.Do(func() {
		// The sorting key source decides what "sorted by transaction" means on
		// this node, and an unrecognized COCKROACH_JUICER_SORT_KEY resolves to
		// the default rather than failing, so the resolved value has to appear
		// in the node log for a run to be interpretable after the fact.
		log.Dev.Infof(ctx, "juicer: sorting key source = %s (COCKROACH_JUICER_SORT_KEY=%q)",
			string(juicerSortKeyMode), juicerSortKeyEnv)
		go func() {
			var lastSplit, lastResp uint64
			var lastFail kvFailureSnapshot
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				s, r := juicerSplitCalls.Load(), juicerResponseCalls.Load()
				log.Dev.Infof(ctx, "juicer: interception hits: +%d batches split, +%d responses observed (totals %d/%d)",
					s-lastSplit, r-lastResp, s, r)
				lastSplit, lastResp = s, r

				f := juicerFailures.snapshot()
				log.Dev.Infof(ctx,
					"juicer: failed responses: +%d total (wto=%d retry=%d[serializable=%d] aborted=%d lock=%d uncert=%d other=%d)%s",
					f.total-lastFail.total,
					f.writeTooOld-lastFail.writeTooOld,
					f.retry-lastFail.retry,
					f.retrySerializable-lastFail.retrySerializable,
					f.aborted-lastFail.aborted,
					f.lockConflict-lastFail.lockConflict,
					f.uncertainty-lastFail.uncertainty,
					f.other-lastFail.other,
					kvOtherBreakdown(f, lastFail))
				lastFail = f
			}
		}()
	})
}

// juicerServerOptions returns the fork ServerOptions that enable Juicer
// interception of Batch/BatchStream, or nil when disabled.
func juicerServerOptions() []grpc.ServerOption {
	if !juicerEnabled {
		return nil
	}
	opts := []grpc.ServerOption{
		grpc.JuicerSPIImpl(newCRDBJuicerSPI()),
		grpc.JuicerUnaryMethods(juicerUnaryBatchPaths),
		grpc.JuicerStreamMethods(juicerStreamBatchPaths),
		grpc.JuicerFilterK(juicerFilterK),
		grpc.JuicerFilterS(juicerFilterS),
	}
	if juicerSkipQueueing {
		opts = append(opts, grpc.JuicerSkipQueueing(true))
	}
	if juicerScaleFactor > 0 {
		opts = append(opts, grpc.JuicerScaleFactor(juicerScaleFactor))
	}
	if juicerWaitStrategy != "" {
		opts = append(opts,
			grpc.JuicerWaitStrategy(juicerWaitStrategy),
			grpc.JuicerWaitMinQLen(juicerWaitMinQLen),
			grpc.JuicerFixedWait(millisToDuration(juicerFixedWaitMillis)))
	}
	if juicerRandomDelay {
		opts = append(opts,
			grpc.JuicerRandomDelay(true),
			grpc.JuicerRandomDelaySeed(juicerRandomDelaySeed),
			grpc.JuicerRandomDelayFixed(millisToDuration(juicerRandomDelayFixedMillis)))
	}
	if juicerHoldUntilDone {
		opts = append(opts,
			grpc.JuicerHoldUntilDone(true),
			grpc.JuicerMaxBusy(millisToDuration(juicerMaxBusyMillis)))
	}
	return opts
}

// crdbJuicerSPI adapts kvpb batch traffic to the Juicer SPI. Keys are hashed
// to int64 (fnv-64a over the roachpb.Key bytes); a hash collision merges two
// keys onto one sorting queue, which over-blocks conservatively but never
// under-blocks. Classification is batch-granular: the strongest operation in
// the batch names the whole batch, mirroring how the interceptor emits one
// event per BatchRequest.
type crdbJuicerSPI struct{}

var _ juicer.JuicerSPI[int64] = crdbJuicerSPI{}

func newCRDBJuicerSPI() crdbJuicerSPI { return crdbJuicerSPI{} }

func juicerKeyHash(k []byte) int64 {
	h := fnv.New64a()
	_, _ = h.Write(k)
	return int64(h.Sum64())
}

// juicerTxnID derives a non-zero int64 txn id from the txn UUID. Zero means
// "unbound" to the enforcer (a head that can never be admitted), so a real
// transaction must never map to it.
func juicerTxnID(ba *kvpb.BatchRequest) int64 {
	if ba == nil || ba.Txn == nil {
		return 0
	}
	b := ba.Txn.ID.GetBytes()
	id := int64(binary.LittleEndian.Uint64(b[8:16]))
	if id == 0 {
		id = int64(binary.LittleEndian.Uint64(b[0:8]))
	}
	if id == 0 {
		id = 1
	}
	return id
}

func asBatchRequest(req interface{}) *kvpb.BatchRequest {
	ba, _ := req.(*kvpb.BatchRequest)
	return ba
}

// classifyBatch returns the batch-level operation type.
//
//   - Any write (incl. a 1PC [writes..., EndTxn] batch) -> OpSet. A 1PC batch
//     is deliberately NOT OpCommit: its commit-ness reaches the dependency
//     rules through the RespReturned release arm (crdbRules), because
//     participants never observe a separate commit message.
//   - Write-free EndTxn -> OpCommit / OpAbort by its Commit flag.
//   - ResolveIntent{,Range} -> OpCommit / OpAbort by intent status (async
//     resolution is the only commit signal non-anchor participants see).
//   - Locking Get (implicit SFU) -> OpGetForPut; plain Get -> OpGet.
//   - Refresh{,Range} -> OpGet. A refresh re-reads a span to check whether
//     anything wrote to it since the transaction's original read timestamp,
//     so it is a read for ordering purposes and must be sortable: it is the
//     single message whose outcome decides whether the transaction commits or
//     restarts, and leaving it unsorted meant the arrival order Juicer
//     arranged for the reads was not the order in which they were validated.
func classifyBatch(ba *kvpb.BatchRequest) juicer.OperationType {
	if ba == nil {
		return juicer.OpUnknown
	}
	var hasWrite, hasLockingGet, hasGet, hasRefresh bool
	var endTxn *kvpb.EndTxnRequest
	var resolve juicer.OperationType
	for i := range ba.Requests {
		switch r := ba.Requests[i].GetInner().(type) {
		case *kvpb.GetRequest:
			if r.KeyLockingStrength != lock.None {
				hasLockingGet = true
			} else {
				hasGet = true
			}
		case *kvpb.PutRequest, *kvpb.ConditionalPutRequest, *kvpb.IncrementRequest, *kvpb.DeleteRequest:
			hasWrite = true
		case *kvpb.RefreshRequest, *kvpb.RefreshRangeRequest:
			hasRefresh = true
		case *kvpb.EndTxnRequest:
			endTxn = r
		case *kvpb.ResolveIntentRequest:
			resolve = resolveStatusOp(r.Status, resolve)
		case *kvpb.ResolveIntentRangeRequest:
			resolve = resolveStatusOp(r.Status, resolve)
		}
	}
	switch {
	case hasWrite:
		return juicer.OpSet
	case endTxn != nil && !endTxn.Commit:
		return juicer.OpAbort
	case endTxn != nil:
		return juicer.OpCommit
	case resolve != juicer.OpUnknown:
		return resolve
	case hasLockingGet:
		return juicer.OpGetForPut
	case hasGet || hasRefresh:
		return juicer.OpGet
	default:
		return juicer.OpUnknown
	}
}

func resolveStatusOp(status roachpb.TransactionStatus, prev juicer.OperationType) juicer.OperationType {
	switch status {
	case roachpb.COMMITTED:
		return juicer.OpCommit
	case roachpb.ABORTED:
		if prev == juicer.OpCommit {
			return prev
		}
		return juicer.OpAbort
	default:
		return prev
	}
}

// sortableJuicerOp reports whether the batch-level op type enters the sorting
// queues (the interceptor sorts OpGet/OpGetForPut/OpSet).
func sortableJuicerOp(op juicer.OperationType) bool {
	return op == juicer.OpGet || op == juicer.OpGetForPut || op == juicer.OpSet
}

// juicerSortKey returns the sorting key of ba's transaction, from whichever
// source juicerSortKeyMode selects. See juicerSortKeySource for what each one
// guarantees.
//
// Nothing in CockroachDB asserts that MinTimestamp is non-zero — AssertInitialized
// checks only ID and WriteTimestamp — so an empty one falls back to the pinned
// table rather than collapsing every such transaction onto the sorting key 0.
// The fallback is not expected to fire: MakeTransaction is the only path to
// ba.Txn and it always sets MinTimestamp from an HLC reading.
func juicerSortKey(ba *kvpb.BatchRequest) juicerTxnSortKey {
	switch juicerSortKeyMode {
	case juicerSortKeyBatchNow:
		// The header documents Now as optional, so an empty one falls down
		// the same ladder as the other modes: MinTimestamp, then the pinned
		// table. In practice everything arriving over gRPC came through a
		// DistSender and carries it.
		if now := ba.Now; !now.IsEmpty() {
			return juicerTxnSortKey{wallTime: now.WallTime, logical: now.Logical}
		}
		fallthrough
	case juicerSortKeyMinTimestamp:
		if ts := ba.Txn.MinTimestamp; !ts.IsEmpty() {
			return juicerTxnSortKey{wallTime: ts.WallTime, logical: ts.Logical}
		}
	}
	return juicerTxnKeys.keyFor(ba.Txn.ID, ba.Txn.ReadTimestamp, timeutil.Now())
}

// SplitMarker emits one marker per point request of a transactional, sortable
// batch. Non-transactional batches return nil: they are never sorted, which
// also keeps txn id 0 (the enforcer's "unbound" sentinel) out of the queues.
//
// A RefreshRange contributes a marker for its start key only. The sorting
// queues are per point key, so a span cannot be represented exactly; the start
// key is the span's position in the same key space and puts the refresh on a
// queue the transaction's own reads are likely to be on. This under-covers a
// wide refresh rather than over-blocking one.
func (crdbJuicerSPI) SplitMarker(req interface{}) []juicer.Marker[int64] {
	juicerSplitCalls.Add(1)
	ba := asBatchRequest(req)
	if ba == nil || ba.Txn == nil {
		return nil
	}
	if !sortableJuicerOp(classifyBatch(ba)) {
		return nil
	}
	txnID := juicerTxnID(ba)
	sortKey := juicerSortKey(ba)
	markers := make([]juicer.Marker[int64], 0, len(ba.Requests))
	for i := range ba.Requests {
		var key []byte
		var opName string
		switch r := ba.Requests[i].GetInner().(type) {
		case *kvpb.GetRequest:
			key = r.Key
			if r.KeyLockingStrength != lock.None {
				opName = "GetForUpdate"
			} else {
				opName = "Get"
			}
		case *kvpb.PutRequest:
			key, opName = r.Key, "Put"
		case *kvpb.ConditionalPutRequest:
			key, opName = r.Key, "CPut"
		case *kvpb.IncrementRequest:
			key, opName = r.Key, "Increment"
		case *kvpb.DeleteRequest:
			key, opName = r.Key, "Delete"
		case *kvpb.RefreshRequest:
			key, opName = r.Key, "Refresh"
		case *kvpb.RefreshRangeRequest:
			key, opName = r.Key, "RefreshRange"
		default:
			continue
		}
		markers = append(markers, juicer.Marker[int64]{
			Timestamp: sortKey.wallTime,
			Logical:   int64(sortKey.logical),
			TxnId:     txnID,
			Key:       juicerKeyHash(key),
			OpIndex:   int64(i),
			OpType:    opName,
		})
	}
	return markers
}

func (crdbJuicerSPI) GetTxnId(req interface{}) int64 {
	return juicerTxnID(asBatchRequest(req))
}

func (crdbJuicerSPI) GetTimestamp(req interface{}) int64 {
	if ba := asBatchRequest(req); ba != nil && ba.Txn != nil {
		return juicerSortKey(ba).wallTime
	}
	return 0
}

func (crdbJuicerSPI) GetOperationCount(req interface{}) int32 {
	if ba := asBatchRequest(req); ba != nil {
		return int32(len(ba.Requests))
	}
	return 0
}

// GetOperationType classifies a batch and, as a side effect, releases the
// transaction's pinned sorting key when the batch is its EndTxn. This is the
// eviction hook: it is the one SPI method the interceptor calls for every
// message on both the sortable and the non-sortable path, and the SPI has no
// lifecycle callback of its own. Transactions whose EndTxn this node never
// sees — 1PC batches (classified OpSet, so no eviction here), or a coordinator
// that moved — are cleaned up by juicerTxnKeyTable's TTL instead.
//
// In juicerSortKeyMinTimestamp mode the table holds nothing worth evicting (it
// is only touched by the empty-MinTimestamp fallback), so the eviction is
// skipped rather than taking a shard lock per EndTxn on the hot path.
func (s crdbJuicerSPI) GetOperationType(req interface{}) juicer.OperationType {
	ba := asBatchRequest(req)
	op := classifyBatch(ba)
	if juicerSortKeyMode == juicerSortKeyPinnedRead &&
		(op == juicer.OpCommit || op == juicer.OpAbort) && ba != nil && ba.Txn != nil {
		juicerTxnKeys.forget(ba.Txn.ID)
	}
	return op
}

// GetKeysFromRequest hashes every keyed request in the batch, including
// EndTxn (anchor key) and ResolveIntent{,Range} (intent key) — the observe
// path uses these to deliver release events to the right queues.
func (crdbJuicerSPI) GetKeysFromRequest(req interface{}) []int64 {
	ba := asBatchRequest(req)
	if ba == nil {
		return nil
	}
	keys := make([]int64, 0, len(ba.Requests))
	for i := range ba.Requests {
		if h := ba.Requests[i].GetInner().Header(); len(h.Key) > 0 {
			keys = append(keys, juicerKeyHash(h.Key))
		}
	}
	return keys
}

func (s crdbJuicerSPI) IsGetRequest(req interface{}) bool {
	op := s.GetOperationType(req)
	return op == juicer.OpGet || op == juicer.OpGetForPut
}

func (s crdbJuicerSPI) IsSetRequest(req interface{}) bool {
	return s.GetOperationType(req) == juicer.OpSet
}

func (s crdbJuicerSPI) IsCommitRequest(req interface{}) bool {
	return s.GetOperationType(req) == juicer.OpCommit
}

func (s crdbJuicerSPI) IsAbortRequest(req interface{}) bool {
	return s.GetOperationType(req) == juicer.OpAbort
}

// IsPrepareRequest is always false: CRDB has no prepare message at this
// layer (parallel commits stage writes inside the commit batch instead).
func (crdbJuicerSPI) IsPrepareRequest(req interface{}) bool { return false }

func (crdbJuicerSPI) IsSelfAbortedResponse(resp interface{}) bool {
	juicerResponseCalls.Add(1)
	br, ok := resp.(*kvpb.BatchResponse)
	if !ok || br == nil || br.Error == nil {
		return false
	}
	juicerFailures.recordFailure(br.Error)
	return true
}

// crdbJuicerLogBridge routes the fork's Juicer logs (queue events, pairing
// violations, block escapes) into CRDB's Dev channel so they appear in the
// normal node logs. Debug-level fork logs are per-operation and only flow
// when COCKROACH_JUICER_DEBUG=true.
type crdbJuicerLogBridge struct{}

func (crdbJuicerLogBridge) Debug(format string, v ...interface{}) {
	log.Dev.Infof(context.Background(), "juicer[debug]: %s", fmt.Sprintf(format, v...))
}
func (crdbJuicerLogBridge) Info(format string, v ...interface{}) {
	log.Dev.Infof(context.Background(), "juicer: %s", fmt.Sprintf(format, v...))
}
func (crdbJuicerLogBridge) Warning(format string, v ...interface{}) {
	log.Dev.Warningf(context.Background(), "juicer: %s", fmt.Sprintf(format, v...))
}
func (crdbJuicerLogBridge) Error(format string, v ...interface{}) {
	log.Dev.Errorf(context.Background(), "juicer: %s", fmt.Sprintf(format, v...))
}

var crdbJuicerLogger = juicer.NewLogger(
	"crdb", crdbJuicerLogBridge{}, "" /* logDir */, 0 /* serverID */, false /* hotkeyLogging */, juicerDebugLogs,
)

func (crdbJuicerSPI) ServerLogger() *juicer.Logger { return crdbJuicerLogger }

// The 2PC helper hooks below serve juicer-cc's manual dispatch modes only;
// the interceptor pipeline used for CRDB never calls them.
func (crdbJuicerSPI) GeneratePrepareRequest(req interface{}) interface{}         { return nil }
func (crdbJuicerSPI) GenerateCommitRequest(req interface{}) interface{}          { return nil }
func (crdbJuicerSPI) ExecuteCommitRequest(req interface{}) (interface{}, error)  { return nil, nil }
func (crdbJuicerSPI) ExecutePrepareRequest(req interface{}) (interface{}, error) { return nil, nil }
func (crdbJuicerSPI) ExecuteAbortRequest(req interface{}) (interface{}, error)   { return nil, nil }
func (crdbJuicerSPI) AbortCurrentRequest(req interface{}) (interface{}, error)   { return nil, nil }

// BuildRules declares CRDB's dependency semantics to the enforcer, in one of
// three strengths selected by COCKROACH_JUICER_RULES. The switch exists because
// Juicer does two separable things — it sorts requests into per-key arrival
// order, and it enforces a dependency relation between them — and a single
// on/off flag cannot tell you which one produced an effect. Measuring "on"
// against "off" with only juicerRulesFull available conflates the value of the
// sorting with the cost of the enforcer.
//
//	full (default)  Locking operations (SFU reads and writes) of different
//	                transactions mutually exclude. This is the relation as
//	                originally written, and the one that cost 85-98% of
//	                throughput while cutting aborts by 90-97%.
//	sfu             Only two cross-txn SFU *reads* exclude. Write-write
//	                exclusion is left entirely to CRDB's lock table, which
//	                already solves it below this layer; Juicer only adds the
//	                ordering guarantee CRDB does not provide, namely that a
//	                reader intending to write is not overtaken. Same release
//	                relation and MaxHold as full.
//	off             The zero-value DependencyRules. Enabled() is false, so
//	                QueueManager builds no enforcer and the per-key queues do
//	                pure timestamp sorting with no blocking. This isolates the
//	                sorting from the enforcement.
//
// In every mode a holder releases when its EndTxn or ResolveIntent is observed
// on the key, when its own write batch response returns (1PC hot path: response
// return == committed), or on any failed response. Note an OpGetForPut response
// deliberately does NOT release: the SFU lock must be held through commit — that
// is what kills the aborts.
func (crdbJuicerSPI) BuildRules() juicer.DependencyRules {
	mode := juicerRulesMode()
	if mode == juicerRulesOff {
		// Zero value: DependencyRules.Enabled() is false and the fork's
		// enforcerFactory returns nil, leaving the queues in sorting-only mode.
		return juicer.DependencyRules{}
	}

	locking := func(o juicer.OperationType) bool {
		return o == juicer.OpGetForPut || o == juicer.OpSet
	}
	blocks := func(h, hd juicer.OpRef) bool {
		if h.TxnID == hd.TxnID {
			return false
		}
		if mode == juicerRulesSFU {
			return h.OpType == juicer.OpGetForPut && hd.OpType == juicer.OpGetForPut
		}
		return locking(h.OpType) && locking(hd.OpType)
	}
	return juicer.DependencyRules{
		Blocks: blocks,
		Releases: func(h juicer.OpRef, ev juicer.Event) bool {
			if ev.TxnID != h.TxnID {
				return false
			}
			if juicer.ReleasedOnCommitAbort(h, ev) {
				return true
			}
			return ev.Kind == juicer.RespReturned && (ev.Failed || ev.OpType == juicer.OpSet)
		},
		MaxHold: time.Duration(juicerMaxHoldMillis) * time.Millisecond,
	}
}
