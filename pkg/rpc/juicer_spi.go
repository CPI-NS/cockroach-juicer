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
	"google.golang.org/grpc"
	"google.golang.org/grpc/juicer"
)

// Juicer wires the fork's request-reordering layer (google.golang.org/grpc is
// replaced by github.com/CPI-NS/juicer-grpc-go, see go.mod) into the KV RPC
// server. It is entirely opt-in: with COCKROACH_JUICER unset (the default) no
// Juicer server options are appended and the gRPC server is byte-identical to
// upstream.
var (
	juicerEnabled      = envutil.EnvOrDefaultBool("COCKROACH_JUICER", false)
	juicerSkipQueueing = envutil.EnvOrDefaultBool("COCKROACH_JUICER_SKIP_QUEUEING", false)
	// juicerMaxHoldMillis is the hold engine's single TTL: the gate
	// relation's quantization clock (campaign #14: E/W ≈ 80% on the winning
	// workpoint — most holds end by this clock, so its value IS the
	// mechanism's wait time and the axis a MAX_HOLD sweep moves), and the
	// residency cap of the sfu/full relations. Non-positive values are
	// clamped upward by the relation constructors: the gate has no no-TTL
	// mode.
	juicerMaxHoldMillis    = envutil.EnvOrDefaultInt("COCKROACH_JUICER_MAX_HOLD_MS", 25)
	juicerDebugLogs        = envutil.EnvOrDefaultBool("COCKROACH_JUICER_DEBUG", false)
	juicerUnaryBatchPaths  = []string{"/cockroach.roachpb.Internal/Batch", "/cockroach.roachpb.KVBatch/Batch"}
	juicerStreamBatchPaths = []string{"/cockroach.roachpb.Internal/BatchStream", "/cockroach.roachpb.KVBatch/BatchStream"}
)

// The standard configuration's remaining wait knob. The completion gate is no
// longer a switch of its own: it is the DEFAULT dependency relation
// (COCKROACH_JUICER_RULES=gate, see BuildRules), and its TTL is
// COCKROACH_JUICER_MAX_HOLD_MS. The former COCKROACH_JUICER_HOLD_UNTIL_DONE /
// COCKROACH_JUICER_MAX_BUSY_MS knobs were removed in the 2026-08-26 merge of
// the gate into the rules engine; reproduce earlier campaigns from their
// pinned tags. Historical measurement arms — the damage-scaled window
// (SCALE_FACTOR), the qlen-gated strategy, the random-delay control, and the
// selectable sorting key — went the same way on 2026-08-13.
var (
	// juicerFixedWaitMillis is the uniform-window knob: a constant window
	// imposed on every sorted message, measured from its arrival, in
	// floating-point milliseconds so a sweep can go below 1 ms. 0 (the
	// default) means no window: pure sorting plus whatever relation RULES
	// selects. Campaign #14 relocated the interesting wait-time axis to the
	// gate's MAX_HOLD_MS; this knob remains for uniform-window controls.
	juicerFixedWaitMillis = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_FIXED_WAIT_MS", 0)
)

// millisToDuration converts a floating-point millisecond knob to a Duration.
// Non-positive values yield 0, which every consumer reads as "not set".
func millisToDuration(ms float64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// juicerTxnSortKey is the value a message sorts by: the pair (wallTime,
// logical), compared lexicographically, taken from an HLC reading so it is a
// total order per clock. It is a position in the sorting queues (an intended
// admission order), not a commit order: the commit order is WriteTimestamp as
// of commit, which no message carries at the time it has to be sorted.
//
// The source is the batch header's Now — the gateway DistSender's clock
// reading, stamped once per client-level send (initAndVerifyBatch,
// pkg/kv/kvclient/kvcoord/dist_sender.go) and shared verbatim by every
// per-range sub-batch of that send. So one wave sorts identically on every
// participating node, while a retry sorts at its retry time: the abort-retry
// path mints a whole new transaction and every client-level re-send re-stamps
// Now, which keeps the key-to-arrival gap at network scale instead of
// transaction age (the defect that sank the per-transaction MinTimestamp key
// at contended workpoints — campaign 10: damage p50 4.6 s against a 0.169 ms
// RTT).
//
// Two knowingly accepted imperfections, from the 2026-08-13 audit:
// DistSender-internal transport retries (NotLeaseHolder and friends) re-send
// without re-stamping, so those arrivals look as old as their client-level
// send — but never as old as the transaction; and a proxied sub-batch is
// re-stamped with the proxying node's clock, so it can disagree with its
// siblings by up to the cluster's clock offset.
//
// Earlier selectable sources (the per-transaction MinTimestamp and the
// campaign-7 per-node pinned read timestamp, with its key table) were removed
// in the 2026-08-13 simplification; reproduce those campaigns from their
// pinned tags.
type juicerTxnSortKey struct {
	wallTime int64
	logical  int32
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
	juicerRulesGate      juicerRules = "gate"
	juicerRulesWrites    juicerRules = "writes"
	juicerRulesWriteGate juicerRules = "writegate"
	juicerRulesFull      juicerRules = "full"
	juicerRulesSFU       juicerRules = "sfu"
	juicerRulesOff       juicerRules = "off"
)

// The default is GATE as of the 2026-08-26 merge: the completion gate — the
// one relation with a demonstrated win (campaigns #11/#12/#14) — is the
// standard configuration, expressed as a dependency relation instead of the
// former HOLD_UNTIL_DONE switch. off/sfu/full remain selectable; RMW and
// other multi-wave workloads MUST select off (campaign #13: the gate is pure
// harm there, −73% throughput at the measured workpoint).
var juicerRulesEnv = envutil.EnvOrDefaultString("COCKROACH_JUICER_RULES", string(juicerRulesGate))

// juicerRulesMode parses COCKROACH_JUICER_RULES. An unrecognized value
// resolves to the DEFAULT (gate): with the standard relation on by default, a
// typo can no longer be allowed to silently disable it either — whichever way
// the fallback points, some typo is mis-served, and the injection banner (and
// the smoke gate asserting it) is the actual guard. "off" must therefore be
// spelled exactly; verify the banner, not the environment.
func juicerRulesMode() juicerRules {
	switch juicerRules(strings.ToLower(strings.TrimSpace(juicerRulesEnv))) {
	case juicerRulesWrites:
		return juicerRulesWrites
	case juicerRulesWriteGate:
		return juicerRulesWriteGate
	case juicerRulesFull:
		return juicerRulesFull
	case juicerRulesSFU:
		return juicerRulesSFU
	case juicerRulesOff:
		return juicerRulesOff
	default:
		return juicerRulesGate
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
		// The banner makes the run interpretable after the fact: it names the
		// (now fixed) sorting key source and the resolved core knobs.
		log.Dev.Infof(ctx,
			"juicer: sorting key source = batch-now (per-send gateway clock; empty Now falls back to MinTimestamp); fixed wait = %v ms; rules = %s (maxHold %d ms)",
			juicerFixedWaitMillis, juicerRulesMode(), juicerMaxHoldMillis)
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
	// The fixed strategy is always selected: with a zero window it is the
	// pure-sorting behaviour, so one code path serves both "no wait" and
	// every FIXED_WAIT_MS sweep point.
	opts = append(opts,
		grpc.JuicerWaitStrategy(juicer.WaitStrategyFixed),
		grpc.JuicerFixedWait(millisToDuration(juicerFixedWaitMillis)))
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

// juicerSortKey returns the sorting key of ba: the batch header's per-send
// clock reading, with the transaction's MinTimestamp as the fallback for an
// empty Now. See juicerTxnSortKey for what the key guarantees.
func juicerSortKey(ba *kvpb.BatchRequest) juicerTxnSortKey {
	// The header documents Now as optional, so an empty one falls back to
	// the transaction's MinTimestamp (assigned at creation, never empty in
	// practice) rather than collapsing every such batch onto key zero. In
	// practice everything arriving over gRPC came through a DistSender and
	// carries Now.
	if now := ba.Now; !now.IsEmpty() {
		return juicerTxnSortKey{wallTime: now.WallTime, logical: now.Logical}
	}
	ts := ba.Txn.MinTimestamp
	return juicerTxnSortKey{wallTime: ts.WallTime, logical: ts.Logical}
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

// GetOperationType classifies a batch. The sorting key is stateless (it is
// read straight off the batch header), so unlike the old pinned-key mode
// there is nothing to evict here.
func (s crdbJuicerSPI) GetOperationType(req interface{}) juicer.OperationType {
	return classifyBatch(asBatchRequest(req))
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

// BuildRules declares CRDB's dependency relation to the enforcer, selected by
// COCKROACH_JUICER_RULES. The switch exists because Juicer does two separable
// things — it sorts requests into per-key arrival order, and it holds them
// against a dependency relation — and a single on/off flag cannot tell you
// which one produced an effect.
//
//	gate (default)  The completion gate, juicer.GateRules: after a message is
//	                released on a key, the next message on that key — whatever
//	                its operation type or transaction — waits for the released
//	                message's response (success or failure) or the MaxHold
//	                TTL. The standard configuration (campaigns #11/#12/#14);
//	                effective behaviour is ≤MaxHold-quantized serialization of
//	                each hot key, which pre-empts deadlock formation. ONLY safe
//	                on single-wave one-shot traffic: RMW / multi-wave workloads
//	                must select off (campaign #13 measured −73% throughput).
//	writes          The unified-rule CANDIDATE (validation pending): only
//	                cross-txn write-write pairs exclude; plain reads and
//	                locking reads pass untouched and hold nothing. Designed to
//	                keep the gate's single-wave wins (on pure-write traffic it
//	                is literally the gate), delete the read tax, and be
//	                neutral on RMW — one always-on relation with no
//	                workload-class caveat. See the case below for the
//	                fact-by-fact derivation.
//	writegate       ABLATION PROBE (campaign #16), not a production candidate:
//	                writes plus one change — EVERY head (reads included) waits
//	                behind a live cross-txn write hold, but reads still hold
//	                nothing. Campaign #15 measured that the gate's mixed-load
//	                advantage over writes comes from read participation; this
//	                relation keeps only the read-waits-behind-writes half and
//	                drops the read-holds half, so the outcome attributes that
//	                advantage to ordering (≈gate) or to the admission throttle
//	                the read holds themselves imposed (≈writes).
//	full            Locking operations (SFU reads and writes) of different
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
//	                pure timestamp sorting with no holds at all.
//
// Release relation for full/sfu, split by held-entry type (the 2026-08-13
// audit's self-release fix): a held WRITE releases when a same-txn write
// response returns — event delivery is key-scoped, so that means "this
// transaction's write covering THIS key landed" (1PC hot path: response
// return == committed) — or on commit/abort arrival, or on any failed
// response. A held LOCKING READ releases only on commit/abort arrival, a
// failed response, or the TTL — never on the transaction's own write
// response: the SFU claim is "a reader intending to write is not overtaken
// until the claim resolves", and the removed write-response arm released it
// mid-transaction (a multi-wave txn's write wave responds long before it
// commits), contradicting the hold-through-commit intent documented beside
// it. An OpGetForPut response also deliberately does NOT release its own
// hold: returning from the locking read is the beginning of the claim, not
// its resolution.
func (crdbJuicerSPI) BuildRules() juicer.DependencyRules {
	mode := juicerRulesMode()
	maxHold := time.Duration(juicerMaxHoldMillis) * time.Millisecond
	switch mode {
	case juicerRulesOff:
		// Zero value: DependencyRules.Enabled() is false and the fork's
		// enforcerFactory returns nil, leaving the queues in sorting-only mode.
		return juicer.DependencyRules{}
	case juicerRulesGate:
		return juicer.GateRules(maxHold)
	case juicerRulesWrites:
		// The unified-rule candidate: space concurrent WRITERS per hot key and
		// touch nothing else. Every clause is pinned to a measured fact —
		// cross-txn write-write spacing is where the single-wave wins came
		// from (campaigns #11/#12/#14); plain reads are exempt because holding
		// them was the one in-class cost (m50 read p50 +40%) and non-locking
		// reads cannot deadlock; locking reads are exempt because holding that
		// wave is where the RMW harm came from (campaign #13: the lock table
		// already owns that serialization; stacking a quantized hold on it was
		// −73%). If the do-no-harm prediction on RMW validates, this relation
		// replaces the gate as the single always-on rule and the
		// workload-class warning dies with the mode switch.
		return juicer.DependencyRules{
			Blocks: func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID &&
					h.OpType == juicer.OpSet && hd.OpType == juicer.OpSet
			},
			// Held entries are only ever writes here, so the release relation
			// is the write arm alone: own write response (key-scoped delivery;
			// 1PC hot path: response return == committed), commit/abort
			// arrival, or any failed response.
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				return ev.Kind == juicer.RespReturned &&
					(ev.Failed || ev.OpType == juicer.OpSet)
			},
			// Only writes can hold, so only writes enter the in-flight set:
			// a held read could block nothing, would be released by no
			// response (Releases wants OpSet), and its TTL expiry would
			// pollute the E/W numerator.
			Holds:        func(h juicer.OpRef) bool { return h.OpType == juicer.OpSet },
			MaxHold:      maxHold,
			AdmitUnbound: true,
		}
	case juicerRulesWriteGate:
		// The campaign #16 ablation probe: identical to writes except that
		// Blocks drops its head-type condition, so reads and locking reads
		// wait behind a live cross-txn write hold instead of passing. Holds
		// and Releases are the writes relation verbatim — the held population
		// is still writes only, which is exactly the point: read-waiting
		// without read-holding. Same-txn heads stay exempt so an RMW
		// transaction is never parked behind its own write.
		return juicer.DependencyRules{
			Blocks: func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID && h.OpType == juicer.OpSet
			},
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				return ev.Kind == juicer.RespReturned &&
					(ev.Failed || ev.OpType == juicer.OpSet)
			},
			Holds:        func(h juicer.OpRef) bool { return h.OpType == juicer.OpSet },
			MaxHold:      maxHold,
			AdmitUnbound: true,
		}
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
			if ev.Kind != juicer.RespReturned {
				return false
			}
			if ev.Failed {
				return true
			}
			// A write response ends a held write (its own batch, by key-scoped
			// delivery) but never a held locking read — see the relation note
			// above.
			return ev.OpType == juicer.OpSet && h.OpType == juicer.OpSet
		},
		// Only locking operations can hold under full/sfu, so only they enter
		// the in-flight set — a tracked plain read could block nothing and
		// its TTL expiry would pollute the E/W numerator.
		Holds:   func(h juicer.OpRef) bool { return locking(h.OpType) },
		MaxHold: maxHold,
	}
}
