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
	juicerEnabled          = envutil.EnvOrDefaultBool("COCKROACH_JUICER", false)
	juicerSkipQueueing     = envutil.EnvOrDefaultBool("COCKROACH_JUICER_SKIP_QUEUEING", false)
	juicerScaleFactor      = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_SCALE_FACTOR", 0)
	juicerMaxHoldMillis    = envutil.EnvOrDefaultInt("COCKROACH_JUICER_MAX_HOLD_MS", 25)
	juicerDebugLogs        = envutil.EnvOrDefaultBool("COCKROACH_JUICER_DEBUG", false)
	juicerUnaryBatchPaths  = []string{"/cockroach.roachpb.Internal/Batch", "/cockroach.roachpb.KVBatch/Batch"}
	juicerStreamBatchPaths = []string{"/cockroach.roachpb.Internal/BatchStream", "/cockroach.roachpb.KVBatch/BatchStream"}
)

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

// Interception-hit counters: SplitMarker/IsSelfAbortedResponse are called
// only by the fork's interceptors, so non-zero deltas prove real traffic is
// flowing through Juicer (a single node short-circuits local ranges via the
// internal client adapter and shows zeros here).
var (
	juicerSplitCalls    atomic.Uint64
	juicerResponseCalls atomic.Uint64
	juicerReporterOnce  sync.Once
)

// juicerFailureCounters classifies the failed BatchResponses the interceptor
// sees, i.e. exactly the population that feeds the fork's abort tracker. It
// exists to answer one question: does CRDB return enough KV-visible conflict
// errors on this path for a Level-1 hot-key filter fed by aborts to have
// anything to work with?
//
// Counts are call-granular (one Add per IsSelfAbortedResponse call carrying an
// error), not response-granular, matching juicerResponseCalls above. All fields
// are written from the RPC fast path and read only by the minute reporter.
//
// byType retains the raw kvpb.ErrorDetailType tally for everything that falls
// into "other", so a dominant unclassified error (NotLeaseHolder, RangeKeyMismatch,
// ...) can be named in the report rather than hidden behind a single number.
type juicerFailureCounters struct {
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

var juicerFailures juicerFailureCounters

// juicerFailureSnapshot is a plain-value copy of juicerFailureCounters so the
// reporter can difference successive ticks without re-reading racing atomics.
type juicerFailureSnapshot struct {
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

func (c *juicerFailureCounters) snapshot() juicerFailureSnapshot {
	s := juicerFailureSnapshot{
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
func (c *juicerFailureCounters) recordFailure(pErr *kvpb.Error) {
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

// juicerOtherBreakdown pre-formats the per-ErrorDetailType tally of the "other"
// bucket for the reporter. It is passed to the log call as a single %s argument
// because fmtsafe requires the format string itself to be a constant.
func juicerOtherBreakdown(cur, prev juicerFailureSnapshot) string {
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

// startJuicerHitReporter logs interception-hit deltas once a minute.
func startJuicerHitReporter(ctx context.Context) {
	juicerReporterOnce.Do(func() {
		go func() {
			var lastSplit, lastResp uint64
			var lastFail juicerFailureSnapshot
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
					juicerOtherBreakdown(f, lastFail))
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
func classifyBatch(ba *kvpb.BatchRequest) juicer.OperationType {
	if ba == nil {
		return juicer.OpUnknown
	}
	var hasWrite, hasLockingGet, hasGet bool
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
	case hasGet:
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

// SplitMarker emits one marker per point request of a transactional, sortable
// batch. Non-transactional batches return nil: they are never sorted, which
// also keeps txn id 0 (the enforcer's "unbound" sentinel) out of the queues.
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
	ts := ba.Txn.ReadTimestamp.WallTime
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
		default:
			continue
		}
		markers = append(markers, juicer.Marker[int64]{
			Timestamp: ts,
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
		return ba.Txn.ReadTimestamp.WallTime
	}
	return 0
}

func (crdbJuicerSPI) GetOperationCount(req interface{}) int32 {
	if ba := asBatchRequest(req); ba != nil {
		return int32(len(ba.Requests))
	}
	return 0
}

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
func (crdbJuicerSPI) GeneratePrepareRequest(req interface{}) interface{}      { return nil }
func (crdbJuicerSPI) GenerateCommitRequest(req interface{}) interface{}       { return nil }
func (crdbJuicerSPI) ExecuteCommitRequest(req interface{}) (interface{}, error)  { return nil, nil }
func (crdbJuicerSPI) ExecutePrepareRequest(req interface{}) (interface{}, error) { return nil, nil }
func (crdbJuicerSPI) ExecuteAbortRequest(req interface{}) (interface{}, error)   { return nil, nil }
func (crdbJuicerSPI) AbortCurrentRequest(req interface{}) (interface{}, error)   { return nil, nil }

// BuildRules declares CRDB's dependency semantics to the enforcer: locking
// operations (SFU reads and writes) of different transactions mutually
// exclude; plain MVCC reads never wait (CRDB readers do not block on
// unreplicated exclusive locks). A holder releases when its EndTxn or
// ResolveIntent is observed on the key, when its own write batch response
// returns (1PC hot path: response return == committed), or on any failed
// response. Note an OpGetForPut response deliberately does NOT release: the
// SFU lock must be held through commit — that is what kills the aborts.
func (crdbJuicerSPI) BuildRules() juicer.DependencyRules {
	locking := func(o juicer.OperationType) bool {
		return o == juicer.OpGetForPut || o == juicer.OpSet
	}
	return juicer.DependencyRules{
		Blocks: func(h, hd juicer.OpRef) bool {
			return h.TxnID != hd.TxnID && locking(h.OpType) && locking(hd.OpType)
		},
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
