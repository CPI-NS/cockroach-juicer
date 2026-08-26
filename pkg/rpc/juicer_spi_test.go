// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc/juicer"
)

// juicerTestTxn builds a transaction whose MinTimestamp and ReadTimestamp
// agree, which is the state a freshly created transaction is in: both come
// from the same clock reading in roachpb.MakeTransaction. Tests that care
// about the difference move ReadTimestamp afterwards, the way a refresh or a
// push does.
func juicerTestTxn(wallTime int64) *roachpb.Transaction {
	txn := &roachpb.Transaction{}
	txn.ID = uuid.MakeV4()
	txn.MinTimestamp = hlc.Timestamp{WallTime: wallTime}
	txn.ReadTimestamp = hlc.Timestamp{WallTime: wallTime}
	return txn
}

func juicerBatch(txn *roachpb.Transaction, reqs ...kvpb.Request) *kvpb.BatchRequest {
	ba := &kvpb.BatchRequest{}
	ba.Txn = txn
	for _, r := range reqs {
		ba.Add(r)
	}
	return ba
}

func lockingGet(key string) *kvpb.GetRequest {
	return &kvpb.GetRequest{
		RequestHeader:      kvpb.RequestHeader{Key: roachpb.Key(key)},
		KeyLockingStrength: lock.Exclusive,
	}
}

func plainGet(key string) *kvpb.GetRequest {
	return &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(key)}}
}

func put(key string) *kvpb.PutRequest {
	return &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(key)}}
}

func endTxn(commit bool) *kvpb.EndTxnRequest {
	return &kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("anchor")},
		Commit:        commit,
	}
}

func refresh(key string) *kvpb.RefreshRequest {
	return &kvpb.RefreshRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(key)}}
}

func refreshRange(start, end string) *kvpb.RefreshRangeRequest {
	return &kvpb.RefreshRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(start), EndKey: roachpb.Key(end)},
	}
}

func TestJuicerSPIClassification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	// (1) Non-transactional batch: never sorted.
	nonTxn := &kvpb.BatchRequest{}
	nonTxn.Add(put("a"))
	if got := spi.SplitMarker(nonTxn); got != nil {
		t.Fatalf("non-txn batch produced markers: %v", got)
	}
	if id := spi.GetTxnId(nonTxn); id != 0 {
		t.Fatalf("non-txn batch txn id = %d, want 0", id)
	}

	// (2) Locking read batch (implicit SFU): one marker, OpGetForPut.
	txn := juicerTestTxn(42)
	sfu := juicerBatch(txn, lockingGet("k1"))
	if op := spi.GetOperationType(sfu); op != juicer.OpGetForPut {
		t.Fatalf("locking get batch op = %v, want OpGetForPut", op)
	}
	markers := spi.SplitMarker(sfu)
	if len(markers) != 1 {
		t.Fatalf("locking get markers = %d, want 1", len(markers))
	}
	if markers[0].TxnId == 0 || markers[0].Timestamp != 42 || markers[0].OpType != "GetForUpdate" {
		t.Fatalf("bad marker: %+v", markers[0])
	}

	// Plain read stays OpGet.
	if op := spi.GetOperationType(juicerBatch(txn, plainGet("k1"))); op != juicer.OpGet {
		t.Fatalf("plain get batch op = %v, want OpGet", op)
	}

	// (3) 1PC commit batch [Put, EndTxn(commit)]: sortable OpSet, marker only
	// for the Put — commit-ness is the release arm's job, not a classification.
	onePC := juicerBatch(txn, put("k1"), endTxn(true))
	if op := spi.GetOperationType(onePC); op != juicer.OpSet {
		t.Fatalf("1PC batch op = %v, want OpSet", op)
	}
	markers = spi.SplitMarker(onePC)
	if len(markers) != 1 || markers[0].OpType != "Put" {
		t.Fatalf("1PC markers = %+v, want single Put marker", markers)
	}

	// (4) Write-free EndTxn(commit=false): abort message, not sortable.
	abortBatch := juicerBatch(txn, endTxn(false))
	if !spi.IsAbortRequest(abortBatch) {
		t.Fatal("EndTxn(commit=false) not classified as abort")
	}
	if got := spi.SplitMarker(abortBatch); got != nil {
		t.Fatalf("abort batch produced markers: %v", got)
	}
	commitBatch := juicerBatch(txn, endTxn(true))
	if !spi.IsCommitRequest(commitBatch) {
		t.Fatal("write-free EndTxn(commit=true) not classified as commit")
	}
	// ResolveIntent carries the commit signal participants actually see.
	resolve := &kvpb.BatchRequest{}
	resolve.Add(&kvpb.ResolveIntentRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k1")},
		Status:        roachpb.COMMITTED,
	})
	if !spi.IsCommitRequest(resolve) {
		t.Fatal("ResolveIntent(COMMITTED) not classified as commit")
	}

	// (5) Failed response detection.
	br := &kvpb.BatchResponse{}
	if spi.IsSelfAbortedResponse(br) {
		t.Fatal("clean response flagged as aborted")
	}
	br.Error = kvpb.NewErrorf("boom")
	if !spi.IsSelfAbortedResponse(br) {
		t.Fatal("errored response not flagged as aborted")
	}
}

// A refresh is the message whose outcome decides whether a transaction
// commits or restarts. Leaving it unsortable meant the order Juicer arranged
// for a key's reads was not the order in which those reads were validated, so
// the ordering could not affect the metric it was supposed to affect.
func TestJuicerSPIRefreshIsSortable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	for _, tc := range []struct {
		name           string
		req            kvpb.Request
		expectedOpName string
	}{
		{name: "point refresh", req: refresh("k1"), expectedOpName: "Refresh"},
		{name: "range refresh", req: refreshRange("k1", "k9"), expectedOpName: "RefreshRange"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := juicerTestTxn(77)
			ba := juicerBatch(txn, tc.req)

			if op := spi.GetOperationType(ba); op != juicer.OpGet {
				t.Fatalf("refresh batch op = %v, want OpGet", op)
			}
			if !sortableJuicerOp(spi.GetOperationType(ba)) {
				t.Fatal("refresh batch is not sortable")
			}
			markers := spi.SplitMarker(ba)
			if len(markers) != 1 {
				t.Fatalf("refresh markers = %d, want 1", len(markers))
			}
			if markers[0].OpType != tc.expectedOpName {
				t.Fatalf("refresh marker op name = %q, want %q", markers[0].OpType, tc.expectedOpName)
			}
			// A RefreshRange is placed on the queue of its start key: the
			// sorting queues are per point key, so a span has no exact
			// representation.
			if markers[0].Key != juicerKeyHash([]byte("k1")) {
				t.Fatalf("refresh marker key = %d, want the hash of the start key", markers[0].Key)
			}
			if markers[0].Timestamp != 77 {
				t.Fatalf("refresh marker timestamp = %d, want the MinTimestamp fallback", markers[0].Timestamp)
			}
		})
	}

	// A refresh mixed with a write is still a write: the batch-level type is
	// the strongest operation in it.
	txn := juicerTestTxn(88)
	if op := spi.GetOperationType(juicerBatch(txn, refresh("k1"), put("k2"))); op != juicer.OpSet {
		t.Fatalf("refresh+write batch op = %v, want OpSet", op)
	}
}

// The sorting key is the batch header's Now — the gateway's per-send clock
// reading — so a retry sorts at its retry time, and every per-range sub-batch
// of one send shares the key by construction (they ShallowCopy one header).
// There is no per-node sorting state: two nodes given the same batch bytes
// compute the same key, which is the cross-node agreement property the old
// pinned key lacked.
func TestJuicerSPISortKeyIsTheBatchNow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	txn := juicerTestTxn(100)
	txn.MinTimestamp = hlc.Timestamp{WallTime: 100, Logical: 4}

	ba := juicerBatch(txn, plainGet("k1"))
	ba.Now = hlc.ClockTimestamp{WallTime: 777, Logical: 3}
	markers := spi.SplitMarker(ba)
	if len(markers) != 1 {
		t.Fatalf("markers = %d, want 1", len(markers))
	}
	if markers[0].Timestamp != 777 || markers[0].Logical != 3 {
		t.Fatalf("sort key = (%d,%d), want the batch Now (777,3)",
			markers[0].Timestamp, markers[0].Logical)
	}
	if got := spi.GetTimestamp(ba); got != 777 {
		t.Fatalf("GetTimestamp = %d, want the batch Now wall time 777", got)
	}

	// The transaction's own timestamps moving does not move the key: a
	// restart or refresh changes what the transaction reads at, not when
	// this batch was sent.
	txn.Restart(roachpb.NormalUserPriority, 0 /* upgradePriority */, hlc.Timestamp{WallTime: 900})
	txn.BumpReadTimestamp(hlc.Timestamp{WallTime: 5000})
	markers = spi.SplitMarker(ba)
	if markers[0].Timestamp != 777 || markers[0].Logical != 3 {
		t.Fatalf("sort key after restart+refresh = (%d,%d), want the unchanged (777,3)",
			markers[0].Timestamp, markers[0].Logical)
	}

	// A later send re-stamps Now, so the same transaction's next attempt
	// sorts at its own send time — the retry-aware property the key exists
	// for.
	resend := juicerBatch(txn, plainGet("k1"))
	resend.Now = hlc.ClockTimestamp{WallTime: 4000, Logical: 1}
	markers = spi.SplitMarker(resend)
	if markers[0].Timestamp != 4000 || markers[0].Logical != 1 {
		t.Fatalf("re-send sort key = (%d,%d), want (4000,1)",
			markers[0].Timestamp, markers[0].Logical)
	}

	// The header documents Now as optional: an empty one falls back to the
	// transaction's MinTimestamp rather than collapsing onto key zero.
	bare := juicerBatch(txn, plainGet("k1"))
	markers = spi.SplitMarker(bare)
	if len(markers) != 1 || markers[0].Timestamp != 100 || markers[0].Logical != 4 {
		t.Fatalf("empty-Now fallback sort key = (%d,%d), want the MinTimestamp (100,4)",
			markers[0].Timestamp, markers[0].Logical)
	}
}

// TestJuicerSPIFailureClassification checks that the per-error-type counters
// feeding the minute reporter bucket a real kvpb error detail correctly. Deltas
// are used rather than absolute values because the counters are package-level
// and other tests in this package also drive IsSelfAbortedResponse.
func TestJuicerSPIFailureClassification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	before := juicerFailures.snapshot()

	// A serializable retry must land in the retry bucket and in its reason
	// sub-counter, and must not leak into any other bucket.
	retryResp := &kvpb.BatchResponse{}
	retryResp.Error = kvpb.NewError(&kvpb.TransactionRetryError{Reason: kvpb.RETRY_SERIALIZABLE})
	if !spi.IsSelfAbortedResponse(retryResp) {
		t.Fatal("TransactionRetryError response not flagged as aborted")
	}

	// An error with no recognized kvpb detail falls into "other" and adds
	// nothing to the per-detail-type tally.
	plainResp := &kvpb.BatchResponse{}
	plainResp.Error = kvpb.NewErrorf("boom")
	if !spi.IsSelfAbortedResponse(plainResp) {
		t.Fatal("plain error response not flagged as aborted")
	}

	// A clean response must not be counted at all.
	if spi.IsSelfAbortedResponse(&kvpb.BatchResponse{}) {
		t.Fatal("clean response flagged as aborted")
	}

	after := juicerFailures.snapshot()
	for _, tc := range []struct {
		name          string
		expectedDelta uint64
		got           uint64
	}{
		{name: "total", expectedDelta: 2, got: after.total - before.total},
		{name: "retry", expectedDelta: 1, got: after.retry - before.retry},
		{name: "retrySerializable", expectedDelta: 1, got: after.retrySerializable - before.retrySerializable},
		{name: "other", expectedDelta: 1, got: after.other - before.other},
		{name: "writeTooOld", expectedDelta: 0, got: after.writeTooOld - before.writeTooOld},
		{name: "aborted", expectedDelta: 0, got: after.aborted - before.aborted},
		{name: "lockConflict", expectedDelta: 0, got: after.lockConflict - before.lockConflict},
		{name: "uncertainty", expectedDelta: 0, got: after.uncertainty - before.uncertainty},
	} {
		if tc.got != tc.expectedDelta {
			t.Errorf("%s delta = %d, want %d", tc.name, tc.got, tc.expectedDelta)
		}
	}

	// Nothing above carried a detail that belongs in the "other" tally, so the
	// pre-formatted breakdown must stay empty.
	if s := kvOtherBreakdown(after, before); s != "" {
		t.Errorf("other-detail breakdown = %q, want empty", s)
	}

	// A WriteTooOldError is the other bucket the campaign cares most about.
	wtoBefore := juicerFailures.snapshot()
	wtoResp := &kvpb.BatchResponse{}
	wtoResp.Error = kvpb.NewError(&kvpb.WriteTooOldError{})
	if !spi.IsSelfAbortedResponse(wtoResp) {
		t.Fatal("WriteTooOldError response not flagged as aborted")
	}
	wtoAfter := juicerFailures.snapshot()
	if d := wtoAfter.writeTooOld - wtoBefore.writeTooOld; d != 1 {
		t.Errorf("writeTooOld delta = %d, want 1", d)
	}
	if d := wtoAfter.retry - wtoBefore.retry; d != 0 {
		t.Errorf("WriteTooOldError leaked into retry bucket (delta %d)", d)
	}
}

// TestJuicerFilterSizesArePositive guards the defect this test was written for:
// the fork's defaultServerOptions leaves juicerFilterK/juicerFilterS at zero, and
// AbortTracker.calculateAndSendDelta bails out when TopNFirstFilter <= 0, so a
// zero here silently disables the entire hot-key pipeline -- aborts are recorded,
// no key is ever promoted, and no per-key queue is ever created. Nothing else in
// the build fails if this regresses, which is exactly why it needs a test.
func TestJuicerFilterSizesArePositive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if juicerFilterK <= 0 {
		t.Errorf("juicerFilterK = %d; a non-positive first-filter size disables hot-key promotion entirely", juicerFilterK)
	}
	if juicerFilterS <= 0 {
		t.Errorf("juicerFilterS = %d; second-filter size must be positive", juicerFilterS)
	}
	if juicerFilterS > juicerFilterK {
		t.Errorf("juicerFilterS (%d) > juicerFilterK (%d): the second filter cannot track more keys than the first admits",
			juicerFilterS, juicerFilterK)
	}
}

// The standard configuration must resolve exactly as documented — wait time
// 0, dependency rules GATE with a 25 ms MaxHold — and Juicer as a whole must
// contribute no server options at all when disabled. That last half is what
// makes the baseline a valid transparency control: a baseline node must be
// byte-identical to upstream, not "upstream plus an interceptor that happens
// to do nothing".
func TestJuicerStandardConfigDefaults(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if juicerFixedWaitMillis != 0 {
		t.Errorf("COCKROACH_JUICER_FIXED_WAIT_MS default = %v, want 0 (no window)",
			juicerFixedWaitMillis)
	}
	if juicerMaxHoldMillis != 25 {
		t.Errorf("COCKROACH_JUICER_MAX_HOLD_MS default = %v, want 25", juicerMaxHoldMillis)
	}
	if got := juicerRulesMode(); got != juicerRulesGate {
		t.Errorf("COCKROACH_JUICER_RULES default mode = %q, want %q (the completion gate is standard)",
			got, juicerRulesGate)
	}

	defer func(saved bool) { juicerEnabled = saved }(juicerEnabled)
	juicerEnabled = false
	if opts := juicerServerOptions(); opts != nil {
		t.Fatalf("juicer disabled but %d server options were returned", len(opts))
	}
}

// The default relation is the completion gate: type-blind blocking, released
// by any same-txn response (success or failure), 25 ms MaxHold, and unbound
// (txn id 0) heads admitted untracked. These properties carried campaigns
// #11/#12/#14 and are what the RULES=gate arm must reproduce.
func TestJuicerGateRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(saved string) { juicerRulesEnv = saved }(juicerRulesEnv)
	juicerRulesEnv = ""

	rules := newCRDBJuicerSPI().BuildRules()
	if !rules.Enabled() {
		t.Fatal("default rules disabled, want the gate relation")
	}
	if !rules.AdmitUnbound {
		t.Fatal("gate relation must admit unbound heads untracked (non-transactional batches)")
	}
	if rules.MaxHold != 25*time.Millisecond {
		t.Fatalf("gate MaxHold = %v, want 25ms (the standard TTL)", rules.MaxHold)
	}

	held := juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}
	// Type-blind and txn-blind blocking: one in-flight message closes the key
	// to everyone, plain reads and the holder's own transaction included.
	for _, head := range []juicer.OpRef{
		{TxnID: 2, OpType: juicer.OpSet},
		{TxnID: 2, OpType: juicer.OpGet},
		{TxnID: 1, OpType: juicer.OpSet},
		{TxnID: 0, OpType: juicer.OpGet},
	} {
		if !rules.Blocks(held, head) {
			t.Errorf("gate did not block head %+v behind an in-flight message", head)
		}
	}

	// Any same-txn response is completion; other txns' responses and mere
	// request arrivals are not.
	if !rules.Releases(held, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 1}) {
		t.Error("own response did not release the gate hold")
	}
	if !rules.Releases(held, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 1, Failed: true}) {
		t.Error("own failed response did not release the gate hold")
	}
	if rules.Releases(held, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 9}) {
		t.Error("another txn's response released the gate hold")
	}
	if rules.Releases(held, juicer.Event{Kind: juicer.ReqArrived, OpType: juicer.OpCommit, TxnID: 1}) {
		t.Error("a request arrival released the gate hold (only responses complete a message)")
	}
}

// The unified-rule candidate: only concurrent cross-txn WRITERS are spaced;
// everything the campaigns identified as tax or harm — plain reads (m50 read
// p50 +40%), locking-read waves (campaign #13's RMW collapse) — passes
// untouched. On pure-write traffic the relation must coincide with the gate.
func TestJuicerWritesRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(saved string) { juicerRulesEnv = saved }(juicerRulesEnv)
	juicerRulesEnv = "writes"

	rules := newCRDBJuicerSPI().BuildRules()
	if !rules.Enabled() {
		t.Fatal("writes rules disabled")
	}
	if !rules.AdmitUnbound {
		t.Fatal("writes relation must admit unbound heads untracked")
	}

	writeA := juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}
	writeB := juicer.OpRef{TxnID: 2, OpType: juicer.OpSet}
	sfuB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGetForPut}
	readB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGet}

	if !rules.Blocks(writeA, writeB) {
		t.Fatal("cross-txn write-write pair did not block (the winning ingredient)")
	}
	// The RMW exemption, both directions: a locking-read wave neither waits
	// behind a held write nor (as a hold, which it can never become) blocks
	// one — stacking holds on lock-table-owned serialization was campaign
	// #13's collapse.
	if rules.Blocks(writeA, sfuB) || rules.Blocks(sfuB, writeA) {
		t.Fatal("locking read participated in blocking under the writes relation")
	}
	if rules.Blocks(writeA, readB) || rules.Blocks(readB, writeA) {
		t.Fatal("plain read participated in blocking under the writes relation")
	}
	if rules.Blocks(writeA, juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}) {
		t.Fatal("same-txn write pair blocked itself")
	}

	// Release relation of a held write: own write response, commit/abort
	// arrival, or a failed response — the gate's completion semantics on the
	// write population.
	if !rules.Releases(writeA, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 1}) {
		t.Fatal("own write response did not release the write hold")
	}
	if !rules.Releases(writeA, juicer.Event{Kind: juicer.ReqArrived, OpType: juicer.OpCommit, TxnID: 1}) {
		t.Fatal("commit arrival did not release the write hold")
	}
	if !rules.Releases(writeA, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpGetForPut, TxnID: 1, Failed: true}) {
		t.Fatal("failed response did not release the write hold")
	}
	if rules.Releases(writeA, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 9}) {
		t.Fatal("another txn's response released the write hold")
	}
	if rules.Releases(writeA, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpGetForPut, TxnID: 1}) {
		t.Fatal("a non-write same-txn response released the write hold")
	}
}

// TestJuicerRulesModes covers the COCKROACH_JUICER_RULES axis: each mode must
// produce the blocking relation it advertises, because the whole point of the
// switch is to attribute an observed effect to sorting or to enforcement. It
// drives juicerRulesEnv directly rather than the process environment, which is
// what juicerRulesMode reads.
func TestJuicerRulesModes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(saved string) { juicerRulesEnv = saved }(juicerRulesEnv)

	sfuA := juicer.OpRef{TxnID: 1, OpType: juicer.OpGetForPut}
	sfuB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGetForPut}
	writeB := juicer.OpRef{TxnID: 2, OpType: juicer.OpSet}

	for _, tc := range []struct {
		name                string
		env                 string
		expectedMode        juicerRules
		expectedEnabled     bool
		expectedTypeBlind   bool // gate: everything blocks everything
		expectedBlocksSFU   bool // Blocks(sfu held, sfu head)
		expectedBlocksWrite bool // Blocks(sfu held, write head)
		expectedBlocksWW    bool // Blocks(write held, write head), cross-txn
	}{
		{
			name: "default is the gate", env: "", expectedMode: juicerRulesGate,
			expectedEnabled: true, expectedTypeBlind: true,
		},
		{
			name: "gate selected by name", env: "gate", expectedMode: juicerRulesGate,
			expectedEnabled: true, expectedTypeBlind: true,
		},
		{
			name: "writes spaces concurrent writers only", env: "writes", expectedMode: juicerRulesWrites,
			expectedEnabled: true, expectedBlocksSFU: false, expectedBlocksWrite: false,
			expectedBlocksWW: true,
		},
		{
			name: "full blocks every locking pair", env: "full", expectedMode: juicerRulesFull,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: true,
			expectedBlocksWW: true,
		},
		{
			name: "sfu leaves write-write to the lock table", env: "sfu", expectedMode: juicerRulesSFU,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: false,
			expectedBlocksWW: false,
		},
		{
			name: "off disables the enforcer entirely", env: "off", expectedMode: juicerRulesOff,
			expectedEnabled: false,
		},
		{
			name: "case and space tolerated", env: "  SFU ", expectedMode: juicerRulesSFU,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: false,
			expectedBlocksWW: false,
		},
		{
			name: "unrecognized value resolves to the default gate", env: "bogus", expectedMode: juicerRulesGate,
			expectedEnabled: true, expectedTypeBlind: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			juicerRulesEnv = tc.env
			if got := juicerRulesMode(); got != tc.expectedMode {
				t.Fatalf("mode = %q, want %q", got, tc.expectedMode)
			}
			rules := newCRDBJuicerSPI().BuildRules()
			if got := rules.Enabled(); got != tc.expectedEnabled {
				t.Fatalf("Enabled() = %v, want %v", got, tc.expectedEnabled)
			}
			if !tc.expectedEnabled {
				// A disabled relation must be the zero value throughout, or the
				// fork would still build an enforcer off a non-nil field.
				if rules.Releases != nil || rules.MaxHold != 0 {
					t.Fatalf("off mode returned a non-zero DependencyRules: %+v", rules)
				}
				return
			}
			readB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGet}
			if tc.expectedTypeBlind {
				// The gate blocks everything behind an in-flight message —
				// reads and same-txn successors included.
				for _, head := range []juicer.OpRef{sfuB, writeB, readB, {TxnID: 1, OpType: juicer.OpSet}} {
					if !rules.Blocks(sfuA, head) {
						t.Errorf("gate did not block head %+v", head)
					}
				}
				return
			}
			if got := rules.Blocks(sfuA, sfuB); got != tc.expectedBlocksSFU {
				t.Errorf("Blocks(sfu, sfu) = %v, want %v", got, tc.expectedBlocksSFU)
			}
			if got := rules.Blocks(sfuA, writeB); got != tc.expectedBlocksWrite {
				t.Errorf("Blocks(sfu, write) = %v, want %v", got, tc.expectedBlocksWrite)
			}
			writeA := juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}
			if got := rules.Blocks(writeA, writeB); got != tc.expectedBlocksWW {
				t.Errorf("Blocks(write, write) = %v, want %v", got, tc.expectedBlocksWW)
			}
			// Same-txn pairs never block in the typed relations: the enforcer
			// evaluates the head against the whole in-flight set, including
			// the txn's own ops.
			if rules.Blocks(sfuA, writeA) || rules.Blocks(writeA, juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}) {
				t.Error("same-txn pair blocked itself")
			}
			// Plain reads never wait in the typed relations.
			if rules.Blocks(sfuA, readB) || rules.Blocks(readB, sfuA) ||
				rules.Blocks(writeA, readB) || rules.Blocks(readB, writeA) {
				t.Error("plain read participated in blocking")
			}
		})
	}
}

func TestJuicerCRDBRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(saved string) { juicerRulesEnv = saved }(juicerRulesEnv)
	juicerRulesEnv = string(juicerRulesFull)

	rules := newCRDBJuicerSPI().BuildRules()
	if !rules.Enabled() {
		t.Fatal("rules disabled")
	}

	sfuA := juicer.OpRef{TxnID: 1, OpType: juicer.OpGetForPut}
	sfuB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGetForPut}
	writeB := juicer.OpRef{TxnID: 2, OpType: juicer.OpSet}
	readB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGet}

	// (6) Blocks: cross-txn locking pairs exclude; reads never wait.
	if !rules.Blocks(sfuA, sfuB) || !rules.Blocks(sfuA, writeB) {
		t.Fatal("cross-txn locking pair did not block")
	}
	if rules.Blocks(sfuA, readB) || rules.Blocks(readB, sfuA) {
		t.Fatal("plain read participated in blocking")
	}
	if rules.Blocks(sfuA, juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}) {
		t.Fatal("same-txn write upgrade blocked itself")
	}

	// Releases, split by held-entry type (the audit's self-release fix): a
	// held WRITE ends when a same-txn write response returns; a held LOCKING
	// READ must survive that same event — its claim resolves only at
	// commit/abort (or a failed response, or the TTL).
	writeA := juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}
	respSet := juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 1}
	if !rules.Releases(writeA, respSet) {
		t.Fatal("write response did not release the same-txn write hold")
	}
	if rules.Releases(sfuA, respSet) {
		t.Fatal("same-txn write response released the locking-read hold before commit (the self-release defect)")
	}
	respGFP := juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpGetForPut, TxnID: 1}
	if rules.Releases(sfuA, respGFP) {
		t.Fatal("SFU response return released the lock before commit")
	}
	if rules.Releases(sfuA, juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 9}) {
		t.Fatal("other txn's response released the holder")
	}
	failedResp := juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpGetForPut, TxnID: 1, Failed: true}
	if !rules.Releases(sfuA, failedResp) {
		t.Fatal("failed response did not release")
	}
	commitMsg := juicer.Event{Kind: juicer.ReqArrived, OpType: juicer.OpCommit, TxnID: 1}
	if !rules.Releases(sfuA, commitMsg) {
		t.Fatal("observed commit message did not release")
	}
	if !rules.Releases(writeA, commitMsg) {
		t.Fatal("observed commit message did not release the write hold")
	}
}
