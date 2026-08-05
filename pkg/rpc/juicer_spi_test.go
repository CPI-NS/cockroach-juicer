// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc/juicer"
)

func juicerTestTxn(wallTime int64) *roachpb.Transaction {
	txn := &roachpb.Transaction{}
	txn.ID = uuid.MakeV4()
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
			defer juicerTxnKeys.forget(txn.ID)
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
				t.Fatalf("refresh marker timestamp = %d, want the txn's pinned key", markers[0].Timestamp)
			}
		})
	}

	// A refresh mixed with a write is still a write: the batch-level type is
	// the strongest operation in it.
	txn := juicerTestTxn(88)
	defer juicerTxnKeys.forget(txn.ID)
	if op := spi.GetOperationType(juicerBatch(txn, refresh("k1"), put("k2"))); op != juicer.OpSet {
		t.Fatalf("refresh+write batch op = %v, want OpSet", op)
	}
}

// The sorting key is (WallTime, Logical) pinned to the transaction's first
// read timestamp. Both halves matter: without Logical the key is not a total
// order, and without the pin a mid-flight timestamp bump moves a transaction's
// later operations to a different position than its earlier ones.
func TestJuicerSPISortKeyIsPinnedAndCarriesLogical(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	txn := juicerTestTxn(100)
	txn.ReadTimestamp = hlc.Timestamp{WallTime: 100, Logical: 4}
	defer juicerTxnKeys.forget(txn.ID)

	markers := spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if len(markers) != 1 {
		t.Fatalf("markers = %d, want 1", len(markers))
	}
	if markers[0].Timestamp != 100 || markers[0].Logical != 4 {
		t.Fatalf("sort key = (%d,%d), want (100,4)", markers[0].Timestamp, markers[0].Logical)
	}

	// CockroachDB pushes the read timestamp forward mid-transaction. The
	// sorting key must not follow it.
	txn.ReadTimestamp = hlc.Timestamp{WallTime: 500, Logical: 0}
	markers = spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 100 || markers[0].Logical != 4 {
		t.Fatalf("sort key after a timestamp bump = (%d,%d), want the pinned (100,4)",
			markers[0].Timestamp, markers[0].Logical)
	}
	if got := spi.GetTimestamp(juicerBatch(txn, plainGet("k1"))); got != 100 {
		t.Fatalf("GetTimestamp = %d, want the pinned wall time 100", got)
	}

	// Observing the transaction's EndTxn releases the pin, so the table does
	// not grow with every transaction the node ever sees.
	if op := spi.GetOperationType(juicerBatch(txn, endTxn(true))); op != juicer.OpCommit {
		t.Fatalf("EndTxn(commit) op = %v, want OpCommit", op)
	}
	markers = spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 500 {
		t.Fatalf("sort key after EndTxn = %d, want a fresh pin at 500", markers[0].Timestamp)
	}
	juicerTxnKeys.forget(txn.ID)

	// An abort releases it too.
	txn2 := juicerTestTxn(200)
	spi.SplitMarker(juicerBatch(txn2, plainGet("k1")))
	if op := spi.GetOperationType(juicerBatch(txn2, endTxn(false))); op != juicer.OpAbort {
		t.Fatalf("EndTxn(abort) op = %v, want OpAbort", op)
	}
	txn2.ReadTimestamp = hlc.Timestamp{WallTime: 999}
	markers = spi.SplitMarker(juicerBatch(txn2, plainGet("k1")))
	if markers[0].Timestamp != 999 {
		t.Fatalf("sort key after abort = %d, want a fresh pin at 999", markers[0].Timestamp)
	}
	juicerTxnKeys.forget(txn2.ID)
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

// Every measurement arm has to be off unless its environment variable is set,
// and Juicer as a whole has to contribute no server options at all when it is
// disabled. The second half is what makes the baseline a valid transparency
// control: a baseline node must be byte-identical to upstream, not "upstream
// plus an interceptor that happens to do nothing".
func TestJuicerMeasurementArmsDefaultOff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if juicerWaitStrategy != "" {
		t.Errorf("COCKROACH_JUICER_WAIT_STRATEGY default = %q, want empty (contention-blind insertion-time strategy)",
			juicerWaitStrategy)
	}
	if juicerWaitMinQLen != 2 {
		t.Errorf("COCKROACH_JUICER_WAIT_MIN_QLEN default = %d, want 2", juicerWaitMinQLen)
	}
	if juicerRandomDelay {
		t.Error("COCKROACH_JUICER_RANDOM_DELAY default = true, want false")
	}
	if juicerRandomDelaySeed != 0 {
		t.Errorf("COCKROACH_JUICER_RANDOM_DELAY_SEED default = %d, want 0 (seed from the clock)",
			juicerRandomDelaySeed)
	}

	defer func(saved bool) { juicerEnabled = saved }(juicerEnabled)
	juicerEnabled = false
	if opts := juicerServerOptions(); opts != nil {
		t.Fatalf("juicer disabled but %d server options were returned", len(opts))
	}
	juicerEnabled = true
	base := len(juicerServerOptions())

	// Each arm adds options only when selected.
	defer func(s string, r bool) { juicerWaitStrategy, juicerRandomDelay = s, r }(juicerWaitStrategy, juicerRandomDelay)
	juicerWaitStrategy = juicer.WaitStrategyQLenGated
	if got := len(juicerServerOptions()); got <= base {
		t.Errorf("selecting a wait strategy added no server options (%d, base %d)", got, base)
	}
	juicerWaitStrategy = ""
	juicerRandomDelay = true
	if got := len(juicerServerOptions()); got <= base {
		t.Errorf("selecting the random delay arm added no server options (%d, base %d)", got, base)
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
		expectedBlocksSFU   bool
		expectedBlocksWrite bool
	}{
		{
			name: "default is full", env: "", expectedMode: juicerRulesFull,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: true,
		},
		{
			name: "full blocks every locking pair", env: "full", expectedMode: juicerRulesFull,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: true,
		},
		{
			name: "sfu leaves write-write to the lock table", env: "sfu", expectedMode: juicerRulesSFU,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: false,
		},
		{
			name: "off disables the enforcer entirely", env: "off", expectedMode: juicerRulesOff,
			expectedEnabled: false,
		},
		{
			name: "case and space tolerated", env: "  SFU ", expectedMode: juicerRulesSFU,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: false,
		},
		{
			name: "unrecognized value falls back to full", env: "bogus", expectedMode: juicerRulesFull,
			expectedEnabled: true, expectedBlocksSFU: true, expectedBlocksWrite: true,
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
			if got := rules.Blocks(sfuA, sfuB); got != tc.expectedBlocksSFU {
				t.Errorf("Blocks(sfu, sfu) = %v, want %v", got, tc.expectedBlocksSFU)
			}
			if got := rules.Blocks(sfuA, writeB); got != tc.expectedBlocksWrite {
				t.Errorf("Blocks(sfu, write) = %v, want %v", got, tc.expectedBlocksWrite)
			}
			// Same-txn pairs never block in any mode: the enforcer evaluates the
			// head against the whole in-flight set, including the txn's own ops.
			if rules.Blocks(sfuA, juicer.OpRef{TxnID: 1, OpType: juicer.OpSet}) {
				t.Error("same-txn write upgrade blocked itself")
			}
			// Plain reads never wait in any mode.
			readB := juicer.OpRef{TxnID: 2, OpType: juicer.OpGet}
			if rules.Blocks(sfuA, readB) || rules.Blocks(readB, sfuA) {
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

	// Releases: commit-batch response returning releases the same txn's SFU
	// holder; an SFU read's own response returning must NOT release it.
	respSet := juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 1}
	if !rules.Releases(sfuA, respSet) {
		t.Fatal("commit-batch response did not release same-txn holder")
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
}
