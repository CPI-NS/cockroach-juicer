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

// withPinnedSortKey switches the package to the campaign-7 sorting key for the
// duration of a test.
func withPinnedSortKey(t *testing.T) {
	t.Helper()
	saved := juicerSortKeyMode
	juicerSortKeyMode = juicerSortKeyPinnedRead
	t.Cleanup(func() { juicerSortKeyMode = saved })
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

// The default sorting key is the transaction header's MinTimestamp: one value
// per transaction, assigned at creation, carried in every batch to every node,
// and untouched by internal retries. Everything Juicer needs from a sorting key
// follows from that, with no per-node state to keep.
func TestJuicerSPISortKeyIsTheTransactionMinTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	if juicerSortKeyMode != juicerSortKeyMinTimestamp {
		t.Fatalf("default sorting key source = %q, want %q",
			juicerSortKeyMode, juicerSortKeyMinTimestamp)
	}

	txn := juicerTestTxn(100)
	txn.MinTimestamp = hlc.Timestamp{WallTime: 100, Logical: 4}
	// The read timestamp starts elsewhere to prove the key does not come
	// from it.
	txn.ReadTimestamp = hlc.Timestamp{WallTime: 300, Logical: 9}

	markers := spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if len(markers) != 1 {
		t.Fatalf("markers = %d, want 1", len(markers))
	}
	if markers[0].Timestamp != 100 || markers[0].Logical != 4 {
		t.Fatalf("sort key = (%d,%d), want the MinTimestamp (100,4)",
			markers[0].Timestamp, markers[0].Logical)
	}
	if got := spi.GetTimestamp(juicerBatch(txn, plainGet("k1"))); got != 100 {
		t.Fatalf("GetTimestamp = %d, want the MinTimestamp wall time 100", got)
	}

	// An internal restart bumps the epoch and moves both timestamps forward.
	// roachpb.Transaction.Restart is called rather than hand-edited fields, so
	// the test fails if a future release starts touching MinTimestamp there.
	before := txn.MinTimestamp
	txn.Restart(roachpb.NormalUserPriority, 0 /* upgradePriority */, hlc.Timestamp{WallTime: 900})
	if txn.Epoch == 0 {
		t.Fatal("Restart did not bump the epoch; the test is not exercising a restart")
	}
	if txn.MinTimestamp != before {
		t.Fatalf("Restart moved MinTimestamp %v -> %v: it is no longer restart-invariant "+
			"and this sorting key source has to be reconsidered", before, txn.MinTimestamp)
	}
	markers = spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 100 || markers[0].Logical != 4 {
		t.Fatalf("sort key after an internal restart = (%d,%d), want (100,4)",
			markers[0].Timestamp, markers[0].Logical)
	}

	// A read refresh moves the read timestamp without a restart.
	txn.BumpReadTimestamp(hlc.Timestamp{WallTime: 5000})
	markers = spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 100 {
		t.Fatalf("sort key after a read refresh = %d, want 100", markers[0].Timestamp)
	}

	// There is no pin to release, so observing the EndTxn changes nothing: the
	// key is a property of the transaction, not of what this node has seen.
	if op := spi.GetOperationType(juicerBatch(txn, endTxn(true))); op != juicer.OpCommit {
		t.Fatalf("EndTxn(commit) op = %v, want OpCommit", op)
	}
	markers = spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 100 {
		t.Fatalf("sort key after EndTxn = %d, want the unchanged 100", markers[0].Timestamp)
	}
}

// Nothing in CockroachDB asserts MinTimestamp is set (AssertInitialized checks
// only ID and WriteTimestamp), so an empty one must not collapse every such
// transaction onto the sorting key 0. It falls back to the pinned table.
func TestJuicerSPISortKeyFallsBackWhenMinTimestampIsEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	txn := juicerTestTxn(0)
	txn.MinTimestamp = hlc.Timestamp{}
	txn.ReadTimestamp = hlc.Timestamp{WallTime: 700, Logical: 2}
	defer juicerTxnKeys.forget(txn.ID)

	markers := spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 700 || markers[0].Logical != 2 {
		t.Fatalf("fallback sort key = (%d,%d), want the read timestamp (700,2)",
			markers[0].Timestamp, markers[0].Logical)
	}
	// And the fallback is pinned, so it is at least stable on this node.
	txn.ReadTimestamp = hlc.Timestamp{WallTime: 800}
	markers = spi.SplitMarker(juicerBatch(txn, plainGet("k1")))
	if markers[0].Timestamp != 700 {
		t.Fatalf("fallback sort key after a bump = %d, want the pinned 700", markers[0].Timestamp)
	}
}

// The defect this sorting key source was introduced to fix: a transaction must
// get the same key on every node that participates in it. Under the pinned key
// it did not — 49.7% of multi-node transactions in the campaign-7 debug cells
// were pinned differently on different nodes — because the pin is taken from
// whatever read timestamp that particular node happened to see first.
func TestJuicerSPISortKeyAgreesAcrossNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Node 1 sees the transaction on its first attempt; node 2 only joins
	// after a refresh has moved the read timestamp forward. Both see the same
	// transaction, so both must sort it into the same position.
	txn := juicerTestTxn(100)
	early := juicerBatch(txn, plainGet("k1"))

	refreshed := txn.Clone()
	refreshed.BumpReadTimestamp(hlc.Timestamp{WallTime: 4000})
	late := juicerBatch(refreshed, plainGet("k1"))

	k1 := sortKeyOnFreshNode(t, early)
	k2 := sortKeyOnFreshNode(t, late)
	if k1 != k2 {
		t.Fatalf("nodes disagree on the sorting key: %+v vs %+v", k1, k2)
	}
	if k1.wallTime != 100 {
		t.Fatalf("sorting key = %+v, want the shared MinTimestamp 100", k1)
	}

	// The same scenario under the campaign-7 key, which is what the change is
	// measured against: the two nodes pin different keys. Asserting the defect
	// keeps the comparison arm honest — if this ever stops diverging, the two
	// modes are no longer measuring different things.
	withPinnedSortKey(t)
	p1 := sortKeyOnFreshNode(t, early)
	p2 := sortKeyOnFreshNode(t, late)
	if p1 == p2 {
		t.Fatalf("pinned mode no longer diverges across nodes (%+v): the campaign-7 "+
			"comparison arm is not reproducing campaign-7 behaviour", p1)
	}
	if p1.wallTime != 100 || p2.wallTime != 4000 {
		t.Fatalf("pinned keys = %+v / %+v, want each node's first-seen read timestamp", p1, p2)
	}
}

// sortKeyOnFreshNode returns the sorting key a node that has never seen this
// transaction assigns to ba. juicerTxnKeys is the entirety of a node's Juicer
// sorting state — crdbJuicerSPI is a stateless value type constructed per
// server — so a fresh table is a faithful stand-in for a second node.
func sortKeyOnFreshNode(t *testing.T, ba *kvpb.BatchRequest) juicerTxnSortKey {
	t.Helper()
	saved := juicerTxnKeys
	juicerTxnKeys = newJuicerTxnKeyTable(juicerTxnKeyTTL, juicerTxnKeySweepInterval)
	defer func() { juicerTxnKeys = saved }()

	spi := newCRDBJuicerSPI()
	markers := spi.SplitMarker(ba)
	if len(markers) != 1 {
		t.Fatalf("markers = %d, want 1", len(markers))
	}
	return juicerTxnSortKey{wallTime: markers[0].Timestamp, logical: int32(markers[0].Logical)}
}

// The campaign-7 sorting key: (WallTime, Logical) pinned to the transaction's
// first read timestamp seen on this node. Both halves matter: without Logical
// the key is not a total order, and without the pin a mid-flight timestamp bump
// moves a transaction's later operations to a different position than its
// earlier ones.
func TestJuicerSPIPinnedSortKeyIsPinnedAndCarriesLogical(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withPinnedSortKey(t)
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
	if juicerFixedWaitMillis != 0 {
		t.Errorf("COCKROACH_JUICER_FIXED_WAIT_MS default = %v, want 0 (zero-window arm)",
			juicerFixedWaitMillis)
	}
	if juicerRandomDelayFixedMillis != 0 {
		t.Errorf("COCKROACH_JUICER_RANDOM_DELAY_FIXED_MS default = %v, want 0 (empirical distribution)",
			juicerRandomDelayFixedMillis)
	}

	// The sorting key source is deliberately NOT an off-by-default arm: the
	// per-node pinned key is a defect (it puts the same transaction in
	// different positions on different nodes), so the corrected key is the
	// default and the old behaviour is what has to be asked for by name.
	if juicerSortKeyMode != juicerSortKeyMinTimestamp {
		t.Errorf("COCKROACH_JUICER_SORT_KEY default = %q, want %q",
			juicerSortKeyMode, juicerSortKeyMinTimestamp)
	}
	for _, tc := range []struct {
		name         string
		env          string
		expectedMode juicerSortKeySource
	}{
		{name: "unset", env: "", expectedMode: juicerSortKeyMinTimestamp},
		{name: "explicit default", env: "min-timestamp", expectedMode: juicerSortKeyMinTimestamp},
		{name: "campaign-7 key", env: "pinned-read-timestamp", expectedMode: juicerSortKeyPinnedRead},
		{name: "case and space tolerated", env: "  Pinned-Read-Timestamp ", expectedMode: juicerSortKeyPinnedRead},
		{name: "unrecognized falls back to the default", env: "bogus", expectedMode: juicerSortKeyMinTimestamp},
	} {
		if got := parseJuicerSortKeySource(tc.env); got != tc.expectedMode {
			t.Errorf("%s: parse(%q) = %q, want %q", tc.name, tc.env, got, tc.expectedMode)
		}
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

// The batch-now sorting key comes from the header Now — the gateway
// DistSender clock reading taken once per client-level send — so a retry
// sorts at its retry time instead of at the transaction birth, and the two
// messages of one wave still agree on every node. An empty Now falls back to
// the MinTimestamp key rather than collapsing onto key zero.
func TestJuicerSPISortKeyBatchNow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()

	saved := juicerSortKeyMode
	juicerSortKeyMode = juicerSortKeyBatchNow
	t.Cleanup(func() { juicerSortKeyMode = saved })

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

	bare := juicerBatch(txn, plainGet("k1"))
	markers = spi.SplitMarker(bare)
	if len(markers) != 1 || markers[0].Timestamp != 100 || markers[0].Logical != 4 {
		t.Fatalf("empty-Now fallback sort key = (%d,%d), want the MinTimestamp (100,4)",
			markers[0].Timestamp, markers[0].Logical)
	}

	if got := parseJuicerSortKeySource("batch-now"); got != juicerSortKeyBatchNow {
		t.Fatalf("parseJuicerSortKeySource(batch-now) = %q", got)
	}
}
