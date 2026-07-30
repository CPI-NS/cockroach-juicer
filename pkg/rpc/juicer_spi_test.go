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

func TestJuicerCRDBRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
