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
