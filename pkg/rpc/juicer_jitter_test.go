// Copyright 2026 The Cockroach Authors.
// Use of this software is governed by the CockroachDB Software License.

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/juicer"
)

func TestJuicerInvalidRuleFailsClosed(t *testing.T) {
	old := juicerRulesEnv
	defer func() { juicerRulesEnv = old }()
	juicerRulesEnv = "none"
	defer func() {
		if recover() == nil {
			t.Fatal("unknown rule silently changed treatment")
		}
	}()
	juicerRulesMode()
}

func TestJuicerMixedMarkersKeepTypes(t *testing.T) {
	ba := juicerBatch(juicerTestTxn(9), plainGet("a"), lockingGet("b"), put("c"),
		&kvpb.ConditionalPutRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("d")}}, endTxn(true))
	got := newCRDBJuicerSPI().SplitMarker(ba)
	want := []juicer.OperationType{juicer.OpGet, juicer.OpGetForPut, juicer.OpSet, juicer.OpSet}
	if len(got) != len(want) {
		t.Fatalf("markers: %+v", got)
	}
	for i := range want {
		if got[i].TypedOpType != want[i] {
			t.Fatalf("marker %d: %+v, want type %v", i, got[i], want[i])
		}
	}
	readAndCommit := juicerBatch(juicerTestTxn(10), plainGet("a"), endTxn(true))
	if got := newCRDBJuicerSPI().SplitMarker(readAndCommit); len(got) != 1 || got[0].TypedOpType != juicer.OpGet {
		t.Fatalf("EndTxn must not suppress a read data marker: %+v", got)
	}
}

func TestJuicerJitterDataSelection(t *testing.T) {
	txn := juicerTestTxn(1)
	for _, ba := range []*kvpb.BatchRequest{nil, {}, juicerBatch(nil, put("x")), juicerBatch(txn, endTxn(true))} {
		if isJitterDataBatch(ba) {
			t.Fatalf("non-data batch selected: %v", ba)
		}
	}
	if !isJitterDataBatch(juicerBatch(txn, plainGet("x"), put("y"), endTxn(true))) {
		t.Fatal("mixed data batch was not selected")
	}
	j := newDataRPCJitter(0, 0, 1)
	j.recordShape(juicerBatch(txn, plainGet("x"), put("y"), endTxn(true)))
	j.recordShape(juicerBatch(txn, endTxn(true)))
	if j.stats.DataBatches != 1 || j.stats.MixedDataBatches != 1 || j.stats.ControlBatches != 1 || j.stats.EndTxnBatches != 2 {
		t.Fatalf("transport shape: %+v", j.stats)
	}
}

func TestJuicerJitterCancellationAndDose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := newDataRPCJitter(20, 200, 17)
	if !errors.Is(j.wait(ctx), context.Canceled) || j.stats.Scheduled != 0 {
		t.Fatal("cancelled request scheduled")
	}
	for i := 0; i < 8; i++ {
		if err := j.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if j.stats.Ready != 8 || j.stats.Cancelled != 0 {
		t.Fatalf("counts: %+v", j.stats)
	}
	if j.stats.RequestedNS < 8*20_000 || j.stats.RequestedNS > 8*200_000 {
		t.Fatalf("nominal dose: %+v", j.stats)
	}
	var buckets uint64
	for _, n := range j.stats.ActualBuckets {
		buckets += n
	}
	if buckets != 8 || j.stats.ActualNS == 0 {
		t.Fatalf("actual dose: %+v", j.stats)
	}

	// A long timer makes cancellation deterministic without a real delay.
	j = newDataRPCJitter(10_000_000, 10_000_000, 17)
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if !errors.Is(j.wait(ctx), context.DeadlineExceeded) || j.stats.Cancelled != 1 || j.stats.Ready != 0 {
		t.Fatalf("cancellation: %+v", j.stats)
	}
}
