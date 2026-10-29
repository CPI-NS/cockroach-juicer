// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"testing"
	"time"

	"google.golang.org/grpc/juicer"
)

// Like juicer_readhold_rules_test.go, these tests exercise the production rule
// with the pinned shared enforcer/queue without importing native CRDB packages.
func TestJuicerWritesAppliedReadPassRelation(t *testing.T) {
	r := crdbWritesAppliedReadPassRules(7 * time.Millisecond)
	if r.Name != juicerWritesAppliedReadPassName || !r.ObserveControlApplied || !r.ReadAheadOfBlockedWrite || !r.AdmitUnbound || r.MaxHold != 7*time.Millisecond {
		t.Fatal("candidate configuration missing")
	}
	for held := juicer.OpUnknown; held < juicer.NumOperationTypes; held++ {
		if r.Holdable(held) != (held == juicer.OpSet) {
			t.Fatalf("holdable(%v)", held)
		}
		for head := juicer.OpUnknown; head < juicer.NumOperationTypes; head++ {
			for _, txn := range []int64{1, 2} {
				for _, ts := range []int64{9, 10, 11} {
					want := txn != 1 && held == juicer.OpSet && head == juicer.OpSet
					if r.Blocks(juicer.OpRef{TxnID: 1, OpType: held, Timestamp: 10}, juicer.OpRef{TxnID: txn, OpType: head, Timestamp: ts}) != want {
						t.Fatal("changed writes-readpass blocking")
					}
				}
			}
		}
	}
	holder := juicer.OpRef{TxnID: 1, OpType: juicer.OpSet, Token: 11}
	for _, tc := range []struct {
		name  string
		event juicer.Event
		want  bool
	}{
		{"successful Set response", juicer.Event{Kind: juicer.RespReturned, TxnID: 1, OpType: juicer.OpSet, Token: 11}, false},
		{"commit arrival", juicer.Event{Kind: juicer.ReqArrived, TxnID: 1, OpType: juicer.OpCommit}, false},
		{"abort arrival", juicer.Event{Kind: juicer.ReqArrived, TxnID: 1, OpType: juicer.OpAbort}, false},
		{"applied commit", juicer.Event{Kind: juicer.ControlApplied, TxnID: 1, OpType: juicer.OpCommit}, true},
		{"applied abort", juicer.Event{Kind: juicer.ControlApplied, TxnID: 1, OpType: juicer.OpAbort}, true},
		{"other txn applied", juicer.Event{Kind: juicer.ControlApplied, TxnID: 2, OpType: juicer.OpCommit}, false},
		{"own failed response", juicer.Event{Kind: juicer.RespReturned, TxnID: 1, OpType: juicer.OpSet, Failed: true, Token: 11}, true},
		{"other same-txn failed response", juicer.Event{Kind: juicer.RespReturned, TxnID: 1, OpType: juicer.OpSet, Failed: true, Token: 12}, false},
		{"tokenless failure", juicer.Event{Kind: juicer.RespReturned, TxnID: 1, OpType: juicer.OpSet, Failed: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if r.Releases(holder, tc.event) != tc.want {
				t.Fatal("incorrect release")
			}
		})
	}
}

func TestJuicerWritesAppliedReadPassOwnToken(t *testing.T) {
	now := time.Unix(100, 0)
	de := juicer.NewDependencyEnforcer(crdbWritesAppliedReadPassRules(time.Second), nil)
	for _, token := range []uint64{11, 12} {
		if de.OnHeadEligible(juicer.OpRef{TxnID: 1, OpType: juicer.OpSet, Token: token}, now) != juicer.EnforcerAllow {
			t.Fatal("same transaction blocked")
		}
	}
	if !de.ObserveAt(juicer.Event{Kind: juicer.RespReturned, TxnID: 1, Failed: true, Token: 12}, now) || de.InflightLen() != 1 {
		t.Fatal("failure did not release just its own token")
	}
	if de.ObserveAt(juicer.Event{Kind: juicer.RespReturned, TxnID: 1, Failed: true, Token: 12}, now) {
		t.Fatal("duplicate failure ended another hold")
	}
	if de.OnHeadEligible(juicer.OpRef{TxnID: 2, OpType: juicer.OpSet, Token: 13}, now) != juicer.EnforcerBlock {
		t.Fatal("earlier same-txn hold was lost")
	}
	if !de.ObserveAt(juicer.Event{Kind: juicer.ControlApplied, TxnID: 1, OpType: juicer.OpAbort}, now) || de.InflightLen() != 0 || de.Stats().Ended[juicer.HoldEndEvent] != 2 {
		t.Fatal("terminal abort did not end remaining hold")
	}
}

func TestJuicerWritesAppliedReadPassQueueLifecycle(t *testing.T) {
	clock := juicer.NewManualClock(time.Unix(100, 0))
	log := juicer.NewLogger("writes-applied", writesAppliedTestLogger{}, "", 0, false, false)
	de := juicer.NewDependencyEnforcer(crdbWritesAppliedReadPassRules(time.Second), log)
	q := juicer.NewPerKeyQueue("k", func(string) time.Duration { return 0 }, de, log, false, juicer.WithClock(clock), juicer.WithManualLoop())
	defer q.Stop()
	enqueue := func(txn, ts int64, op juicer.OperationType) *juicer.QueueMarker {
		m := &juicer.QueueMarker{Key: "k", Timestamp: ts, OpType: op, ReadyChan: make(chan struct{})}
		m.TxnId.Store(txn)
		q.Enqueue(m)
		return m
	}
	ready := func(m *juicer.QueueMarker) bool {
		select {
		case <-m.ReadyChan:
			return true
		default:
			return false
		}
	}
	first := enqueue(1, 10, juicer.OpSet)
	q.Pump()
	second := enqueue(2, 20, juicer.OpSet)
	read := enqueue(3, 30, juicer.OpGet)
	third := enqueue(4, 40, juicer.OpSet)
	q.Pump()
	if !ready(first) || ready(second) || !ready(read) || ready(third) || de.InflightLen() != 1 || de.Stats().Created != 1 {
		t.Fatal("unholdable read did not pass blocked writers")
	}
	for _, event := range []juicer.Event{
		{Kind: juicer.ReqArrived, TxnID: 1, OpType: juicer.OpCommit},
		{Kind: juicer.RespReturned, TxnID: 1, OpType: juicer.OpSet},
	} {
		q.Observe(event)
		q.Pump()
		if ready(second) || de.InflightLen() != 1 {
			t.Fatal("arrival/successful write released control hold")
		}
	}
	applied := juicer.Event{Kind: juicer.ControlApplied, TxnID: 1, OpType: juicer.OpCommit}
	q.Observe(applied)
	q.Pump()
	if !ready(second) || ready(third) || de.InflightLen() != 1 {
		t.Fatal("applied commit lost write exclusion")
	}
	q.Observe(applied)
	if de.Stats().Ended[juicer.HoldEndEvent] != 1 {
		t.Fatal("duplicate applied event double-ended hold")
	}
	// Cancel the queued third marker; it must never be delivered after release.
	if !q.Cancel(third) {
		t.Fatal("queued cancellation failed")
	}
	q.Observe(juicer.Event{Kind: juicer.ControlApplied, TxnID: 2, OpType: juicer.OpAbort})
	q.Pump()
	if de.InflightLen() != 0 {
		t.Fatal("applied abort did not release next writer")
	}
	fourth := enqueue(5, 50, juicer.OpSet)
	q.Pump()
	fifth := enqueue(6, 60, juicer.OpSet)
	q.Pump()
	if !ready(fourth) || ready(fifth) {
		t.Fatal("TTL fixture lost exclusion")
	}
	if !clock.FireNext(clock.Now().Add(time.Second)) {
		t.Fatal("blocked writer did not arm the existing TTL timer")
	}
	q.Pump()
	if !ready(fifth) || de.Stats().Ended[juicer.HoldEndExpiry] != 1 || de.InflightLen() != 1 || de.Stats().Created != 4 {
		t.Fatal("missing terminal event lost TTL backstop or canceled writer was admitted")
	}
}

type writesAppliedTestLogger struct{}

func (writesAppliedTestLogger) Debug(string, ...interface{})   {}
func (writesAppliedTestLogger) Info(string, ...interface{})    {}
func (writesAppliedTestLogger) Warning(string, ...interface{}) {}
func (writesAppliedTestLogger) Error(string, ...interface{})   {}
