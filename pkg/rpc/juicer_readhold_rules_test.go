// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/juicer"
)

// Exercise the production helper and pinned shared enforcers without importing
// the rest of CRDB. The native RPC target also runs these tests and SPI wiring.
func TestJuicerReadHoldOrderedPairs(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		heldWall, heldLogical int64
		headWall, headLogical int64
		earlier               bool
	}{
		{"wall earlier outweighs logical", 10, 9, 20, 0, true},
		{"wall later outweighs logical", 20, 0, 10, 9, false},
		{"logical earlier", 10, 1, 10, 2, true},
		{"logical later", 10, 2, 10, 1, false},
		{"exact HLC tie", 10, 2, 10, 2, false},
		{"zero HLC tie", 0, 0, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, ids := range [][2]int64{{1, 9}, {9, 1}, {1, 1}} {
				for heldType := juicer.OpUnknown; heldType < juicer.NumOperationTypes; heldType++ {
					for headType := juicer.OpUnknown; headType < juicer.NumOperationTypes; headType++ {
						held := juicer.OpRef{TxnID: ids[0], OpType: heldType, Timestamp: tc.heldWall, Logical: tc.heldLogical}
						head := juicer.OpRef{TxnID: ids[1], OpType: headType, Timestamp: tc.headWall, Logical: tc.headLogical}
						oldWant := ids[0] != ids[1] && headType == juicer.OpSet &&
							(heldType == juicer.OpGet || heldType == juicer.OpSet)
						for _, ordered := range []bool{false, true} {
							want := oldWant && (!ordered || tc.earlier)
							if got := crdbReadHoldRules(time.Millisecond, ordered).Blocks(held, head); got != want {
								t.Fatalf("ordered=%v Blocks(%+v, %+v)=%v, want %v", ordered, held, head, got, want)
							}
						}
					}
				}
			}
		})
	}
}

// This compiles the actual CRDB relation with the shared queue; named SPI
// selection and startup validation are separately covered by the native test.
func TestJuicerReadPassRetainsCRDBReadHolds(t *testing.T) {
	rules := crdbReadHoldRules(25*time.Millisecond, true)
	rules.ReadAheadOfBlockedWrite = true
	clock := juicer.NewManualClock(time.Unix(100, 0))
	logger := juicer.NewLogger("readpass", readHoldTestLogger{}, "", 0, false, false)
	de := juicer.NewDependencyEnforcer(rules, logger)
	q := juicer.NewPerKeyQueue("k", func(string) time.Duration { return 0 }, de, logger, false,
		juicer.WithClock(clock), juicer.WithManualLoop())
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
	enqueue(1, 10, juicer.OpGet)
	q.Pump()
	write := enqueue(2, 20, juicer.OpSet)
	read := enqueue(3, 30, juicer.OpGet)
	q.Pump()
	if ready(write) || !ready(read) || de.InflightLen() != 2 {
		t.Fatal("CRDB readpass did not retain both read holds behind its blocked write")
	}
	q.Observe(juicer.Event{Kind: juicer.RespReturned, TxnID: 1, OpType: juicer.OpGet})
	q.Pump()
	if !ready(write) || de.InflightLen() != 2 || de.Stats().Ended[juicer.HoldEndExpiry] != 0 {
		t.Fatal("newer read hold blocked older write or progress used the TTL")
	}
	q.Observe(juicer.Event{Kind: juicer.RespReturned, TxnID: 3, OpType: juicer.OpGet, Failed: true})
	q.Observe(juicer.Event{Kind: juicer.RespReturned, TxnID: 2, OpType: juicer.OpSet})
	if de.InflightLen() != 0 {
		t.Fatal("original CRDB read/write response rules left holds behind")
	}
}

func TestJuicerReadHoldOrderedHoldabilityAndReleases(t *testing.T) {
	old, ordered := crdbReadHoldRules(25*time.Millisecond, false), crdbReadHoldRules(25*time.Millisecond, true)
	if !ordered.Enabled() || ordered.MaxHold != old.MaxHold || ordered.AdmitUnbound != old.AdmitUnbound ||
		ordered.ObserveControlApplied != old.ObserveControlApplied {
		t.Fatal("candidate changed hold configuration")
	}
	for op := juicer.OpUnknown; op < juicer.NumOperationTypes; op++ {
		want := op == juicer.OpGet || op == juicer.OpSet
		if ordered.Holdable(op) != want || ordered.Holdable(op) != old.Holdable(op) {
			t.Fatalf("holdable(%v): old=%v ordered=%v, want %v", op, old.Holdable(op), ordered.Holdable(op), want)
		}
	}
	for _, heldType := range []juicer.OperationType{juicer.OpGet, juicer.OpSet} {
		held := juicer.OpRef{TxnID: 1, OpType: heldType, Timestamp: 10, Logical: 2, Token: 7}
		for _, txn := range []int64{1, 9} {
			for _, kind := range []juicer.EventKind{juicer.ReqArrived, juicer.RespReturned, juicer.ControlApplied} {
				for op := juicer.OpUnknown; op < juicer.NumOperationTypes; op++ {
					for _, failed := range []bool{false, true} {
						for _, token := range []uint64{0, 7, 8} {
							ev := juicer.Event{Kind: kind, OpType: op, TxnID: txn, Failed: failed, Token: token}
							want := txn == held.TxnID && ((kind == juicer.ReqArrived &&
								(op == juicer.OpCommit || op == juicer.OpAbort)) ||
								(kind == juicer.RespReturned && (failed || op == heldType)))
							if old.Releases(held, ev) != want || ordered.Releases(held, ev) != want {
								t.Fatalf("held=%+v ev=%+v: old=%v ordered=%v, want %v",
									held, ev, old.Releases(held, ev), ordered.Releases(held, ev), want)
							}
						}
					}
				}
			}
		}
	}
}

func TestJuicerReadHoldOrderedPartialAdmissions(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered=%v", ordered), func(t *testing.T) {
			rules := crdbReadHoldRules(25*time.Millisecond, ordered)
			a, b := juicer.NewDependencyEnforcer(rules, nil), juicer.NewDependencyEnforcer(rules, nil)
			now := time.Unix(100, 0)
			// Numeric IDs run opposite to the HLC order. Each request first
			// acquires one key, then attempts the other, without any response.
			early := juicer.OpRef{TxnID: 9, OpType: juicer.OpSet, Timestamp: 10, Logical: 1}
			later := juicer.OpRef{TxnID: 1, OpType: juicer.OpSet, Timestamp: 10, Logical: 2}
			if a.OnHeadEligible(early, now) != juicer.EnforcerAllow || b.OnHeadEligible(later, now) != juicer.EnforcerAllow {
				t.Fatal("first partial admissions failed")
			}
			if a.OnHeadEligible(later, now) != juicer.EnforcerBlock {
				t.Fatal("newer request must still wait behind the earlier hold")
			}
			want := juicer.EnforcerBlock
			if ordered {
				want = juicer.EnforcerAllow
			}
			if got := b.OnHeadEligible(early, now); got != want {
				t.Fatalf("earlier request on second key = %v, want %v", got, want)
			}
			if !ordered {
				return // Both requests still wait; neither response can run.
			}
			for _, enforcer := range []*juicer.DependencyEnforcer{a, b} {
				if !enforcer.ObserveAt(juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: early.TxnID}, now) {
					t.Fatal("earlier response did not release its hold")
				}
				if enforcer.Stats().Ended[juicer.HoldEndExpiry] != 0 {
					t.Fatal("progress relied on TTL expiry")
				}
			}
			if a.InflightLen() != 0 || b.InflightLen() != 1 || a.OnHeadEligible(later, now) != juicer.EnforcerAllow {
				t.Fatal("earlier response did not let the newer request complete admission")
			}
		})
	}
}

func TestJuicerReadHoldOrderedTTLAndCancellation(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, op := range []juicer.OperationType{juicer.OpGet, juicer.OpSet} {
			t.Run(fmt.Sprintf("ordered=%v/%v", ordered, op), func(t *testing.T) {
				const ttl = 25 * time.Millisecond
				de := juicer.NewDependencyEnforcer(crdbReadHoldRules(ttl, ordered), nil)
				now := time.Unix(100, 0)
				for _, token := range []uint64{1, 2} {
					if de.OnHeadEligible(juicer.OpRef{TxnID: 1, OpType: op, Timestamp: 10, Token: token}, now) != juicer.EnforcerAllow {
						t.Fatal("same-transaction sibling was blocked")
					}
				}
				if de.MaxHold() != ttl || de.NextExpiry(now) != ttl {
					t.Fatal("hold deadline changed")
				}
				if de.CancelHold(0, now) || !de.CancelHold(1, now) || de.CancelHold(1, now) {
					t.Fatal("cancellation must affect exactly one nonzero token, once")
				}
				head := juicer.OpRef{TxnID: 2, OpType: juicer.OpSet, Timestamp: 20, Token: 3}
				if de.InflightLen() != 1 || de.OnHeadEligible(head, now.Add(ttl-time.Nanosecond)) != juicer.EnforcerBlock {
					t.Fatal("cancel removed the sibling or expiry occurred early")
				}
				if de.OnHeadEligible(head, now.Add(ttl)) != juicer.EnforcerAllow {
					t.Fatal("TTL boundary did not release the remaining hold")
				}
				st := de.Stats()
				if st.Ended[juicer.HoldEndCancel] != 1 || st.Ended[juicer.HoldEndExpiry] != 1 || st.Live != 1 {
					t.Fatalf("unexpected hold accounting: %+v", st)
				}
				de.ReleaseAll(now.Add(ttl))
				if de.Stats().Ended[juicer.HoldEndEvicted] != 1 || de.NextExpiry(now.Add(ttl)) != 0 {
					t.Fatal("shutdown did not evict the last hold")
				}
				// Unbound heads still respect a live hold, then pass untracked.
				unbound := juicer.OpRef{OpType: juicer.OpSet, Timestamp: 30}
				de.OnHeadEligible(head, now.Add(ttl))
				if de.OnHeadEligible(unbound, now.Add(ttl)) != juicer.EnforcerBlock || !de.CancelHold(3, now.Add(ttl)) ||
					de.OnHeadEligible(unbound, now.Add(ttl)) != juicer.EnforcerAllow || de.InflightLen() != 0 {
					t.Fatal("unbound admission behavior changed")
				}
			})
		}
	}
}

type readHoldTestLogger struct{}

func (readHoldTestLogger) Debug(string, ...interface{})   {}
func (readHoldTestLogger) Info(string, ...interface{})    {}
func (readHoldTestLogger) Warning(string, ...interface{}) {}
func (readHoldTestLogger) Error(string, ...interface{})   {}

// W=Set(a,b)@10, V=Set(a)@20, R=Get(a,b)@30. R.b is admitted
// before W arrives; R.a remains behind V.a in the heap. With readhold this
// makes W wait for R, R wait at the head of line behind V, and V wait for W.
// This is a queue-level reproduction, not just a pairwise Blocks assertion.
func TestJuicerReadHoldOrderedHeadOfLine(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered=%v", ordered), func(t *testing.T) {
			const ttl = 25 * time.Millisecond
			start := time.Unix(100, 0)
			clock := juicer.NewManualClock(start)
			logger := juicer.NewLogger("readhold-test", readHoldTestLogger{}, "", 0, false, false)
			rules := crdbReadHoldRules(ttl, ordered)
			ea, eb := juicer.NewDependencyEnforcer(rules, logger), juicer.NewDependencyEnforcer(rules, logger)
			newQueue := func(key string, de *juicer.DependencyEnforcer) *juicer.PerKeyQueue {
				q := juicer.NewPerKeyQueue(key, func(string) time.Duration { return 0 }, de, logger, false,
					juicer.WithClock(clock), juicer.WithManualLoop(), juicer.WithLatePolicy(juicer.LateQueue))
				t.Cleanup(q.Stop)
				return q
			}
			a, b := newQueue("a", ea), newQueue("b", eb)
			enqueue := func(q *juicer.PerKeyQueue, key string, txn, ts int64, op juicer.OperationType) *juicer.QueueMarker {
				m := &juicer.QueueMarker{Key: key, OpType: op, Timestamp: ts, ReadyChan: make(chan struct{})}
				m.TxnId.Store(txn)
				if !q.Enqueue(m) || m.BypassQueue {
					t.Fatal("fixture must use real queued admission, including late W.b")
				}
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
			ra := enqueue(a, "a", 3, 30, juicer.OpGet)
			rb := enqueue(b, "b", 3, 30, juicer.OpGet)
			b.Pump()
			if ready(ra) || !ready(rb) || eb.InflightLen() != 1 {
				t.Fatal("R must hold only b, with a still pending")
			}
			wa := enqueue(a, "a", 1, 10, juicer.OpSet)
			wb := enqueue(b, "b", 1, 10, juicer.OpSet)
			va := enqueue(a, "a", 2, 20, juicer.OpSet)
			a.Pump()
			if !ready(wa) || ready(va) || ready(ra) {
				t.Fatal("V must wait behind W.a, with R.a behind V")
			}
			b.Pump()
			if ready(wb) != ordered || ready(va) || ready(ra) {
				t.Fatal("only the ordered candidate may complete W's admission")
			}
			if !ordered {
				// No whole request is admitted, so no handler can respond.
				// Re-pumping cannot move the cycle, even just before the TTL.
				clock.Set(start.Add(ttl - time.Nanosecond))
				a.Pump()
				b.Pump()
				if ready(wb) || ready(va) || ready(ra) {
					t.Fatal("old readhold unexpectedly progressed before its TTL")
				}
				for clock.FireNext(start.Add(ttl)) {
					a.Pump()
					b.Pump()
				}
				clock.Set(start.Add(ttl))
				a.Pump()
				b.Pump()
				if !ready(wb) || !ready(va) || !ready(ra) ||
					ea.Stats().Ended[juicer.HoldEndExpiry] != 1 || eb.Stats().Ended[juicer.HoldEndExpiry] != 1 {
					t.Fatal("old readhold must progress through expiry of W.a and R.b")
				}
				return
			}
			// W now has both markers, so model its returned write response
			// on both keys. V can then run, letting the read head R.a pass.
			event := juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 1}
			a.Observe(event)
			b.Observe(event)
			a.Pump()
			b.Pump()
			if !ready(va) || !ready(ra) || !ready(rb) || ea.InflightLen() != 2 || eb.InflightLen() != 1 {
				t.Fatal("W response did not release its holds and admit V and R")
			}
			if !clock.Now().Equal(start) || ea.Stats().Ended[juicer.HoldEndExpiry] != 0 || eb.Stats().Ended[juicer.HoldEndExpiry] != 0 {
				t.Fatal("ordered progress must use responses without advancing to the TTL")
			}
			a.Observe(juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpSet, TxnID: 2})
			for _, q := range []*juicer.PerKeyQueue{a, b} {
				q.Observe(juicer.Event{Kind: juicer.RespReturned, OpType: juicer.OpGet, TxnID: 3})
			}
			if ea.InflightLen() != 0 || eb.InflightLen() != 0 {
				t.Fatal("V and R responses left holds behind")
			}
		})
	}
}
