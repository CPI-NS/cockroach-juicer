package juicertrace

import (
	"testing"
	"time"
)

func testState(cap int) *state {
	return testStateBase(cap)
}
func TestBothArrivalOrdersAndSignedGap(t *testing.T) {
	b := time.Unix(0, 100)
	s := testState(10)
	s.holdReleased(1, "k", "Set", b, b.Add(20*time.Nanosecond), "event")
	s.lockUpdated(1, "k", "COMMITTED", b.Add(30*time.Nanosecond))
	s.lockUpdated(2, "k", "ABORTED", b.Add(40*time.Nanosecond))
	s.holdReleased(2, "k", "GetForPut", b, b.Add(50*time.Nanosecond), "expiry")
	if s.snap.Matched != 2 || s.snap.GapNS.Negative != 1 || s.snap.GapNS.NonNegative != 1 {
		t.Fatalf("%+v", s.snap)
	}
}
func TestDuplicateBoundAndNoLockCoverage(t *testing.T) {
	b := time.Unix(0, 1)
	s := testState(1)
	s.holdReleased(1, "a", "Get", b, b, "event")
	if s.snap.ExpectedNoLock != 1 {
		t.Fatal(s.snap)
	}
	s.holdReleased(1, "a", "Set", b, b, "event")
	s.holdReleased(1, "a", "Set", b, b, "event")
	if s.snap.DuplicateHolds != 1 {
		t.Fatal(s.snap)
	}
	s.holdReleased(2, "b", "Set", b, b, "event")
	if s.snap.EvictedPendingHolds != 1 || len(s.entries) > 1 {
		t.Fatal(s.snap)
	}
}
func TestNativeRetainedAndOldNativeDoesNotMatchReentry(t *testing.T) {
	b := time.Unix(0, 1)
	s := testState(10)
	s.lockUpdated(1, "a", "COMMITTED", b.Add(2*time.Nanosecond))
	s.holdReleased(1, "a", "Set", b, b.Add(time.Nanosecond), "event")
	s.holdReleased(1, "a", "Set", b.Add(3*time.Nanosecond), b.Add(4*time.Nanosecond), "event")
	if s.snap.Matched != 1 || s.pending != 1 {
		t.Fatalf("matched=%d pending=%d", s.snap.Matched, s.pending)
	}
	s.lockUpdated(1, "a", "ABORTED", b.Add(5*time.Nanosecond))
	if s.snap.Matched != 2 || s.pending != 0 {
		t.Fatal(s.snap)
	}
}
func TestGroupedGaps(t *testing.T) {
	b := time.Unix(0, 1)
	s := testState(10)
	s.holdReleased(1, "a", "Set", b, b, "expiry")
	s.lockUpdated(1, "a", "COMMITTED", b)
	if s.snap.GapByClass["Set|expiry|COMMITTED"].Count != 1 {
		t.Fatal(s.snap.GapByClass)
	}
}

func TestPerPairHoldHistoryIsBounded(t *testing.T) {
	b := time.Unix(0, 1)
	s := testState(10)
	s.lockUpdated(1, "a", "COMMITTED", b.Add(time.Second))
	for i := 0; i < maxHoldsPerPair+10; i++ {
		created := b.Add(time.Duration(i) * time.Nanosecond)
		s.holdReleased(1, "a", "Set", created, created, "event")
	}
	if len(s.entries[pair{1, "a"}].seen) != maxHoldsPerPair || s.snap.PairHoldLimitDropped != 10 {
		t.Fatal(s.snap)
	}
}

func TestSampledQueueDropsWithoutBlocking(t *testing.T) {
	s := testState(10)
	s.q = make(chan event, 1)
	b := time.Unix(0, 1)
	e := event{kind: 1, txn: 16, key: "k", op: "Set", created: b, released: b, reason: "event"}
	s.enqueue(e, 1) // sampled out
	if len(s.q) != 0 {
		t.Fatal("non-sampled event entered queue")
	}
	s.enqueue(e, 16)
	s.enqueue(e, 16) // full; must return without waiting for a consumer
	if s.hot.callbacks.Load() != 2 || s.hot.dropped.Load() != 1 {
		t.Fatal("drop accounting")
	}
	s.process(<-s.q)
	if s.snapshot().UnmatchedHolds != 1 {
		t.Fatal("queued event lost")
	}
}
