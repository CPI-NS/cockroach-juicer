package juicertrace

import (
	"container/list"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const samplingDenominator = 16
const maxHoldsPerPair = 64

type GapStats struct {
	Count       uint64 `json:"count"`
	Sum         int64  `json:"sum"`
	Min         int64  `json:"min"`
	Max         int64  `json:"max"`
	Negative    uint64 `json:"negative"`
	NonNegative uint64 `json:"non_negative"`
}
type SnapshotData struct {
	Enabled              bool                `json:"enabled"`
	Capacity             int                 `json:"capacity"`
	SamplingDenominator  int                 `json:"sampling_denominator"`
	Callbacks            uint64              `json:"callbacks"`
	CallbackNS           uint64              `json:"callback_ns"`
	CallbackMaxNS        uint64              `json:"callback_max_ns"`
	EnqueueDropped       uint64              `json:"enqueue_dropped"`
	SnapshotTimeouts     uint64              `json:"snapshot_timeouts"`
	CollectorBacklog     int                 `json:"collector_backlog"`
	SnapshotRequestedNS  int64               `json:"snapshot_requested_ns"`
	SnapshotCollectedNS  int64               `json:"snapshot_collected_ns"`
	SnapshotLatenessNS   int64               `json:"snapshot_lateness_ns"`
	HoldReleases         uint64              `json:"hold_releases"`
	LockUpdates          uint64              `json:"lock_updates"`
	Matched              uint64              `json:"matched"`
	DuplicateHolds       uint64              `json:"duplicate_holds"`
	DuplicateNative      uint64              `json:"duplicate_native"`
	EvictedEntries       uint64              `json:"evicted_entries"`
	EvictedPendingHolds  uint64              `json:"evicted_pending_holds"`
	PairHoldLimitDropped uint64              `json:"pair_hold_limit_dropped"`
	UnmatchedHolds       int                 `json:"unmatched_holds"`
	Entries              int                 `json:"entries"`
	PointUpdates         uint64              `json:"point_updates"`
	SpanUpdates          uint64              `json:"span_updates"`
	ExpectedNoLock       uint64              `json:"expected_no_lock"`
	GapNS                GapStats            `json:"release_to_native_gap_ns"`
	GapByClass           map[string]GapStats `json:"gap_by_op_reason_status"`
}
type pair struct {
	txn int64
	key string
}
type hold struct {
	created, released time.Time
	op, reason, id    string
}
type native struct {
	at     time.Time
	status string
}
type entry struct {
	p       pair
	pending []hold
	seen    map[string]struct{}
	natives []native
	elem    *list.Element
}
type event struct {
	kind                    uint8
	txn                     int64
	key, op, reason, status string
	created, released, at   time.Time
	barrier                 chan SnapshotData
}
type hotStats struct{ callbacks, ns, maxNS, dropped, snapshotTimeouts atomic.Uint64 }
type state struct {
	enabled      bool
	cap, pending int
	entries      map[pair]*entry
	lru          *list.List
	snap         SnapshotData
	q            chan event
	hot          hotStats
}

var global = newState()

func newState() *state {
	cap := 100000
	if v, e := strconv.Atoi(os.Getenv("COCKROACH_JUICER_TRACE_CAPACITY")); e == nil && v > 0 {
		cap = v
	}
	enabled := strings.EqualFold(os.Getenv("COCKROACH_JUICER_TRACE"), "true") || os.Getenv("COCKROACH_JUICER_TRACE") == "1"
	s := testStateBase(cap)
	s.enabled = enabled
	s.snap.Enabled = enabled
	if enabled {
		s.q = make(chan event, min(cap, 65536))
		go s.collect()
	}
	return s
}
func testStateBase(cap int) *state {
	s := &state{enabled: true, cap: cap, entries: map[pair]*entry{}, lru: list.New()}
	s.snap = SnapshotData{Enabled: true, Capacity: cap, SamplingDenominator: samplingDenominator, GapByClass: map[string]GapStats{}}
	return s
}
func Enabled() bool          { return global.enabled }
func sampled(txn int64) bool { return (uint64(txn)*0x9e3779b97f4a7c15)%samplingDenominator == 0 }
func HoldReleased(txn int64, key, op string, created, released time.Time, reason string) {
	global.enqueue(event{kind: 1, txn: txn, key: key, op: op, created: created, released: released, reason: reason}, txn)
}
func LockUpdated(txn int64, key, status string, at time.Time) {
	global.enqueue(event{kind: 2, txn: txn, key: key, status: status, at: at}, txn)
}
func SpanLockUpdated() { global.enqueue(event{kind: 3}, 0) }
func (s *state) enqueue(e event, txn int64) {
	if !s.enabled || (e.kind != 3 && !sampled(txn)) {
		return
	}
	begin := time.Now()
	s.hot.callbacks.Add(1)
	select {
	case s.q <- e:
	default:
		s.hot.dropped.Add(1)
	}
	n := uint64(time.Since(begin))
	s.hot.ns.Add(n)
	for old := s.hot.maxNS.Load(); n > old && !s.hot.maxNS.CompareAndSwap(old, n); old = s.hot.maxNS.Load() {
	}
}
func (s *state) collect() {
	for e := range s.q {
		if e.kind == 4 {
			x := s.snapshot()
			x.SnapshotRequestedNS = e.at.UnixNano()
			x.SnapshotCollectedNS = time.Now().UnixNano()
			x.SnapshotLatenessNS = x.SnapshotCollectedNS - x.SnapshotRequestedNS
			e.barrier <- x
			continue
		}
		s.process(e)
	}
}
func Snapshot() SnapshotData {
	if !global.enabled {
		return global.snapshot()
	}
	b := make(chan SnapshotData, 1)
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()
	requested := time.Now()
	select {
	case global.q <- event{kind: 4, barrier: b, at: requested}:
	case <-t.C:
		global.hot.snapshotTimeouts.Add(1)
		return global.timeoutSnapshot(requested)
	}
	select {
	case x := <-b:
		return x
	case <-t.C:
		global.hot.snapshotTimeouts.Add(1)
		return global.timeoutSnapshot(requested)
	}
}
func (s *state) timeoutSnapshot(requested time.Time) SnapshotData {
	return SnapshotData{Enabled: s.enabled, Capacity: s.cap, SamplingDenominator: samplingDenominator,
		Callbacks: s.hot.callbacks.Load(), CallbackNS: s.hot.ns.Load(), CallbackMaxNS: s.hot.maxNS.Load(),
		EnqueueDropped: s.hot.dropped.Load(), SnapshotTimeouts: s.hot.snapshotTimeouts.Load(),
		CollectorBacklog: len(s.q), SnapshotRequestedNS: requested.UnixNano(), SnapshotCollectedNS: time.Now().UnixNano()}
}
func (s *state) snapshot() SnapshotData {
	x := s.snap
	x.UnmatchedHolds = s.pending
	x.Entries = len(s.entries)
	if s.q != nil {
		x.CollectorBacklog = len(s.q)
	}
	x.Callbacks = s.hot.callbacks.Load()
	x.CallbackNS = s.hot.ns.Load()
	x.CallbackMaxNS = s.hot.maxNS.Load()
	x.EnqueueDropped = s.hot.dropped.Load()
	x.SnapshotTimeouts = s.hot.snapshotTimeouts.Load()
	x.GapByClass = make(map[string]GapStats, len(s.snap.GapByClass))
	for k, v := range s.snap.GapByClass {
		x.GapByClass[k] = v
	}
	return x
}
func relevant(op string) bool {
	x := strings.ToLower(op)
	return x == "set" || x == "put" || x == "getforput" || x == "get_for_put"
}
func (s *state) process(v event) {
	switch v.kind {
	case 1:
		s.holdReleased(v.txn, v.key, v.op, v.created, v.released, v.reason)
	case 2:
		s.lockUpdated(v.txn, v.key, v.status, v.at)
	case 3:
		s.snap.SpanUpdates++
	}
}
func (s *state) get(p pair) *entry {
	if e := s.entries[p]; e != nil {
		s.lru.MoveToBack(e.elem)
		return e
	}
	e := &entry{p: p, seen: map[string]struct{}{}}
	e.elem = s.lru.PushBack(e)
	s.entries[p] = e
	s.bound()
	return e
}
func (s *state) holdReleased(txn int64, key, op string, created, released time.Time, reason string) {
	s.snap.HoldReleases++
	if !relevant(op) {
		s.snap.ExpectedNoLock++
		return
	}
	e := s.get(pair{txn, key})
	id := op + "\x00" + strconv.FormatInt(created.UnixNano(), 10) + "\x00" + strconv.FormatInt(released.UnixNano(), 10) + "\x00" + reason
	if _, ok := e.seen[id]; ok {
		s.snap.DuplicateHolds++
		return
	}
	if len(e.seen) >= maxHoldsPerPair {
		s.snap.PairHoldLimitDropped++
		return
	}
	e.seen[id] = struct{}{}
	h := hold{created, released, op, reason, id}
	for _, n := range e.natives {
		if !n.at.Before(created) {
			s.match(h, n)
			return
		}
	}
	if len(e.pending) >= maxHoldsPerPair {
		s.snap.PairHoldLimitDropped++
		return
	}
	e.pending = append(e.pending, h)
	s.pending++
}
func (s *state) lockUpdated(txn int64, key, status string, at time.Time) {
	s.snap.LockUpdates++
	s.snap.PointUpdates++
	e := s.get(pair{txn, key})
	for _, n := range e.natives {
		if n.at.Equal(at) && n.status == status {
			s.snap.DuplicateNative++
			return
		}
	}
	n := native{at, status}
	if len(e.natives) < maxHoldsPerPair {
		e.natives = append(e.natives, n)
	} else {
		s.snap.DuplicateNative++
	}
	kept := e.pending[:0]
	for _, h := range e.pending {
		if at.Before(h.created) {
			kept = append(kept, h)
		} else {
			s.match(h, n)
			s.pending--
		}
	}
	e.pending = kept
}
func (s *state) match(h hold, n native) {
	g := n.at.Sub(h.released).Nanoseconds()
	add := func(x GapStats) GapStats {
		if x.Count == 0 || g < x.Min {
			x.Min = g
		}
		if x.Count == 0 || g > x.Max {
			x.Max = g
		}
		x.Count++
		x.Sum += g
		if g < 0 {
			x.Negative++
		} else {
			x.NonNegative++
		}
		return x
	}
	s.snap.GapNS = add(s.snap.GapNS)
	k := h.op + "|" + h.reason + "|" + n.status
	s.snap.GapByClass[k] = add(s.snap.GapByClass[k])
	s.snap.Matched++
}
func (s *state) bound() {
	for len(s.entries) > s.cap {
		e := s.lru.Front().Value.(*entry)
		delete(s.entries, e.p)
		s.lru.Remove(e.elem)
		s.pending -= len(e.pending)
		s.snap.EvictedEntries++
		s.snap.EvictedPendingHolds += uint64(len(e.pending))
	}
}
