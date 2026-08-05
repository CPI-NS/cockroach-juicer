// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// juicerTxnKeyShards is the fan-out of juicerTxnKeyTable. Every KV batch of a
// transactional workload consults the table on the RPC hot path, so a single
// mutex would serialize all of them; 64 shards keyed off the first UUID byte
// (which is random for a V4 UUID) keeps the critical section uncontended at
// the concurrencies these experiments run at.
const juicerTxnKeyShards = 64

// juicerTxnKeyTTL bounds how long an entry survives without being touched.
// The explicit removal on EndTxn covers transactions that finish on this node;
// the TTL covers everything else — a transaction whose coordinator moved, one
// that committed in a single 1PC batch (no separate EndTxn message is ever
// observed), one abandoned by a disconnected client. Without it the table is
// an unbounded leak for the lifetime of the process.
//
// The value only has to exceed the lifetime of a transaction: an entry that
// expires while its transaction is still running is not a correctness problem,
// it just re-pins the key to the read timestamp current at that moment. One
// minute is roughly 250x the p50 transaction latency measured on this
// workload.
const juicerTxnKeyTTL = time.Minute

// juicerTxnKeySweepInterval is how often a shard scans itself for expired
// entries. Sweeping is lazy — driven from the insert path rather than a
// background goroutine — so the table needs no lifecycle management and
// cannot outlive its own traffic.
const juicerTxnKeySweepInterval = 10 * time.Second

// juicerTxnSortKey is the sorting key Juicer orders a transaction's operations
// by: the (WallTime, Logical) pair of an hlc.Timestamp, which is CockroachDB's
// commit order and is a total order, unlike WallTime alone.
type juicerTxnSortKey struct {
	wallTime int64
	logical  int32
}

type juicerTxnKeyEntry struct {
	key juicerTxnSortKey
	// lastSeen is refreshed on every lookup, so an entry expires only after
	// the transaction has been quiet for the whole TTL.
	lastSeen time.Time
}

type juicerTxnKeyShard struct {
	syncutil.Mutex
	m         map[uuid.UUID]juicerTxnKeyEntry
	lastSweep time.Time
}

// juicerTxnKeyTable pins each transaction's Juicer sorting key to the read
// timestamp first observed for it, and holds that pin for the transaction's
// lifetime.
//
// Why the pin is needed: CockroachDB advances a transaction's ReadTimestamp
// mid-flight — a refresh, a push, or an uncertainty restart all move it
// forward. Deriving the sorting key from the timestamp on each batch therefore
// makes a transaction's own operations arrive at successive per-key queues
// under different, increasing keys. Sorting cannot express "this transaction
// goes before that one" when the thing being sorted changes underneath it, and
// each bump also pushes the operation past the queue's last released key,
// where the bypass rule lets it through unsorted.
//
// Lifecycle: an entry is created by the first batch of a transaction that
// reaches keyFor, refreshed by every later one, and removed either by forget
// (called when this node observes the transaction's EndTxn) or by the TTL
// sweep. The table exists only while Juicer is enabled — nothing constructs it
// otherwise — so a baseline node pays nothing for it.
type juicerTxnKeyTable struct {
	shards        [juicerTxnKeyShards]juicerTxnKeyShard
	ttl           time.Duration
	sweepInterval time.Duration
}

func newJuicerTxnKeyTable(ttl, sweepInterval time.Duration) *juicerTxnKeyTable {
	t := &juicerTxnKeyTable{ttl: ttl, sweepInterval: sweepInterval}
	for i := range t.shards {
		t.shards[i].m = make(map[uuid.UUID]juicerTxnKeyEntry)
	}
	return t
}

func (t *juicerTxnKeyTable) shard(id uuid.UUID) *juicerTxnKeyShard {
	return &t.shards[int(id[0])%juicerTxnKeyShards]
}

// keyFor returns the pinned sorting key of transaction id, pinning it to ts on
// first sight. now is injected so tests can drive expiry deterministically.
func (t *juicerTxnKeyTable) keyFor(
	id uuid.UUID, ts hlc.Timestamp, now time.Time,
) juicerTxnSortKey {
	s := t.shard(id)
	s.Lock()
	defer s.Unlock()
	if e, ok := s.m[id]; ok {
		e.lastSeen = now
		s.m[id] = e
		return e.key
	}
	t.sweepLocked(s, now)
	key := juicerTxnSortKey{wallTime: ts.WallTime, logical: ts.Logical}
	s.m[id] = juicerTxnKeyEntry{key: key, lastSeen: now}
	return key
}

// forget drops a transaction's pin. It is idempotent and safe for a
// transaction that was never pinned.
func (t *juicerTxnKeyTable) forget(id uuid.UUID) {
	s := t.shard(id)
	s.Lock()
	defer s.Unlock()
	delete(s.m, id)
}

// sweepLocked removes expired entries, at most once per sweepInterval per
// shard. It runs only on the insert path: a shard that stops receiving new
// transactions has nothing left to leak beyond what it already holds, and that
// residue is bounded by the transactions in flight when the traffic stopped.
func (t *juicerTxnKeyTable) sweepLocked(s *juicerTxnKeyShard, now time.Time) {
	if now.Sub(s.lastSweep) < t.sweepInterval {
		return
	}
	s.lastSweep = now
	for id, e := range s.m {
		if now.Sub(e.lastSeen) >= t.ttl {
			delete(s.m, id)
		}
	}
}
