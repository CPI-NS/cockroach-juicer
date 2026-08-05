// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// size reports the number of pinned transactions.
func (t *juicerTxnKeyTable) size() int {
	n := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.Lock()
		n += len(s.m)
		s.Unlock()
	}
	return n
}

// juicerTxnKeyTestID builds a UUID whose shard is fixed by its first byte, so
// eviction tests can put several transactions on one shard deterministically
// (sweeping is per shard and driven by that shard's own insert path).
func juicerTxnKeyTestID(shard, n byte) uuid.UUID {
	var id uuid.UUID
	id[0] = shard
	id[15] = n
	return id
}

// The pin is the whole point: a transaction whose ReadTimestamp is bumped
// mid-flight (refresh, push, uncertainty restart) must keep sorting at the
// position it first took, or its own operations reach successive queues under
// increasing keys and cannot be ordered against anything.
func TestJuicerTxnKeyIsPinnedForTheTxnLifetime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	table := newJuicerTxnKeyTable(juicerTxnKeyTTL, juicerTxnKeySweepInterval)
	id := uuid.MakeV4()
	now := time.Unix(0, 0)

	first := table.keyFor(id, hlc.Timestamp{WallTime: 100, Logical: 3}, now)
	require.Equal(t, juicerTxnSortKey{wallTime: 100, logical: 3}, first)

	// The transaction's timestamp moved forward; the pinned key must not.
	bumped := table.keyFor(id, hlc.Timestamp{WallTime: 900, Logical: 0}, now.Add(time.Second))
	require.Equal(t, first, bumped)

	// A different transaction gets its own pin.
	other := table.keyFor(uuid.MakeV4(), hlc.Timestamp{WallTime: 50, Logical: 1}, now)
	require.Equal(t, juicerTxnSortKey{wallTime: 50, logical: 1}, other)
	require.Equal(t, 2, table.size())
}

// forget is the primary eviction path: it runs when this node observes the
// transaction's EndTxn.
func TestJuicerTxnKeyForgetReleasesThePin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	table := newJuicerTxnKeyTable(juicerTxnKeyTTL, juicerTxnKeySweepInterval)
	id := uuid.MakeV4()
	now := time.Unix(0, 0)

	table.keyFor(id, hlc.Timestamp{WallTime: 100}, now)
	require.Equal(t, 1, table.size())

	table.forget(id)
	require.Equal(t, 0, table.size())

	// Idempotent, and safe for a transaction that was never pinned.
	table.forget(id)
	table.forget(uuid.MakeV4())
	require.Equal(t, 0, table.size())

	// After forgetting, the next batch re-pins to the current timestamp.
	repinned := table.keyFor(id, hlc.Timestamp{WallTime: 900, Logical: 2}, now)
	require.Equal(t, juicerTxnSortKey{wallTime: 900, logical: 2}, repinned)
}

// The TTL is the backstop for transactions whose EndTxn this node never sees:
// a 1PC batch (no separate EndTxn message exists), a coordinator that moved,
// a client that disconnected. Without it the table grows for the lifetime of
// the process.
func TestJuicerTxnKeyTTLEvictsAbandonedTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const ttl = time.Minute
	const sweep = 10 * time.Second
	const shard = 7
	table := newJuicerTxnKeyTable(ttl, sweep)
	base := time.Unix(0, 0)

	abandoned := make([]uuid.UUID, 8)
	for i := range abandoned {
		abandoned[i] = juicerTxnKeyTestID(shard, byte(i))
		table.keyFor(abandoned[i], hlc.Timestamp{WallTime: 1}, base)
	}
	require.Equal(t, len(abandoned), table.size())

	// An arrival before the sweep interval elapses must not sweep: the point
	// of the interval is that the scan is not on every insert.
	table.keyFor(juicerTxnKeyTestID(shard, 100), hlc.Timestamp{WallTime: 1}, base.Add(sweep/2))
	require.Equal(t, len(abandoned)+1, table.size())

	// Past the TTL, one arrival on the shard evicts every stale entry on it.
	// Only the arrival that triggered the sweep survives.
	table.keyFor(juicerTxnKeyTestID(shard, 200), hlc.Timestamp{WallTime: 2}, base.Add(ttl+sweep))
	require.Equal(t, 1, table.size())

	// A transaction that keeps sending survives arbitrarily many sweeps:
	// lastSeen is refreshed on every lookup.
	long := juicerTxnKeyTestID(shard, 201)
	want := table.keyFor(long, hlc.Timestamp{WallTime: 3}, base.Add(ttl+sweep))
	require.Equal(t, juicerTxnSortKey{wallTime: 3}, want)
	for i := 1; i <= 5; i++ {
		at := base.Add(ttl + sweep + time.Duration(i)*(ttl-time.Second))
		// Fresh transactions keep arriving and keep triggering sweeps.
		table.keyFor(juicerTxnKeyTestID(shard, byte(210+i)), hlc.Timestamp{WallTime: 9}, at)
		require.Equal(t, want, table.keyFor(long, hlc.Timestamp{WallTime: 99}, at))
	}
}
