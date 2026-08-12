// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ycsbt

import (
	"math/rand/v2"
	"regexp"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/stretchr/testify/require"
)

// The read/write split is fixed for a run and decides the statement's
// placeholder layout, so it has to be exactly predictable from the flags.
func TestSplitCounts(t *testing.T) {
	tests := []struct {
		name           string
		opsPerTxn      int
		readPct        int
		expectedReads  int
		expectedWrites int
	}{
		{name: "round-8 mix", opsPerTxn: 10, readPct: 90, expectedReads: 9, expectedWrites: 1},
		{name: "package defaults round half up", opsPerTxn: 5, readPct: 50, expectedReads: 3, expectedWrites: 2},
		{name: "even split", opsPerTxn: 10, readPct: 50, expectedReads: 5, expectedWrites: 5},
		{name: "all reads", opsPerTxn: 10, readPct: 100, expectedReads: 10, expectedWrites: 0},
		{name: "all writes", opsPerTxn: 10, readPct: 0, expectedReads: 0, expectedWrites: 10},
		// Rounding must not silently wipe out the side the caller asked for.
		{name: "writes survive a rounding that would erase them", opsPerTxn: 5, readPct: 95, expectedReads: 4, expectedWrites: 1},
		{name: "reads survive a rounding that would erase them", opsPerTxn: 5, readPct: 5, expectedReads: 1, expectedWrites: 4},
		{name: "single op mostly reads still writes", opsPerTxn: 1, readPct: 90, expectedReads: 0, expectedWrites: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reads, writes := splitCounts(tc.opsPerTxn, tc.readPct)
			require.Equal(t, tc.expectedReads, reads, "reads")
			require.Equal(t, tc.expectedWrites, writes, "writes")
			require.Equal(t, tc.opsPerTxn, reads+writes, "split must account for every key")
		})
	}
}

// The statement is the workload. Pinning its text means a change to it is a
// visible change to the experiment rather than a silent one, and it documents
// the placeholder layout the argument list depends on.
func TestBuildStmt(t *testing.T) {
	t.Run("round-8 mix: 9 reads, 1 write", func(t *testing.T) {
		require.Equal(t,
			`WITH r AS (SELECT ycsb_key, field0 FROM usertable WHERE ycsb_key IN `+
				`($1, $2, $3, $4, $5, $6, $7, $8, $9)), `+
				`w AS (UPDATE usertable SET field0 = field0 + 1 WHERE ycsb_key IN ($10) `+
				`RETURNING ycsb_key) `+
				`SELECT (SELECT count(*) FROM r), (SELECT count(*) FROM w)`,
			buildStmt(9, 1))
	})

	t.Run("read only", func(t *testing.T) {
		require.Equal(t,
			`WITH r AS (SELECT ycsb_key, field0 FROM usertable WHERE ycsb_key IN ($1, $2)) `+
				`SELECT (SELECT count(*) FROM r), 0`,
			buildStmt(2, 0))
	})

	t.Run("write only", func(t *testing.T) {
		require.Equal(t,
			`WITH w AS (UPDATE usertable SET field0 = field0 + 1 WHERE ycsb_key IN ($1, $2) `+
				`RETURNING ycsb_key) `+
				`SELECT 0, (SELECT count(*) FROM w)`,
			buildStmt(0, 2))
	})

	// Whatever the split, the placeholders must be exactly $1..$n in order,
	// with no gap and no repeat: run passes the key slice positionally, so a
	// gap would bind a key to the wrong side of the transaction.
	placeholderRE := regexp.MustCompile(`\$(\d+)`)
	for _, tc := range []struct{ reads, writes int }{{9, 1}, {3, 2}, {1, 9}, {5, 5}, {12, 3}} {
		stmt := buildStmt(tc.reads, tc.writes)
		var got []int
		for _, m := range placeholderRE.FindAllStringSubmatch(stmt, -1) {
			n, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			got = append(got, n)
		}
		want := make([]int, 0, tc.reads+tc.writes)
		for i := 1; i <= tc.reads+tc.writes; i++ {
			want = append(want, i)
		}
		require.Equalf(t, want, got, "%d reads / %d writes: placeholder sequence", tc.reads, tc.writes)

		// A mutating CTE whose output nothing consumes may not be evaluated.
		if tc.writes > 0 {
			require.Contains(t, stmt, "RETURNING")
			require.Contains(t, stmt, "FROM w")
		}
	}
}

// Same discipline as TestBuildStmt: the blind statement is the workload, so
// its exact text is pinned, and the (key, value) pair placeholders must be
// exactly $1..$2n in order because runBlind fills them positionally.
func TestBuildBlindStmt(t *testing.T) {
	require.Equal(t,
		`UPSERT INTO usertable (ycsb_key, field0) VALUES ($1, $2), ($3, $4)`,
		buildBlindStmt(2))

	placeholderRE := regexp.MustCompile(`\$(\d+)`)
	for _, writes := range []int{1, 5, 10} {
		stmt := buildBlindStmt(writes)
		var got []int
		for _, m := range placeholderRE.FindAllStringSubmatch(stmt, -1) {
			n, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			got = append(got, n)
		}
		want := make([]int, 0, 2*writes)
		for i := 1; i <= 2*writes; i++ {
			want = append(want, i)
		}
		require.Equalf(t, want, got, "%d writes: placeholder sequence", writes)

		// The blind statement must never read: no RETURNING, no CTE, no scan
		// of existing rows. Its text starting with UPSERT is the cheap proxy
		// this test can check for that plan property.
		require.NotContains(t, stmt, "RETURNING")
		require.NotContains(t, stmt, "SELECT")
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name        string
		flags       []string
		expectedErr string
	}{
		{name: "defaults"},
		{name: "round-8 parameters", flags: []string{
			`--keys=1000`, `--ops-per-txn=10`, `--read-pct=90`, `--zipf-theta=0.99`}},
		{name: "more keys per txn than keys exist", flags: []string{`--keys=4`, `--ops-per-txn=5`},
			expectedErr: "must not exceed"},
		{name: "read percentage above 100", flags: []string{`--read-pct=101`},
			expectedErr: "between 0 and 100"},
		{name: "theta of exactly one", flags: []string{`--zipf-theta=1`},
			expectedErr: "not exactly 1"},
		{name: "negative theta", flags: []string{`--zipf-theta=-0.5`},
			expectedErr: "not exactly 1"},
		{name: "no keys", flags: []string{`--keys=0`, `--ops-per-txn=1`},
			expectedErr: "--keys (0) must be at least 1"},
		{name: "more splits than keys", flags: []string{`--keys=10`, `--splits=10`},
			expectedErr: "must be less than --keys"},
		{name: "blind write only", flags: []string{`--blind`, `--read-pct=0`}},
		{name: "blind mode must not silently drop requested reads",
			flags:       []string{`--blind`, `--read-pct=50`},
			expectedErr: "requires --read-pct 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gen := workload.FromFlags(ycsbtMeta, tc.flags...)
			err := gen.(*ycsbt).validateConfig()
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// The table is what the transaction assumes exists: every key in [0, --keys)
// is present, and its counter starts at a known value so the increments are
// the only thing that ever changes it.
func TestTables(t *testing.T) {
	gen := workload.FromFlags(ycsbtMeta, `--keys=1000`).(*ycsbt)
	tables := gen.Tables()
	require.Len(t, tables, 1)
	require.Equal(t, `usertable`, tables[0].Name)
	require.Equal(t, usertableSchema, tables[0].Schema)
	require.Equal(t, 1000, tables[0].InitialRows.NumBatches)
	require.Equal(t, 0, tables[0].Splits.NumBatches, "no splits unless asked for")

	withSplits := workload.FromFlags(ycsbtMeta, `--keys=1000`, `--splits=9`).(*ycsbt)
	require.Equal(t, 9, withSplits.Tables()[0].Splits.NumBatches)
}

// Keys must be distinct (the write set increments each key once) and inside
// the loaded range (a key outside it has no row, and run fails the whole
// transaction on a short row count).
func TestDrawKeysAreDistinctAndInRange(t *testing.T) {
	const keys = 1000
	for _, opsPerTxn := range []int{1, 5, 10, 64} {
		op := newTestOp(t, keys, opsPerTxn, 0.99)
		for iter := 0; iter < 500; iter++ {
			op.drawKeys()
			seen := make(map[int64]struct{}, opsPerTxn)
			for _, k := range op.keys {
				require.GreaterOrEqual(t, k, int64(0))
				require.Less(t, k, int64(keys))
				_, dup := seen[k]
				require.Falsef(t, dup, "ops-per-txn=%d: key %d drawn twice", opsPerTxn, k)
				seen[k] = struct{}{}
			}
		}
	}
}

// The pathological case the probe fallback exists for: asking for every key in
// the space. Rejection sampling alone would not terminate in useful time here.
func TestDrawKeysTerminatesWhenAskingForTheWholeKeySpace(t *testing.T) {
	const keys = 64
	op := newTestOp(t, keys, keys, 0.99)
	op.drawKeys()
	seen := make(map[int64]struct{}, keys)
	for _, k := range op.keys {
		seen[k] = struct{}{}
	}
	require.Len(t, seen, keys, "drawing the whole key space must yield every key exactly once")
}

// The draw stays skewed: the workload's entire premise is contention on a hot
// key, so a bug that flattened the distribution would silently turn it into a
// uniform workload that measures nothing.
func TestDrawKeysIsSkewed(t *testing.T) {
	const keys = 1000
	const iters = 2000
	op := newTestOp(t, keys, 10, 0.99)
	hits := make(map[int64]int)
	for i := 0; i < iters; i++ {
		op.drawKeys()
		for _, k := range op.keys {
			hits[k]++
		}
	}
	total := iters * 10
	// Under a uniform draw the ten hottest of a thousand keys would take about
	// 1% of the traffic. Zipf(0.99) puts far more there; the bar is set low
	// enough that it cannot fail by chance and high enough that a uniform draw
	// cannot pass.
	var topTen int
	for k := int64(0); k < 10; k++ {
		topTen += hits[k]
	}
	require.Greaterf(t, float64(topTen)/float64(total), 0.10,
		"the ten hottest keys took %d of %d draws; the distribution is not skewed", topTen, total)
}

func newTestOp(t *testing.T, keys, opsPerTxn int, theta float64) *ycsbtOp {
	t.Helper()
	zipf, err := workloadimpl.NewZipfGenerator(
		rand.New(rand.NewPCG(1, 2)), 0 /* iMin */, uint64(keys-1), theta, false /* verbose */)
	require.NoError(t, err)
	reads, writes := splitCounts(opsPerTxn, 90)
	return &ycsbtOp{
		reads:    reads,
		writes:   writes,
		keySpace: int64(keys),
		zipf:     zipf,
		rng:      rand.New(rand.NewPCG(3, 4)),
		keys:     make([]int64, opsPerTxn),
		args:     make([]interface{}, opsPerTxn),
	}
}
