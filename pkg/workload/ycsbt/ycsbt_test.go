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
		{name: "mixed one-shot", flags: []string{`--blind`, `--read-pct=0`, `--read-txn-pct=50`}},
		{name: "transaction mix outside the blind mode",
			flags:       []string{`--read-txn-pct=50`},
			expectedErr: "requires --blind"},
		{name: "transaction mix above 100",
			flags:       []string{`--blind`, `--read-pct=0`, `--read-txn-pct=101`},
			expectedErr: "between 0 and 100"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Not workload.FromFlags: that helper runs the Validate hook and,
			// by documented contract, panics on its error -- which is exactly
			// the value the negative cases below need to observe instead.
			gen := ycsbtMeta.New().(*ycsbt)
			require.NoError(t, gen.flags.Parse(tc.flags))
			err := gen.validateConfig()
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
	return newTestOpScramble(t, keys, opsPerTxn, theta, false)
}

// newTestOpScramble builds a worker whose random streams depend only on its
// arguments, so two ops built with the same arguments draw the same ranks in
// the same order and shuffle them the same way. That is what lets the scramble
// tests below compare a scrambled op against an unscrambled one position by
// position.
func newTestOpScramble(t *testing.T, keys, opsPerTxn int, theta float64, scramble bool) *ycsbtOp {
	t.Helper()
	zipf, err := workloadimpl.NewZipfGenerator(
		rand.New(rand.NewPCG(1, 2)), 0 /* iMin */, uint64(keys-1), theta, false /* verbose */)
	require.NoError(t, err)
	reads, writes := splitCounts(opsPerTxn, 90)
	var mul int64
	if scramble {
		mul = scrambleMultiplier(int64(keys))
	}
	return &ycsbtOp{
		reads:       reads,
		writes:      writes,
		keySpace:    int64(keys),
		scrambleMul: mul,
		zipf:        zipf,
		rng:         rand.New(rand.NewPCG(3, 4)),
		keys:        make([]int64, opsPerTxn),
		args:        make([]interface{}, opsPerTxn),
	}
}

// The multiplier is the entire contract of --key-scramble: it must be the
// largest integer within the 0.733 ceiling that is coprime with the key space,
// and it must be that same integer every time the workload starts, on every
// worker and every client, or the workers would not contend on the same keys.
func TestScrambleMultiplier(t *testing.T) {
	for _, tc := range []struct {
		keys int64
		want int64
	}{
		// 733 is prime, so it is coprime with 1000 and the ceiling is met exactly.
		{keys: 1000, want: 733},
		// floor(0.733 * 100000) = 73300 = 2^2 * 5^2 * 733 shares factors with
		// 100000 = 2^5 * 5^5, so the search walks down to the first coprime.
		{keys: 100000, want: 73299},
		{keys: 7, want: 5},
		// Key spaces too small for a non-trivial stride fall back to identity.
		{keys: 2, want: 1},
		{keys: 1, want: 1},
	} {
		t.Run(strconv.FormatInt(tc.keys, 10), func(t *testing.T) {
			got := scrambleMultiplier(tc.keys)
			require.Equal(t, tc.want, got)
			require.Equal(t, got, scrambleMultiplier(tc.keys), "must be deterministic")
			require.EqualValues(t, 1, gcd(got, tc.keys), "A must be coprime with the key space")
			if tc.keys > 2 {
				ceiling := tc.keys * 733 / 1000
				require.LessOrEqual(t, got, ceiling, "A must not exceed floor(0.733*keys)")
				// Nothing between A and the ceiling may be coprime, or A was
				// not the largest candidate.
				for a := got + 1; a <= ceiling; a++ {
					require.NotEqualValues(t, 1, gcd(a, tc.keys),
						"a=%d is coprime with keys=%d and larger than A=%d", a, tc.keys, got)
				}
			}
		})
	}
}

// Bijectivity is what makes the scramble a pure relabelling: every key is the
// image of exactly one rank, so each key inherits exactly one rank's Zipf
// weight and the distribution's shape is untouched. It is also what guarantees
// drawKeys still returns distinct keys — ten distinct ranks cannot collide.
func TestScrambleIsABijection(t *testing.T) {
	for _, keys := range []int64{1, 2, 7, 1000, 100000} {
		t.Run(strconv.FormatInt(keys, 10), func(t *testing.T) {
			a := scrambleMultiplier(keys)
			seen := make(map[int64]int64, keys)
			for r := int64(0); r < keys; r++ {
				k := (r * a) % keys
				require.GreaterOrEqual(t, k, int64(0))
				require.Less(t, k, keys)
				prev, dup := seen[k]
				require.Falsef(t, dup, "keys=%d: ranks %d and %d both map to key %d", keys, prev, r, k)
				seen[k] = r
			}
			require.Len(t, seen, int(keys), "the image must cover the whole key space")
			if keys == 1 {
				require.EqualValues(t, 0, (int64(0)*a)%keys, "a single-key space must map identically")
			}
		})
	}
}

// The point of the round the flag was written for: the hottest ranks must stop
// sharing a range. With --keys 1000 and --splits 30 the ranges are 32 keys
// wide, so the range a key lives in is k/32.
func TestScrambleScattersTheHottestRanks(t *testing.T) {
	const keys = 1000
	a := scrambleMultiplier(keys)
	want := []int64{0, 733, 466, 199, 932, 665, 398, 131, 864, 597, 330, 63, 796, 529, 262, 995}
	got := make([]int64, 0, len(want))
	ranges := make(map[int64]int64, len(want))
	for r := int64(0); r < int64(len(want)); r++ {
		k := (r * a) % keys
		got = append(got, k)
		prev, dup := ranges[k/32]
		require.Falsef(t, dup, "ranks %d and %d share the 32-key range %d", prev, r, k/32)
		ranges[k/32] = r
	}
	// Pinned rather than recomputed: this exact sequence is what the flag's
	// help text promises, so a change to the multiplier is a visible change to
	// the documented layout.
	require.Equal(t, want, got)
	require.Len(t, ranges, len(want), "the sixteen hottest ranks must occupy sixteen distinct ranges")

	// Contrast: unscrambled, the same sixteen ranks are the sixteen lowest
	// keys, which is half of a single range.
	unscrambled := make(map[int64]struct{})
	for r := int64(0); r < int64(len(want)); r++ {
		unscrambled[r/32] = struct{}{}
	}
	require.Len(t, unscrambled, 1)
}

// The flag must change nothing except which key a rank names. Both ops here
// draw from identical random streams, so with the scramble off they must agree
// key for key (the pre-flag behaviour), and with it on the scrambled op's keys
// must be exactly the unscrambled op's keys pushed through the bijection —
// same positions, same shuffle, same everything else.
func TestDrawKeysScrambleIsExactlyTheBijection(t *testing.T) {
	for _, keys := range []int{7, 1000, 100000} {
		t.Run(strconv.Itoa(keys), func(t *testing.T) {
			opsPerTxn := 10
			if keys < opsPerTxn {
				opsPerTxn = keys
			}
			a := scrambleMultiplier(int64(keys))
			plain := newTestOpScramble(t, keys, opsPerTxn, 0.99, false)
			plainAgain := newTestOpScramble(t, keys, opsPerTxn, 0.99, false)
			scrambled := newTestOpScramble(t, keys, opsPerTxn, 0.99, true)
			scrambledAgain := newTestOpScramble(t, keys, opsPerTxn, 0.99, true)
			for iter := 0; iter < 500; iter++ {
				plain.drawKeys()
				plainAgain.drawKeys()
				scrambled.drawKeys()
				scrambledAgain.drawKeys()

				// Determinism, on both sides of the flag.
				require.Equal(t, plain.keys, plainAgain.keys, "iter %d: unscrambled draw is not deterministic", iter)
				require.Equal(t, scrambled.keys, scrambledAgain.keys, "iter %d: scrambled draw is not deterministic", iter)

				// The scramble-off path must be byte-for-byte the old one, and
				// the scramble-on path must be it composed with the bijection.
				want := make([]int64, len(plain.keys))
				for i, r := range plain.keys {
					want[i] = (r * a) % int64(keys)
				}
				require.Equalf(t, want, scrambled.keys, "iter %d: scrambled keys are not the bijection of the plain draw", iter)

				// Still a well-formed transaction: distinct, in range.
				seen := make(map[int64]struct{}, opsPerTxn)
				for _, k := range scrambled.keys {
					require.GreaterOrEqual(t, k, int64(0))
					require.Less(t, k, int64(keys))
					_, dup := seen[k]
					require.Falsef(t, dup, "iter %d: key %d drawn twice under the scramble", iter, k)
					seen[k] = struct{}{}
				}
			}
		})
	}
}

// The scramble moves the hot keys; it must not cool them down. The ten hottest
// RANKS still have to take the same share of the traffic they took before,
// under their new names.
func TestDrawKeysStaysSkewedUnderTheScramble(t *testing.T) {
	const keys = 1000
	const iters = 2000
	a := scrambleMultiplier(keys)
	op := newTestOpScramble(t, keys, 10, 0.99, true)
	hits := make(map[int64]int)
	for i := 0; i < iters; i++ {
		op.drawKeys()
		for _, k := range op.keys {
			hits[k]++
		}
	}
	var topTen int
	for r := int64(0); r < 10; r++ {
		topTen += hits[(r*a)%keys]
	}
	require.Greaterf(t, float64(topTen)/float64(iters*10), 0.10,
		"the ten hottest ranks took %d of %d draws after the scramble; the skew did not survive", topTen, iters*10)
}

// The flag is run-time only: it decides which keys the load draws, never which
// rows are loaded. A scrambled run and an unscrambled one must be able to share
// one initialised table, which is also what lets the harness flip it per cell.
func TestKeyScrambleIsRuntimeOnly(t *testing.T) {
	gen := ycsbtMeta.New().(*ycsbt)
	require.False(t, gen.keyScramble, "the scramble must be off unless asked for")
	require.True(t, gen.Flags().Meta[`key-scramble`].RuntimeOnly,
		"--key-scramble must be RuntimeOnly: it must not change the loaded data")

	on := workload.FromFlags(ycsbtMeta, `--keys=1000`, `--key-scramble`).(*ycsbt)
	require.True(t, on.keyScramble)
	off := workload.FromFlags(ycsbtMeta, `--keys=1000`).(*ycsbt)
	require.Equal(t, off.Tables()[0].InitialRows.NumBatches, on.Tables()[0].InitialRows.NumBatches)
	require.Equal(t, 1000, on.Tables()[0].InitialRows.NumBatches)
}

// The read side of the mixed mode is one plain SELECT: pinned text, exact
// placeholder sequence, and — the property the mode exists for — nothing
// locking and nothing mutating in it.
func TestBuildReadStmt(t *testing.T) {
	require.Equal(t,
		`SELECT count(*) FROM usertable WHERE ycsb_key IN ($1, $2)`,
		buildReadStmt(2))

	placeholderRE := regexp.MustCompile(`\$(\d+)`)
	for _, reads := range []int{1, 5, 10} {
		stmt := buildReadStmt(reads)
		var got []int
		for _, m := range placeholderRE.FindAllStringSubmatch(stmt, -1) {
			n, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			got = append(got, n)
		}
		want := make([]int, 0, reads)
		for i := 1; i <= reads; i++ {
			want = append(want, i)
		}
		require.Equalf(t, want, got, "%d reads: placeholder sequence", reads)

		require.NotContains(t, stmt, "UPDATE")
		require.NotContains(t, stmt, "UPSERT")
		require.NotContains(t, stmt, "FOR UPDATE")
	}
}
