// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ycsbt

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/stretchr/testify/require"
)

// Since 2026-10-19 the keys of a transaction are drawn independently by
// default, so a transaction can touch a key more than once; --distinct-keys
// brings the old distinct draw back. The default workload has ten million
// keys.

func TestRepeatedKeysDefaults(t *testing.T) {
	gen := ycsbtMeta.New().(*ycsbt)
	require.NoError(t, gen.flags.Parse(nil))
	require.False(t, gen.distinctKeys, "--distinct-keys must default to off")
	require.Equal(t, 10000000, gen.keys)
	require.Equal(t, 5, gen.opsPerTxn)
	require.Equal(t, 0.99, gen.zipfTheta)
	require.True(t, gen.flags.Meta[`distinct-keys`].RuntimeOnly,
		"--distinct-keys changes the draw, never the loaded table")
}

func newIndependentOp(t *testing.T, keys, opsPerTxn int, scramble bool) *ycsbtOp {
	t.Helper()
	op := newTestOpScramble(t, keys, opsPerTxn, 0.99, scramble)
	op.distinct = false
	return op
}

// The default draw is exactly opsPerTxn independent Zipf draws, scrambled and
// shuffled: a mirror built from the same streams reproduces it.
func TestDrawKeysIndependentIsPlainZipf(t *testing.T) {
	for _, scramble := range []bool{false, true} {
		op := newIndependentOp(t, 1000, 5, scramble)
		zipf, err := workloadimpl.NewZipfGenerator(rand.New(rand.NewPCG(1, 2)), 0, 999, 0.99, false)
		require.NoError(t, err)
		shuffle := rand.New(rand.NewPCG(3, 4))
		mul := scrambleMultiplier(1000)
		for iter := 0; iter < 200; iter++ {
			op.drawKeys()
			want := make([]int64, 5)
			for i := range want {
				want[i] = int64(zipf.Uint64())
				if scramble {
					want[i] = want[i] * mul % 1000
				}
			}
			shuffle.Shuffle(5, func(i, j int) { want[i], want[j] = want[j], want[i] })
			require.Equal(t, want, op.keys, "scramble=%v iter %d", scramble, iter)
		}
	}
}

// With few keys repetitions must actually happen, and every key stays in range.
func TestDrawKeysIndependentRepeats(t *testing.T) {
	op := newIndependentOp(t, 10, 5, false)
	repeated := 0
	for iter := 0; iter < 500; iter++ {
		op.drawKeys()
		for _, k := range op.keys {
			require.GreaterOrEqual(t, k, int64(0))
			require.Less(t, k, int64(10))
		}
		if distinctCount(op.keys) < len(op.keys) {
			repeated++
		}
	}
	require.Greater(t, repeated, 100, "independent draws over 10 keys must repeat often")
	// More draws than keys is fine without --distinct-keys.
	op = newIndependentOp(t, 3, 5, false)
	op.drawKeys()
	require.LessOrEqual(t, distinctCount(op.keys), 3)
}

func TestDistinctCount(t *testing.T) {
	require.Equal(t, 0, distinctCount(nil))
	require.Equal(t, 1, distinctCount([]int64{7}))
	require.Equal(t, 3, distinctCount([]int64{1, 2, 1, 3, 2}))
	require.Equal(t, 1, distinctCount([]int64{4, 4, 4}))
}

// Ops builds every worker's generator from one precomputed distribution
// (WithRand); each must draw what a generator built from scratch on the same
// stream draws, so the change is invisible in the key sequence, and the
// workers inherit --distinct-keys.
func TestOpsWorkersDrawLikeFreshGenerators(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, distinct := range []bool{false, true} {
		gen := ycsbtMeta.New().(*ycsbt)
		flags := []string{`--keys=1000`, `--seed=7`, `--concurrency=3`}
		if distinct {
			flags = append(flags, `--distinct-keys`)
		}
		require.NoError(t, gen.Flags().Parse(flags))
		ql, err := gen.Ops(cancelled, []string{"postgresql://root@127.0.0.1:1/ycsbt?sslmode=disable"},
			histogram.NewRegistry(10*time.Second, "ycsbt"))
		require.NoError(t, err)
		require.Len(t, ql.WorkerFns, 3)
		for i, fn := range ql.WorkerFns {
			op := workerOf(t, fn)
			require.Equal(t, distinct, op.distinct)
			fresh, err := workloadimpl.NewZipfGenerator(rand.New(rand.NewPCG(7, uint64(i))), 0, 999, 0.99, false)
			require.NoError(t, err)
			for j := 0; j < 1000; j++ {
				require.Equal(t, fresh.Uint64(), op.zipf.Uint64(), "worker %d draw %d", i, j)
			}
		}
	}
}
