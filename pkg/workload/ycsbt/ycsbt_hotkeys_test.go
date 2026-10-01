// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ycsbt

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/stretchr/testify/require"
)

// hotKeyWorkloads are the two formal workloads of the crdb-manual-hot-20260930
// experiment, with the generator flags exactly as its harness passes them to
// `cockroach workload run ycsbt`. checkpoints are per-worker transaction
// counts at which the per-key tallies are recorded: roughly the realized
// per-worker counts of one 60 s ramp + 180 s measurement cell, and 15625
// (one million transactions over 64 workers).
var hotKeyWorkloads = []struct {
	name        string
	seed        uint64
	readTxnPct  int
	checkpoints []int
}{
	{name: "read50", seed: 202609230062, readTxnPct: 50, checkpoints: []int{178, 15625}},
	{name: "read90", seed: 202609230063, readTxnPct: 90, checkpoints: []int{1500, 15625}},
}

const hotKeyConcurrency = 64

// workerOf returns the *ycsbtOp a worker function returned by Ops is bound to.
// Ops hands out method values (op.runMixed); in the gc toolchain a method
// value is a pointer to a closure whose first word is the code pointer and
// whose second word is the captured receiver. The caller validates the
// result field by field before trusting it.
func workerOf(t *testing.T, fn func(context.Context) error) *ycsbtOp {
	t.Helper()
	type methodValue struct {
		code uintptr
		recv *ycsbtOp
	}
	mv := *(**methodValue)(unsafe.Pointer(&fn))
	require.NotNil(t, mv)
	require.NotNil(t, mv.recv)
	return mv.recv
}

// keyTally is one checkpoint's per-key count of transactions that contain the
// key (a transaction touches a key at most once: drawKeys draws distinct keys).
type keyTally struct {
	PerWorkerTxns int        `json:"per_worker_txns"`
	Txns          int        `json:"txns"`
	ReadTxns      int        `json:"read_txns"`
	WriteTxns     int        `json:"write_txns"`
	Keys          [][4]int64 `json:"keys"` // [key, txns including it, of which read, of which write]
}

// TestYCSBTHotKeysFromRealOps replays the formal workloads' key draws through
// the production code path and records which keys are hottest.
//
// The workers are built by the real Ops with the real flags, so the seeding
// (one PCG stream per worker for the Zipf draw and one for everything else),
// the scramble multiplier and the read/write mix are exactly those of a run.
// Each worker function is then called with an already-cancelled context: the
// real runMixed/runReadOnly/runBlind draw their keys and values from the
// worker's streams exactly as in a run, and the statement then fails at
// connection acquisition before anything is sent. A client transaction's key
// set therefore depends only on the seed and on how many transactions the
// worker has issued before, never on timing or on the outcome of earlier
// transactions (a failed transaction is not re-drawn: the next call draws
// fresh keys, and server-side automatic retries re-send the same statement).
//
// An independent mirror of the draw model (the model the experiment's
// main-session estimate used) is advanced in lock step and must produce the
// same key sets, read/write choices and write values.
func TestYCSBTHotKeysFromRealOps(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	out := map[string]interface{}{}
	for _, wl := range hotKeyWorkloads {
		t.Run(wl.name, func(t *testing.T) {
			gen := ycsbtMeta.New().(*ycsbt)
			require.NoError(t, gen.Flags().Parse([]string{
				fmt.Sprintf("--concurrency=%d", hotKeyConcurrency),
				fmt.Sprintf("--seed=%d", wl.seed),
				"--keys=1000", "--ops-per-txn=10", "--read-pct=0",
				fmt.Sprintf("--read-txn-pct=%d", wl.readTxnPct),
				"--zipf-theta=0.99", "--blind", "--key-scramble=true",
			}))
			require.Equal(t, wl.seed, RandomSeed.Seed())
			ql, err := gen.Ops(cancelled, []string{"postgresql://root@127.0.0.1:1/ycsbt?sslmode=disable"},
				histogram.NewRegistry(10*time.Second, "ycsbt"))
			require.NoError(t, err)
			require.Len(t, ql.WorkerFns, hotKeyConcurrency)

			live := make([]*ycsbtOp, hotKeyConcurrency)
			mirror := make([]*ycsbtOp, hotKeyConcurrency)
			for i, fn := range ql.WorkerFns {
				op := workerOf(t, fn)
				require.Equal(t, int64(1000), op.keySpace)
				require.Equal(t, int64(733), op.scrambleMul)
				require.Equal(t, wl.readTxnPct, op.readTxnPct)
				require.Len(t, op.keys, 10)
				require.Len(t, op.args, 20)
				require.Len(t, op.readArgs, 10)
				require.NotNil(t, op.zipf)
				require.NotNil(t, op.rng)
				live[i] = op
				zipf, err := workloadimpl.NewZipfGenerator(
					rand.New(rand.NewPCG(wl.seed, uint64(i))), 0, 999, 0.99, false /* verbose */)
				require.NoError(t, err)
				mirror[i] = &ycsbtOp{
					keySpace:    1000,
					scrambleMul: 733,
					zipf:        zipf,
					rng:         rand.New(rand.NewPCG(wl.seed, uint64(i)+hotKeyConcurrency)),
					keys:        make([]int64, 10),
				}
			}

			counts := make([][3]int64, 1000) // txns including the key, read, write
			var txns, readTxns, writeTxns int
			var tallies []keyTally
			record := func(perWorker int) {
				tl := keyTally{PerWorkerTxns: perWorker, Txns: txns, ReadTxns: readTxns, WriteTxns: writeTxns}
				for k, c := range counts {
					tl.Keys = append(tl.Keys, [4]int64{int64(k), c[0], c[1], c[2]})
				}
				tallies = append(tallies, tl)
			}
			last := wl.checkpoints[len(wl.checkpoints)-1]
			next := 0
			for txn := 1; txn <= last; txn++ {
				for i, fn := range ql.WorkerFns {
					if err := fn(cancelled); err == nil {
						t.Fatalf("worker %d txn %d: a statement reached a server", i, txn)
					}
					op, m := live[i], mirror[i]
					isRead := m.rng.IntN(100) < wl.readTxnPct
					m.drawKeys()
					for j, k := range op.keys {
						if m.keys[j] != k {
							t.Fatalf("worker %d txn %d: key set %v, mirror %v", i, txn, op.keys, m.keys)
						}
						if isRead {
							if got, ok := op.readArgs[j].(int64); !ok || got != k {
								t.Fatalf("worker %d txn %d: read arg %d = %v, want %d", i, txn, j, op.readArgs[j], k)
							}
						} else {
							if got, ok := op.args[2*j].(int64); !ok || got != k {
								t.Fatalf("worker %d txn %d: write key %d = %v, want %d", i, txn, j, op.args[2*j], k)
							}
							if got, ok := op.args[2*j+1].(int64); !ok || got != m.rng.Int64N(1<<30) {
								t.Fatalf("worker %d txn %d: write value %d = %v differs from the mirror", i, txn, j, op.args[2*j+1])
							}
						}
					}
					txns++
					if isRead {
						readTxns++
					} else {
						writeTxns++
					}
					for _, k := range op.keys {
						counts[k][0]++
						if isRead {
							counts[k][1]++
						} else {
							counts[k][2]++
						}
					}
				}
				if next < len(wl.checkpoints) && txn == wl.checkpoints[next] {
					record(txn)
					next++
				}
			}

			// The ranking at every checkpoint, hottest first (ties by key).
			for _, tl := range tallies {
				order := make([]int, 1000)
				for k := range order {
					order[k] = k
				}
				sort.SliceStable(order, func(a, b int) bool {
					ca, cb := tl.Keys[order[a]][1], tl.Keys[order[b]][1]
					return ca > cb || (ca == cb && order[a] < order[b])
				})
				for pos := 0; pos < 8; pos++ {
					k := order[pos]
					t.Logf("%s per-worker=%d txns=%d pos=%d key=%d including=%d share=%.4f reads=%d writes=%d",
						wl.name, tl.PerWorkerTxns, tl.Txns, pos+1, k, tl.Keys[k][1],
						float64(tl.Keys[k][1])/float64(tl.Txns), tl.Keys[k][2], tl.Keys[k][3])
				}
				if tl.PerWorkerTxns == last {
					// The experiment froze the five hottest keys as 0, 733, 466,
					// 199, 932 (ranks 0-4 under the scramble) with 665 sixth.
					require.Equal(t, []int{0, 733, 466, 199, 932, 665}, order[:6],
						"%s: hottest keys at %d transactions", wl.name, tl.Txns)
				}
			}
			out[wl.name] = map[string]interface{}{
				"seed": wl.seed, "read_txn_pct": wl.readTxnPct, "concurrency": hotKeyConcurrency,
				"flags":         "--keys=1000 --ops-per-txn=10 --read-pct=0 --zipf-theta=0.99 --blind --key-scramble=true",
				"mirror_equals": true, "tallies": tallies,
			}
		})
	}
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		data, err := json.Marshal(out)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "ycsbt-hotkey-draws.json"), data, 0644))
	}
}
