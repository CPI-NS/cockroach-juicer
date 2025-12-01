// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// WorkloadConfig holds configuration for workload execution.
type WorkloadConfig struct {
	TxCount        int
	OpsPerTx       int
	KeyRange       int
	KeyPrefix      string
	Distribution   string  // "uniform" or "zipfian"
	ZipfianS       float64
	ZipfianV       float64
	ReadWriteRatio float64 // 0.0 = all writes, 1.0 = all reads
	Workers        int
	Protocol       string // "2PL" or "2PL-WW"
	JuicerEnabled  bool
	WorkloadType   string // "mixed" (default) or "rmw" (read-modify-write)
	UseHashKeys    bool   // Use hash-based keys for even distribution
}

// WorkloadResults holds the results of workload execution.
type WorkloadResults struct {
	TotalTxs       int64
	CommittedTxs   int64
	AbortedTxs     int64
	TotalAttempts  int64 // Total attempts including retries
	Latencies      []time.Duration
	StartTime      time.Time
	EndTime        time.Time
	TotalDuration  time.Duration
}

// RunWorkload executes the workload and returns results.
func RunWorkload(ctx context.Context, db *kv.DB, cfg WorkloadConfig) (*WorkloadResults, error) {
	if cfg.TxCount <= 0 {
		return nil, errors.New("TxCount must be > 0")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}

	fmt.Printf("Starting workload:\n")
	fmt.Printf("  Transactions: %d\n", cfg.TxCount)
	fmt.Printf("  Operations per TX: %d\n", cfg.OpsPerTx)
	fmt.Printf("  Key range: 1-%d (prefix: %s)\n", cfg.KeyRange, cfg.KeyPrefix)
	fmt.Printf("  Distribution: %s", cfg.Distribution)
	if cfg.Distribution == "zipfian" {
		fmt.Printf(" (s=%.2f, v=%.2f)", cfg.ZipfianS, cfg.ZipfianV)
	}
	fmt.Printf("\n")
	fmt.Printf("  Read/Write ratio: %.2f\n", cfg.ReadWriteRatio)
	fmt.Printf("  Workers: %d\n", cfg.Workers)
	fmt.Printf("  Protocol: %s\n", cfg.Protocol)
	fmt.Printf("  Juicer: %v\n", cfg.JuicerEnabled)
	fmt.Printf("\n")

	results := &WorkloadResults{
		Latencies: make([]time.Duration, 0, cfg.TxCount),
	}

	// Create work queue
	type workItem struct {
		txID int
	}
	workChan := make(chan workItem, cfg.Workers*2)

	// Synchronization
	var wg sync.WaitGroup
	var latenciesMu sync.Mutex

	// Atomic counters
	var totalTxs, committedTxs, abortedTxs, totalAttempts int64

	// Start time
	results.StartTime = time.Now()

	// Start workers
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Create per-worker random source
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			zipfGen := NewZipfGenerator(rng, cfg.KeyRange, cfg.ZipfianS, cfg.ZipfianV)

			for item := range workChan {
				start := time.Now()
				committed := false

				// Execute transaction with retries and timeout
				txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				for retries := 0; retries < 10; retries++ {
					atomic.AddInt64(&totalAttempts, 1) // Count every attempt
					err := executeTransaction(txCtx, db, cfg, rng, zipfGen, item.txID)
					if err == nil {
						committed = true
						break
					}
					// Transaction aborted or failed, retry
					if txCtx.Err() != nil {
						// Context timeout/cancellation
						break
					}
				}
				cancel()

				latency := time.Since(start)

				atomic.AddInt64(&totalTxs, 1)
				if committed {
					atomic.AddInt64(&committedTxs, 1)
					func() {
						latenciesMu.Lock()
						defer latenciesMu.Unlock()
						results.Latencies = append(results.Latencies, latency)
					}()
				} else {
					atomic.AddInt64(&abortedTxs, 1)
				}

				// Progress reporting
				if item.txID > 0 && item.txID%100 == 0 {
					fmt.Fprintf(os.Stderr, "  Progress: %d/%d transactions (%.1f%%)\n",
						item.txID, cfg.TxCount, float64(item.txID)/float64(cfg.TxCount)*100)
				}
			}
		}(i)
	}

	// Distribute work
	go func() {
		for i := 0; i < cfg.TxCount; i++ {
			workChan <- workItem{txID: i}
		}
		close(workChan)
	}()

	// Wait for completion
	wg.Wait()

	results.EndTime = time.Now()
	results.TotalDuration = results.EndTime.Sub(results.StartTime)
	results.TotalTxs = totalTxs
	results.CommittedTxs = committedTxs
	results.AbortedTxs = abortedTxs
	results.TotalAttempts = totalAttempts

	return results, nil
}

// hashKey generates a hash-based key for even distribution across shards.
// This ensures hot keys (from Zipfian distribution) are spread across all servers.
func hashKey(keyID int, keyRange int, prefix string) roachpb.Key {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%d", keyID)))
	hashValue := h.Sum64()
	hashedID := int(hashValue % uint64(keyRange))
	return roachpb.Key(fmt.Sprintf("%s-%d", prefix, hashedID))
}

// executeRMW performs a single read-modify-write operation.
func executeRMW(ctx context.Context, txn *kv.Txn, key roachpb.Key) error {
	// Read current value
	batch := txn.NewBatch()
	batch.Get(key)
	if err := txn.Run(ctx, batch); err != nil {
		return err
	}

	result := batch.Results[0]
	var newValue int64 = 1

	// Modify: increment existing value or start at 1
	if result.Rows[0].Value != nil {
		currentVal, err := result.Rows[0].Value.GetInt()
		if err == nil {
			newValue = currentVal + 1
		}
	}

	// Write back
	batch2 := txn.NewBatch()
	batch2.Put(key, strconv.FormatInt(newValue, 10))
	return txn.Run(ctx, batch2)
}

func executeTransaction(
	ctx context.Context,
	db *kv.DB,
	cfg WorkloadConfig,
	rng *rand.Rand,
	zipfGen *ZipfGenerator,
	txID int,
) error {
	// Create transaction
	var txn *kv.Txn
	if cfg.JuicerEnabled {
		txn = db.NewJuicerTxn(ctx, "juicer_benchmark")
	} else {
		txn = db.NewTxn(ctx, "benchmark")
	}

	// Handle RMW workload type (for evaluation)
	if cfg.WorkloadType == "rmw" {
		// Single read-modify-write operation per transaction
		var keyID int
		if cfg.Distribution == "zipfian" {
			keyID = zipfGen.Next()
		} else {
			keyID = rng.Intn(cfg.KeyRange) + 1
		}

		var key roachpb.Key
		if cfg.UseHashKeys {
			key = hashKey(keyID, cfg.KeyRange, cfg.KeyPrefix)
		} else {
			key = roachpb.Key(fmt.Sprintf("%s-%d", cfg.KeyPrefix, keyID))
		}

		if err := executeRMW(ctx, txn, key); err != nil {
			return err
		}
		return txn.Commit(ctx)
	}

	// Default mixed workload: multiple read/write operations
	for opIdx := 0; opIdx < cfg.OpsPerTx; opIdx++ {
		// Determine key
		var keyID int
		if cfg.Distribution == "zipfian" {
			keyID = zipfGen.Next()
		} else {
			keyID = rng.Intn(cfg.KeyRange) + 1
		}

		var key roachpb.Key
		if cfg.UseHashKeys {
			key = hashKey(keyID, cfg.KeyRange, cfg.KeyPrefix)
		} else {
			key = roachpb.Key(fmt.Sprintf("%s-%d", cfg.KeyPrefix, keyID))
		}

		// Determine read or write
		isRead := rng.Float64() < cfg.ReadWriteRatio

		if isRead {
			// Read operation
			batch := txn.NewBatch()
			if cfg.Protocol == "2PL-WW" {
				batch.GetForUpdate(key, kvpb.BestEffort)
			} else {
				batch.Get(key)
			}
			if err := txn.Run(ctx, batch); err != nil {
				return err
			}
		} else {
			// Write operation
			value := fmt.Sprintf("tx%d-op%d-val", txID, opIdx)
			batch := txn.NewBatch()
			batch.Put(key, value)
			if err := txn.Run(ctx, batch); err != nil {
				return err
			}
		}
	}

	// Commit
	return txn.Commit(ctx)
}

// PrintResults prints workload results in a format that the framework can parse.
func PrintResults(results *WorkloadResults) {
	fmt.Printf("\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("WORKLOAD RESULTS\n")
	fmt.Printf("================================================================================\n")

	// Calculate statistics
	p50, p95, p99 := calculatePercentiles(results.Latencies)
	avgLatency := calculateAverage(results.Latencies)
	throughput := float64(results.CommittedTxs) / results.TotalDuration.Seconds()

	// Accurate abort rate: (total_attempts - committed) / total_attempts
	// This counts retries: if 1 tx aborted 7 times then succeeded, abort_rate = 7/8 = 87.5%
	abortRate := 0.0
	if results.TotalAttempts > 0 {
		abortRate = float64(results.TotalAttempts-results.CommittedTxs) / float64(results.TotalAttempts) * 100.0
	}

	fmt.Printf("Total Transactions: %d\n", results.TotalTxs)
	fmt.Printf("Total Attempts (including retries): %d\n", results.TotalAttempts)
	fmt.Printf("Committed: %d\n", results.CommittedTxs)
	fmt.Printf("Aborted: %d\n", results.AbortedTxs)
	fmt.Printf("Abort Rate: %.2f%%\n", abortRate)
	fmt.Printf("\n")

	fmt.Printf("Duration: %v\n", results.TotalDuration)
	fmt.Printf("Throughput: %.2f txn/sec\n", throughput)
	fmt.Printf("\n")

	fmt.Printf("Latency (committed transactions):\n")
	fmt.Printf("  Average: %.2f ms\n", avgLatency)
	fmt.Printf("  P50: %.2f ms\n", p50)
	fmt.Printf("  P95: %.2f ms\n", p95)
	fmt.Printf("  P99: %.2f ms\n", p99)
	fmt.Printf("\n")

	// Print in parseable format for framework
	fmt.Printf("METRICS:\n")
	fmt.Printf("latency_p50_ms=%.2f\n", p50)
	fmt.Printf("latency_p95_ms=%.2f\n", p95)
	fmt.Printf("latency_p99_ms=%.2f\n", p99)
	fmt.Printf("latency_avg_ms=%.2f\n", avgLatency)
	fmt.Printf("throughput_ops_per_sec=%.2f\n", throughput)
	fmt.Printf("abort_rate_percent=%.2f\n", abortRate)
	fmt.Printf("total_txs=%d\n", results.TotalTxs)
	fmt.Printf("total_attempts=%d\n", results.TotalAttempts)
	fmt.Printf("committed_txs=%d\n", results.CommittedTxs)
	fmt.Printf("aborted_txs=%d\n", results.AbortedTxs)
}

func calculatePercentiles(latencies []time.Duration) (p50, p95, p99 float64) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	// Sort latencies
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)

	// Simple bubble sort (good enough for benchmarks)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	p50Idx := int(float64(len(sorted)) * 0.50)
	p95Idx := int(float64(len(sorted)) * 0.95)
	p99Idx := int(float64(len(sorted)) * 0.99)

	if p50Idx >= len(sorted) {
		p50Idx = len(sorted) - 1
	}
	if p95Idx >= len(sorted) {
		p95Idx = len(sorted) - 1
	}
	if p99Idx >= len(sorted) {
		p99Idx = len(sorted) - 1
	}

	return sorted[p50Idx].Seconds() * 1000,
		sorted[p95Idx].Seconds() * 1000,
		sorted[p99Idx].Seconds() * 1000
}

func calculateAverage(latencies []time.Duration) float64 {
	if len(latencies) == 0 {
		return 0
	}

	var total time.Duration
	for _, lat := range latencies {
		total += lat
	}

	return float64(total) / float64(len(latencies)) / float64(time.Millisecond)
}

// ZipfGenerator generates keys according to Zipfian distribution.
type ZipfGenerator struct {
	rng      *rand.Rand
	keyRange int
	s        float64
	v        float64
	alpha    float64
	eta      float64
	theta    float64
	zeta2    float64
	zetaN    float64
}

// NewZipfGenerator creates a new Zipfian distribution generator.
func NewZipfGenerator(rng *rand.Rand, keyRange int, s, v float64) *ZipfGenerator {
	gen := &ZipfGenerator{
		rng:      rng,
		keyRange: keyRange,
		s:        s,
		v:        v,
	}

	gen.theta = s
	gen.zeta2 = gen.zeta(2)
	gen.alpha = 1.0 / (1.0 - gen.theta)
	gen.zetaN = gen.zeta(keyRange)
	gen.eta = (1 - math.Pow(2.0/float64(keyRange), 1-gen.theta)) / (1 - gen.zeta2/gen.zetaN)

	return gen
}

// Next returns the next key ID according to Zipfian distribution.
func (z *ZipfGenerator) Next() int {
	u := z.rng.Float64()
	uz := u * z.zetaN

	if uz < 1.0 {
		return 1
	}

	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 2
	}

	return 1 + int(float64(z.keyRange)*math.Pow(z.eta*u-z.eta+1, z.alpha))
}

func (z *ZipfGenerator) zeta(n int) float64 {
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), z.theta)
	}
	return sum
}
