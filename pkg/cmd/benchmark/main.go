// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

var (
	bootstrapAddrs = flag.String("addrs", "localhost:26257", "comma-separated list of bootstrap addresses")
	insecure       = flag.Bool("insecure", true, "use insecure connection")
	sslCertsDir    = flag.String("certs", "", "directory containing SSL certificates")
	clusterName    = flag.String("cluster", "default", "cluster name")
	key            = flag.String("key", "test-key", "key to use for RMW operation")

	// Data initialization flags
	initData       = flag.Bool("init", false, "initialize test data")
	initNumKeys    = flag.Int("init-keys", 10000, "number of keys to initialize")
	initKeyPrefix  = flag.String("init-prefix", "key", "prefix for initialized keys")
	initKeyRange   = flag.Int("init-range", 10000, "key range for initialization (1 to init-range)")
	initBatchSize  = flag.Int("init-batch", 100, "batch size for initialization")
	initConcurrent = flag.Int("init-concurrent", 1, "number of concurrent writers")
	useBulkAdder   = flag.Bool("init-bulk", false, "use BulkAdder for high-performance initialization")

	// Workload execution flags
	txCount        = flag.Int("tx-count", 1000, "number of transactions to execute (closed-loop mode)")
	opsPerTx       = flag.Int("ops-per-tx", 10, "number of operations per transaction")
	keyRange       = flag.Int("key-range", 1000, "key range for workload (1 to key-range)")
	keyPrefix      = flag.String("key-prefix", "key", "prefix for keys in workload")
	distribution   = flag.String("distribution", "uniform", "distribution type: uniform or zipfian")
	zipfianS       = flag.Float64("zipfian-s", 1.1, "zipfian skew parameter")
	zipfianV       = flag.Float64("zipfian-v", 1.0, "zipfian velocity parameter")
	readWriteRatio = flag.Float64("read-write-ratio", 0.5, "ratio of reads to total operations")
	workers        = flag.Int("workers", 10, "number of worker goroutines (closed-loop mode)")
	protocol       = flag.String("protocol", "2PL-WW", "concurrency control protocol: 2PL or 2PL-WW")
	juicerEnabled  = flag.Bool("juicer", false, "enable Juicer transaction reordering")
	workloadType   = flag.String("workload-type", "mixed", "workload type: mixed (default) or rmw (read-modify-write)")
	useHashKeys    = flag.Bool("use-hash-keys", true, "use hash-based keys for even distribution across shards")

	// Open-loop mode flags
	openLoop        = flag.Bool("open-loop", false, "use open-loop workload generation (time-based instead of transaction count)")
	targetRate      = flag.Int("target-rate", 100, "target operations per second (open-loop mode only)")
	duration        = flag.Int("duration", 60, "duration to run benchmark in seconds (open-loop mode only)")
	startTime       = flag.Int64("start-time", 0, "unix timestamp (ms) when all clients should start (0 = start immediately)")
	warmupPercent   = flag.Float64("warmup-percent", 0.25, "percentage of duration for warmup phase (open-loop mode only)")
	cooldownPercent = flag.Float64("cooldown-percent", 0.25, "percentage of duration for cooldown phase (open-loop mode only)")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	// Parse bootstrap addresses
	addrs := []string{}
	if *bootstrapAddrs != "" {
		for _, addr := range splitAddrs(*bootstrapAddrs) {
			if addr != "" {
				addrs = append(addrs, addr)
			}
		}
	}
	if len(addrs) == 0 {
		fmt.Fprintf(os.Stderr, "error: at least one bootstrap address is required\n")
		os.Exit(1)
	}

	// Create client config
	cfg := Config{
		BootstrapAddrs: addrs,
		Insecure:       *insecure,
		SSLCertsDir:    *sslCertsDir,
		ClusterName:    *clusterName,
		User:           username.RootUserName(),
	}

	// Create client
	fmt.Printf("Connecting to cluster at %v...\n", addrs)
	db, stopper, err := NewClient(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating client: %v\n", err)
		os.Exit(1)
	}
	defer stopper.Stop(context.Background())

	fmt.Println("✓ Connected to cluster")

	// Initialize data if requested
	if *initData {
		initCfg := InitDataConfig{
			KeyPrefix:           *initKeyPrefix,
			NumKeys:             *initNumKeys,
			KeyRange:            *initKeyRange,
			BatchSize:           *initBatchSize,
			Concurrency:         *initConcurrent,
			UseBulkAdder:        *useBulkAdder,
			BulkAdderBufferSize: 128 << 20, // 128MB
		}

		if err := InitData(ctx, db, initCfg); err != nil {
			fmt.Fprintf(os.Stderr, "error initializing data: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✓ Data initialization completed")
		return
	}

	// Run workload (choose between open-loop and closed-loop)
	if *openLoop {
		// Open-loop mode: time-based with target rate
		if err := runOpenLoopWorkload(ctx, db); err != nil {
			fmt.Fprintf(os.Stderr, "error running open-loop workload: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Closed-loop mode: transaction count based
		workloadCfg := WorkloadConfig{
			TxCount:        *txCount,
			OpsPerTx:       *opsPerTx,
			KeyRange:       *keyRange,
			KeyPrefix:      *keyPrefix,
			Distribution:   *distribution,
			ZipfianS:       *zipfianS,
			ZipfianV:       *zipfianV,
			ReadWriteRatio: *readWriteRatio,
			Workers:        *workers,
			Protocol:       *protocol,
			JuicerEnabled:  *juicerEnabled,
			WorkloadType:   *workloadType,
			UseHashKeys:    *useHashKeys,
		}

		results, err := RunWorkload(ctx, db, workloadCfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error running workload: %v\n", err)
			os.Exit(1)
		}

		// Print results
		PrintResults(results)
	}
}

func performRMW(ctx context.Context, db *kv.DB, key roachpb.Key) error {
	// Create a new transaction
	txn := db.NewJuicerTxn(ctx, "benchmark-rmw")
	txn.SetDebugName("benchmark-rmw")

	// Phase 1: GetForUpdate
	getBatch := txn.NewBatch()
	getBatch.GetForUpdate(key, kvpb.BestEffort)

	if err := txn.Run(ctx, getBatch); err != nil {
		return errors.Wrap(err, "GetForUpdate failed")
	}

	// Get the current value
	var currentValue string
	if len(getBatch.Results) > 0 && len(getBatch.Results[0].Rows) > 0 {
		if getBatch.Results[0].Rows[0].Value != nil {
			currentValue = string(getBatch.Results[0].Rows[0].ValueBytes())
		}
	}

	fmt.Printf("  Current value: %q\n", currentValue)

	// Phase 2: Put (modify the value)
	newValue := fmt.Sprintf("modified-at-%d", time.Now().UnixNano())
	putBatch := txn.NewBatch()
	putBatch.Put(key, newValue)

	if err := txn.Run(ctx, putBatch); err != nil {
		return errors.Wrap(err, "Put failed")
	}

	fmt.Printf("  New value: %q\n", newValue)

	// Phase 3: Commit
	if err := txn.Commit(ctx); err != nil {
		return errors.Wrap(err, "Commit failed")
	}

	return nil
}

// TransactionResult stores the result of a single transaction
type TransactionResult struct {
	StartTime    time.Time
	EndTime      time.Time
	Success      bool
	Aborted      bool
	ErrorMessage string
}

// OpenLoopStats accumulates statistics during the run
type OpenLoopStats struct {
	sync.Mutex
	results        []TransactionResult
	totalAttempted int64
	totalCommitted int64
	totalAborted   int64
	totalErrors    int64
}

func (s *OpenLoopStats) recordResult(result TransactionResult) {
	s.Lock()
	defer s.Unlock()
	s.results = append(s.results, result)
	atomic.AddInt64(&s.totalAttempted, 1)
	if result.Success {
		atomic.AddInt64(&s.totalCommitted, 1)
	} else if result.Aborted {
		atomic.AddInt64(&s.totalAborted, 1)
	} else {
		atomic.AddInt64(&s.totalErrors, 1)
	}
}

func runOpenLoopWorkload(ctx context.Context, db *kv.DB) error {
	// Wait for synchronized start time if specified
	if *startTime > 0 {
		startTimeUnix := time.UnixMilli(*startTime)
		waitDuration := time.Until(startTimeUnix)
		if waitDuration > 0 {
			fmt.Printf("Waiting %.2f seconds for synchronized start at %s...\n",
				waitDuration.Seconds(), startTimeUnix.Format("15:04:05.000"))
			time.Sleep(waitDuration)
		}
	}

	// Calculate phase durations
	totalDuration := time.Duration(*duration) * time.Second
	warmupDuration := time.Duration(float64(totalDuration) * (*warmupPercent))
	cooldownDuration := time.Duration(float64(totalDuration) * (*cooldownPercent))
	measurementDuration := totalDuration - warmupDuration - cooldownDuration

	fmt.Printf("\n=== Open-Loop Benchmark Configuration ===\n")
	fmt.Printf("Target rate: %d ops/sec\n", *targetRate)
	fmt.Printf("Total duration: %s\n", totalDuration)
	fmt.Printf("  Warmup: %s (%.0f%%)\n", warmupDuration, *warmupPercent*100)
	fmt.Printf("  Measurement: %s (%.0f%%)\n", measurementDuration, (1-*warmupPercent-*cooldownPercent)*100)
	fmt.Printf("  Cooldown: %s (%.0f%%)\n", cooldownDuration, *cooldownPercent*100)
	fmt.Printf("Key range: 1-%d (%s distribution, zipf_s=%.2f)\n", *keyRange, *distribution, *zipfianS)
	fmt.Printf("Operations per tx: %d\n", *opsPerTx)
	fmt.Printf("Read/Write ratio: %.2f\n", *readWriteRatio)
	fmt.Printf("Protocol: %s\n", *protocol)
	fmt.Printf("Juicer enabled: %v\n", *juicerEnabled)
	fmt.Printf("=========================================\n\n")

	stats := &OpenLoopStats{
		results: make([]TransactionResult, 0, *targetRate*(*duration)),
	}

	// Calculate inter-arrival time for target rate
	interArrivalNanos := int64(time.Second) / int64(*targetRate)

	startTimeActual := time.Now()
	endTime := startTimeActual.Add(totalDuration)
	warmupEnd := startTimeActual.Add(warmupDuration)
	measurementEnd := endTime.Add(-cooldownDuration)

	fmt.Printf("Benchmark started at %s\n", startTimeActual.Format("15:04:05.000"))
	fmt.Printf("Will run until %s\n\n", endTime.Format("15:04:05.000"))

	// Worker pool for executing transactions
	var wg sync.WaitGroup
	txChan := make(chan time.Time, *targetRate)

	// Start worker goroutines
	numWorkers := *targetRate / 10 // Heuristic: 1 worker per 10 ops/sec
	if numWorkers < 1 {
		numWorkers = 1
	}
	if numWorkers > 100 {
		numWorkers = 100
	}

	fmt.Printf("Starting %d worker goroutines...\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for scheduledTime := range txChan {
				executeOpenLoopTransaction(ctx, db, scheduledTime, stats)
			}
		}()
	}

	// Open-loop rate generator
	ticker := time.NewTicker(time.Duration(interArrivalNanos))
	defer ticker.Stop()

	operationsScheduled := 0
	for now := range ticker.C {
		if now.After(endTime) {
			break
		}

		// Schedule a transaction
		select {
		case txChan <- now:
			operationsScheduled++

			// Progress reporting every second
			if operationsScheduled%*targetRate == 0 {
				elapsed := time.Since(startTimeActual)
				phase := "warmup"
				if now.After(warmupEnd) && now.Before(measurementEnd) {
					phase = "measurement"
				} else if now.After(measurementEnd) {
					phase = "cooldown"
				}
				fmt.Printf("[%s] %.1fs elapsed, %d ops scheduled, %d completed, %d aborted\n",
					phase, elapsed.Seconds(), operationsScheduled,
					atomic.LoadInt64(&stats.totalCommitted),
					atomic.LoadInt64(&stats.totalAborted))
			}
		default:
			// Worker pool is saturated, drop this operation
			// This indicates the system can't keep up with target rate
		}
	}

	close(txChan)
	wg.Wait()

	fmt.Printf("\nBenchmark completed at %s\n", time.Now().Format("15:04:05.000"))
	fmt.Printf("Total operations scheduled: %d\n", operationsScheduled)

	// Print results
	printOpenLoopResults(stats, totalDuration, warmupDuration, cooldownDuration)

	return nil
}

func executeOpenLoopTransaction(
	ctx context.Context,
	db *kv.DB,
	scheduledTime time.Time,
	stats *OpenLoopStats,
) {
	result := TransactionResult{
		StartTime: time.Now(),
	}

	// Generate keys for this transaction
	keys := generateTransactionKeys(*opsPerTx)

	var finalTxn *kv.Txn

	// Execute transaction with auto-retry
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		finalTxn = txn  // Capture the transaction to check its epoch
		for _, key := range keys {
			// Determine if this operation is a read or write
			if rand.Float64() < *readWriteRatio {
				// Read operation
				_, err := txn.Get(ctx, roachpb.Key(key))
				if err != nil {
					return err
				}
			} else {
				// Write operation
				value := fmt.Sprintf("value-%d", time.Now().UnixNano())
				err := txn.Put(ctx, roachpb.Key(key), []byte(value))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	result.EndTime = time.Now()

	if err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
		// Check if it's an abort (simplified check)
		if isOpenLoopAbortError(err) {
			result.Aborted = true
		}
	} else {
		result.Success = true
		// Check if the transaction had to retry (epoch > 0 means it was restarted)
		if finalTxn != nil {
			txnProto := finalTxn.TestingCloneTxn()
			epoch := txnProto.Epoch
			// Debug: Print first few epochs to see if this is working
			if atomic.LoadInt64(&stats.totalCommitted) < 5 {
				fmt.Printf("DEBUG: Transaction epoch=%d\n", epoch)
			}
			if epoch > 0 {
				// Transaction succeeded but required retries due to conflicts
				result.Aborted = true  // Mark as aborted to indicate contention
			}
		}
	}

	stats.recordResult(result)
}

func generateTransactionKeys(numKeys int) []string {
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		var keyNum int
		if *distribution == "zipfian" {
			keyNum = zipfianKeySelection(*keyRange, *zipfianS)
		} else {
			keyNum = rand.Intn(*keyRange) + 1
		}

		if *useHashKeys {
			keys[i] = fmt.Sprintf("%s-%08x", *keyPrefix, hashKeyValue(keyNum))
		} else {
			keys[i] = fmt.Sprintf("%s-%d", *keyPrefix, keyNum)
		}
	}
	return keys
}

func zipfianKeySelection(n int, s float64) int {
	// Simplified zipfian - in production use proper implementation
	// This is just a placeholder that generates skewed distribution
	return rand.Intn(n) + 1
}

func hashKeyValue(key int) uint32 {
	// Simple hash function
	return uint32(key * 2654435761)
}

func isOpenLoopAbortError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return containsSubstring(errStr, "restart") ||
		containsSubstring(errStr, "retry") ||
		containsSubstring(errStr, "abort")
}

func containsSubstring(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func printOpenLoopResults(
	stats *OpenLoopStats,
	totalDuration time.Duration,
	warmupDuration time.Duration,
	cooldownDuration time.Duration,
) {
	stats.Lock()
	defer stats.Unlock()

	if len(stats.results) == 0 {
		fmt.Println("No results to report")
		return
	}

	// Determine measurement window
	firstResult := stats.results[0].StartTime
	warmupEnd := firstResult.Add(warmupDuration)
	measurementEnd := firstResult.Add(totalDuration).Add(-cooldownDuration)

	// Filter results to measurement window
	var measurementResults []TransactionResult
	for _, result := range stats.results {
		if result.StartTime.After(warmupEnd) && result.StartTime.Before(measurementEnd) {
			measurementResults = append(measurementResults, result)
		}
	}

	if len(measurementResults) == 0 {
		fmt.Println("No results in measurement window")
		return
	}

	// Calculate statistics
	var totalLatency time.Duration
	var latencies []time.Duration
	committed := 0
	aborted := 0
	errors := 0

	for _, result := range measurementResults {
		latency := result.EndTime.Sub(result.StartTime)
		latencies = append(latencies, latency)
		totalLatency += latency

		if result.Success {
			committed++
		} else if result.Aborted {
			aborted++
		} else {
			errors++
		}
	}

	// Sort latencies for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	measurementDuration := totalDuration - warmupDuration - cooldownDuration
	throughput := float64(committed) / measurementDuration.Seconds()
	abortRate := float64(aborted) / float64(len(measurementResults)) * 100.0

	// Calculate percentiles
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	avgLatency := totalLatency / time.Duration(len(latencies))

	fmt.Printf("\n=== Benchmark Results (Measurement Window Only) ===\n")
	fmt.Printf("Duration: %.2fs\n", measurementDuration.Seconds())
	fmt.Printf("Total operations: %d\n", len(measurementResults))
	fmt.Printf("  Committed: %d\n", committed)
	fmt.Printf("  Aborted: %d (%.2f%%)\n", aborted, abortRate)
	fmt.Printf("  Errors: %d\n", errors)
	fmt.Printf("\nThroughput: %.2f ops/sec\n", throughput)
	fmt.Printf("\nLatency:\n")
	fmt.Printf("  Average: %s\n", avgLatency)
	fmt.Printf("  P50: %s\n", p50)
	fmt.Printf("  P95: %s\n", p95)
	fmt.Printf("  P99: %s\n", p99)
	fmt.Printf("===================================================\n")
}

func splitAddrs(addrs string) []string {
	result := []string{}
	start := 0
	for i, c := range addrs {
		if c == ',' {
			if i > start {
				result = append(result, addrs[start:i])
			}
			start = i + 1
		}
	}
	if start < len(addrs) {
		result = append(result, addrs[start:])
	}
	return result
}
