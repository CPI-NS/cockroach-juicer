// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// InitDataConfig holds configuration parameters for data initialization.
type InitDataConfig struct {
	// KeyPrefix is the prefix for keys, e.g., "key".
	KeyPrefix string

	// NumKeys is the number of keys to initialize.
	NumKeys int

	// KeyRange is the range for generating key IDs (1 to KeyRange).
	KeyRange int

	// BatchSize is the batch size for writes (used by method 1 and method 3).
	BatchSize int

	// Concurrency is the number of concurrent writer goroutines (used by method 3).
	Concurrency int

	// UseBulkAdder specifies whether to use high-performance BulkAdder (method 2).
	UseBulkAdder bool

	// BulkAdderBufferSize is the buffer size for BulkAdder in bytes.
	BulkAdderBufferSize int64
}

// InitData initializes test data, providing three methods:
// 1. Simple batch writes (UseBulkAdder=false, Concurrency=1)
// 2. High-performance BulkAdder (UseBulkAdder=true)
// 3. Concurrent batch writes (UseBulkAdder=false, Concurrency>1)
func InitData(ctx context.Context, db *kv.DB, cfg InitDataConfig) error {
	if cfg.NumKeys <= 0 {
		return errors.New("NumKeys must be > 0")
	}
	if cfg.KeyRange <= 0 {
		cfg.KeyRange = cfg.NumKeys
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BulkAdderBufferSize <= 0 {
		cfg.BulkAdderBufferSize = 128 << 20 // 128MB
	}

	fmt.Printf("Initializing %d keys with prefix '%s' (range: 1-%d)...\n",
		cfg.NumKeys, cfg.KeyPrefix, cfg.KeyRange)
	os.Stdout.Sync() // Force flush

	start := time.Now()

	var err error
	if cfg.UseBulkAdder {
		err = initDataWithBulkAdder(ctx, db, cfg)
	} else if cfg.Concurrency > 1 {
		err = initDataConcurrent(ctx, db, cfg)
	} else {
		err = initDataSimple(ctx, db, cfg)
	}

	if err != nil {
		return err
	}

	elapsed := time.Since(start)
	fmt.Printf("✓ Initialized %d keys in %s (%.2f keys/sec)\n",
		cfg.NumKeys, elapsed, float64(cfg.NumKeys)/elapsed.Seconds())

	return nil
}

// initDataSimple implements method 1: simple batch writes.
func initDataSimple(ctx context.Context, db *kv.DB, cfg InitDataConfig) error {
	fmt.Println("Using simple batch writes...")

	keysWritten := 0
	for keysWritten < cfg.NumKeys {
		batch := db.NewBatch()
		batchSize := cfg.BatchSize
		if keysWritten+batchSize > cfg.NumKeys {
			batchSize = cfg.NumKeys - keysWritten
		}

		for i := 0; i < batchSize; i++ {
			keyID := (keysWritten + i) % cfg.KeyRange
			if keyID == 0 {
				keyID = cfg.KeyRange
			}
			key := roachpb.Key(fmt.Sprintf("%s-%d", cfg.KeyPrefix, keyID))
			value := fmt.Sprintf("init-value-%d", keysWritten+i)
			batch.Put(key, value)
		}

		if err := db.Run(ctx, batch); err != nil {
			return errors.Wrapf(err, "failed to write batch at offset %d", keysWritten)
		}

		keysWritten += batchSize
		if keysWritten%1000 == 0 {
			fmt.Printf("  Progress: %d/%d keys (%.1f%%)\n",
				keysWritten, cfg.NumKeys, float64(keysWritten)/float64(cfg.NumKeys)*100)
		}
	}

	return nil
}

// initDataConcurrent implements method 3: concurrent batch writes.
func initDataConcurrent(ctx context.Context, db *kv.DB, cfg InitDataConfig) error {
	fmt.Printf("Using concurrent batch writes (%d goroutines)...\n", cfg.Concurrency)
	fmt.Println("Starting workers...")
	os.Stdout.Sync() // Force flush

	type workItem struct {
		startIdx int
		count    int
	}

	workChan := make(chan workItem, cfg.Concurrency*2)
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // Only need buffer of 1 - we stop on first error

	// Create a cancellable context so we can abort workers on error
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track progress
	var progressMu sync.Mutex
	keysProcessed := 0

	// Start worker goroutines
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return // Context cancelled, exit cleanly
				case item, ok := <-workChan:
					if !ok {
						return // Channel closed, no more work
					}

					batch := db.NewBatch()
					for j := 0; j < item.count; j++ {
						keyID := (item.startIdx + j) % cfg.KeyRange
						if keyID == 0 {
							keyID = cfg.KeyRange
						}
						key := roachpb.Key(fmt.Sprintf("%s-%d", cfg.KeyPrefix, keyID))
						value := fmt.Sprintf("init-value-%d", item.startIdx+j)
						batch.Put(key, value)
					}

					// Use worker context so this can be cancelled
					if err := db.Run(workerCtx, batch); err != nil {
						// Send error and cancel context to abort other workers
						select {
						case errChan <- errors.Wrapf(err, "failed to write batch at offset %d (worker %d)", item.startIdx, workerID):
						default:
							// Error channel full, another error already reported
						}
						cancel() // Cancel context to stop other workers
						return
					}

					// Update progress
					func() {
						progressMu.Lock()
						defer progressMu.Unlock()
						keysProcessed += item.count
						if keysProcessed%10000 == 0 || keysProcessed >= cfg.NumKeys {
							fmt.Printf("  Progress: %d/%d keys (%.1f%%)\n",
								keysProcessed, cfg.NumKeys, float64(keysProcessed)/float64(cfg.NumKeys)*100)
							os.Stdout.Sync() // Force flush immediately
						}
					}()
				}
			}
		}(i)
	}

	// Distribute work
	go func() {
		defer close(workChan)
		keysWritten := 0
		for keysWritten < cfg.NumKeys {
			// Check if context cancelled (error occurred)
			select {
			case <-workerCtx.Done():
				return // Stop distributing work
			default:
			}

			batchSize := cfg.BatchSize
			if keysWritten+batchSize > cfg.NumKeys {
				batchSize = cfg.NumKeys - keysWritten
			}

			select {
			case workChan <- workItem{startIdx: keysWritten, count: batchSize}:
				keysWritten += batchSize
			case <-workerCtx.Done():
				return // Context cancelled, stop distributing
			}
		}
	}()

	// Wait for completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errChan:
		cancel()   // Ensure context is cancelled
		wg.Wait()  // Wait for all workers to exit
		return err
	case <-done:
		return nil
	case <-ctx.Done():
		cancel()   // Cancel worker context
		wg.Wait()  // Wait for all workers to exit
		return ctx.Err()
	}
}

// initDataWithBulkAdder implements method 2: using high-performance BulkAdder (SST batch import).
func initDataWithBulkAdder(ctx context.Context, db *kv.DB, cfg InitDataConfig) error {
	fmt.Println("Using BulkAdder (SST batch import)...")

	// Get DistSender and RangeCache
	ds := db.NonTransactionalSender()
	distSender, ok := ds.(*kvcoord.DistSender)
	if !ok {
		return errors.New("failed to get DistSender from DB")
	}

	rangeCache := rangecache.NewRangeCache(
		cluster.MakeTestingClusterSettings(),
		distSender,
		func() int64 { return 2 << 20 }, // 2MB cache size
		db.Context().Stopper,
	)

	// Create memory monitor
	bulkMon := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name: mon.MakeName("benchmark-init-data"),
	})
	defer bulkMon.Stop(ctx)

	// Create concurrency limiter
	sendLimiter := limit.MakeConcurrentRequestLimiter("bulk-send", 10)

	// Create BulkAdder
	bulkAdder, err := bulk.MakeBulkAdder(
		ctx,
		db,
		rangeCache,
		cluster.MakeTestingClusterSettings(),
		hlc.Timestamp{}, // Use current time
		kvserverbase.BulkAdderOptions{
			Name:          "benchmark-init",
			MaxBufferSize: func() int64 { return cfg.BulkAdderBufferSize },
			MinBufferSize: 32 << 20, // 32MB
		},
		bulkMon,
		sendLimiter,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create BulkAdder")
	}
	defer bulkAdder.Close(ctx)

	// Write data
	for i := 0; i < cfg.NumKeys; i++ {
		keyID := (i + 1) % cfg.KeyRange
		if keyID == 0 {
			keyID = cfg.KeyRange
		}
		key := roachpb.Key(fmt.Sprintf("%s-%d", cfg.KeyPrefix, keyID))
		value := roachpb.MakeValueFromString(fmt.Sprintf("init-value-%d", i))
		value.InitChecksum(key)

		if err := bulkAdder.Add(ctx, key, value.RawBytes); err != nil {
			return errors.Wrapf(err, "failed to add key at index %d", i)
		}

		// Periodically flush (BulkAdder also auto-flushes)
		if (i+1)%1000 == 0 {
			if err := bulkAdder.Flush(ctx); err != nil {
				return errors.Wrapf(err, "failed to flush at index %d", i)
			}
			fmt.Printf("  Progress: %d/%d keys (%.1f%%)\n",
				i+1, cfg.NumKeys, float64(i+1)/float64(cfg.NumKeys)*100)
		}
	}

	// Final flush
	if err := bulkAdder.Flush(ctx); err != nil {
		return errors.Wrap(err, "failed to final flush")
	}

	summary := bulkAdder.GetSummary()
	fmt.Printf("  BulkAdder summary: %d bytes written\n", summary.DataSize)

	return nil
}
