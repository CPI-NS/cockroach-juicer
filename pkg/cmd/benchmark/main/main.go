// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/benchmark"
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
	cfg := benchmark.Config{
		BootstrapAddrs: addrs,
		Insecure:       *insecure,
		SSLCertsDir:    *sslCertsDir,
		ClusterName:    *clusterName,
		User:           username.RootUserName(),
	}

	// Create client
	fmt.Printf("Connecting to cluster at %v...\n", addrs)
	db, stopper, err := benchmark.NewClient(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating client: %v\n", err)
		os.Exit(1)
	}
	defer stopper.Stop(context.Background())

	fmt.Println("✓ Connected to cluster")

	// Perform RMW operation
	keyBytes := roachpb.Key(*key)
	fmt.Printf("Performing RMW operation on key: %s\n", *key)

	if err := performRMW(ctx, db, keyBytes); err != nil {
		fmt.Fprintf(os.Stderr, "error performing RMW: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✓ RMW operation completed successfully")
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

