// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// A mode-independent KV-layer error baseline.
//
// The juicerFailures counters in juicer_spi.go can only ever be non-zero when
// COCKROACH_JUICER=true, because they are incremented from the fork's
// interceptor callbacks. That makes them proof of interception but useless as a
// baseline: a Juicer-off run reports nothing at all, so an on-vs-off comparison
// of the conflict rate has no left-hand column to compare against. There is also
// no SQL-layer substitute — txn_restarts_count reads exactly zero for
// single-statement implicit transactions while the KV layer is returning
// thousands of TransactionRetryErrors per minute, because TxnCoordSender absorbs
// and retries them below SQL.
//
// ObserveKVResponse closes that gap. It is called from the KV server's single
// batch choke point whether or not Juicer is enabled, and classifies errors with
// the same kvFailureCounters the Juicer path uses, so every cell of an
// experiment — Juicer on or off, any rules mode — carries the identical
// failed-per-minute metric.
//
// Two deliberate differences from juicerFailures:
//
//   - Node.Batch also serves node-local batches routed through the internal
//     client adapter. Those never touch gRPC and so are invisible to the
//     interceptors, which means this baseline observes strictly more responses
//     than juicerFailures does on the same node. That is intended: it is the
//     node's whole KV error rate rather than the subset that happened to be
//     remote. Compare baseline to baseline, not baseline to juicer.
//   - Counting is unconditional so the metric cannot silently depend on a flag,
//     but it is cheap: one nil check and one atomic increment on the success
//     path, with the type switch reached only by responses that carry an error.
//     Only the minute *reporter* is gated, on COCKROACH_KV_ERRLOG.
var kvErrLogEnabled = envutil.EnvOrDefaultBool("COCKROACH_KV_ERRLOG", false)

var (
	kvBaselineResponses atomic.Uint64
	kvBaselineFailures  kvFailureCounters
	kvErrLogOnce        sync.Once
)

// ObserveKVResponse classifies one BatchResponse into the mode-independent
// KV-layer error counters, and starts the minute reporter on first use when
// COCKROACH_KV_ERRLOG is set. A nil response is ignored. Callers must invoke it
// exactly once per BatchResponse handed back to a client, or the observed count
// stops being a response count.
func ObserveKVResponse(br *kvpb.BatchResponse) {
	if br == nil {
		return
	}
	if kvErrLogEnabled {
		// sync.Once's fast path is a single atomic load once initialization has
		// run, so this is cheap enough for the batch path. It is started here
		// rather than at server construction because the reporter is only
		// meaningful on a node that actually serves batches.
		startKVErrLogReporter()
	}
	kvBaselineResponses.Add(1)
	if br.Error == nil {
		return
	}
	kvBaselineFailures.recordFailure(br.Error)
}

// startKVErrLogReporter logs KV-layer error deltas once a minute under the
// "kvbaseline:" prefix, deliberately distinct from the "juicer:" prefix so a
// single log grep separates the mode-independent baseline from the interception
// proof. The goroutine runs for the process lifetime; it holds no resources
// beyond a ticker and is only started when the operator asked for it.
func startKVErrLogReporter() {
	kvErrLogOnce.Do(func() {
		ctx := context.Background()
		log.Dev.Infof(ctx, "kvbaseline: KV-layer error reporting enabled (COCKROACH_KV_ERRLOG)")
		go func() {
			var lastResp uint64
			var last kvFailureSnapshot
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				r := kvBaselineResponses.Load()
				f := kvBaselineFailures.snapshot()
				log.Dev.Infof(ctx,
					"kvbaseline: batch responses: +%d observed, +%d failed (wto=%d retry=%d[serializable=%d] aborted=%d lock=%d uncert=%d other=%d) (totals %d/%d)%s",
					r-lastResp,
					f.total-last.total,
					f.writeTooOld-last.writeTooOld,
					f.retry-last.retry,
					f.retrySerializable-last.retrySerializable,
					f.aborted-last.aborted,
					f.lockConflict-last.lockConflict,
					f.uncertainty-last.uncertainty,
					f.other-last.other,
					r, f.total,
					kvOtherBreakdown(f, last))
				lastResp, last = r, f
			}
		}()
	})
}
