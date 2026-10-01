// Copyright 2026 The Cockroach Authors.
// Use of this software is governed by the CockroachDB Software License.

package rpc

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This experiment-only transport perturbation is independent of Juicer's
// enablement. The caller invokes it AFTER constructing/stamping each outgoing
// range batch and BEFORE invoking Batch, including each transport resend.
var dataJitter = newDataRPCJitter(
	envutil.EnvOrDefaultInt("COCKROACH_JUICER_DATA_JITTER_MIN_US", 0),
	envutil.EnvOrDefaultInt("COCKROACH_JUICER_DATA_JITTER_MAX_US", 0),
	int64(envutil.EnvOrDefaultInt("COCKROACH_JUICER_DATA_JITTER_SEED", 1)),
)

var dataRPCTraceEnabled = envutil.EnvOrDefaultBool("COCKROACH_JUICER_RPC_TRACE", false)

// DataRPCJitterMetrics contains cumulative process counters. Wall-clock sleep
// includes timer/scheduler overshoot; it is not CPU time. Buckets are disjoint.
type DataRPCJitterMetrics struct {
	DataBatches          uint64    `json:"data_batch_attempts"`
	MixedDataBatches     uint64    `json:"mixed_data_batch_attempts"`
	ReadOnlyDataBatches  uint64    `json:"read_only_data_batch_attempts"`
	WriteOnlyDataBatches uint64    `json:"write_only_data_batch_attempts"`
	GetOps               uint64    `json:"get_ops_in_data_attempts"`
	WriteOps             uint64    `json:"write_ops_in_data_attempts"`
	ControlBatches       uint64    `json:"control_only_batch_attempts"`
	EndTxnBatches        uint64    `json:"batch_attempts_with_end_txn"`
	Scheduled            uint64    `json:"scheduled"`
	Ready                uint64    `json:"ready_to_send"`
	Cancelled            uint64    `json:"cancelled"`
	RequestedNS          uint64    `json:"requested_ns"`
	ActualNS             uint64    `json:"actual_ns"`
	ActualMinNS          uint64    `json:"actual_min_ns"`
	ActualMaxNS          uint64    `json:"actual_max_ns"`
	ActualBuckets        [6]uint64 `json:"actual_buckets_le_20_200_500_1000_5000us_over"`
	MinUS                int       `json:"min_us"`
	MaxUS                int       `json:"max_us"`
}

type dataRPCJitter struct {
	mu           sync.Mutex
	rng          *rand.Rand
	minUS, maxUS int
	stats        DataRPCJitterMetrics
	once         sync.Once
}

func newDataRPCJitter(minUS, maxUS int, seed int64) *dataRPCJitter {
	if minUS < 0 || maxUS < minUS || (minUS == 0 && maxUS != 0) {
		panic("data RPC jitter requires 0/0 or 0 < min <= max")
	}
	return &dataRPCJitter{rng: rand.New(rand.NewSource(seed)), minUS: minUS, maxUS: maxUS,
		stats: DataRPCJitterMetrics{MinUS: minUS, MaxUS: maxUS}}
}

func isJitterDataBatch(ba *kvpb.BatchRequest) bool {
	if ba == nil || ba.Txn == nil {
		return false
	}
	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *kvpb.GetRequest, *kvpb.ScanRequest, *kvpb.ReverseScanRequest,
			*kvpb.PutRequest, *kvpb.ConditionalPutRequest,
			*kvpb.IncrementRequest, *kvpb.DeleteRequest, *kvpb.DeleteRangeRequest:
			return true
		}
	}
	return false
}

// JuicerDataRPCDelay does not mutate the batch or its timestamps. Control-only
// traffic, including EndTxn/PushTxn/ResolveIntent, is not independently delayed.
func JuicerDataRPCDelay(ctx context.Context, ba *kvpb.BatchRequest) error {
	if dataJitter.maxUS == 0 && !dataRPCTraceEnabled {
		return nil
	}
	if ba == nil || ba.Txn == nil {
		return nil
	}
	dataJitter.recordShape(ba)
	if dataJitter.maxUS == 0 || !isJitterDataBatch(ba) {
		return nil
	}
	dataJitter.once.Do(func() {
		log.Dev.Infof(ctx, "juicer-jitter: enabled min_us=%d max_us=%d after_timestamp=true", dataJitter.minUS, dataJitter.maxUS)
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for range t.C {
				b, _ := json.Marshal(JuicerDataRPCJitterSnapshot())
				log.Dev.Infof(context.Background(), "juicer-jitter: %s", b)
			}
		}()
	})
	return dataJitter.wait(ctx)
}

// These are transport invocation attempts after message construction, including
// resends and attempts cancelled during jitter. They are not logical batches or
// completed transactions. The observer is identical for off/on treatments.
func (j *dataRPCJitter) recordShape(ba *kvpb.BatchRequest) {
	var gets, writes uint64
	var end bool
	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *kvpb.GetRequest, *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
			gets++
		case *kvpb.PutRequest, *kvpb.ConditionalPutRequest, *kvpb.IncrementRequest, *kvpb.DeleteRequest, *kvpb.DeleteRangeRequest:
			writes++
		case *kvpb.EndTxnRequest:
			end = true
		}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if gets+writes > 0 {
		j.stats.DataBatches++
		j.stats.GetOps += gets
		j.stats.WriteOps += writes
		switch {
		case gets > 0 && writes > 0:
			j.stats.MixedDataBatches++
		case gets > 0:
			j.stats.ReadOnlyDataBatches++
		default:
			j.stats.WriteOnlyDataBatches++
		}
	} else {
		j.stats.ControlBatches++
	}
	if end {
		j.stats.EndTxnBatches++
	}
}

func (j *dataRPCJitter) wait(ctx context.Context) error {
	if j.maxUS == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d := j.schedule()
	start := time.Now()
	t := time.NewTimer(d)
	defer t.Stop()
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-t.C:
		err = ctx.Err()
	}
	actual := uint64(time.Since(start))
	j.mu.Lock()
	defer j.mu.Unlock()
	if err != nil {
		j.stats.Cancelled++
	} else {
		j.stats.Ready++
	}
	j.stats.ActualNS += actual
	if j.stats.ActualMinNS == 0 || actual < j.stats.ActualMinNS {
		j.stats.ActualMinNS = actual
	}
	if actual > j.stats.ActualMaxNS {
		j.stats.ActualMaxNS = actual
	}
	i := 0
	for _, us := range []uint64{20, 200, 500, 1000, 5000} {
		if actual <= us*1000 {
			break
		}
		i++
	}
	j.stats.ActualBuckets[i]++
	return err
}

func (j *dataRPCJitter) schedule() time.Duration {
	j.mu.Lock()
	defer j.mu.Unlock()
	d := time.Duration(j.minUS+j.rng.Intn(j.maxUS-j.minUS+1)) * time.Microsecond
	j.stats.Scheduled++
	j.stats.RequestedNS += uint64(d)
	return d
}

// JuicerDataRPCJitterSnapshot is used at exact benchmark window boundaries as
// well as by the periodic process reporter.
func JuicerDataRPCJitterSnapshot() DataRPCJitterMetrics {
	dataJitter.mu.Lock()
	defer dataJitter.mu.Unlock()
	return dataJitter.stats
}
