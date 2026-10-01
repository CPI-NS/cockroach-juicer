package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	crdberrors "github.com/cockroachdb/errors"
)

const schemaVersion = "juicerbench.v1"

type emitter struct {
	mu                           sync.Mutex
	f                            *os.File
	enc                          *json.Encoder
	client, region, cell, config string
	err                          error
	cancel                       context.CancelFunc
}

func newEmitter(path, client, region, cell, config string) (*emitter, error) {
	f := os.Stdout
	if path != "" && path != "-" {
		var e error
		f, e = os.Create(path)
		if e != nil {
			return nil, e
		}
	}
	return &emitter{f: f, enc: json.NewEncoder(f), client: client, region: region, cell: cell, config: config}, nil
}
func (e *emitter) emit(typ string, v map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v["schema"] = schemaVersion
	v["type"] = typ
	v["ts_unix_ns"] = time.Now().UnixNano()
	if e.client != "" {
		v["client_id"] = e.client
	}
	if e.region != "" {
		v["region"] = e.region
	}
	if e.cell != "" {
		v["cell_id"] = e.cell
	}
	if e.config != "" {
		v["config_label"] = e.config
	}
	if e.err == nil {
		e.err = e.enc.Encode(v)
	}
	if e.err != nil && e.cancel != nil {
		e.cancel()
	}
}
func (e *emitter) Err() error                          { e.mu.Lock(); defer e.mu.Unlock(); return e.err }
func (e *emitter) setCancel(cancel context.CancelFunc) { e.mu.Lock(); e.cancel = cancel; e.mu.Unlock() }
func (e *emitter) close() {
	if e.f != os.Stdout {
		_ = e.f.Close()
	}
}

type baseFlags struct {
	addrs, certs, cluster, prefix, output, client, region, cell, config string
	insecure                                                            bool
	seed                                                                uint64
}

func addBase(fs *flag.FlagSet, b *baseFlags) {
	fs.StringVar(&b.addrs, "addrs", "localhost:26257", "comma-separated KV RPC addresses")
	fs.BoolVar(&b.insecure, "insecure", false, "use insecure RPC")
	fs.StringVar(&b.certs, "certs-dir", "", "certificate directory")
	fs.StringVar(&b.cluster, "cluster-name", "", "expected cluster name")
	fs.StringVar(&b.prefix, "prefix", "/juicerbench/c34/", "raw KV key prefix")
	fs.StringVar(&b.output, "output", "-", "JSONL output")
	fs.StringVar(&b.client, "client-id", "client-0", "")
	fs.StringVar(&b.region, "region", "unknown", "")
	fs.StringVar(&b.cell, "cell-id", "", "")
	fs.StringVar(&b.config, "config-label", "", "")
	fs.Uint64Var(&b.seed, "seed", 1, "")
}
func config(b baseFlags) clientConfig {
	return clientConfig{strings.Split(b.addrs, ","), b.insecure, b.certs, b.cluster}
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: juicerbench init|run [flags]")
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = initCommand(os.Args[2:])
	case "run":
		err = runCommand(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatalf("%v", err)
	}
}
func fatalf(f string, a ...any) { fmt.Fprintf(os.Stderr, "juicerbench: "+f+"\n", a...); os.Exit(1) }

func initCommand(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	var b baseFlags
	addBase(fs, &b)
	keys := fs.Int64("keys", 1_000_000, "")
	splits := fs.Int("splits", 30, "")
	batch := fs.Int("batch-size", 1000, "")
	reset := fs.Bool("reset", false, "")
	scatter := fs.Bool("scatter", true, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	out, err := newEmitter(b.output, b.client, b.region, b.cell, b.config)
	if err != nil {
		return err
	}
	defer out.close()
	ctx := context.Background()
	c, err := newClient(ctx, config(b))
	if err != nil {
		return err
	}
	defer c.close()
	p := []byte(b.prefix)
	if *reset {
		if _, err = c.db.DelRange(ctx, p, prefixEnd(p), false); err != nil {
			return err
		}
	}
	for start := int64(0); start < *keys; start += int64(*batch) {
		ba := c.db.NewBatch()
		end := start + int64(*batch)
		if end > *keys {
			end = *keys
		}
		for i := start; i < end; i++ {
			ba.Put(encodeKey(p, i), make([]byte, 16))
		}
		if err := c.db.Run(ctx, ba); err != nil {
			return fmt.Errorf("init batch at %d: %w", start, err)
		}
	}
	var splitKeys []string
	for i := 1; i <= *splits; i++ {
		n := int64(i) * *keys / int64(*splits+1)
		k := roachpb.Key(encodeKey(p, n))
		if err := c.db.AdminSplit(ctx, k, hlc.MaxTimestamp); err != nil {
			return err
		}
		splitKeys = append(splitKeys, fmt.Sprintf("%x", []byte(k)))
		if *scatter {
			if _, err := c.db.AdminScatter(ctx, k, 0); err != nil {
				return err
			}
		}
	}
	out.emit("init_summary", map[string]any{"keys": *keys, "prefix": b.prefix, "splits": *splits, "split_keys": splitKeys, "scatter_requested": *scatter, "seed": b.seed})
	return out.Err()
}

type counters struct {
	started, completed, commits, failures, restarts, readOps, writeOps atomic.Int64
	mu                                                                 sync.Mutex
	measureLat, firstLat, retriedLat                                   []int64
	failBy, retryBy                                                    map[string]int64
	active                                                             map[uint64]time.Time
	measureCommits, measureFailures, measureRetries                    int64
	measureStarted, measureReadOps, measureWriteOps                    int64
	drainCommits, drainFailures, drainCancelled                        int64
	t0Active, t1Unfinished                                             int
	t0Age, t1Age                                                       latencySummary
	jitterT0, jitterT1                                                 rpc.DataRPCJitterMetrics
	metricsT0, metricsT1                                               txnMetricSnapshot
	exactT0Ages, exactT1Ages                                           []int64
}

func newCounters() *counters {
	return &counters{failBy: map[string]int64{}, retryBy: map[string]int64{}, active: map[uint64]time.Time{}}
}
func (c *counters) recordStart(id uint64, at time.Time, measure bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active[id] = at
	if measure {
		c.measureStarted++
	}
}
func (c *counters) recordRetry(reason string, measure bool) {
	if !measure {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retryBy[reason]++
	c.measureRetries++
}
func (c *counters) recordEnd(id uint64, start, end time.Time, w window) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.active, id)
	if activeAt(start, end, w.T0) {
		c.exactT0Ages = append(c.exactT0Ages, w.T0.Sub(start).Nanoseconds())
	}
	if activeAt(start, end, w.T1) {
		c.exactT1Ages = append(c.exactT1Ages, w.T1.Sub(start).Nanoseconds())
	}
}
func (c *counters) recordOutcome(phase string, reads, writes int, latency int64, attempts int, reason string, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if phase == "measure" {
		c.measureReadOps += int64(reads)
		c.measureWriteOps += int64(writes)
		if success {
			c.measureCommits++
			c.measureLat = append(c.measureLat, latency)
			if attempts == 1 {
				c.firstLat = append(c.firstLat, latency)
			} else {
				c.retriedLat = append(c.retriedLat, latency)
			}
		} else {
			c.measureFailures++
			c.failBy[reason]++
		}
	} else if phase == "drain" {
		if success {
			c.drainCommits++
		} else if reason == "cancelled" {
			c.drainCancelled++
		} else {
			c.drainFailures++
		}
	}
}
func (c *counters) recordWindow(phase string, ages []int64, jitter rpc.DataRPCJitterMetrics, metrics txnMetricSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if phase == "t0" {
		c.t0Active, c.t0Age, c.jitterT0, c.metricsT0 = len(ages), summarize(ages), jitter, metrics
	} else if phase == "t1" {
		c.t1Unfinished, c.t1Age, c.jitterT1, c.metricsT1 = len(ages), summarize(ages), jitter, metrics
	}
}
func classify(err error) string {
	if err == nil {
		return ""
	}
	if crdberrors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
		return "transaction_retry_proto_refresh"
	}
	if crdberrors.HasType(err, (*kvpb.TransactionAbortedError)(nil)) {
		return "transaction_aborted"
	}
	if crdberrors.HasType(err, (*kvpb.WriteTooOldError)(nil)) {
		return "write_too_old"
	}
	if crdberrors.HasType(err, (*kvpb.ReadWithinUncertaintyIntervalError)(nil)) {
		return "read_within_uncertainty"
	}
	s := err.Error()
	for _, x := range []struct{ k, v string }{{"TransactionRetry", "transaction_retry"}, {"WriteTooOld", "write_too_old"}, {"context deadline", "timeout"}, {"context canceled", "cancelled"}, {"connection", "connection"}} {
		if strings.Contains(s, x.k) {
			return x.v
		}
	}
	return "other"
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var b baseFlags
	addBase(fs, &b)
	keys := fs.Int64("keys", 1_000_000, "")
	ops := fs.Int("ops-per-txn", 5, "")
	readPct := fs.Float64("read-pct", 50, "")
	theta := fs.Float64("zipf-theta", .99, "")
	scramble := fs.Bool("key-scramble", true, "")
	conc := fs.Int("concurrency", 1, "")
	startRaw := fs.String("start-at", "", "RFC3339Nano UTC")
	warm := fs.Duration("warmup", 30*time.Second, "")
	measure := fs.Duration("measure", 60*time.Second, "")
	drain := fs.Duration("drain", 30*time.Second, "")
	timeout := fs.Duration("txn-timeout", 30*time.Second, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var start time.Time
	if *startRaw != "" {
		var err error
		start, err = time.Parse(time.RFC3339Nano, *startRaw)
		if err != nil {
			return err
		}
	}
	out, err := newEmitter(b.output, b.client, b.region, b.cell, b.config)
	if err != nil {
		return err
	}
	defer out.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out.setCancel(cancel)
	c, err := newClient(ctx, config(b))
	if err != nil {
		return err
	}
	defer c.close()
	generators := make([]*specGenerator, *conc)
	for worker := 0; worker < *conc; worker++ {
		g, e := newSpecGenerator(*keys, *ops, *readPct, *theta, workerSeed(b.seed, b.client, worker), b.prefix, *scramble)
		if e != nil {
			return e
		}
		g.nextID = uint64(worker) << 48
		generators[worker] = g
	}
	readyAt := time.Now()
	if *startRaw == "" {
		start = readyAt.Add(2 * time.Second)
	} else if err := validateStartReady(start, readyAt); err != nil {
		out.emit("startup_failure", map[string]any{"reason": "client_ready_after_start", "start_ns": start.UnixNano(), "client_ready_ns": readyAt.UnixNano(), "lateness_ns": readyAt.Sub(start).Nanoseconds(), "error": err.Error()})
		if emitErr := out.Err(); emitErr != nil {
			return emitErr
		}
		return err
	}
	w := makeWindow(start, *warm, *measure, *drain)
	out.emit("startup_ready", map[string]any{"start_ns": start.UnixNano(), "client_ready_ns": readyAt.UnixNano(), "lead_ns": start.Sub(readyAt).Nanoseconds(), "warmup_ns": (*warm).Nanoseconds(), "measure_ns": (*measure).Nanoseconds()})
	if d := time.Until(start); d > 0 {
		time.Sleep(d)
	}
	startedAt := time.Now()
	out.emit("startup_begin", map[string]any{"start_ns": start.UnixNano(), "observed_ns": startedAt.UnixNano(), "timer_lateness_ns": startedAt.Sub(start).Nanoseconds()})
	ct := newCounters()
	var wg sync.WaitGroup
	for _, g := range generators {
		wg.Add(1)
		go func(g *specGenerator) {
			defer wg.Done()
			for time.Now().Before(w.T1) && ctx.Err() == nil {
				if ctx.Err() != nil {
					return
				}
				spec := g.next()
				execute(ctx, c.db, spec, *timeout, w, out, ct)
			}
		}(g)
	}
	go progressLoop(w, out, ct)
	go func() {
		if d := time.Until(w.T0); d > 0 {
			time.Sleep(d)
		}
		emitWindow("t0", w, out, ct, c)
	}()
	timer := time.NewTimer(time.Until(w.T1))
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		wg.Wait()
		return out.Err()
	}
	emitWindow("t1", w, out, ct, c)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(*drain):
		cancel()
		<-done
	}
	emitSummary(w, out, ct, c.wire, *keys, *ops, *readPct, *theta, *conc, b.seed)
	// The 1 Hz external resource observer needs a post-T1 process sample to
	// bracket cumulative CPU counters. Keep the fully idle process alive through
	// T1+3s; time already spent draining counts toward this tail.
	if wait := observationTailWait(time.Now(), w.T1, 3*time.Second); wait > 0 {
		time.Sleep(wait)
	}
	return out.Err()
}
func workerSeed(seed uint64, client string, worker int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(client))
	return seed ^ h.Sum64() ^ (uint64(worker) * 0x9e3779b97f4a7c15)
}

func execute(parent context.Context, db *kv.DB, s txnSpec, timeout time.Duration, w window, out *emitter, c *counters) {
	start := time.Now()
	c.started.Add(1)
	c.recordStart(s.ID, start, w.phase(start) == "measure")
	attempts := 0
	var lastErr error
	var previousTxnID string
	var previousEpoch int32
	ctx, cancel := context.WithTimeout(parent, timeout)
	err := db.TxnWithAdmissionControl(ctx, kvpb.AdmissionHeader_FROM_SQL, admissionpb.NormalPri, kv.SteppingEnabled, func(ctx context.Context, txn *kv.Txn) error {
		attempts++
		proto := txn.TestingCloneTxn()
		if attempts > 1 {
			eventAt := time.Now()
			reason := classify(lastErr)
			c.restarts.Add(1)
			c.recordRetry(reason, w.phase(eventAt) == "measure")
			out.emit("retry", map[string]any{"txn_id": s.ID, "event_ns": eventAt.UnixNano(), "attempt": attempts, "previous_kv_txn_id": previousTxnID, "new_kv_txn_id": proto.ID.String(), "previous_epoch": previousEpoch, "new_epoch": proto.Epoch, "reason_class": reason, "reason": fmt.Sprint(lastErr)})
		}
		previousTxnID, previousEpoch = proto.ID.String(), int32(proto.Epoch)
		txn.SetBufferedWritesEnabled(false)
		ba := txn.NewBatch()
		for i, k := range s.Keys {
			if s.Reads[i] {
				ba.Get(k)
			} else {
				ba.Put(k, s.Values[i])
			}
		}
		lastErr = txn.CommitInBatch(ctx, ba)
		if lastErr == nil {
			for i, isRead := range s.Reads {
				if isRead && (len(ba.Results[i].Rows) != 1 || ba.Results[i].Rows[0].Value == nil) {
					lastErr = fmt.Errorf("read operation %d returned %d rows", i, len(ba.Results[i].Rows))
					break
				}
			}
		}
		return lastErr
	})
	cancel()
	end := time.Now()
	c.completed.Add(1)
	c.recordEnd(s.ID, start, end, w)
	reads := 0
	for _, r := range s.Reads {
		if r {
			reads++
		}
	}
	writes := len(s.Reads) - reads
	c.readOps.Add(int64(reads))
	c.writeOps.Add(int64(writes))
	phase := w.phase(end)
	outcome := "commit"
	reason := ""
	if err != nil {
		outcome = "final_failure"
		reason = classify(err)
		c.failures.Add(1)
	} else {
		c.commits.Add(1)
	}
	shape := "rw"
	if reads == 0 {
		shape = "write_only"
	} else if writes == 0 {
		shape = "read_only"
	}
	lat := end.Sub(start).Nanoseconds()
	c.recordOutcome(phase, reads, writes, lat, attempts, reason, err == nil)
	out.emit("txn", map[string]any{"txn_id": s.ID, "start_ns": start.UnixNano(), "end_ns": end.UnixNano(), "completion_phase": phase, "latency_ns": lat, "outcome": outcome, "attempts": attempts, "restarts": attempts - 1, "read_ops": reads, "write_ops": writes, "shape": shape, "final_error_class": reason, "final_error": fmt.Sprint(err)})
}

func activeAges(c *counters) []int64 {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	v := make([]int64, 0, len(c.active))
	for _, s := range c.active {
		v = append(v, now.Sub(s).Nanoseconds())
	}
	return v
}
func emitWindow(phase string, w window, out *emitter, c *counters, client *kvClient) {
	a := activeAges(c)
	snap := rpc.JuicerDataRPCJitterSnapshot()
	ms := client.metricSnapshot()
	observed := time.Now()
	c.recordWindow(phase, a, snap, ms)
	out.emit("window", map[string]any{"phase": phase, "scheduled_start_ns": w.Start.UnixNano(), "t0_ns": w.T0.UnixNano(), "t1_ns": w.T1.UnixNano(), "observed_ns": observed.UnixNano(), "snapshot_lateness_ns": func() int64 {
		if phase == "t0" {
			return observed.Sub(w.T0).Nanoseconds()
		}
		return observed.Sub(w.T1).Nanoseconds()
	}(), "active_observed": len(a), "active_age_observed_ns": summarize(a), "data_rpc_jitter_cumulative": snap, "txn_metrics_cumulative": ms})
}
func progressLoop(w window, out *emitter, c *counters) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for now := range t.C {
		if now.After(w.DrainEnd) {
			return
		}
		out.emit("progress", map[string]any{"phase": w.phase(now), "started": c.started.Load(), "completed": c.completed.Load(), "commits": c.commits.Load(), "final_failures": c.failures.Load(), "restarts": c.restarts.Load(), "active": len(activeAges(c)), "read_ops": c.readOps.Load(), "write_ops": c.writeOps.Load()})
	}
}
func emitSummary(w window, out *emitter, c *counters, wire *wireCounters, keys int64, ops int, readPct, theta float64, conc int, seed uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t0Active, t1Active := len(c.exactT0Ages), len(c.exactT1Ages)
	lhs := int64(t0Active) + c.measureStarted
	rhs := c.measureCommits + c.measureFailures + int64(t1Active)
	out.emit("summary", map[string]any{"window": map[string]any{"t0_ns": w.T0.UnixNano(), "t1_ns": w.T1.UnixNano()}, "params": map[string]any{"keys": keys, "ops_per_txn": ops, "read_pct": readPct, "zipf_theta": theta, "concurrency": conc, "seed": seed}, "logical_started": c.measureStarted, "commits": c.measureCommits, "final_failures": c.measureFailures, "retry_events": c.measureRetries, "read_ops": c.measureReadOps, "write_ops": c.measureWriteOps, "first_success_count": int64(len(c.firstLat)), "retried_success_count": int64(len(c.retriedLat)), "t0_active": t0Active, "t0_active_age_ns": summarize(c.exactT0Ages), "t1_unfinished": t1Active, "t1_active_age_ns": summarize(c.exactT1Ages), "window_identity_lhs": lhs, "window_identity_rhs": rhs, "window_identity_ok": lhs == rhs, "data_rpc_jitter_t0": c.jitterT0, "data_rpc_jitter_t1": c.jitterT1, "data_rpc_jitter_window_delta": jitterDelta(c.jitterT0, c.jitterT1), "txn_metrics_t0": c.metricsT0, "txn_metrics_t1": c.metricsT1, "txn_metrics_window_delta": txnMetricDelta(c.metricsT0, c.metricsT1), "commits_all_phases": c.commits.Load(), "final_failures_all_phases": c.failures.Load(), "retry_events_all_phases": c.restarts.Load(), "read_ops_all_phases": c.readOps.Load(), "write_ops_all_phases": c.writeOps.Load(), "latency_ns": summarize(c.measureLat), "first_success_latency_ns": summarize(c.firstLat), "retried_success_latency_ns": summarize(c.retriedLat), "failures_by_reason": c.failBy, "retries_by_reason": c.retryBy, "drain_commits": c.drainCommits, "drain_failures": c.drainFailures, "drain_cancelled": c.drainCancelled, "wire_summary_all_phases": map[string]any{"logical_batches": wire.batches.Load(), "mixed_get_put_batches": wire.mixed.Load(), "batches_with_end_txn": wire.withEnd.Load()}})
}
func txnMetricDelta(a, b txnMetricSnapshot) txnMetricSnapshot {
	return txnMetricSnapshot{b.Commits - a.Commits, b.Aborts - a.Aborts, b.OnePC - a.OnePC, b.Parallel - a.Parallel, b.WriteTooOld - a.WriteTooOld, b.Serializable - a.Serializable, b.AsyncWrite - a.AsyncWrite, b.Deadline - a.Deadline, b.Uncertainty - a.Uncertainty, b.Exclusion - a.Exclusion, b.TxnAborted - a.TxnAborted, b.TxnPush - a.TxnPush, b.Unknown - a.Unknown}
}

func jitterDelta(a, b rpc.DataRPCJitterMetrics) rpc.DataRPCJitterMetrics {
	d := b
	d.DataBatches -= a.DataBatches
	d.MixedDataBatches -= a.MixedDataBatches
	d.ReadOnlyDataBatches -= a.ReadOnlyDataBatches
	d.WriteOnlyDataBatches -= a.WriteOnlyDataBatches
	d.GetOps -= a.GetOps
	d.WriteOps -= a.WriteOps
	d.ControlBatches -= a.ControlBatches
	d.EndTxnBatches -= a.EndTxnBatches
	d.Scheduled -= a.Scheduled
	d.Ready -= a.Ready
	d.Cancelled -= a.Cancelled
	d.RequestedNS -= a.RequestedNS
	d.ActualNS -= a.ActualNS
	// Cumulative extrema cannot be differenced into window extrema.
	d.ActualMinNS, d.ActualMaxNS = 0, 0
	for i := range d.ActualBuckets {
		d.ActualBuckets[i] -= a.ActualBuckets[i]
	}
	return d
}
