// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/juicer"
)

// Tests for the Juicer spec alignment (docs/eval-plan-20261019/JUICER-SPEC.md
// in the experiment workspace): defaults, startup checks, the conflict-table
// relation and the commit/abort events that end its control-class holds.

// saveJuicerSpecState snapshots every package-level knob these tests flip.
func saveJuicerSpecState() func() {
	enabled, rules, maxHold, fixed := juicerEnabled, juicerRulesEnv, juicerMaxHoldMillis, juicerFixedWaitMillis
	k, s, epoch, idle, late, diag, stageTiming := juicerFilterK, juicerFilterS, juicerEpochMillis, juicerIdleEpochs,
		juicerLatePolicy, juicerDiagnostics, juicerStageTiming
	aw := []interface{}{juicerAW, juicerAWMaxUsRaw, juicerAWMaxUsSet, juicerAWScaleRaw, juicerAWScaleSet,
		juicerAWInvRaw, juicerAWInvSet, juicerAWLaRaw, juicerAWLaSet, juicerAWLaUsRaw, juicerAWLaUsSet,
		juicerAWMinRaw, juicerAWMinSet, juicerManualQueueKeysSet, juicerHoldCalibration}
	return func() {
		juicerEnabled, juicerRulesEnv, juicerMaxHoldMillis, juicerFixedWaitMillis = enabled, rules, maxHold, fixed
		juicerFilterK, juicerFilterS, juicerEpochMillis, juicerIdleEpochs = k, s, epoch, idle
		juicerLatePolicy, juicerDiagnostics, juicerStageTiming = late, diag, stageTiming
		juicerAW = aw[0].(bool)
		juicerAWMaxUsRaw, juicerAWMaxUsSet = aw[1].(string), aw[2].(bool)
		juicerAWScaleRaw, juicerAWScaleSet = aw[3].(string), aw[4].(bool)
		juicerAWInvRaw, juicerAWInvSet = aw[5].(string), aw[6].(bool)
		juicerAWLaRaw, juicerAWLaSet = aw[7].(string), aw[8].(bool)
		juicerAWLaUsRaw, juicerAWLaUsSet = aw[9].(string), aw[10].(bool)
		juicerAWMinRaw, juicerAWMinSet = aw[11].(string), aw[12].(bool)
		juicerManualQueueKeysSet = aw[13].(bool)
		juicerHoldCalibration = aw[14].(bool)
	}
}

// Spec 12.1: the code defaults are the spec's default configuration (the
// hold limit is a placeholder until it is calibrated).
func TestJuicerSpecDefaults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if got := juicerRulesMode(); got != juicerRulesConflictTable {
		t.Errorf("RULES default = %q, want %q", got, juicerRulesConflictTable)
	}
	if juicerFilterK != 100 || juicerFilterS != 1 {
		t.Errorf("FILTER_K/S defaults = %d/%d, want 100/1", juicerFilterK, juicerFilterS)
	}
	if juicerEpochMillis != 10000 || juicerIdleEpochs != 3 {
		t.Errorf("EPOCH_MS/IDLE_EPOCHS defaults = %d/%d, want 10000/3", juicerEpochMillis, juicerIdleEpochs)
	}
	if juicerLatePolicy != "queue" {
		t.Errorf("LATE_POLICY default = %q, want queue", juicerLatePolicy)
	}
	if juicerDiagnostics {
		t.Error("DIAG default = true, want false (per-message diagnostics off)")
	}
	if juicerMaxHoldMillis != 25 || juicerFixedWaitMillis != 0 {
		t.Errorf("MAX_HOLD_MS/FIXED_WAIT_MS defaults = %d/%v, want 25/0", juicerMaxHoldMillis, juicerFixedWaitMillis)
	}
	banner := juicerSpecBanner()
	for _, want := range []string{
		"spec=1.2 ", "rules=conflict-table", "K=100", "S=1", "epoch=10000ms", "idle_epochs=3", "nominate=epoch-end",
		"max_hold=25ms", "buffer=fixed(0ms)", "late=queue", "removal=drain", "sort_key=batch-now", "diag=off",
		"hold_calibration=off", "abort_sources=failed-batch,endtxn-abort,resolve-intent-aborted",
		"caller_abandon=withdraw", "event_log=off",
		"hold_end=request(get,refresh):own_reply|own_failure|ttl;control(put,cput,inc,del,locking-get):applied(endtxn|resolve-intent|1pc-reply)|own_failure|ttl",
	} {
		if !strings.Contains(banner, want) {
			t.Errorf("spec banner %q lacks %q", banner, want)
		}
	}
}

// Spec 6.2/6.3, 2.7, 7.3: invalid settings refuse to start.
func TestJuicerSpecStartupChecks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerSpecState()()
	for _, tc := range []struct {
		name string
		set  func()
		want string
	}{
		{"zero hold limit", func() { juicerMaxHoldMillis = 0 }, "MAX_HOLD_MS"},
		{"fixed wait above the hold limit", func() { juicerMaxHoldMillis, juicerFixedWaitMillis = 2, 2.5 }, "FIXED_WAIT_MS"},
		{"zero epoch", func() { juicerEpochMillis = 0 }, "EPOCH_MS"},
		{"zero idle epochs", func() { juicerIdleEpochs = 0 }, "IDLE_EPOCHS"},
		{"unknown late policy", func() { juicerLatePolicy = "put-queue" }, "LATE_POLICY"},
		{"unknown rules mode", func() { juicerRulesEnv = "writes-read-pass" }, "RULES"},
		{"empty late policy", func() { juicerLatePolicy = "" }, "LATE_POLICY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer saveJuicerSpecState()()
			juicerEnabled = true
			tc.set()
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("invalid configuration did not panic")
				}
				if msg, _ := r.(string); !strings.Contains(msg, tc.want) {
					t.Fatalf("panic %v does not name %s", r, tc.want)
				}
			}()
			_ = juicerServerOptions()
		})
	}
	// The boundary case is accepted: a fixed wait equal to the hold limit.
	juicerEnabled, juicerMaxHoldMillis, juicerFixedWaitMillis = true, 2, 2
	if len(juicerServerOptions()) == 0 {
		t.Fatal("no options for a valid configuration")
	}
}

// Spec 5.3/5.4 for CockroachDB (table 5-B): writes and locking reads count as
// writes and are control-class; plain reads and Refresh are request-class.
func TestJuicerConflictTableRulesCRDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerSpecState()()
	juicerRulesEnv = ""
	rules := newCRDBJuicerSPI().BuildRules()
	if !rules.Enabled() || !rules.ObserveControlApplied || !rules.AdmitUnbound {
		t.Fatalf("conflict-table rules: enabled=%v observeControlApplied=%v admitUnbound=%v",
			rules.Enabled(), rules.ObserveControlApplied, rules.AdmitUnbound)
	}
	if rules.MaxHold != 25*time.Millisecond {
		t.Fatalf("MaxHold = %v, want 25ms", rules.MaxHold)
	}
	get, sfu, set := juicer.OpGet, juicer.OpGetForPut, juicer.OpSet
	for _, tc := range []struct {
		held, head juicer.OperationType
		want       bool
	}{
		{get, get, false}, {get, set, true}, {set, get, true}, {set, set, true},
		{sfu, get, true}, {get, sfu, true}, {sfu, sfu, true}, {sfu, set, true},
	} {
		h := juicer.OpRef{TxnID: 1, OpType: tc.held}
		if got := rules.Blocks(h, juicer.OpRef{TxnID: 2, OpType: tc.head}); got != tc.want {
			t.Errorf("Blocks(%v held, %v head) = %v, want %v", tc.held, tc.head, got, tc.want)
		}
		if rules.Blocks(h, juicer.OpRef{TxnID: 1, OpType: tc.head}) {
			t.Errorf("a transaction blocked itself (%v held, %v head)", tc.held, tc.head)
		}
	}
	read := juicer.OpRef{TxnID: 1, OpType: get, Token: 7}
	write := juicer.OpRef{TxnID: 1, OpType: set, Token: 8}
	ownOK := func(tok uint64) juicer.Event {
		return juicer.Event{Kind: juicer.RespReturned, TxnID: 1, Token: tok}
	}
	if !rules.Releases(read, ownOK(7)) {
		t.Error("a read hold must end with its own response (request-class)")
	}
	if rules.Releases(write, ownOK(8)) {
		t.Error("a write hold must outlive its own successful response (control-class)")
	}
	if rules.Releases(write, juicer.Event{Kind: juicer.ReqArrived, OpType: juicer.OpCommit, TxnID: 1}) {
		t.Error("a write hold must outlive the commit's arrival")
	}
	if !rules.Releases(write, juicer.Event{Kind: juicer.ControlApplied, OpType: juicer.OpCommit, TxnID: 1}) {
		t.Error("an applied commit must end the write hold")
	}
	if !rules.Releases(write, juicer.Event{Kind: juicer.RespReturned, TxnID: 1, Token: 8, Failed: true}) {
		t.Error("the write's own failure must end its hold")
	}
}

func eventsByKey(evs []juicer.ControlEvent[int64]) map[int64]juicer.ControlEvent[int64] {
	m := make(map[int64]juicer.ControlEvent[int64], len(evs))
	for _, e := range evs {
		m[e.Key] = e
	}
	return m
}

func keyHash(s string) int64 { return juicerKeyHash(roachpb.Key(s)) }

// Table 5-C for CockroachDB: which batches apply which transaction's commit or
// abort, on which keys. The ResolveIntent case is the one that used to carry
// transaction id 0 (the batch has no transaction of its own).
func TestJuicerAppliedControlEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()
	respWith := func(txn *roachpb.Transaction, status roachpb.TransactionStatus) *kvpb.BatchResponse {
		br := &kvpb.BatchResponse{}
		br.Txn = txn.Clone()
		br.Txn.Status = status
		return br
	}

	t.Run("explicit commit: anchor, local lock spans, in-flight writes", func(t *testing.T) {
		txn := juicerTestTxn(10)
		et := endTxn(true)
		et.LockSpans = []roachpb.Span{
			{Key: roachpb.Key("a")},
			{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, // ranged: no point key
		}
		et.InFlightWrites = []roachpb.SequencedWrite{{Key: roachpb.Key("w"), Sequence: 3}}
		ba := juicerBatch(txn, et)
		evs := spi.AppliedControlEvents(ba, respWith(txn, roachpb.COMMITTED), nil)
		got := eventsByKey(evs)
		for _, k := range []string{"anchor", "a", "w"} {
			e, ok := got[keyHash(k)]
			if !ok || e.OpType != juicer.OpCommit || e.TxnID != juicerTxnID(ba) || e.TxnID == 0 {
				t.Errorf("key %s: event %+v (present=%v), want commit of txn %d", k, e, ok, juicerTxnID(ba))
			}
		}
		if _, ok := got[keyHash("b")]; ok || len(got) != 3 {
			t.Errorf("events %+v: want exactly anchor, a, w", evs)
		}
	})
	t.Run("rollback applies an abort", func(t *testing.T) {
		txn := juicerTestTxn(10)
		ba := juicerBatch(txn, endTxn(false))
		evs := spi.AppliedControlEvents(ba, respWith(txn, roachpb.ABORTED), nil)
		if len(evs) != 1 || evs[0].OpType != juicer.OpAbort || evs[0].Key != keyHash("anchor") {
			t.Fatalf("events %+v", evs)
		}
	})
	t.Run("one-phase commit: the write batch commits its own keys", func(t *testing.T) {
		txn := juicerTestTxn(10)
		ba := juicerBatch(txn, put("x"), put("y"), put("x"), endTxn(true))
		got := eventsByKey(spi.AppliedControlEvents(ba, respWith(txn, roachpb.COMMITTED), nil))
		for _, k := range []string{"x", "y", "anchor"} {
			if e, ok := got[keyHash(k)]; !ok || e.OpType != juicer.OpCommit || e.TxnID != juicerTxnID(ba) {
				t.Errorf("key %s: %+v present=%v", k, e, ok)
			}
		}
	})
	t.Run("staged parallel commit and plain batches apply nothing", func(t *testing.T) {
		txn := juicerTestTxn(10)
		if evs := spi.AppliedControlEvents(juicerBatch(txn, put("x"), endTxn(true)), respWith(txn, roachpb.STAGING), nil); len(evs) != 0 {
			t.Errorf("STAGING applied %+v", evs)
		}
		if evs := spi.AppliedControlEvents(juicerBatch(txn, plainGet("x")), respWith(txn, roachpb.PENDING), nil); len(evs) != 0 {
			t.Errorf("a read applied %+v", evs)
		}
	})
	t.Run("failed batches apply nothing", func(t *testing.T) {
		txn := juicerTestTxn(10)
		ba := juicerBatch(txn, endTxn(true))
		br := respWith(txn, roachpb.COMMITTED)
		br.Error = kvpb.NewError(&kvpb.TransactionAbortedError{})
		if evs := spi.AppliedControlEvents(ba, br, nil); len(evs) != 0 {
			t.Errorf("errored response applied %+v", evs)
		}
		if evs := spi.AppliedControlEvents(ba, nil, context.Canceled); len(evs) != 0 {
			t.Errorf("handler error applied %+v", evs)
		}
	})
	t.Run("batched intent resolution names each intent's transaction", func(t *testing.T) {
		a, b, c := juicerTestTxn(1), juicerTestTxn(2), juicerTestTxn(3)
		ba := &kvpb.BatchRequest{} // non-transactional, as the intent resolver sends it
		ba.Add(
			&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k1")},
				IntentTxn: a.TxnMeta, Status: roachpb.COMMITTED},
			&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k2")},
				IntentTxn: b.TxnMeta, Status: roachpb.ABORTED},
			// A push that only moved the intent's timestamp applies nothing.
			&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k3")},
				IntentTxn: c.TxnMeta, Status: roachpb.PENDING},
			&kvpb.ResolveIntentRangeRequest{
				RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("r"), EndKey: roachpb.Key("s")},
				IntentTxn:     c.TxnMeta, Status: roachpb.COMMITTED},
		)
		if juicerTxnID(ba) != 0 {
			t.Fatal("the resolution batch should carry no transaction")
		}
		evs := spi.AppliedControlEvents(ba, &kvpb.BatchResponse{}, nil)
		sort.Slice(evs, func(i, j int) bool { return evs[i].Key < evs[j].Key })
		got := eventsByKey(evs)
		if len(got) != 2 {
			t.Fatalf("events %+v, want k1 and k2 only", evs)
		}
		if e := got[keyHash("k1")]; e.TxnID != juicerTxnID(juicerBatch(a)) || e.TxnID == 0 || e.OpType != juicer.OpCommit {
			t.Errorf("k1: %+v, want commit of txn %d", e, juicerTxnID(juicerBatch(a)))
		}
		if e := got[keyHash("k2")]; e.TxnID != juicerTxnID(juicerBatch(b)) || e.OpType != juicer.OpAbort {
			t.Errorf("k2: %+v, want abort of txn %d", e, juicerTxnID(juicerBatch(b)))
		}
	})
}

// conflictTableHarness drives the fork's unary interceptor with the real CRDB
// SPI, in manual mode with a queue for every listed key, so that the
// conflict-table relation and the applied events meet real batches.
type conflictTableHarness struct {
	t         *testing.T
	ji        *grpc.JuicerInterceptor
	intercept grpc.UnaryServerInterceptor
	info      *grpc.UnaryServerInfo
}

func newConflictTableHarness(t *testing.T, keys ...string) *conflictTableHarness {
	queueKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		queueKeys = append(queueKeys, juicerKeyHashString(roachpb.Key(k)))
	}
	ji := grpc.NewJuicerInterceptorWithConfig(newCRDBJuicerSPI(), grpc.JuicerConfig{
		SkipTracking: true, ManualQueueKeys: queueKeys, FilterK: 100, FilterS: 1,
		WaitStrategy: juicer.WaitStrategyFixed,
	})
	t.Cleanup(ji.Stop)
	method := juicerUnaryBatchPaths[0]
	return &conflictTableHarness{
		t: t, ji: ji, intercept: ji.GetInterceptor(map[string]bool{method: true}),
		info: &grpc.UnaryServerInfo{FullMethod: method},
	}
}

// send runs one batch through the interceptor on its own goroutine. The
// handler closes ran when it starts and answers with a response carrying the
// batch's transaction in the given status (nil: unchanged).
func (h *conflictTableHarness) send(
	ba *kvpb.BatchRequest, status *roachpb.TransactionStatus,
) (ran chan struct{}, done chan error) {
	return h.sendHeld(ba, status, nil)
}

// sendHeld is send whose handler, once started, waits for release (if
// non-nil) before it answers: a message in flight.
func (h *conflictTableHarness) sendHeld(
	ba *kvpb.BatchRequest, status *roachpb.TransactionStatus, release chan struct{},
) (ran chan struct{}, done chan error) {
	ran, done = make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := h.intercept(context.Background(), ba, h.info, func(context.Context, interface{}) (interface{}, error) {
			close(ran)
			if release != nil {
				<-release
			}
			br := &kvpb.BatchResponse{}
			if ba.Txn != nil {
				br.Txn = ba.Txn.Clone()
				if status != nil {
					br.Txn.Status = *status
				}
			}
			return br, nil
		})
		done <- err
	}()
	return ran, done
}

func (h *conflictTableHarness) admitted(ran chan struct{}, what string) {
	h.t.Helper()
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		h.t.Fatalf("%s was not admitted", what)
	}
}

func (h *conflictTableHarness) held(ran chan struct{}, what string) {
	h.t.Helper()
	select {
	case <-ran:
		h.t.Fatalf("%s was admitted while it should wait", what)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestJuicerConflictTableThroughInterceptor(t *testing.T) {
	defer saveJuicerSpecState()()
	juicerRulesEnv, juicerMaxHoldMillis = "", 60000 // only events may end holds here
	committed, aborted := roachpb.COMMITTED, roachpb.ABORTED

	t.Run("a write holds until its intent is resolved, by the intent's transaction", func(t *testing.T) {
		h := newConflictTableHarness(t, "k")
		a := juicerTestTxn(10)
		ran, done := h.send(juicerBatch(a, put("k")), nil)
		h.admitted(ran, "txn A's write")
		<-done
		b := juicerTestTxn(20)
		ranB, doneB := h.send(juicerBatch(b, plainGet("k")), nil)
		h.held(ranB, "txn B's read behind A's write")
		// A's commit arrives elsewhere; here only the async resolution comes.
		resolve := &kvpb.BatchRequest{}
		resolve.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k")},
			IntentTxn: a.TxnMeta, Status: roachpb.COMMITTED})
		ranR, doneR := h.send(resolve, nil)
		h.admitted(ranR, "the resolution (a control message)")
		<-doneR
		h.admitted(ranB, "txn B's read after A's intent was resolved")
		<-doneB
	})
	t.Run("a one-phase commit releases its own write at its response", func(t *testing.T) {
		h := newConflictTableHarness(t, "k", "anchor")
		c := juicerTestTxn(10)
		ran, done := h.send(juicerBatch(c, put("k"), endTxn(true)), &committed)
		h.admitted(ran, "txn C's one-phase commit")
		<-done
		d := juicerTestTxn(20)
		ranD, doneD := h.send(juicerBatch(d, put("k")), nil)
		h.admitted(ranD, "txn D's write after C's one-phase commit")
		<-doneD
	})
	t.Run("the anchor's rollback releases a write in its lock spans", func(t *testing.T) {
		h := newConflictTableHarness(t, "k")
		e := juicerTestTxn(10)
		ran, done := h.send(juicerBatch(e, put("k")), nil)
		h.admitted(ran, "txn E's write")
		<-done
		f := juicerTestTxn(20)
		ranF, doneF := h.send(juicerBatch(f, put("k")), nil)
		h.held(ranF, "txn F's write behind E's")
		rb := endTxn(false)
		rb.LockSpans = []roachpb.Span{{Key: roachpb.Key("k")}}
		ranRB, doneRB := h.send(juicerBatch(e, rb), &aborted)
		h.admitted(ranRB, "E's rollback")
		<-doneRB
		h.admitted(ranF, "txn F's write after E's rollback was applied")
		<-doneF
	})
	t.Run("reads never wait for reads; a write waits for the in-flight reads", func(t *testing.T) {
		h := newConflictTableHarness(t, "k")
		release := make(chan struct{})
		ranG, doneG := h.sendHeld(juicerBatch(juicerTestTxn(10), plainGet("k")), nil, release)
		h.admitted(ranG, "txn G's read")
		ranH, doneH := h.sendHeld(juicerBatch(juicerTestTxn(12), plainGet("k")), nil, release)
		h.admitted(ranH, "txn H's read next to G's in-flight read")
		ranW, doneW := h.send(juicerBatch(juicerTestTxn(15), put("k")), nil)
		h.held(ranW, "a write behind two in-flight reads")
		close(release)
		<-doneG
		<-doneH
		h.admitted(ranW, "the write once both reads returned (request-class holds)")
		<-doneW
	})
	t.Run("a batch touching a key twice queues one marker, as a write", func(t *testing.T) {
		h := newConflictTableHarness(t, "k")
		y := juicerTestTxn(10)
		ran, done := h.send(juicerBatch(y, plainGet("k"), put("k"), put("k")), nil)
		h.admitted(ran, "the batch")
		<-done
		if got := h.ji.SnapshotMetrics().Queue.Enqueued; got != 1 {
			t.Fatalf("enqueued=%d, want one marker for the repeated key", got)
		}
		// The merged marker is a write: another transaction's read waits.
		ranR, doneR := h.send(juicerBatch(juicerTestTxn(20), plainGet("k")), nil)
		h.held(ranR, "a read behind the merged write")
		resolve := &kvpb.BatchRequest{}
		resolve.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k")},
			IntentTxn: y.TxnMeta, Status: roachpb.COMMITTED})
		_, doneRes := h.send(resolve, nil)
		<-doneRes
		h.admitted(ranR, "the read after the write's resolution")
		<-doneR
	})
	t.Run("a transaction touching a key twice is not blocked by itself", func(t *testing.T) {
		h := newConflictTableHarness(t, "k")
		x := juicerTestTxn(10)
		ran1, done1 := h.send(juicerBatch(x, put("k")), nil)
		h.admitted(ran1, "the first write")
		<-done1
		x2 := x.Clone()
		ran2, done2 := h.send(juicerBatch(x2, plainGet("k"), put("k")), nil)
		h.admitted(ran2, "the same transaction's second batch on the key")
		<-done2
	})
}

// Spec 4: the adaptive buffer is off by default; when on, W_max defaults to H
// and every invalid or inconsistent setting refuses to start.
func TestJuicerAdaptiveWaitKnobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerSpecState()()
	if juicerAW {
		t.Fatal("COCKROACH_JUICER_AW must default to off")
	}
	if c, err := juicerAdaptiveWaitConfig(); err != nil || c.Enabled {
		t.Fatalf("off: %+v %v", c, err)
	}
	juicerAW = true
	c, err := juicerAdaptiveWaitConfig()
	if err != nil || !c.Enabled || c.MaxWait != 25*time.Millisecond || c.ScaleMilli != 1000 ||
		c.LaMode != juicer.AwLaConst || c.LaConst != time.Millisecond || c.AllInversions || c.MinLaSamples != 10 {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	if !strings.Contains(juicerSpecBanner(), "buffer=adaptive(W_max=25ms") {
		t.Fatalf("banner %q", juicerSpecBanner())
	}
	juicerAWMaxUsRaw, juicerAWMaxUsSet = "0", true
	juicerAWScaleRaw, juicerAWScaleSet = "1.5", true
	juicerAWInvRaw, juicerAWInvSet = "all", true
	juicerAWLaRaw, juicerAWLaSet = "online", true
	juicerAWLaUsRaw, juicerAWLaUsSet = "700", true
	juicerAWMinRaw, juicerAWMinSet = "5", true
	c, err = juicerAdaptiveWaitConfig()
	if err != nil || c.MaxWait != 0 || c.ScaleMilli != 1500 || !c.AllInversions || c.LaMode != juicer.AwLaOnline ||
		c.LaConst != 700*time.Microsecond || c.MinLaSamples != 5 {
		t.Fatalf("set: %+v %v", c, err)
	}
	juicerEnabled = true
	for _, tc := range []struct {
		name string
		set  func()
		want string
	}{
		{"cap above H", func() { juicerAWMaxUsRaw = "25001" }, "exceeds"},
		{"negative cap", func() { juicerAWMaxUsRaw = "-1" }, "AW_MAX_US"},
		{"bad r", func() { juicerAWInvRaw = "some" }, "AW_INVERSIONS"},
		{"bad L_a mode", func() { juicerAWLaRaw = "wound" }, "L_a mode"},
		{"bad scale", func() { juicerAWScaleRaw = "0" }, "AW_SCALE"},
		{"fixed wait", func() { juicerFixedWaitMillis = 1 }, "FIXED_WAIT_MS"},
		{"manual queues", func() { juicerManualQueueKeysSet = true }, "MANUAL_QUEUE_KEYS"},
		{"knob with the feature off", func() { juicerAW = false }, "while COCKROACH_JUICER_AW is off"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer saveJuicerSpecState()()
			tc.set()
			if _, err := juicerAdaptiveWaitConfig(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want %q", err, tc.want)
			}
		})
	}
	// On, the server gets exactly one more option.
	juicerAWMaxUsRaw = "1000"
	on := juicerServerOptions()
	juicerAW = false
	juicerAWMaxUsSet, juicerAWScaleSet, juicerAWInvSet, juicerAWLaSet, juicerAWLaUsSet, juicerAWMinSet =
		false, false, false, false, false, false
	if base := juicerServerOptions(); len(on) != len(base)+1 {
		t.Fatalf("options with AW %d, without %d", len(on), len(base))
	}
}

func TestJuicerStageTimingKnob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerSpecState()()
	t.Setenv("JUICER_STAGE_TIMING", "") // Isolate the CRDB option from the shared env override.
	if juicerStageTiming {
		t.Fatal("COCKROACH_JUICER_STAGE_TIMING must default to off")
	}
	juicerEnabled = true
	for _, tc := range []struct {
		name    string
		enabled bool
	}{{"off", false}, {"on", true}} {
		t.Run(tc.name, func(t *testing.T) {
			juicerStageTiming = tc.enabled
			srv := grpc.NewServer(juicerServerOptions()...)
			defer srv.Stop()
			ji, intercept := srv.Juicer(), srv.JuicerUnaryInterceptor()
			if ji == nil || intercept == nil {
				t.Fatal("server options did not install the CRDB interceptor")
			}
			if got := ji.SnapshotStageTiming(); (got != nil) != tc.enabled {
				t.Fatalf("stage timing enabled=%v, snapshot=%+v", tc.enabled, got)
			}
			for _, req := range []kvpb.Request{plainGet("k"), put("k")} {
				ba := juicerBatch(juicerTestTxn(1), req)
				called := false
				_, err := intercept(context.Background(), ba,
					&grpc.UnaryServerInfo{FullMethod: juicerUnaryBatchPaths[0]},
					func(context.Context, interface{}) (interface{}, error) {
						called = true
						br := &kvpb.BatchResponse{}
						br.Txn = ba.Txn.Clone()
						return br, nil
					})
				if err != nil || !called {
					t.Fatalf("batch did not reach its handler: called=%v err=%v", called, err)
				}
			}
			snapshot := ji.SnapshotStageTiming()
			if !tc.enabled {
				if snapshot != nil {
					t.Fatal("disabled stage timing recorded unary batches")
				}
				return
			}
			if snapshot == nil {
				t.Fatal("enabled stage timing lost its snapshot")
			}
			for _, class := range []string{"read_only", "write_containing"} {
				for _, stage := range []string{"invocation", "classify_prepare", "admission_wall", "handler", "response_observation"} {
					key := "unary/" + class + "/" + stage
					if got := snapshot.Stages[key].Count; got != 1 {
						t.Fatalf("stage %s count=%d, want 1", key, got)
					}
				}
			}
		})
	}
	juicerEnabled = false
	juicerStageTiming = true
	if opts := juicerServerOptions(); opts != nil {
		t.Fatalf("stage timing enabled Juicer by itself: %d options", len(opts))
	}
}

// Spec 6.2: the calibration mode is off by default, adds one option, says so
// in the banner, and refuses the adaptive buffer.
func TestJuicerHoldCalibrationKnob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerSpecState()()
	if juicerHoldCalibration {
		t.Fatal("COCKROACH_JUICER_HOLD_CALIBRATION must default to off")
	}
	juicerEnabled = true
	base := juicerServerOptions()
	juicerHoldCalibration = true
	if on := juicerServerOptions(); len(on) != len(base)+1 {
		t.Fatalf("options %d with calibration, %d without", len(on), len(base))
	}
	if !strings.Contains(juicerSpecBanner(), "hold_calibration=on") {
		t.Fatalf("banner %q", juicerSpecBanner())
	}
	juicerAW = true
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "HOLD_CALIBRATION") {
			t.Fatalf("calibration with AW: %v", r)
		}
	}()
	_ = juicerServerOptions()
}

// Spec 9.4(d): an abort message reports each key with the transaction it
// speaks for, the batch's own for a rollback and IntentTxn for an intent
// resolution, which carries none (so the first level can count once per
// transaction and key).
func TestJuicerAbortedKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	spi := newCRDBJuicerSPI()
	txn := juicerTestTxn(10)
	ba := juicerBatch(txn, endTxn(false))
	if !spi.IsAbortRequest(ba) {
		t.Fatal("a rollback should be an abort request")
	}
	got := spi.AbortedKeys(ba)
	if len(got) != 1 || got[0].Key != keyHash("anchor") || got[0].TxnID != juicerTxnID(ba) || got[0].TxnID == 0 {
		t.Fatalf("rollback: %+v", got)
	}
	a, b := juicerTestTxn(1), juicerTestTxn(2)
	res := &kvpb.BatchRequest{}
	res.Add(
		&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k1")},
			IntentTxn: a.TxnMeta, Status: roachpb.ABORTED},
		&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k2")},
			IntentTxn: b.TxnMeta, Status: roachpb.ABORTED},
	)
	if !spi.IsAbortRequest(res) {
		t.Fatal("an ABORTED resolution should be an abort request")
	}
	evs := eventsByKey(spi.AbortedKeys(res))
	if len(evs) != 2 || evs[keyHash("k1")].TxnID != juicerTxnID(juicerBatch(a)) ||
		evs[keyHash("k2")].TxnID != juicerTxnID(juicerBatch(b)) {
		t.Fatalf("resolution: %+v", evs)
	}
	keys := spi.GetKeysFromRequest(res)
	if len(keys) != 2 || keys[0] != keyHash("k1") || keys[1] != keyHash("k2") {
		t.Fatalf("AbortedKeys must name the GetKeysFromRequest keys: %v", keys)
	}
}
