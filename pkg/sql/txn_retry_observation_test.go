// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type observationSender struct {
	*kv.MockTransactionalSender
	retry *kvpb.TransactionRetryWithProtoRefreshError
	fail  error
}

func (s *observationSender) GetRetryableErr(context.Context) *kvpb.TransactionRetryWithProtoRefreshError {
	return s.retry
}
func (s *observationSender) ClearRetryableErr(context.Context) error { return s.fail }

type observationFactory struct {
	kv.TxnSenderFactory
	sender *observationSender
}

func (f *observationFactory) RootTransactionalSender(txn *roachpb.Transaction, pri roachpb.UserPriority) kv.TxnSender {
	f.sender = &observationSender{MockTransactionalSender: kv.NewMockTransactionalSender(nil, txn, pri)}
	return f.sender
}
func (*observationFactory) NonTransactionalSender() kv.Sender { return nil }

type observationWriter struct{ records int }

func (w *observationWriter) Record(contentionpb.ResolvedTxnID) { w.records++ }
func (*observationWriter) DrainWriteBuffer()                   {}

func TestSQLRetryObservationLifecycle(t *testing.T) {
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	ambient := log.MakeTestingAmbientCtxWithNewTracer()
	defer ambient.Tracer.Close()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	factory := &observationFactory{}
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	db := kv.NewDB(ambient, factory, clock, stopper)
	root := mon.NewMonitor(mon.Options{Name: mon.MakeName("observation root"), Settings: settings})
	root.StartNoReserved(ctx, nil)
	defer root.Stop(ctx)
	ts := txnState{txnMon: mon.NewMonitor(mon.Options{Name: mon.MakeName("observation txn"), Settings: settings})}
	reset := func() {
		txn := kv.NewTxn(ctx, db, 0)
		ts.resetForNewSQLTxn(ctx, explicitTxn, timeutil.Now(), nil,
			roachpb.UnspecifiedUserPriority, tree.ReadWrite, txn,
			transitionCtx{settings: settings, tracer: ambient.Tracer, connMon: root, sessionTracing: &SessionTracing{}},
			sessiondatapb.Normal, isolation.Serializable, false, false, rand.New(rand.NewSource(1)))
	}
	finish := func() {
		ts.txnMon.Stop(ts.Ctx)
		tracing.SpanFromContext(ts.Ctx).Finish()
		ts.txnCancelFn()
	}
	reset()
	for _, msg := range []redact.RedactableString{
		"TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
		"TransactionRetryError: retry txn (RETRY_WRITE_TOO_OLD)",
		"injected retriable error",
	} {
		err := factory.sender.GenerateForcedRetryableErr(ctx, hlc.Timestamp{}, true, msg)
		if got := classifySQLRetryText(err); got == sqlRetryTextUnknown && strings.HasPrefix(string(msg), "TransactionRetryError:") {
			t.Fatalf("native-looking forced text: %v", err)
		}
		if err := prepareTxnForRetryWithRewind(fsm.Args{Extended: &ts, Payload: eventRetryableErrPayload{err: err}}); err != nil {
			t.Fatal(err)
		}
		if adv := ts.consumeAdvanceInfo(); adv.code != rewind || adv.txnEvent.eventType != txnRestart {
			t.Fatalf("changed retry advance: %+v", adv)
		}
	}
	// A failure in PrepareForRetry must still count, matching the original total.
	current := factory.sender.TestingCloneTxn()
	retry := kvpb.NewTransactionRetryWithProtoRefreshError("forced failure", current.ID, current.Epoch, *current)
	retry.NextTransaction.Epoch++
	factory.sender.retry, factory.sender.fail = retry, errors.New("clear retry failure")
	if err := prepareTxnForRetryWithRewind(fsm.Args{Extended: &ts, Payload: eventRetryableErrPayload{err: retry}}); !errors.Is(err, factory.sender.fail) {
		t.Fatalf("PrepareForRetry failure changed: %v", err)
	}
	if ts.mu.autoRetryCounter != 4 || ts.mu.autoRetryTextCounts[sqlRetryTextSerializable] != 1 || ts.mu.autoRetryTextCounts[sqlRetryTextWriteTooOld] != 1 || ts.mu.autoRetryTextCounts[sqlRetryTextUnknown] != 2 {
		t.Fatalf("mixed counts: %d %+v", ts.mu.autoRetryCounter, ts.mu.autoRetryTextCounts)
	}
	for _, terminalErr := range []error{nil, errors.New("terminal error")} {
		metrics := makeMetrics(false, &settings.SV)
		writer := &observationWriter{}
		ex := connExecutor{server: &Server{ServerMetrics: makeServerMetrics(&ExecutorConfig{HistogramWindowInterval: time.Second})}, metrics: &metrics,
			sessionDataStack: sessiondata.NewStack(&sessiondata.SessionData{}),
			txnIDCacheWriter: writer, totalActiveTimeStopWatch: timeutil.NewStopWatch()}
		ex.state.mu.autoRetryCounter = ts.mu.autoRetryCounter
		ex.state.mu.autoRetryTextCounts = ts.mu.autoRetryTextCounts
		// Empty statement fingerprints exercise the first existing early return.
		ev := txnEvent{eventType: txnCommit}
		if terminalErr != nil {
			ev.eventType = txnRollback
		}
		if err := ex.recordTransactionFinish(ctx, 0, ev, false, crtime.NowMono(), terminalErr); err != nil {
			t.Fatal(err)
		}
		if metrics.EngineMetrics.TxnRetryCount.Count() != 4 || writer.records != 1 {
			t.Fatal("terminal total or existing cache recording changed")
		}
		var sum int64
		for cause, count := range ts.mu.autoRetryTextCounts {
			got := metrics.EngineMetrics.TxnRetryTextReasonCounts[cause].Count()
			if got != count {
				t.Fatalf("terminal %v cause %d: %d != %d", terminalErr, cause, got, count)
			}
			sum += got
		}
		if sum != metrics.EngineMetrics.TxnRetryCount.Count() {
			t.Fatal("SQL terminal sum differs")
		}
	}
	finish()
	reset()
	if ts.mu.autoRetryCounter != 0 || ts.mu.autoRetryReason != nil || ts.mu.autoRetryTextCounts != (sqlRetryTextCounts{}) {
		t.Fatal("SQL transaction reset retained retry observations")
	}
	finish()
}

func TestSQLRetryObservationRegistration(t *testing.T) {
	for _, internal := range []bool{false, true} {
		settings := cluster.MakeTestingClusterSettings()
		metrics := makeMetrics(internal, &settings.SV)
		registry := metric.NewRegistry()
		registry.AddMetricStruct(metrics.EngineMetrics)
		found := map[string]bool{}
		registry.Each(func(name string, _ interface{}) { found[name] = true })
		exporter := metric.MakePrometheusExporter()
		exporter.ScrapeRegistry(registry)
		families, err := exporter.Gather()
		if err != nil {
			t.Fatal(err)
		}
		exposed := map[string]bool{}
		for _, family := range families {
			exposed[family.GetName()] = true
		}
		for cause, counter := range metrics.EngineMetrics.TxnRetryTextReasonCounts {
			name := sqlRetryTextMetricName(sqlRetryTextCause(cause))
			if internal {
				name += ".internal"
			}
			if internal && counter.GetMetadata().Visibility != metric.Metadata_INTERNAL {
				t.Fatalf("incorrect internal visibility: %s", name)
			}
			if !exposed[strings.ReplaceAll(name, ".", "_")] {
				t.Fatalf("metric not exposed: %s", name)
			}
			if !found[name] || counter.GetName(false) != name {
				t.Fatalf("unregistered SQL text metric %s", name)
			}
		}
	}

}
