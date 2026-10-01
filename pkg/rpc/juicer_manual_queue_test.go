// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/juicer"
)

// The parser is the only thing standing between a typo in the environment and
// the fork's "empty manual set means every key" branch, so every rejection
// path is pinned here, and so is the normalization to the fork's own queue-key
// spelling (fmt.Sprint of the int64).
func TestJuicerParseManualQueueKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		raw  string
		want []string
	}{
		{raw: "1817670077699909844", want: []string{"1817670077699909844"}},
		{
			raw:  " 1817670077699909844 ,6960948776747566164,\t4712501373694096178 ",
			want: []string{"1817670077699909844", "6960948776747566164", "4712501373694096178"},
		},
		{raw: "-2238594559466323628", want: []string{"-2238594559466323628"}},
		{raw: "+5,007,-0", want: []string{"5", "7", "0"}},
		{
			raw:  "-9223372036854775808,9223372036854775807",
			want: []string{"-9223372036854775808", "9223372036854775807"},
		},
	} {
		got, err := parseJuicerManualQueueKeys(tc.raw)
		if err != nil {
			t.Fatalf("parse(%q): unexpected error %v", tc.raw, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("parse(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
	for _, raw := range []string{
		// Empty: the fork would queue every key.
		"", " ", "\t\n",
		// Empty elements.
		",", "1,", ",1", "1,,2", "1, ,2",
		// Not a signed base-10 int64.
		"abc", "1.5", "0x10", "1e3", "1 2", "9223372036854775808", "-9223372036854775809", "--1",
		// Duplicates after normalization.
		"1,1", "5,+5", "7,007", " 3, 3", "0,-0",
	} {
		if got, err := parseJuicerManualQueueKeys(raw); err == nil {
			t.Fatalf("parse(%q) accepted %q; want an error", raw, got)
		}
	}
}

// saveJuicerManualState snapshots every package-level knob these tests flip.
func saveJuicerManualState() func() {
	enabled, skip, diag := juicerEnabled, juicerSkipQueueing, juicerFilterDiagnostic
	raw, set, rules := juicerManualQueueKeysRaw, juicerManualQueueKeysSet, juicerRulesEnv
	return func() {
		juicerEnabled, juicerSkipQueueing, juicerFilterDiagnostic = enabled, skip, diag
		juicerManualQueueKeysRaw, juicerManualQueueKeysSet, juicerRulesEnv = raw, set, rules
	}
}

// Unset must mean "exactly the c35 option list"; set must add exactly the two
// existing manual-mode options; and the variable must never turn Juicer on by
// itself. grpc.ServerOption is an opaque closure, so the option identity is
// asserted by count (the same technique as TestJuicerFifoQueueOption) and the
// resulting server must build and expose the local-path interceptor.
func TestJuicerManualQueueKeysOptionWiring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerManualState()()

	if juicerManualQueueKeysSet {
		t.Fatalf("COCKROACH_JUICER_MANUAL_QUEUE_KEYS must be unset in the test environment, got %q",
			juicerManualQueueKeysRaw)
	}
	juicerEnabled, juicerSkipQueueing, juicerFilterDiagnostic = true, false, "off"
	juicerManualQueueKeysSet, juicerManualQueueKeysRaw = false, ""
	if keys := juicerManualQueueKeysOrPanic(); keys != nil {
		t.Fatalf("unset variable produced manual keys %q", keys)
	}
	baseline := juicerServerOptions()
	if len(baseline) == 0 {
		t.Fatal("juicer enabled but no server options were returned")
	}

	juicerManualQueueKeysSet = true
	juicerManualQueueKeysRaw = "1817670077699909844, 6960948776747566164"
	if got, want := juicerManualQueueKeysOrPanic(),
		[]string{"1817670077699909844", "6960948776747566164"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("manual keys = %q, want %q", got, want)
	}
	manual := juicerServerOptions()
	if len(manual) != len(baseline)+2 {
		t.Fatalf("manual mode returned %d options, want %d (baseline + SkipTracking + ManualQueueKeys)",
			len(manual), len(baseline)+2)
	}
	for i, o := range manual {
		if o == nil {
			t.Fatalf("manual-mode server option %d is nil", i)
		}
	}
	s := grpc.NewServer(manual...)
	if s.JuicerUnaryInterceptor() == nil {
		t.Fatal("manual-mode server has no Juicer interceptor")
	}
	s.Stop()

	juicerEnabled = false
	if opts := juicerServerOptions(); opts != nil {
		t.Fatalf("COCKROACH_JUICER_MANUAL_QUEUE_KEYS enabled juicer by itself: %d options", len(opts))
	}
}

// Manual mode is exclusive with the modes that would bypass its queues, and a
// malformed or empty list is a startup panic, never a silent fallback to
// "queue every key" or to normal mode.
func TestJuicerManualQueueKeysRejects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer saveJuicerManualState()()

	for _, tc := range []struct {
		name, raw, diagnostic string
		skip                  bool
	}{
		{name: "empty", raw: "", diagnostic: "off"},
		{name: "blank", raw: "  ", diagnostic: "off"},
		{name: "empty element", raw: "1,,2", diagnostic: "off"},
		{name: "not int64", raw: "0x10", diagnostic: "off"},
		{name: "duplicate", raw: "5,+5", diagnostic: "off"},
		{name: "filter diagnostic none", raw: "1", diagnostic: "none"},
		{name: "filter diagnostic l1", raw: "1", diagnostic: "l1"},
		{name: "filter diagnostic l1l2", raw: "1", diagnostic: "l1l2"},
		{name: "skip queueing", raw: "1", diagnostic: "off", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			juicerEnabled = true
			juicerManualQueueKeysSet, juicerManualQueueKeysRaw = true, tc.raw
			juicerFilterDiagnostic, juicerSkipQueueing = tc.diagnostic, tc.skip
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("invalid manual configuration did not panic")
				}
				if msg, _ := r.(string); !strings.Contains(msg, "COCKROACH_JUICER_MANUAL_QUEUE_KEYS") {
					t.Fatalf("panic does not name the variable: %v", r)
				}
			}()
			_ = juicerServerOptions()
		})
	}
}

// manualTestKey is the family-0 primary-index key of ycsbt row k in table 106,
// the exact bytes a SQL point read or blind write of that row carries.
func manualTestKey(k int64) roachpb.Key {
	key := keys.SystemSQLCodec.IndexPrefix(106, 1)
	key = encoding.EncodeVarintAscending(key, k)
	return keys.MakeFamilyKey(key, 0)
}

// The fork's manual mode is driven here through the real CRDB SPI: SplitMarker
// hashes the request keys, the interceptor turns each hash into the queue-key
// string, and the queue manager creates a queue only for the listed key. The
// same traffic through a normal-mode interceptor creates no queue at all
// (nothing has been selected by the filters yet) but does feed the reorder
// filter. These are the snapshot fields the experiment's treatment-identity
// checks rely on, so the JSON is logged verbatim.
//
// No leaktest here: the fork's interceptor owns background reporter goroutines
// whose shutdown after Stop is asynchronous (the fork's own tests likewise
// only register ji.Stop as cleanup).
func TestJuicerManualModeSnapshotThroughCRDBSPI(t *testing.T) {
	defer saveJuicerManualState()()
	// A relation would add holds but never changes which keys get queues.
	juicerRulesEnv = string(juicerRulesOff)

	hot := manualTestKey(0)
	hotQueueKey := juicerKeyHashString(hot)
	if hotQueueKey != "1817670077699909844" {
		t.Fatalf("queue key of /Table/106/1/0/0 = %s, want 1817670077699909844", hotQueueKey)
	}
	traffic := []*kvpb.BatchRequest{
		juicerBatch(juicerTestTxn(10),
			&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: manualTestKey(0)}},
			&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: manualTestKey(1)}}),
		juicerBatch(juicerTestTxn(20),
			&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: manualTestKey(0)}},
			&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: manualTestKey(2)}},
			endTxn(true)),
	}
	run := func(cfg grpc.JuicerConfig) grpc.JuicerMetricsSnapshot {
		ji := grpc.NewJuicerInterceptorWithConfig(newCRDBJuicerSPI(), cfg)
		defer ji.Stop()
		method := juicerUnaryBatchPaths[0]
		intercept := ji.GetInterceptor(map[string]bool{method: true})
		info := &grpc.UnaryServerInfo{FullMethod: method}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return &kvpb.BatchResponse{}, nil
		}
		for i, ba := range traffic {
			if _, err := intercept(context.Background(), ba, info, handler); err != nil {
				t.Fatalf("batch %d: %v", i, err)
			}
		}
		return ji.SnapshotMetrics()
	}
	common := grpc.JuicerConfig{
		FilterK: 1000, FilterS: 100,
		WaitStrategy: juicer.WaitStrategyFixed, FixedWait: 0,
	}

	manualCfg := common
	manualCfg.SkipTracking, manualCfg.ManualQueueKeys = true, []string{hotQueueKey}
	manual := run(manualCfg)
	manualJSON, err := json.Marshal(manual)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("manual-mode snapshot: %s", manualJSON)
	if manual.Mode != "" || manual.FilterK != 1000 || manual.FilterS != 100 {
		t.Fatalf("manual snapshot identity fields: mode=%q K=%d S=%d", manual.Mode, manual.FilterK, manual.FilterS)
	}
	if manual.L1 != (juicer.FilterMetrics{}) || manual.L2 != (juicer.FilterMetrics{}) {
		t.Fatalf("manual mode must not run either filter: L1=%+v L2=%+v", manual.L1, manual.L2)
	}
	if manual.Queue.Queues != 1 || manual.Queue.Enqueued != 2 {
		t.Fatalf("manual mode: live_queues=%d enqueued=%d, want 1 and 2 (the Get and the Put of row 0 only)",
			manual.Queue.Queues, manual.Queue.Enqueued)
	}

	normal := run(common)
	normalJSON, err := json.Marshal(normal)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("normal-mode snapshot: %s", normalJSON)
	if normal.Queue.Queues != 0 || normal.Queue.Enqueued != 0 {
		t.Fatalf("normal mode created queues before any filter selection: live_queues=%d enqueued=%d",
			normal.Queue.Queues, normal.Queue.Enqueued)
	}
	if normal.L2.UpdateCount == 0 {
		t.Fatal("normal mode did not feed the reorder filter (l2.update_count = 0)")
	}
}

func juicerKeyHashString(k roachpb.Key) string {
	markers := crdbJuicerSPI{}.SplitMarker(juicerBatch(juicerTestTxn(1),
		&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}}))
	if len(markers) != 1 {
		panic("expected exactly one marker")
	}
	// The fork's interceptor names a marker's queue fmt.Sprint(marker.Key)
	// (juicer_interceptor.go prepareSortable); do exactly the same here.
	return fmt.Sprint(markers[0].Key)
}
