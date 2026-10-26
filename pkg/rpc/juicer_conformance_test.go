// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/juicer"
	"google.golang.org/grpc/juicer/spectest"
)

// The Juicer spec conformance suite's adapter and banner layers
// (docs/eval-plan-20261019/spec-conformance in the experiment workspace,
// provided by the pinned shared gRPC submodule): the core layer runs
// in the shared grpc fork (juicer); the scenarios that need
// CockroachDB's SPI — table 5-C's applied events, non-transactional
// messages, the first level's abort sources — and its configuration
// declaration line run here. Each scenario configures the package's knobs as
// the COCKROACH_JUICER_* environment would, builds a grpc.Server from
// juicerServerOptions (the production option list) plus JuicerStepped, and
// drives the server's own unary interceptor with real BatchRequests on an
// injected clock.

// crdbConformanceKeyPrefix is the table prefix the scenario keys get
// (FORMAT 5.3).
const crdbConformanceKeyPrefix = "/conformance/"

// crdbKnobs is the package configuration a scenario sets.
type crdbKnobs struct {
	enabled                          bool
	rules                            string
	maxHold, k, s, epochMs, idle     int
	fixedWait                        float64
	late                             string
	diag, aw, manualSet, calibration bool
	manualRaw                        string
}

func readCRDBKnobs() crdbKnobs {
	return crdbKnobs{enabled: juicerEnabled, rules: juicerRulesEnv, maxHold: juicerMaxHoldMillis,
		k: juicerFilterK, s: juicerFilterS, epochMs: juicerEpochMillis, idle: juicerIdleEpochs,
		fixedWait: juicerFixedWaitMillis, late: juicerLatePolicy, diag: juicerDiagnostics, aw: juicerAW,
		manualSet: juicerManualQueueKeysSet, manualRaw: juicerManualQueueKeysRaw, calibration: juicerHoldCalibration}
}

func (c crdbKnobs) set() {
	juicerEnabled, juicerRulesEnv, juicerMaxHoldMillis = c.enabled, c.rules, c.maxHold
	juicerFilterK, juicerFilterS, juicerEpochMillis, juicerIdleEpochs = c.k, c.s, c.epochMs, c.idle
	juicerFixedWaitMillis, juicerLatePolicy, juicerDiagnostics, juicerAW = c.fixedWait, c.late, c.diag, c.aw
	juicerManualQueueKeysSet, juicerManualQueueKeysRaw, juicerHoldCalibration = c.manualSet, c.manualRaw, c.calibration
}

// crdbConformance maps neutral scenarios to the real CockroachDB SPI.
type crdbConformance struct {
	defaults crdbKnobs
	keys     map[string]roachpb.Key
	anchors  map[int64]roachpb.Key
	onePC    map[int64]bool
}

func (s *crdbConformance) key(k string) roachpb.Key {
	return roachpb.Key(crdbConformanceKeyPrefix + k)
}

// Prepare maps the scenario's keys and looks ahead for the transactions
// whose commit is a one-phase commit (their write batch carries the EndTxn)
// and for each transaction's anchor key.
func (s *crdbConformance) Prepare(sc *spectest.Scenario, cfg *spectest.Config) error {
	s.keys, s.anchors, s.onePC = map[string]roachpb.Key{}, map[int64]roachpb.Key{}, map[int64]bool{}
	if cfg.Filter != nil {
		for _, k := range cfg.Filter.ManagedKeys {
			s.keys[k] = s.key(k)
		}
	}
	for _, in := range sc.Inputs {
		for _, op := range in.Ops {
			s.keys[op.Key] = s.key(op.Key)
			if _, ok := s.anchors[in.Txn]; !ok && op.Access == "write" {
				s.anchors[in.Txn] = s.key(op.Key)
			}
		}
		for _, k := range in.Keys {
			s.keys[k] = s.key(k)
		}
		switch in.Via {
		case "one_pc_reply":
			s.onePC[in.Txn] = true
		case "endtxn_anchor":
			if len(in.Keys) > 0 {
				s.anchors[in.Txn] = s.key(in.Keys[0])
			}
		}
	}
	return nil
}

func (s *crdbConformance) QueueKey(k string) string { return juicerKeyHashString(s.key(k)) }

// Scenario transaction n uses UUID bytes 8..16, as read by juicerTxnIDFromUUID.
func crdbConformanceUUID(txn int64) uuid.UUID {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[8:], uint64(txn))
	return uuid.FromBytesOrNil(b[:])
}

func (s *crdbConformance) txn(n int64, sk crdbConformanceSortKey) *roachpb.Transaction {
	txn := &roachpb.Transaction{}
	txn.ID = crdbConformanceUUID(n)
	txn.Key = s.anchors[n]
	ts := hlc.Timestamp{WallTime: sk.Wall, Logical: int32(sk.Logical)}
	txn.MinTimestamp, txn.ReadTimestamp, txn.WriteTimestamp = ts, ts, ts
	txn.Status = roachpb.PENDING
	return txn
}

func (s *crdbConformance) Arrive(in *spectest.Input, sk crdbConformanceSortKey) (string, any, error) {
	ba := &kvpb.BatchRequest{}
	ba.Txn = s.txn(in.Txn, sk)
	ba.Now = hlc.ClockTimestamp{WallTime: sk.Wall, Logical: int32(sk.Logical)}
	for _, op := range in.Ops {
		k := s.key(op.Key)
		switch op.Access {
		case "read":
			if op.Class == "control" {
				return "", nil, errors.Newf("%s: CockroachDB reads hold request-class", in.Msg)
			}
			ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
		case "write":
			ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k},
				Value: roachpb.MakeValueFromString(in.PayloadOrName())})
		case "lock_read":
			ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}, KeyLockingStrength: lock.Exclusive})
		default:
			return "", nil, errors.Newf("access %q", op.Access)
		}
	}
	if s.onePC[in.Txn] {
		ba.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: s.anchors[in.Txn]}, Commit: true})
	}
	return juicerUnaryBatchPaths[0], ba, nil
}

func (s *crdbConformance) Reply(req any, outcome string) (any, error) {
	ba, ok := req.(*kvpb.BatchRequest)
	if !ok {
		return &PingResponse{}, nil // a heartbeat
	}
	br := &kvpb.BatchResponse{}
	if ba.Txn != nil {
		br.Txn = ba.Txn.Clone()
	}
	switch outcome {
	case "failed":
		br.Error = kvpb.NewError(kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, "conformance"))
	case "ok":
		if ba.Txn != nil && s.onePC[juicerTxnID(ba)] {
			br.Txn.Status = roachpb.COMMITTED
		}
	}
	return br, nil
}

func (s *crdbConformance) Payload(req any) string {
	if ba, ok := req.(*kvpb.BatchRequest); ok {
		b, err := ba.Marshal()
		if err != nil {
			return "marshal: " + err.Error()
		}
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}
	return fmt.Sprintf("%T:%v", req, req)
}

// NonTxn is a heartbeat: not a Batch method, so not intercepted.
func (s *crdbConformance) NonTxn(in *spectest.Input) (string, any, error) {
	return "/cockroach.rpc.Heartbeat/Ping", &PingRequest{Ping: in.Msg}, nil
}

// Control turns a control input into the batch table 5-C names: an
// EndTxn on the anchor (also for a control input without via), a
// non-transactional ResolveIntent naming the transaction (IntentTxn), or
// nothing for a one-phase commit, whose reply carried the commit.
func (s *crdbConformance) Control(in *spectest.Input) ([]crdbConformanceControl, error) {
	var status roachpb.TransactionStatus
	switch in.Kind {
	case "commit":
		status = roachpb.COMMITTED
	case "abort":
		status = roachpb.ABORTED
	default:
		return nil, errors.Newf("CockroachDB has no %s message", in.Kind)
	}
	method := juicerUnaryBatchPaths[0]
	switch in.Via {
	case "one_pc_reply":
		return nil, nil
	case "resolve_intent":
		ba := &kvpb.BatchRequest{}
		meta := s.txn(in.Txn, crdbConformanceSortKey{Wall: in.TUs}).TxnMeta
		for _, k := range in.Keys {
			ba.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: s.key(k)}, IntentTxn: meta, Status: status})
		}
		return []crdbConformanceControl{{Method: method, Req: ba, Resp: &kvpb.BatchResponse{}}}, nil
	case "", "endtxn_anchor", "endtxn_lock_span":
		if len(in.Keys) == 0 {
			return nil, errors.New("control input without keys")
		}
		anchor := s.key(in.Keys[0])
		if in.Via == "endtxn_lock_span" {
			anchor = roachpb.Key(fmt.Sprintf("%sanchor-%d", crdbConformanceKeyPrefix, in.Txn))
		}
		et := &kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: anchor}, Commit: status == roachpb.COMMITTED}
		for _, k := range in.Keys {
			et.LockSpans = append(et.LockSpans, roachpb.Span{Key: s.key(k)})
		}
		ba := &kvpb.BatchRequest{}
		ba.Txn = s.txn(in.Txn, crdbConformanceSortKey{Wall: in.TUs})
		ba.Txn.Key = anchor
		ba.Add(et)
		br := &kvpb.BatchResponse{}
		br.Txn = ba.Txn.Clone()
		if in.Type == "control_applied" {
			br.Txn.Status = status
		} // control_arrive: the message has arrived, nothing is applied yet
		return []crdbConformanceControl{{Method: method, Req: ba, Resp: br}}, nil
	}
	return nil, errors.Newf("via %q is not a CockroachDB event", in.Via)
}

// apply sets the package knobs as the COCKROACH_JUICER_* environment
// would for cfg; fields the scenario does not write keep the code defaults.
func (s *crdbConformance) apply(cfg *spectest.Config) error {
	k := s.defaults
	k.enabled = true
	if mode := cfg.FilterMode(); mode != "fixed" && mode != "hotspot" {
		return errors.Newf("invalid filter mode %q", mode)
	}
	if f := cfg.Filter; f != nil {
		if cfg.FilterMode() == "fixed" {
			var hashes []string
			for _, mk := range f.ManagedKeys {
				hashes = append(hashes, juicerKeyHashString(s.key(mk)))
			}
			k.manualSet, k.manualRaw = true, strings.Join(hashes, ",")
		}
		if f.K != nil {
			k.k = *f.K
		}
		if f.S != nil {
			k.s = *f.S
		}
		if f.IdleEpochs != nil {
			k.idle = *f.IdleEpochs
		}
		if f.EpochUs != nil {
			if *f.EpochUs%1000 != 0 {
				return errors.Newf("epoch %d us is not whole milliseconds", *f.EpochUs)
			}
			k.epochMs = int(*f.EpochUs / 1000)
		}
	}
	if cfg.Rules != nil {
		k.rules = *cfg.Rules
	}
	if cfg.HUs != nil {
		if *cfg.HUs%1000 != 0 {
			return errors.Newf("H %d us is not whole milliseconds", *cfg.HUs)
		}
		k.maxHold = int(*cfg.HUs / 1000)
	}
	if b := cfg.Buffer; b != nil {
		switch b.Policy {
		case "fixed":
			k.aw = false
			if b.WUs != nil {
				k.fixedWait = float64(*b.WUs) / 1000
			}
		case "adaptive":
			k.aw = b.Enabled != nil && *b.Enabled
		}
	}
	if cfg.LatePolicy != nil {
		k.late = *cfg.LatePolicy
	}
	if cfg.Diag != nil {
		k.diag = *cfg.Diag
	}
	if cfg.MaxHeld != nil {
		return errors.New("CockroachDB has no max_held")
	}
	if cfg.QueueRemoval != nil && *cfg.QueueRemoval != "drain" {
		return errors.Newf("queue removal %q", *cfg.QueueRemoval)
	}
	k.set()
	return nil
}

// serverOptions runs juicerServerOptions, turning its startup refusal (a
// panic) into an error.
func crdbConformanceServerOptions() (opts []grpc.ServerOption, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Newf("refused: %v", r)
		}
	}()
	return juicerServerOptions(), nil
}

func (s *crdbConformance) Build(
	cfg *spectest.Config, clock juicer.Clock,
) (*grpc.Server, error) {
	if err := s.apply(cfg); err != nil {
		return nil, err
	}
	opts, err := crdbConformanceServerOptions()
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(append(opts, grpc.JuicerStepped(clock, nil))...)
	ji := srv.Juicer()
	if ji == nil {
		srv.Stop()
		return nil, errors.New("the server has no Juicer interceptor")
	}
	return srv, nil
}

func (s *crdbConformance) Banner(cfg *spectest.Config) (map[string]any, string, error) {
	if err := s.apply(cfg); err != nil {
		return nil, "", err
	}
	if _, err := crdbConformanceServerOptions(); err != nil {
		return nil, "", err
	}
	line := juicerSpecBanner()
	return parseCRDBSpecBanner(line), line, nil
}

// splitOutside splits s at sep where sep is outside parentheses.
func splitOutside(s string, sep rune) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case sep:
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// bannerTokens splits the declaration line into its key=value fields.
func bannerTokens(line string) map[string]string {
	out := map[string]string{}
	for _, tok := range splitOutside(line, ' ') {
		if k, v, ok := strings.Cut(strings.TrimSpace(tok), "="); ok {
			out[k] = v
		}
	}
	return out
}

func trimParens(s string) string {
	if i := strings.Index(s, "("); i >= 0 {
		return s[:i]
	}
	return s
}

func millisField(v string) (int64, bool) {
	ms, err := strconv.ParseFloat(strings.TrimSuffix(v, "ms"), 64)
	return int64(ms * 1000), err == nil && strings.HasSuffix(v, "ms")
}

// parseCRDBSpecBanner maps the "juicer: spec config:" line onto the neutral
// fields of the suite's FORMAT 6.2.
func parseCRDBSpecBanner(line string) map[string]any {
	t := bannerTokens(line)
	out := map[string]any{}
	str := func(field, tok string) {
		if v, ok := t[tok]; ok {
			out[field] = v
		}
	}
	num := func(field, tok string) {
		if v, ok := t[tok]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				out[field] = n
			}
		}
	}
	list := func(field, tok string) {
		if v, ok := t[tok]; ok {
			out[field] = strings.Split(v, ",")
		}
	}
	str("spec_version", "spec")
	list("intercepted_types", "intercept")
	list("level1_abort_sources", "abort_sources")
	num("K", "K")
	num("S", "S")
	num("idle_epochs", "idle_epochs")
	if v, ok := t["epoch"]; ok {
		if us, ok := millisField(v); ok {
			out["epoch_us"] = us
		}
	}
	if v, ok := t["max_hold"]; ok {
		if us, ok := millisField(v); ok {
			out["H_us"] = us
		}
	}
	if v, ok := t["nominate"]; ok {
		out["nomination"] = strings.ReplaceAll(v, "-", "_")
	}
	str("rules", "rules")
	if v, ok := t["hold_end"]; ok {
		ev := map[string]any{}
		for _, part := range splitOutside(v, ';') {
			class, events, ok := strings.Cut(part, ":")
			if !ok {
				continue
			}
			var names []string
			for _, e := range splitOutside(events, '|') {
				names = append(names, trimParens(e))
			}
			ev[trimParens(class)] = names
		}
		out["hold_end_events"] = ev
	}
	if v, ok := t["buffer"]; ok {
		b := map[string]any{"policy": trimParens(v), "adaptive": strings.HasPrefix(v, "adaptive")}
		if strings.HasPrefix(v, "fixed(") {
			if us, ok := millisField(strings.TrimSuffix(strings.TrimPrefix(v, "fixed("), ")")); ok {
				b["W_us"] = us
			}
		}
		out["buffer"] = b
	}
	str("late_policy", "late")
	str("queue_removal", "removal")
	if v, ok := t["sort_key"]; ok {
		if strings.HasPrefix(v, "batch-now") {
			out["sort_key"] = "batch_clock"
		} else {
			out["sort_key"] = v
		}
	}
	str("caller_abandon", "caller_abandon")
	str("per_message_diag", "diag")
	if v, ok := t["event_log"]; ok {
		out["event_log"] = trimParens(v)
	}
	return out
}

// TestJuicerSpecConformanceAdapter drives every CRDB adapter/banner scenario
// through production server options, the real SPI and the current shared core.
func TestJuicerSpecConformanceAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	saved := readCRDBKnobs()
	defer saved.set()
	// Keep the suite and parser at the same submodule revision. The former
	// CRDB copy has identical scenario bytes but older README/AMBIGUITIES files,
	// hence a different integrity manifest. Do not bypass the integrity check.
	var suiteDir string
	if bazel.BuiltWithBazel() {
		sums, err := bazel.Runfile("external/org_golang_google_grpc/juicer/testdata/spec-conformance/SHA256SUMS")
		if err != nil {
			t.Fatal(err)
		}
		suiteDir = filepath.Dir(sums)
	} else {
		suiteDir = datapathutils.RewritableDataPath(t, "juicer-grpc-go", "juicer", "testdata", "spec-conformance")
	}
	suite, err := spectest.LoadSuite(suiteDir)
	if err != nil {
		t.Fatal(err)
	}
	var results []spectest.Result
	for i := range suite {
		sc := &suite[i]
		for _, v := range sc.Variants() {
			r := spectest.NewResult(sc, v, "cockroach-juicer", os.Getenv("JUICER_CONFORMANCE_COMMIT"))
			r.System = "crdb"
			switch {
			case !sc.Applies("crdb"):
				r.Result, r.SkipReason = "skip", "not_applicable"
			case sc.Status != "active":
				r.Result, r.SkipReason = "skip", sc.Status
			case sc.Layer != "adapter" && sc.Layer != "banner":
				r.Result, r.SkipReason = "skip", "unsupported_layer"
			default:
				t.Run(r.ID(), func(t *testing.T) {
					r = runCRDBConformance(sc, v, saved, r)
					if r.Result == "error" {
						t.Error(r.Error)
					}
					for _, d := range r.Diffs {
						t.Errorf("%s: expected %v, actual %v", d.Field, d.Expected, d.Actual)
					}
					for _, w := range r.Warnings {
						t.Logf("warning: %s expected %v, actual %v", w.Field, w.Expected, w.Actual)
					}
				})
			}
			results = append(results, r)
		}
	}
	sum := spectest.Summarize(results)
	sum.System = "crdb"
	report := os.Getenv("JUICER_CONFORMANCE_REPORT")
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); report == "" && dir != "" {
		report = filepath.Join(dir, "conformance-adapter.jsonl")
	}
	if report != "" {
		f, err := os.Create(report)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, r := range results {
			if err := enc.Encode(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := enc.Encode(sum); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("results %v", sum.Counts)
}

// The banner parser reads the declaration line's spec terms.
func TestJuicerConformanceBannerParse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	saved := readCRDBKnobs()
	defer saved.set()
	f := parseCRDBSpecBanner(juicerSpecBanner())
	he, _ := f["hold_end_events"].(map[string]any)
	if fmt.Sprint(he["control"]) != "[applied own_failure ttl]" || fmt.Sprint(he["request"]) != "[own_reply own_failure ttl]" {
		t.Fatalf("hold_end_events %v from %q", he, juicerSpecBanner())
	}
	if f["caller_abandon"] != "withdraw" || f["event_log"] != "off" || f["sort_key"] != "batch_clock" {
		t.Fatalf("fields %v", f)
	}
}

// The shared suite's scalar helpers select juicer-cc. CRDB keeps its HLC
// sorting keys and configuration adapter here, while sharing parsing and all
// neutral trace comparisons with spectest.
type crdbConformanceSortKey struct{ Wall, Logical int64 }

func crdbConformanceTimestamp(in *spectest.Input) (crdbConformanceSortKey, error) {
	raw := in.Ts
	if alt, ok := in.TsBySystem["crdb"]; ok {
		raw = alt
	}
	var wall int64
	if string(raw) != "null" && json.Unmarshal(raw, &wall) == nil {
		return crdbConformanceSortKey{Wall: wall}, nil
	}
	var pair []int64
	if json.Unmarshal(raw, &pair) == nil && len(pair) == 2 {
		return crdbConformanceSortKey{Wall: pair[0], Logical: pair[1]}, nil
	}
	return crdbConformanceSortKey{}, errors.Newf("%s: invalid CRDB sorting key %s", in.Msg, raw)
}

type crdbConformanceControl struct {
	Method    string
	Req, Resp any
	Err       error
}

// Select the CRDB branch of every per_system assertion, including nested
// banner fields. Keep the wrapper so null retains its "must not print" meaning.
func crdbConformanceExpect(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var value any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var selectSystem func(any)
	selectSystem = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if per, ok := x["per_system"].(map[string]any); ok {
				x["per_system"] = map[string]any{spectest.System: per["crdb"]}
			}
			for _, child := range x {
				selectSystem(child)
			}
		case []any:
			for _, child := range x {
				selectSystem(child)
			}
		}
	}
	selectSystem(value)
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("active scenario without expect")
	}
	return out, nil
}

func runCRDBConformance(sc *spectest.Scenario, v *spectest.Variant, defaults crdbKnobs, r spectest.Result) spectest.Result {
	cfg, err := sc.MergedConfig(v)
	if err != nil {
		r.Result, r.Error = "error", err.Error()
		return r
	}
	expect, err := crdbConformanceExpect(sc.Expect)
	if err != nil {
		r.Result, r.Error = "error", err.Error()
		return r
	}
	if v != nil && v.ExpectStartup != "" {
		expect = map[string]json.RawMessage{"startup": json.RawMessage(strconv.Quote(v.ExpectStartup))}
	}
	sys := &crdbConformance{defaults: defaults}
	if err := sys.Prepare(sc, &cfg); err != nil {
		r.Result, r.Error = "error", err.Error()
		return r
	}
	tr := spectest.NewTrace()
	startup, startupErr := "accept", ""
	if sc.Layer == "banner" && (v == nil || v.ExpectStartup == "") {
		tr.Banner, _, err = sys.Banner(&cfg)
	} else {
		h := newCRDBConformanceHarness(sys, sc, &cfg)
		h.server, err = sys.Build(&cfg, h.clock)
		if err == nil {
			if h.server.Juicer().Stepper() == nil {
				err = errors.New("server did not install a ManualDriver")
			} else if v == nil || v.ExpectStartup == "" {
				err = h.run()
				tr = h.tr
			}
			if closeErr := h.close(); err == nil {
				err = closeErr
			}
			if err != nil {
				r.Result, r.Error = "error", err.Error()
				return r
			}
		}
	}
	if err != nil {
		startup, startupErr = "reject", err.Error()
	}
	var diffs []spectest.Diff
	if raw, ok := expect["startup"]; ok {
		var want string
		if err := json.Unmarshal(raw, &want); err != nil {
			r.Result, r.Error = "error", err.Error()
			return r
		}
		if want != startup {
			diffs = append(diffs, spectest.Diff{Field: "startup", Expected: want, Actual: startup + " " + startupErr})
		}
		delete(expect, "startup")
	}
	if startup == "reject" && len(expect) != 0 && len(diffs) == 0 {
		diffs = append(diffs, spectest.Diff{Field: "startup", Expected: "accept", Actual: startupErr})
	}
	if startup == "accept" {
		diffs = append(diffs, spectest.Compare(expect, tr, sc.Tolerance())...)
	}
	r.Finish(diffs)
	return r
}

type crdbConformanceMessage struct {
	name                       string
	req                        any
	ctx                        context.Context
	cancel                     context.CancelFunc
	reply                      chan crdbConformanceReply
	done                       chan struct{}
	control, nonTxn, answering bool
	expectedAdmissions         int
	admissions                 []juicer.Admission
	payload                    string
	trace                      *spectest.MsgTrace
}

type crdbConformanceReply struct {
	resp any
	err  error
}
type crdbConformanceToken struct {
	msg, key  string
	hold      int
	admission juicer.Admission
}

var crdbConformanceBase = time.Unix(1_700_000_000, 0)

const crdbConformanceTimeout = 10 * time.Second

type crdbConformanceHarness struct {
	sys                               *crdbConformance
	sc                                *spectest.Scenario
	cfg                               *spectest.Config
	clock                             *juicer.ManualClock
	server                            *grpc.Server
	mu                                sync.Mutex
	frozen                            bool
	current                           *crdbConformanceMessage
	msgs                              map[string]*crdbConformanceMessage
	all                               []*crdbConformanceMessage
	tokens                            map[uint64]*crdbConformanceToken
	released                          map[uint64]bool
	keys                              map[string]string // core hash -> scenario key
	liveQueues, maxQueues, waitTimers int
	ctlIntercepted, ctlDirect         int64
	tr                                *spectest.Trace
}

func newCRDBConformanceHarness(sys *crdbConformance, sc *spectest.Scenario, cfg *spectest.Config) *crdbConformanceHarness {
	h := &crdbConformanceHarness{sys: sys, sc: sc, cfg: cfg, clock: juicer.NewManualClock(crdbConformanceBase),
		msgs: map[string]*crdbConformanceMessage{}, tokens: map[uint64]*crdbConformanceToken{},
		released: map[uint64]bool{}, keys: map[string]string{}, tr: spectest.NewTrace()}
	h.tr.Hotspot = cfg.FilterMode() == "hotspot"
	for k := range sys.keys {
		h.keys[sys.QueueKey(k)] = k
	}
	return h
}

func (h *crdbConformanceHarness) now() int64 {
	return h.clock.Now().Sub(crdbConformanceBase).Microseconds()
}
func crdbConformanceUs(at time.Time) int64 { return at.Sub(crdbConformanceBase).Microseconds() }
func crdbConformanceReady(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
func (h *crdbConformanceHarness) fault(field string, expected, actual any) {
	h.tr.Faults = append(h.tr.Faults, spectest.Diff{Field: "invariant." + field, Expected: expected, Actual: actual})
}

// Hooks record implementation decisions only; they never consult expect.
func (h *crdbConformanceHarness) record(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.frozen {
		f()
	}
}

func (h *crdbConformanceHarness) observer() *juicer.Observer {
	return &juicer.Observer{
		Admit: func(key string, token uint64, _ int64, _ int64, _ juicer.OperationType, _ bool) {
			h.record(func() {
				if h.current == nil {
					h.fault("runner", "marker during admission", token)
					return
				}
				if _, dup := h.tokens[token]; dup {
					h.fault("I3", "unique admission token", token)
				}
				h.tokens[token] = &crdbConformanceToken{msg: h.current.name, key: h.keys[key], hold: -1}
			})
		},
		Release: func(key string, token uint64, at time.Time) {
			h.record(func() {
				ref := h.tokens[token]
				if ref == nil {
					h.fault("runner", "known release token", token)
					return
				}
				if h.released[token] {
					h.fault("I3", "one release per marker", token)
				}
				h.released[token] = true
				h.tr.Releases[h.keys[key]] = append(h.tr.Releases[h.keys[key]], spectest.Release{Msg: ref.msg, T: crdbConformanceUs(at)})
			})
		},
		Late: func(key string, token uint64, bypass bool) {
			h.record(func() {
				// Late is emitted during Enqueue, before the Admit callback.
				if h.current == nil {
					h.fault("runner", "late marker during admission", token)
					return
				}
				handling := "queued"
				if bypass {
					handling = "bypass"
				}
				h.tr.Late = append(h.tr.Late, spectest.Late{Msg: h.current.name, Key: h.keys[key], Handling: handling})
			})
		},
		HoldStart: func(token uint64, at time.Time) {
			h.record(func() {
				ref := h.tokens[token]
				if ref == nil {
					h.fault("runner", "known hold token", token)
					return
				}
				if ref.hold >= 0 {
					h.fault("I3", "one hold per marker", token)
					return
				}
				ref.hold = len(h.tr.Holds)
				h.tr.Holds = append(h.tr.Holds, spectest.Hold{Msg: ref.msg, Key: ref.key, Start: crdbConformanceUs(at)})
			})
		},
		HoldEnd: func(token uint64, reason juicer.HoldEndReason, at time.Time) {
			h.record(func() {
				ref := h.tokens[token]
				if ref == nil || ref.hold < 0 || h.tr.Holds[ref.hold].Ended {
					h.fault("I3", "end of a live hold", token)
					return
				}
				hold := &h.tr.Holds[ref.hold]
				hold.Ended, hold.End = true, crdbConformanceUs(at)
				hold.Reason = map[juicer.HoldEndReason]string{juicer.HoldEndEvent: "event", juicer.HoldEndExpiry: "ttl", juicer.HoldEndCancel: "cancel", juicer.HoldEndEvicted: "queue_deleted"}[reason]
				if hold.Reason == "" {
					h.fault("runner", "known hold-end reason", reason)
				}
			})
		},
		Lifecycle: func(key, event string, at time.Time) {
			h.record(func() {
				h.tr.QueueEvents = append(h.tr.QueueEvents, spectest.QueueEvent{Key: h.keys[key], Event: event, T: crdbConformanceUs(at)})
				switch event {
				case "created":
					h.liveQueues++
				case "deleted":
					h.liveQueues--
				}
				if h.liveQueues > h.maxQueues {
					h.maxQueues = h.liveQueues
				}
			})
		},
		EpochEnd: func(end juicer.EpochEnd) {
			h.record(func() {
				counts := map[string]int64{}
				for key, stat := range end.Keys {
					counts[h.keys[key]] = int64(stat.Reorders)
				}
				h.tr.Epochs[end.Epoch] = counts
			})
		},
		WaitTimer: func(string, time.Duration) { h.record(func() { h.waitTimers++ }) },
	}
}

// Wrap the installed production admission callback, preserving its return
// value and cancellation function. This observes the real wait channels
// without requiring the retired dispatcher's OnAdmissionWait test hook.
func (h *crdbConformanceHarness) observeAdmissions() {
	td := h.server.Juicer().Stepper().Dispatcher()
	enqueue := td.EnqueueLogical
	td.EnqueueLogical = func(key string, op juicer.OperationType, ts, logical, txn int64) juicer.Admission {
		a := enqueue(key, op, ts, logical, txn)
		h.mu.Lock()
		if !h.frozen && h.current != nil {
			m := h.current
			m.admissions = append(m.admissions, a)
			if ref := h.tokens[a.Token]; ref != nil {
				ref.admission = a
			}
		}
		h.mu.Unlock()
		return a
	}
	h.server.Juicer().Stepper().QueueManager().SetObserver(h.observer())
}

func (h *crdbConformanceHarness) settle() error {
	deadline := time.Now().Add(crdbConformanceTimeout)
	stable := false
	for {
		h.server.Juicer().Stepper().Pump()
		h.mu.Lock()
		quiet := true
		for _, m := range h.all {
			if crdbConformanceReady(m.done) {
				continue
			}
			if m.trace != nil && m.trace.State == spectest.Delivered && !m.answering {
				continue
			}
			blocked := false
			for _, a := range m.admissions {
				blocked = blocked || !crdbConformanceReady(a.Ready)
			}
			if len(m.admissions) != m.expectedAdmissions || !blocked || m.ctx.Err() != nil {
				quiet = false
			}
		}
		h.mu.Unlock()
		if quiet && stable {
			return nil
		}
		stable = quiet
		if quiet {
			continue
		}
		if time.Now().After(deadline) {
			return errors.New("CRDB interceptor did not quiesce")
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *crdbConformanceHarness) start(in *spectest.Input, method string, req any, control, nonTxn bool, reply crdbConformanceReply) error {
	m := &crdbConformanceMessage{name: in.Msg, req: req, control: control, nonTxn: nonTxn,
		reply: make(chan crdbConformanceReply, 1), done: make(chan struct{}), payload: h.sys.Payload(req)}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	if !control && !nonTxn {
		td := h.server.Juicer().Stepper().Dispatcher()
		ra := h.server.Juicer().Stepper().ReorderAnalyzer()
		if !td.SkipQueueing && !td.RecordOnly && (ra == nil || !ra.QueueingOff()) {
			for _, marker := range newCRDBJuicerSPI().SplitMarker(req) {
				if ra == nil || ra.IsTracking(fmt.Sprint(marker.Key)) {
					m.expectedAdmissions++
				}
			}
		}
	}
	h.mu.Lock()
	if !control {
		if _, dup := h.msgs[m.name]; dup {
			h.mu.Unlock()
			m.cancel()
			return errors.Newf("duplicate message %s", m.name)
		}
		h.msgs[m.name] = m
		m.trace = h.tr.AddMsg(m.name, 0)
	}
	h.all, h.current = append(h.all, m), m
	h.mu.Unlock()
	go func() {
		defer close(m.done)
		handler := func(_ context.Context, actual any) (any, error) {
			if control {
				return reply.resp, reply.err
			}
			h.mu.Lock()
			if m.trace.State != spectest.Held {
				h.fault("I1", "exactly one delivery", m.name)
			}
			if payload := h.sys.Payload(actual); payload != m.payload {
				h.fault("I2", m.payload, payload)
			}
			m.trace.State, m.trace.T, m.trace.Path = spectest.Delivered, h.now(), "direct"
			if nonTxn {
				m.trace.Path = "not_intercepted"
			}
			for _, a := range m.admissions {
				if a.Queued {
					m.trace.Path = "queued"
				}
			}
			h.mu.Unlock()
			r := <-m.reply
			return r.resp, r.err
		}
		_, err := h.server.JuicerUnaryInterceptor()(m.ctx, req, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		h.mu.Lock()
		if !control && m.trace.State == spectest.Held && err != nil {
			m.trace.State, m.trace.T = spectest.Dropped, h.now()
		}
		h.mu.Unlock()
	}()
	err := h.settle()
	h.mu.Lock()
	h.current = nil
	h.mu.Unlock()
	return err
}

func (h *crdbConformanceHarness) apply(in *spectest.Input) error {
	switch in.Type {
	case "arrive":
		sk, err := crdbConformanceTimestamp(in)
		if err != nil {
			return err
		}
		method, req, err := h.sys.Arrive(in, sk)
		if err != nil {
			return err
		}
		return h.start(in, method, req, false, false, crdbConformanceReply{})
	case "arrive_non_txn":
		method, req, err := h.sys.NonTxn(in)
		if err != nil {
			return err
		}
		return h.start(in, method, req, false, true, crdbConformanceReply{})
	case "reply":
		h.mu.Lock()
		m := h.msgs[in.Msg]
		if m == nil || m.trace.State != spectest.Delivered || m.answering || crdbConformanceReady(m.done) {
			h.mu.Unlock()
			return errors.Newf("reply to undelivered message %s", in.Msg)
		}
		m.answering = true
		h.mu.Unlock()
		if in.Outcome != "ok" && in.Outcome != "failed" {
			return errors.Newf("unknown outcome %q", in.Outcome)
		}
		resp, err := h.sys.Reply(m.req, in.Outcome)
		m.reply <- crdbConformanceReply{resp, err}
	case "control_arrive", "control_applied":
		msgs, err := h.sys.Control(in)
		if err != nil {
			return err
		}
		for _, cm := range msgs {
			before := h.server.Juicer().Stepper().Dispatcher().Counters()
			if err := h.start(in, cm.Method, cm.Req, true, false, crdbConformanceReply{cm.Resp, cm.Err}); err != nil {
				return err
			}
			after := h.server.Juicer().Stepper().Dispatcher().Counters()
			h.ctlIntercepted += after.Intercepted - before.Intercepted
			h.ctlDirect += after.Direct - before.Direct
		}
	case "cancel":
		m := h.msgs[in.Msg]
		if m == nil {
			return errors.Newf("cancel of unknown message %s", in.Msg)
		}
		m.cancel()
	case "level1_abort":
		keys := make([]string, len(in.Keys))
		for i, k := range in.Keys {
			keys[i] = h.sys.QueueKey(k)
		}
		h.server.Juicer().Stepper().Level1Abort(in.Txn, keys)
	case "epoch_end":
		h.server.Juicer().Stepper().EndEpoch()
	case "advance":
	default:
		return errors.Newf("unknown input %q", in.Type)
	}
	return h.settle()
}

func (h *crdbConformanceHarness) advance(us int64) error {
	limit := crdbConformanceBase.Add(time.Duration(us) * time.Microsecond)
	for h.clock.FireNext(limit) {
		if err := h.settle(); err != nil {
			return err
		}
	}
	h.clock.Set(limit)
	return h.settle()
}

func (h *crdbConformanceHarness) counters() map[string]any {
	c := h.server.Juicer().Stepper().Dispatcher().Counters()
	q := h.server.Juicer().Stepper().QueueManager().AggregateCounters()
	byType := func(a [juicer.NumOperationTypes]int64) map[string]int64 {
		return map[string]int64{"read": a[juicer.OpGet], "write": a[juicer.OpSet], "lock_read": a[juicer.OpGetForPut]}
	}
	return map[string]any{"intercepted": c.Intercepted - h.ctlIntercepted, "direct": c.Direct - h.ctlDirect,
		"queued_delivered": c.Queued, "dropped": c.Dropped, "held_now": c.Held,
		"holds_created": q.Holds.Created, "hold_end": map[string]int64{"event": q.Holds.Ended[juicer.HoldEndEvent],
			"ttl": q.Holds.Ended[juicer.HoldEndExpiry], "cancel": q.Holds.Ended[juicer.HoldEndCancel], "queue_deleted": q.Holds.Ended[juicer.HoldEndEvicted]},
		"late_queued": byType(q.LateQueued), "late_bypass": byType(q.LateBypassed),
		"queues_now": int64(q.Queues), "queues_max": int64(h.maxQueues)}
}

// Check the implementation's queue/hold counts against actual admission
// readiness and hold callbacks, as well as message states and conservation.
func (h *crdbConformanceHarness) checkpoint() spectest.Checkpoint {
	cp := spectest.Checkpoint{QueueLen: map[string]int{}, Counters: map[string]int64{}}
	counts := h.counters()
	for key, value := range counts {
		if n, ok := value.(int64); ok {
			cp.Counters[key] = n
		}
	}
	queues := map[string]juicer.QueueCounters{}
	for hash, key := range h.keys {
		if q, ok := h.server.Juicer().Stepper().QueueManager().QueueFor(hash); ok {
			c := q.Counters()
			queues[key] = c
			cp.QueueLen[key] = c.Len
			if c.Draining {
				cp.Draining = append(cp.Draining, key)
			} else {
				cp.Managed = append(cp.Managed, key)
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	queued, holds := map[string]int{}, map[string]int64{}
	pairs := map[spectest.MsgKey]int{}
	for token, ref := range h.tokens {
		if ref.admission.Queued && !crdbConformanceReady(ref.admission.Ready) {
			queued[ref.key]++
		}
		if ref.hold < 0 {
			continue
		}
		hd := &h.tr.Holds[ref.hold]
		pairs[spectest.MsgKey{Msg: hd.Msg, Key: hd.Key}]++
		if hd.Ended {
			continue
		}
		holds[ref.key]++
		if !h.released[token] || !ref.admission.Queued || !crdbConformanceReady(ref.admission.Ready) {
			h.fault("I3", "hold of a released marker outside the heap", token)
		}
		if h.cfg.HUs != nil && h.now()-hd.Start >= *h.cfg.HUs {
			hd.Aged = true
		} else {
			cp.OpenHolds = append(cp.OpenHolds, spectest.MsgKey{Msg: hd.Msg, Key: hd.Key})
		}
	}
	for pair, n := range pairs {
		if n > 1 {
			h.fault("I3", "one hold per (message, key)", pair)
		}
	}
	for key, q := range queues {
		if q.Len != queued[key] || q.Holds.Live != holds[key] {
			h.fault("I3", fmt.Sprintf("%s queued=%d live_holds=%d", key, queued[key], holds[key]), q)
		}
	}
	for key, n := range holds {
		if _, ok := queues[key]; !ok && n != 0 {
			h.fault("I3", "live holds belong to live queues", key)
		}
	}
	var arrived, delivered, dropped, pending int64
	for _, m := range h.msgs {
		if m.nonTxn {
			continue
		}
		arrived++
		switch m.trace.State {
		case spectest.Delivered:
			delivered++
		case spectest.Dropped:
			dropped++
		default:
			pending++
			cp.HeldMsgs = append(cp.HeldMsgs, m.name)
		}
	}
	c := cp.Counters
	if c["intercepted"] != c["direct"]+c["queued_delivered"]+c["dropped"]+c["held_now"] {
		h.fault("I4", "intercepted = direct + queued + dropped + held", c)
	}
	if arrived != c["intercepted"] || delivered != c["direct"]+c["queued_delivered"] || dropped != c["dropped"] || pending != c["held_now"] {
		h.fault("I1", map[string]int64{"arrived": arrived, "delivered": delivered, "dropped": dropped, "held": pending}, c)
	}
	return cp
}

func (h *crdbConformanceHarness) run() error {
	h.observeAdmissions()
	checkpoints := append([]int64(nil), h.sc.CheckpointTimes...)
	sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i] < checkpoints[j] })
	take := func(before int64, inclusive bool) error {
		for len(checkpoints) != 0 && (checkpoints[0] < before || inclusive && checkpoints[0] == before) {
			t := checkpoints[0]
			if err := h.advance(t); err != nil {
				return err
			}
			h.tr.Checkpoints[t] = h.checkpoint()
			checkpoints = checkpoints[1:]
		}
		return nil
	}
	last := int64(0)
	for i := range h.sc.Inputs {
		in := &h.sc.Inputs[i]
		if in.TUs < last {
			return errors.Newf("input %d goes backward in time", i)
		}
		last = in.TUs
		if err := take(in.TUs, false); err != nil {
			return err
		}
		if err := h.advance(in.TUs); err != nil {
			return err
		}
		if err := h.apply(in); err != nil {
			return errors.Wrapf(err, "input %d (%s)", i, in.Type)
		}
		h.checkpoint()
	}
	if last > h.sc.EndUs {
		return errors.New("end_us precedes last input")
	}
	if err := take(h.sc.EndUs, true); err != nil {
		return err
	}
	if err := h.advance(h.sc.EndUs); err != nil {
		return err
	}
	h.checkpoint()
	h.tr.Counters = h.counters()
	h.tr.WaitTimers = &h.waitTimers
	// Use HLC-pair ranks only for spectest's equal-key release-order grouping.
	// Timestamps delivered to CRDB remain the original wall/logical values.
	var keys []crdbConformanceSortKey
	byMsg := map[string]crdbConformanceSortKey{}
	for i := range h.sc.Inputs {
		in := &h.sc.Inputs[i]
		if in.Type != "arrive" {
			continue
		}
		key, err := crdbConformanceTimestamp(in)
		if err != nil {
			return err
		}
		keys, byMsg[in.Msg] = append(keys, key), key
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Wall < keys[j].Wall || keys[i].Wall == keys[j].Wall && keys[i].Logical < keys[j].Logical
	})
	ranks := map[crdbConformanceSortKey]int64{}
	for _, key := range keys {
		if _, ok := ranks[key]; !ok {
			ranks[key] = int64(len(ranks))
		}
	}
	for msg, key := range byMsg {
		h.tr.Msgs[msg].Ts = ranks[key]
	}
	return nil
}

func (h *crdbConformanceHarness) close() error {
	h.mu.Lock()
	h.frozen = true
	for _, m := range h.all {
		m.cancel()
		// Teardown never contributes observations at end_us. Complete any
		// handler, including one entering after its cancellation was signalled.
		resp, err := h.sys.Reply(m.req, "")
		select {
		case m.reply <- crdbConformanceReply{resp, err}:
		default:
		}
	}
	h.mu.Unlock()
	defer h.server.Stop()
	deadline := time.NewTimer(crdbConformanceTimeout)
	defer deadline.Stop()
	for _, m := range h.all {
		select {
		case <-m.done:
		case <-deadline.C:
			return errors.New("interceptor goroutine did not stop")
		}
	}
	return nil
}
