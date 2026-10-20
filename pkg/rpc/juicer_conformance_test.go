// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

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
	"google.golang.org/grpc/juicer/conformance"
)

// The Juicer spec conformance suite's adapter and banner layers
// (docs/eval-plan-20261019/spec-conformance in the experiment workspace,
// copied byte for byte into testdata/spec-conformance): the core layer runs
// in the grpc fork (juicer/conformance); the scenarios that need
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

// crdbConformance is the conformance.System of CockroachDB's adapter layer.
type crdbConformance struct {
	defaults crdbKnobs
	keys     map[string]roachpb.Key
	anchors  map[int64]roachpb.Key
	onePC    map[int64]bool
}

var _ conformance.System = (*crdbConformance)(nil)

func (s *crdbConformance) Name() string           { return "crdb" }
func (s *crdbConformance) Runs(layer string) bool { return layer == "adapter" || layer == "banner" }

func (s *crdbConformance) key(k string) roachpb.Key {
	return roachpb.Key(crdbConformanceKeyPrefix + k)
}

// Prepare maps the scenario's keys and looks ahead for the transactions
// whose commit is a one-phase commit (their write batch carries the EndTxn)
// and for each transaction's anchor key.
func (s *crdbConformance) Prepare(sc *conformance.Scenario, cfg *conformance.Config) error {
	s.keys, s.anchors, s.onePC = map[string]roachpb.Key{}, map[int64]roachpb.Key{}, map[int64]bool{}
	for _, k := range cfg.ManagedKeys {
		s.keys[k] = s.key(k)
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

// TxnID: the scenario's transaction n has the UUID whose bytes 8..16 are n
// little-endian, so juicerTxnIDFromUUID maps it back to n.
func (s *crdbConformance) TxnID(txn int64) int64 { return txn }

func crdbConformanceUUID(txn int64) uuid.UUID {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[8:], uint64(txn))
	return uuid.FromBytesOrNil(b[:])
}

func (s *crdbConformance) txn(n int64, sk conformance.SortKey) *roachpb.Transaction {
	txn := &roachpb.Transaction{}
	txn.ID = crdbConformanceUUID(n)
	txn.Key = s.anchors[n]
	ts := hlc.Timestamp{WallTime: sk.Wall, Logical: int32(sk.Logical)}
	txn.MinTimestamp, txn.ReadTimestamp, txn.WriteTimestamp = ts, ts, ts
	txn.Status = roachpb.PENDING
	return txn
}

func (s *crdbConformance) Arrive(in *conformance.Input, sk conformance.SortKey) (string, any, error) {
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
				Value: roachpb.MakeValueFromString(in.PayloadOrMsg())})
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

func (s *crdbConformance) Reply(_ *conformance.Input, req any, outcome string) (any, error) {
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
func (s *crdbConformance) NonTxn(in *conformance.Input) (string, any, error) {
	return "/cockroach.rpc.Heartbeat/Ping", &PingRequest{Ping: in.Msg}, nil
}

// Control turns a control input into the batch table 5-C names: an
// EndTxn on the anchor (also for a control input without via), a
// non-transactional ResolveIntent naming the transaction (IntentTxn), or
// nothing for a one-phase commit, whose reply carried the commit.
func (s *crdbConformance) Control(in *conformance.Input) ([]conformance.ControlMessage, bool, error) {
	var status roachpb.TransactionStatus
	switch in.Kind {
	case "commit":
		status = roachpb.COMMITTED
	case "abort":
		status = roachpb.ABORTED
	default:
		return nil, false, errors.Newf("CockroachDB has no %s message", in.Kind)
	}
	method := juicerUnaryBatchPaths[0]
	switch in.Via {
	case "one_pc_reply":
		return nil, false, nil
	case "resolve_intent":
		ba := &kvpb.BatchRequest{}
		meta := s.txn(in.Txn, conformance.SortKey{Wall: in.TUs}).TxnMeta
		for _, k := range in.Keys {
			ba.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: s.key(k)}, IntentTxn: meta, Status: status})
		}
		return []conformance.ControlMessage{{Method: method, Req: ba, Resp: &kvpb.BatchResponse{}}}, false, nil
	case "", "endtxn_anchor", "endtxn_lock_span":
		if len(in.Keys) == 0 {
			return nil, false, errors.New("control input without keys")
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
		ba.Txn = s.txn(in.Txn, conformance.SortKey{Wall: in.TUs})
		ba.Txn.Key = anchor
		ba.Add(et)
		br := &kvpb.BatchResponse{}
		br.Txn = ba.Txn.Clone()
		if in.Type == "control_applied" {
			br.Txn.Status = status
		} // control_arrive: the message has arrived, nothing is applied yet
		return []conformance.ControlMessage{{Method: method, Req: ba, Resp: br}}, false, nil
	}
	return nil, false, errors.Newf("via %q is not a CockroachDB event", in.Via)
}

// apply sets the package knobs as the COCKROACH_JUICER_* environment
// would for cfg; fields the scenario does not write keep the code defaults.
func (s *crdbConformance) apply(cfg *conformance.Config) error {
	k := s.defaults
	k.enabled = true
	if cfg.FilterMode == "fixed" {
		var hashes []string
		for _, mk := range cfg.ManagedKeys {
			hashes = append(hashes, juicerKeyHashString(s.key(mk)))
		}
		k.manualSet, k.manualRaw = true, strings.Join(hashes, ",")
	}
	if cfg.K != nil {
		k.k = *cfg.K
	}
	if cfg.S != nil {
		k.s = *cfg.S
	}
	if cfg.IdleEpochs != nil {
		k.idle = *cfg.IdleEpochs
	}
	if cfg.EpochUs != nil {
		if *cfg.EpochUs%1000 != 0 {
			return errors.Newf("epoch %d us is not whole milliseconds", *cfg.EpochUs)
		}
		k.epochMs = int(*cfg.EpochUs / 1000)
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
	cfg *conformance.Config, clock juicer.Clock, tracer juicer.Tracer, _ juicer.ILogger,
) (*conformance.Built, error) {
	if err := s.apply(cfg); err != nil {
		return nil, err
	}
	opts, err := crdbConformanceServerOptions()
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(append(opts, grpc.JuicerStepped(clock, tracer))...)
	ji := srv.Juicer()
	if ji == nil {
		srv.Stop()
		return nil, errors.New("the server has no Juicer interceptor")
	}
	return &conformance.Built{Interceptor: ji, Unary: srv.JuicerUnaryInterceptor(), Close: srv.Stop}, nil
}

func (s *crdbConformance) Banner(cfg *conformance.Config) (map[string]any, string, error) {
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

// TestJuicerSpecConformanceAdapter runs the suite's adapter and banner
// scenarios for crdb. TEST_UNDECLARED_OUTPUTS_DIR (bazel) or
// JUICER_CONFORMANCE_REPORT receives the JSONL report;
// JUICER_CONFORMANCE_COMMIT is the commit recorded in it.
func TestJuicerSpecConformanceAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	saved := readCRDBKnobs()
	defer saved.set()
	suite, err := conformance.LoadSuite(datapathutils.TestDataPath(t, "spec-conformance"), conformance.SuiteDigest)
	if err != nil {
		t.Fatal(err)
	}
	sys := &crdbConformance{defaults: saved}
	results := conformance.Run(conformance.Options{Suite: suite, System: sys, Repo: "cockroach-juicer",
		Commit: os.Getenv("JUICER_CONFORMANCE_COMMIT")})
	sum := conformance.Summarize("crdb", results, "adapter+banner:cockroach-juicer pkg/rpc")
	report := os.Getenv("JUICER_CONFORMANCE_REPORT")
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); report == "" && dir != "" {
		report = filepath.Join(dir, "conformance-adapter.jsonl")
	}
	if report != "" {
		if err := conformance.WriteReport(report, results, sum); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	for _, r := range results {
		if r.Result == "fail" || r.Result == "error" {
			t.Errorf("%s", r.Describe())
		}
		if r.Result == "pass" {
			ids = append(ids, r.Scenario+"/"+r.Variant)
		}
		for _, w := range r.Warnings {
			t.Logf("%s: warning: %s expected %v, printed %v", r.Scenario, w.Field, w.Expected, w.Actual)
		}
	}
	sort.Strings(ids)
	t.Logf("results %v; passed %v", sum.Counts, ids)
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
