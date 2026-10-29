// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/juicertrace"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/juicer"
)

// Juicer wires the fork's request-reordering layer (google.golang.org/grpc is
// replaced by github.com/CPI-NS/juicer-grpc-go, see go.mod) into the KV RPC
// server. It is entirely opt-in: with COCKROACH_JUICER unset (the default) no
// Juicer server options are appended and the gRPC server is byte-identical to
// upstream.
var (
	juicerEnabled      = envutil.EnvOrDefaultBool("COCKROACH_JUICER", false)
	juicerSkipQueueing = envutil.EnvOrDefaultBool("COCKROACH_JUICER_SKIP_QUEUEING", false)
	// Matched direct-path diagnostics retain classification and association,
	// but never create queues or holds. Full Juicer off is a separate reference.
	juicerFilterDiagnostic = envutil.EnvOrDefaultString("COCKROACH_JUICER_FILTER_DIAGNOSTIC", "off")
	// juicerFifo degrades every per-key queue from sorting-key order to
	// arrival order (grpc.JuicerFifoQueue -> JuicerInterceptorConfig.FifoMode
	// -> PerKeyQueue appends and pops instead of using its min-heap). It is
	// ORTHOGONAL to COCKROACH_JUICER_RULES: which messages hold, what
	// releases a hold, and the MaxHold TTL are all unchanged; only the order
	// in which a key's queued messages are offered to the enforcer changes.
	// That is the point — it is the control arm that isolates Juicer's
	// reordering from its holding (campaign #22).
	juicerFifo = envutil.EnvOrDefaultBool("COCKROACH_JUICER_FIFO", false)
	// juicerMaxHoldMillis is the hold engine's single TTL, H in the Juicer
	// spec (section 6): no hold outlives it, and it bounds every other timed
	// wait (FIXED_WAIT_MS, the adaptive buffer's cap). It must be at least
	// 1 ms; juicerServerOptions refuses anything else (it used to be clamped
	// to 25 ms by the gate and to the fork's 500 ms default by the other
	// relations). The 25 ms default is a placeholder carried over from the
	// gate era: the spec calibrates H per system as the p99, on the baseline
	// at the workpoint, of "a hot-key transaction's first message arrives ->
	// its commit or abort has been applied on this server", which has not
	// been measured yet.
	juicerMaxHoldMillis    = envutil.EnvOrDefaultInt("COCKROACH_JUICER_MAX_HOLD_MS", 25)
	juicerDebugLogs        = envutil.EnvOrDefaultBool("COCKROACH_JUICER_DEBUG", false)
	juicerUnaryBatchPaths  = []string{"/cockroach.roachpb.Internal/Batch", "/cockroach.roachpb.KVBatch/Batch"}
	juicerStreamBatchPaths = []string{"/cockroach.roachpb.Internal/BatchStream", "/cockroach.roachpb.KVBatch/BatchStream"}
)

// The standard configuration's remaining wait knob. The completion gate is no
// longer a switch of its own: it is the DEFAULT dependency relation
// (COCKROACH_JUICER_RULES=gate, see BuildRules), and its TTL is
// COCKROACH_JUICER_MAX_HOLD_MS. The former COCKROACH_JUICER_HOLD_UNTIL_DONE /
// COCKROACH_JUICER_MAX_BUSY_MS knobs were removed in the 2026-08-26 merge of
// the gate into the rules engine; reproduce earlier campaigns from their
// pinned tags. Historical measurement arms — the damage-scaled window
// (SCALE_FACTOR), the qlen-gated strategy, the random-delay control, and the
// selectable sorting key — went the same way on 2026-08-13.
var (
	// juicerFixedWaitMillis is the uniform-window knob: a constant window
	// imposed on every sorted message, measured from its arrival, in
	// floating-point milliseconds so a sweep can go below 1 ms. 0 (the
	// default) means no window: pure sorting plus whatever relation RULES
	// selects. Campaign #14 relocated the interesting wait-time axis to the
	// gate's MAX_HOLD_MS; this knob remains for uniform-window controls.
	juicerFixedWaitMillis = envutil.EnvOrDefaultFloat64("COCKROACH_JUICER_FIXED_WAIT_MS", 0)
)

// millisToDuration converts a floating-point millisecond knob to a Duration.
// Non-positive values yield 0, which every consumer reads as "not set".
func millisToDuration(ms float64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// juicerTxnSortKey is the value a message sorts by: the pair (wallTime,
// logical), compared lexicographically, taken from an HLC reading so it is a
// total order per clock. It is a position in the sorting queues (an intended
// admission order), not a commit order: the commit order is WriteTimestamp as
// of commit, which no message carries at the time it has to be sorted.
//
// The source is the batch header's Now — the gateway DistSender's clock
// reading, stamped once per client-level send (initAndVerifyBatch,
// pkg/kv/kvclient/kvcoord/dist_sender.go) and shared verbatim by every
// per-range sub-batch of that send. So one wave sorts identically on every
// participating node, while a retry sorts at its retry time: the abort-retry
// path mints a whole new transaction and every client-level re-send re-stamps
// Now, which keeps the key-to-arrival gap at network scale instead of
// transaction age (the defect that sank the per-transaction MinTimestamp key
// at contended workpoints — campaign 10: damage p50 4.6 s against a 0.169 ms
// RTT).
//
// Two knowingly accepted imperfections, from the 2026-08-13 audit:
// DistSender-internal transport retries (NotLeaseHolder and friends) re-send
// without re-stamping, so those arrivals look as old as their client-level
// send — but never as old as the transaction; and a proxied sub-batch is
// re-stamped with the proxying node's clock, so it can disagree with its
// siblings by up to the cluster's clock offset.
//
// Earlier selectable sources (the per-transaction MinTimestamp and the
// campaign-7 per-node pinned read timestamp, with its key table) were removed
// in the 2026-08-13 simplification; reproduce those campaigns from their
// pinned tags.
//
// EXCEPTION to the Juicer spec (clause 3.1, which sorts by a timestamp the
// transaction's originator assigns once at its start): CockroachDB keeps the
// per-send batch clock, by user decision of 2026-10-19 — sorting by the
// transaction's original timestamp lets the retry chain stretch lateness to
// seconds (campaign 10, above). Distinct transactions may therefore share a
// sorting key. The configuration declaration line says so
// (juicerSpecBanner: sort_key=batch-now(spec-3.1-exception)).
type juicerTxnSortKey struct {
	wallTime int64
	logical  int32
}

// Sizes of the fork's two hot-key filters: K keys admitted by the abort tracker
// (Level 1), S of those nominated for a queue by their out-of-order arrivals
// (Level 2).
//
// These MUST be passed explicitly. The fork's defaultServerOptions does not
// initialize juicerFilterK/juicerFilterS, so omitting the ServerOptions leaves
// both at Go's zero value, and AbortTracker.calculateAndSendDelta returns
// immediately when TopNFirstFilter <= 0. Level 1 then never promotes a key, never
// emits a TopKeysDelta, and no per-key queue is ever created — however many
// aborts are recorded.
//
// Defaults K = 100, S = 1 (Juicer spec 2.6/2.8, user decision 2026-10-19;
// they were 1000/100 until then — reproduce older runs by setting both).
var (
	juicerFilterK = envutil.EnvOrDefaultInt("COCKROACH_JUICER_FILTER_K", 100)
	juicerFilterS = envutil.EnvOrDefaultInt("COCKROACH_JUICER_FILTER_S", 1)
)

// The hot-key filters' statistics epoch and the lazy-removal threshold (Juicer
// spec 2.5, 2.7). Since 2026-10-19 the fork nominates keys only at an epoch
// boundary and retires a queue only after IDLE_EPOCHS consecutive epochs
// without an out-of-order arrival; both used to be hard-coded (10 s, 3).
var (
	juicerEpochMillis = envutil.EnvOrDefaultInt("COCKROACH_JUICER_EPOCH_MS", 10000)
	juicerIdleEpochs  = envutil.EnvOrDefaultInt("COCKROACH_JUICER_IDLE_EPOCHS", 3)
)

// COCKROACH_JUICER_LATE_POLICY says what a queue does with a late marker (one
// whose sorting key is below the largest key the queue has released): "queue"
// (the default since 2026-10-19, Juicer spec 7.2) queues it like any other
// marker — buffer time, dependency enforcer, hold — reads included; "bypass"
// lets it through at once without a hold, the behaviour of every earlier run.
// Any other value refuses to start.
var juicerLatePolicy = envutil.EnvOrDefaultString("COCKROACH_JUICER_LATE_POLICY", "queue")

// COCKROACH_JUICER_DIAG turns on the fork's per-message diagnostics (Juicer
// spec 11.2): the filter-cost timers, the dispatch-order history (a global
// lock per message), the interceptor latency samples and the raw-age debug
// line. Off by default since 2026-10-19 — every earlier run had them on. None
// of them feeds a decision. The FILTER_DIAGNOSTIC arms keep their own timers.
var juicerDiagnostics = envutil.EnvOrDefaultBool("COCKROACH_JUICER_DIAG", false)

// Stage timing observes admission stages without changing queue/hold policy.
var juicerStageTiming = envutil.EnvOrDefaultBool("COCKROACH_JUICER_STAGE_TIMING", false)

// The adaptive buffer time (Juicer spec section 4; the fork's
// juicer.AdaptiveWaitConfig). OFF by default: the spec leaves its formula's
// measurement choices open (user decision 2026-10-19), so these knobs select
// among them and nothing changes unless COCKROACH_JUICER_AW=true.
//
//	COCKROACH_JUICER_AW                 false; true turns it on
//	COCKROACH_JUICER_AW_MAX_US          W_max; unset = H (MAX_HOLD_MS); 0 =
//	                                    decide and report, never wait
//	COCKROACH_JUICER_AW_SCALE           s, default 1.0
//	COCKROACH_JUICER_AW_INVERSIONS      conflicting (default) | all
//	COCKROACH_JUICER_AW_LA              const (default) | online
//	COCKROACH_JUICER_AW_LA_US           L_a constant (and online start),
//	                                    default 1000 us: a placeholder, to be
//	                                    calibrated
//	COCKROACH_JUICER_AW_MIN_LA_SAMPLES  online L_a: spans an epoch needs, 10
//
// It needs the hot-key filters (its decisions run at their epoch boundary),
// so manual queues refuse it, as does a FIXED_WAIT_MS above zero (it
// replaces the fixed wait). Setting any AW_* knob with the feature off also
// refuses to start: a run must not believe it measured something it did not.
var (
	juicerAW                           = envutil.EnvOrDefaultBool("COCKROACH_JUICER_AW", false)
	juicerAWMaxUsRaw, juicerAWMaxUsSet = envutil.EnvString("COCKROACH_JUICER_AW_MAX_US", 0)
	juicerAWScaleRaw, juicerAWScaleSet = envutil.EnvString("COCKROACH_JUICER_AW_SCALE", 0)
	juicerAWInvRaw, juicerAWInvSet     = envutil.EnvString("COCKROACH_JUICER_AW_INVERSIONS", 0)
	juicerAWLaRaw, juicerAWLaSet       = envutil.EnvString("COCKROACH_JUICER_AW_LA", 0)
	juicerAWLaUsRaw, juicerAWLaUsSet   = envutil.EnvString("COCKROACH_JUICER_AW_LA_US", 0)
	juicerAWMinRaw, juicerAWMinSet     = envutil.EnvString("COCKROACH_JUICER_AW_MIN_LA_SAMPLES", 0)
)

// COCKROACH_JUICER_HOLD_CALIBRATION runs the fork's observe-only calibration of
// the hold limit (Juicer spec 6.2): the hot-key filters nominate keys as
// usual, but a nominated key is only watched — nothing is held, delayed or
// sorted, whatever RULES says — and each epoch logs, per watched key and
// pooled, the p99 of "a transaction's first message on the key -> its commit
// or abort applied on this server" (AppliedControlEvents). For baseline
// calibration runs only; refused with the adaptive buffer and with manual
// queues (it reports at the filters' epoch boundaries).
var juicerHoldCalibration = envutil.EnvOrDefaultBool("COCKROACH_JUICER_HOLD_CALIBRATION", false)

// juicerAdaptiveWaitConfig resolves the AW_* knobs. It returns the disabled
// zero value when COCKROACH_JUICER_AW is off, and an error for any invalid or
// inconsistent setting.
func juicerAdaptiveWaitConfig() (juicer.AdaptiveWaitConfig, error) {
	anySet := juicerAWMaxUsSet || juicerAWScaleSet || juicerAWInvSet || juicerAWLaSet ||
		juicerAWLaUsSet || juicerAWMinSet
	if !juicerAW {
		if anySet {
			return juicer.AdaptiveWaitConfig{}, errors.New("COCKROACH_JUICER_AW_* set while COCKROACH_JUICER_AW is off")
		}
		return juicer.AdaptiveWaitConfig{}, nil
	}
	hold := time.Duration(juicerMaxHoldMillis) * time.Millisecond
	c := juicer.AdaptiveWaitConfig{
		Enabled: true, MaxWait: hold, ScaleMilli: 1000, LaMode: juicer.AwLaConst,
		LaConst: time.Millisecond, MinLaSamples: 10,
	}
	if juicerAWMaxUsSet {
		v, err := strconv.ParseInt(strings.TrimSpace(juicerAWMaxUsRaw), 10, 64)
		if err != nil || v < 0 {
			return c, errors.Newf("COCKROACH_JUICER_AW_MAX_US %q is not a non-negative integer", juicerAWMaxUsRaw)
		}
		c.MaxWait = time.Duration(v) * time.Microsecond
	}
	if c.MaxWait > hold {
		return c, errors.Newf("COCKROACH_JUICER_AW_MAX_US (%v) exceeds COCKROACH_JUICER_MAX_HOLD_MS (%v) (spec 4.3)", c.MaxWait, hold)
	}
	if juicerAWScaleSet {
		v, err := strconv.ParseFloat(strings.TrimSpace(juicerAWScaleRaw), 64)
		if err != nil || v <= 0 || v > 1000 {
			return c, errors.Newf("COCKROACH_JUICER_AW_SCALE %q is not in (0, 1000]", juicerAWScaleRaw)
		}
		c.ScaleMilli = uint64(v*1000 + 0.5)
	}
	if juicerAWInvSet {
		switch strings.TrimSpace(juicerAWInvRaw) {
		case "conflicting":
		case "all":
			c.AllInversions = true
		default:
			return c, errors.Newf("COCKROACH_JUICER_AW_INVERSIONS %q (want conflicting or all)", juicerAWInvRaw)
		}
	}
	if juicerAWLaSet {
		c.LaMode = strings.TrimSpace(juicerAWLaRaw)
	}
	if juicerAWLaUsSet {
		v, err := strconv.ParseInt(strings.TrimSpace(juicerAWLaUsRaw), 10, 64)
		if err != nil || v < 1 {
			return c, errors.Newf("COCKROACH_JUICER_AW_LA_US %q is not a positive integer", juicerAWLaUsRaw)
		}
		c.LaConst = time.Duration(v) * time.Microsecond
	}
	if juicerAWMinSet {
		v, err := strconv.Atoi(strings.TrimSpace(juicerAWMinRaw))
		if err != nil || v < 1 {
			return c, errors.Newf("COCKROACH_JUICER_AW_MIN_LA_SAMPLES %q is not a positive integer", juicerAWMinRaw)
		}
		c.MinLaSamples = v
	}
	if juicerFixedWaitMillis > 0 {
		return c, errors.New("COCKROACH_JUICER_AW replaces the fixed wait; COCKROACH_JUICER_FIXED_WAIT_MS must be 0")
	}
	if juicerManualQueueKeysSet {
		return c, errors.New("COCKROACH_JUICER_AW needs the hot-key filters' epochs; it cannot run with COCKROACH_JUICER_MANUAL_QUEUE_KEYS")
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// COCKROACH_JUICER_MANUAL_QUEUE_KEYS pins the set of keys that get a per-key
// queue instead of letting the two hot-key filters choose them. Its value is a
// comma-separated list of signed decimal int64 queue keys: the fork's queue
// key for a request key k is fmt.Sprint(juicerKeyHash(k)), so an entry is the
// FNV-64a hash of the exact roachpb.Key bytes a point request carries, cast to
// int64.
//
// When the variable is unset nothing below changes the option list: the
// server is configured exactly as before the variable existed. When it is set,
// the fork's existing manual mode is selected (grpc.JuicerSkipTracking(true):
// no abort filter, no reorder filter, no L1/L2 updates) and queue creation is
// restricted to the listed keys (grpc.JuicerManualQueueKeys). FILTER_K and
// FILTER_S are still parsed and still validated, but manual mode never
// consults them; the banner says so.
//
// An empty list must never reach the fork: its QueueManager reads an empty
// manual key set as "create a queue for EVERY key", the opposite of what a
// short list means. Parsing therefore rejects the empty string, empty
// elements, anything that is not a base-10 int64, and duplicates after
// normalization, and juicerServerOptions panics at startup on any of them, the
// same way it refuses an invalid FILTER_S.
var juicerManualQueueKeysRaw, juicerManualQueueKeysSet = envutil.EnvString(
	"COCKROACH_JUICER_MANUAL_QUEUE_KEYS", 0)

// parseJuicerManualQueueKeys turns the environment value into the fork's
// queue-key strings. Each element is trimmed, parsed as a signed base-10
// int64, and re-rendered as fmt.Sprint(int64) — the exact string the fork's
// interceptor builds from a marker key — so "+5", " 5" and "5" all name the
// same queue. The order of the input is preserved.
func parseJuicerManualQueueKeys(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("empty list (an empty manual key set would queue every key)")
	}
	parts := strings.Split(raw, ",")
	keys := make([]string, 0, len(parts))
	seen := make(map[string]int, len(parts))
	for i, part := range parts {
		s := strings.TrimSpace(part)
		if s == "" {
			return nil, errors.Newf("element %d is empty", i)
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "element %d (%q) is not a signed decimal int64", i, s)
		}
		key := fmt.Sprint(v)
		if j, dup := seen[key]; dup {
			return nil, errors.Newf("element %d (%q) duplicates element %d (queue key %s)", i, s, j, key)
		}
		seen[key] = i
		keys = append(keys, key)
	}
	return keys, nil
}

// juicerRules names the strength of the dependency relation BuildRules hands to
// the enforcer. See BuildRules for what each one means and why the axis exists.
type juicerRules string

const (
	juicerRulesConflictTable   juicerRules = juicer.ConflictTableRulesName
	juicerRulesGate            juicerRules = "gate"
	juicerRulesWrites          juicerRules = "writes"
	juicerRulesWritesReadPass  juicerRules = "writes-readpass"
	juicerRulesWritesApplied   juicerRules = juicerWritesAppliedReadPassName
	juicerRulesWriteGate       juicerRules = "writegate"
	juicerRulesReadHold        juicerRules = "readhold"
	juicerRulesReadHoldOrdered juicerRules = "readhold-ordered"
	juicerRulesReadPass        juicerRules = "readhold-ordered-readpass"
	juicerRulesReadPassGuarded juicerRules = "readpass-guarded"
	juicerRulesReadOnly        juicerRules = "readonly"
	juicerRulesNoReadRead      juicerRules = "noreadread"
	juicerRulesTypeGate        juicerRules = "typegate"
	juicerRulesReadWait        juicerRules = "readwait"
	juicerRulesReadGate        juicerRules = "readgate"
	juicerRulesReadFirst       juicerRules = "readfirst"
	juicerRulesFull            juicerRules = "full"
	juicerRulesSFU             juicerRules = "sfu"
	juicerRulesOff             juicerRules = "off"
)

// The default is CONFLICT-TABLE as of 2026-10-19 (Juicer spec section 5, user
// decision): the one relation the spec prescribes for every system. From
// 2026-08-26 to then it was GATE, the completion gate; reproduce runs of that
// period with RULES=gate (and the other knobs whose defaults moved the same
// day, see juicerSpecBanner). All the earlier relations remain selectable.
var juicerRulesEnv = envutil.EnvOrDefaultString("COCKROACH_JUICER_RULES", string(juicerRulesConflictTable))

// juicerRulesMode parses COCKROACH_JUICER_RULES. Empty means the default
// conflict-table; an unrecognized nonempty value is an error, never a silent
// rule substitution.
func juicerRulesMode() juicerRules {
	switch juicerRules(strings.ToLower(strings.TrimSpace(juicerRulesEnv))) {
	case "", juicerRulesConflictTable:
		return juicerRulesConflictTable
	case juicerRulesGate:
		return juicerRulesGate
	case juicerRulesWrites:
		return juicerRulesWrites
	case juicerRulesWritesReadPass:
		return juicerRulesWritesReadPass
	case juicerRulesWritesApplied:
		return juicerRulesWritesApplied
	case juicerRulesWriteGate:
		return juicerRulesWriteGate
	case juicerRulesReadHold:
		return juicerRulesReadHold
	case juicerRulesReadHoldOrdered:
		return juicerRulesReadHoldOrdered
	case juicerRulesReadPass:
		return juicerRulesReadPass
	case juicerRulesReadPassGuarded:
		return juicerRulesReadPassGuarded
	case juicerRulesReadOnly:
		return juicerRulesReadOnly
	case juicerRulesNoReadRead:
		return juicerRulesNoReadRead
	case juicerRulesTypeGate:
		return juicerRulesTypeGate
	case juicerRulesReadWait:
		return juicerRulesReadWait
	case juicerRulesReadGate:
		return juicerRulesReadGate
	case juicerRulesReadFirst:
		return juicerRulesReadFirst
	case juicerRulesFull:
		return juicerRulesFull
	case juicerRulesSFU:
		return juicerRulesSFU
	case juicerRulesOff:
		return juicerRulesOff
	default:
		panic("invalid COCKROACH_JUICER_RULES: " + juicerRulesEnv)
	}
}

// Interception-hit counters: SplitMarker/IsSelfAbortedResponse are called
// only by the fork's interceptors, so non-zero deltas prove real traffic is
// flowing through Juicer (a single node short-circuits local ranges via the
// internal client adapter and shows zeros here).
var (
	juicerSplitCalls    atomic.Uint64
	juicerResponseCalls atomic.Uint64
	juicerSpanSkipped   atomic.Uint64
	juicerReporterOnce  sync.Once
)

// kvFailureCounters buckets errored BatchResponses by kvpb error detail. Two
// independent instances exist and they must classify identically so their
// numbers can be compared, which is why the type is shared rather than
// duplicated:
//
//   - juicerFailures below, incremented from the fork's interceptor callback.
//     Reachable only when COCKROACH_JUICER=true, so it doubles as proof that
//     interception happened — and is therefore unusable as a baseline.
//   - kvBaselineFailures in kv_errlog.go, incremented from the KV server's batch
//     choke point regardless of mode. See that file for why both are needed.
//
// The question the buckets are shaped to answer: does CRDB return enough
// KV-visible conflict errors on this path for a Level-1 hot-key filter fed by
// aborts to have anything to work with, and does queue-based sorting change that
// rate?
//
// For juicerFailures, counts are call-granular (one Add per
// IsSelfAbortedResponse call carrying an error), not response-granular, matching
// juicerResponseCalls above. All fields are written from the RPC fast path and
// read only by a minute reporter.
//
// byType retains the raw kvpb.ErrorDetailType tally for everything that falls
// into "other", so a dominant unclassified error (NotLeaseHolder, RangeKeyMismatch,
// ...) can be named in the report rather than hidden behind a single number.
type kvFailureCounters struct {
	total             atomic.Uint64
	writeTooOld       atomic.Uint64
	retry             atomic.Uint64
	retrySerializable atomic.Uint64
	aborted           atomic.Uint64
	lockConflict      atomic.Uint64
	uncertainty       atomic.Uint64
	other             atomic.Uint64
	byType            [kvpb.NumErrors]atomic.Uint64
}

var juicerFailures kvFailureCounters

// kvFailureSnapshot is a plain-value copy of kvFailureCounters so the
// reporter can difference successive ticks without re-reading racing atomics.
type kvFailureSnapshot struct {
	total             uint64
	writeTooOld       uint64
	retry             uint64
	retrySerializable uint64
	aborted           uint64
	lockConflict      uint64
	uncertainty       uint64
	other             uint64
	byType            [kvpb.NumErrors]uint64
}

func (c *kvFailureCounters) snapshot() kvFailureSnapshot {
	s := kvFailureSnapshot{
		total:             c.total.Load(),
		writeTooOld:       c.writeTooOld.Load(),
		retry:             c.retry.Load(),
		retrySerializable: c.retrySerializable.Load(),
		aborted:           c.aborted.Load(),
		lockConflict:      c.lockConflict.Load(),
		uncertainty:       c.uncertainty.Load(),
		other:             c.other.Load(),
	}
	for i := range c.byType {
		s.byType[i] = c.byType[i].Load()
	}
	return s
}

// recordFailure buckets one errored BatchResponse. The kvpb.Error detail is the
// authoritative discriminator: br.Error.TransactionRestart() only says whether a
// restart is possible, and merges WriteTooOld with serializable retries.
func (c *kvFailureCounters) recordFailure(pErr *kvpb.Error) {
	c.total.Add(1)
	detail := pErr.GetDetail()
	switch d := detail.(type) {
	case *kvpb.WriteTooOldError:
		c.writeTooOld.Add(1)
	case *kvpb.TransactionRetryError:
		c.retry.Add(1)
		if d.Reason == kvpb.RETRY_SERIALIZABLE {
			c.retrySerializable.Add(1)
		}
	case *kvpb.TransactionAbortedError:
		c.aborted.Add(1)
	case *kvpb.LockConflictError, *kvpb.WriteIntentError:
		c.lockConflict.Add(1)
	case *kvpb.ReadWithinUncertaintyIntervalError:
		c.uncertainty.Add(1)
	default:
		c.other.Add(1)
		// A nil detail means the error carries no recognized kvpb detail (a
		// plain internal or communication error); there is no type to tally.
		if detail != nil {
			if t := int(detail.Type()); t >= 0 && t < len(c.byType) {
				c.byType[t].Add(1)
			}
		}
	}
}

// kvOtherBreakdown pre-formats the per-ErrorDetailType tally of the "other"
// bucket for the reporter. It is passed to the log call as a single %s argument
// because fmtsafe requires the format string itself to be a constant.
func kvOtherBreakdown(cur, prev kvFailureSnapshot) string {
	var parts []string
	for i := range cur.byType {
		if delta := cur.byType[i] - prev.byType[i]; delta > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", kvpb.ErrorDetailType(i), delta))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " other-detail[" + strings.Join(parts, " ") + "]"
}

// juicerOrderLabel names the release order the per-key queues use, for the
// startup banner: "fifo" when COCKROACH_JUICER_FIFO degrades them to arrival
// order, "sorted" for the standard sorting-key order. The banner is what the
// drivers' banner_verify gate reads, so a mislabelled arm is caught before its
// numbers are believed.
func juicerOrderLabel() string {
	if juicerFifo {
		return "fifo"
	}
	return "sorted"
}

// juicerSpecVersion names the Juicer spec version this SPI conforms to, as
// the spec writes it (A14, decided 2026-10-20: every system prints the
// spec's version number; the conformance suite passes for it).
const juicerSpecVersion = "1.2"

// juicerSpecBanner renders the configuration declaration line of the Juicer
// spec (clause 11.1): one line with every setting the spec asks a server to
// declare, so a run's treatment can be checked field by field against the
// spec's default table.
func juicerSpecBanner() string {
	mode := juicerRulesMode()
	holdEnd := "per-relation(see BuildRules)"
	if mode == juicerRulesConflictTable {
		// The events that end each class of hold (spec 5.4, table 5-C), in
		// the spec's terms: a request-class hold ends with its own reply
		// (success or failure), a control-class one when its transaction's
		// commit or abort is applied here or its own request fails; both
		// end at max_hold (ttl).
		holdEnd = "request(get,refresh):own_reply|own_failure|ttl;" +
			"control(put,cput,inc,del,locking-get):applied(endtxn|resolve-intent|1pc-reply)|own_failure|ttl"
	}
	if mode == juicerRulesWritesApplied {
		holdEnd = crdbWritesAppliedReadPassRules(time.Duration(juicerMaxHoldMillis) * time.Millisecond).HoldEnds
	}
	// Per-event logging (11.3): the native release trace.
	eventLog := "off"
	if juicertrace.Enabled() {
		eventLog = "on(COCKROACH_JUICER_TRACE)"
	}
	manual := "off"
	if juicerManualQueueKeysSet {
		manual = "on"
	}
	calibration := "off"
	if juicerHoldCalibration {
		calibration = "on(observe-only; rules not enforced)"
	}
	diag := "off"
	if juicerDiagnostics {
		diag = "on"
	}
	buffer := fmt.Sprintf("fixed(%vms)", juicerFixedWaitMillis)
	if aw, err := juicerAdaptiveWaitConfig(); err == nil && aw.Enabled {
		buffer = aw.String()
	}
	return fmt.Sprintf(
		"spec=%s intercept=%s K=%d S=%d epoch=%dms idle_epochs=%d nominate=epoch-end removal=drain"+
			" abort_sources=failed-batch,endtxn-abort,resolve-intent-aborted l2_counts_from=candidate-epoch"+
			" rules=%s hold_end=%s max_hold=%dms buffer=%s late=%s order=%s"+
			" sort_key=batch-now(spec-3.1-exception) caller_abandon=withdraw event_log=%s"+
			" diag=%s filter_diag=%s manual_queues=%s hold_calibration=%s",
		juicerSpecVersion, strings.Join(append(append([]string{}, juicerUnaryBatchPaths...), juicerStreamBatchPaths...), ","),
		juicerFilterK, juicerFilterS, juicerEpochMillis, juicerIdleEpochs,
		mode, holdEnd, juicerMaxHoldMillis, buffer, juicerLatePolicy, juicerOrderLabel(),
		eventLog, diag, juicerFilterDiagnostic, manual, calibration)
}

// startJuicerHitReporter logs the resolved configuration once, then
// interception-hit deltas every minute.
func startJuicerHitReporter(ctx context.Context) {
	juicerReporterOnce.Do(func() {
		// The banner makes the run interpretable after the fact: it names the
		// (now fixed) sorting key source and the resolved core knobs.
		log.Dev.Infof(ctx,
			"juicer: sorting key source = batch-now (per-send gateway clock; empty Now falls back to MinTimestamp); fixed wait = %v ms; rules = %s (maxHold %d ms); order = %s",
			juicerFixedWaitMillis, juicerRulesMode(), juicerMaxHoldMillis, juicerOrderLabel())
		log.Dev.Infof(ctx, "juicer: spec config: %s", juicerSpecBanner())
		go func() {
			var lastSplit, lastResp uint64
			var lastFail kvFailureSnapshot
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				s, r := juicerSplitCalls.Load(), juicerResponseCalls.Load()
				log.Dev.Infof(ctx, "juicer: interception hits: +%d batches split, +%d responses observed (totals %d/%d)",
					s-lastSplit, r-lastResp, s, r)
				lastSplit, lastResp = s, r
				log.Dev.Infof(ctx, "juicer: span requests without point markers (cumulative) = %d", juicerSpanSkipped.Load())

				f := juicerFailures.snapshot()
				log.Dev.Infof(ctx,
					"juicer: failed responses: +%d total (wto=%d retry=%d[serializable=%d] aborted=%d lock=%d uncert=%d other=%d)%s",
					f.total-lastFail.total,
					f.writeTooOld-lastFail.writeTooOld,
					f.retry-lastFail.retry,
					f.retrySerializable-lastFail.retrySerializable,
					f.aborted-lastFail.aborted,
					f.lockConflict-lastFail.lockConflict,
					f.uncertainty-lastFail.uncertainty,
					f.other-lastFail.other,
					kvOtherBreakdown(f, lastFail))
				lastFail = f
			}
		}()
	})
}

// juicerServerOptions returns the fork ServerOptions that enable Juicer
// interception of Batch/BatchStream, or nil when disabled.
func juicerServerOptions() []grpc.ServerOption {
	startJuicerTraceReporter()
	if !juicerEnabled {
		return nil
	}
	switch juicerFilterDiagnostic {
	case "off", "none", "l1", "l1l2":
	default:
		panic("invalid COCKROACH_JUICER_FILTER_DIAGNOSTIC: " + juicerFilterDiagnostic)
	}
	if juicerFilterK <= 0 || juicerFilterS <= 0 || juicerFilterS > juicerFilterK {
		panic("Juicer requires 0 < FILTER_S <= FILTER_K")
	}
	if juicerMaxHoldMillis < 1 {
		panic("Juicer requires COCKROACH_JUICER_MAX_HOLD_MS >= 1 (spec 6.2: every hold is bounded, and 0 never means unbounded)")
	}
	// Spec 6.3: no timed wait may exceed the hold limit.
	if millisToDuration(juicerFixedWaitMillis) > time.Duration(juicerMaxHoldMillis)*time.Millisecond {
		panic(fmt.Sprintf("Juicer requires COCKROACH_JUICER_FIXED_WAIT_MS (%v) <= COCKROACH_JUICER_MAX_HOLD_MS (%d)",
			juicerFixedWaitMillis, juicerMaxHoldMillis))
	}
	if juicerEpochMillis <= 0 || juicerIdleEpochs <= 0 {
		panic("Juicer requires positive COCKROACH_JUICER_EPOCH_MS and COCKROACH_JUICER_IDLE_EPOCHS")
	}
	if _, err := juicer.ParseLatePolicy(juicerLatePolicy); err != nil || juicerLatePolicy == "" {
		panic("invalid COCKROACH_JUICER_LATE_POLICY (want queue or bypass): " + juicerLatePolicy)
	}
	manualKeys := juicerManualQueueKeysOrPanic()
	awCfg, err := juicerAdaptiveWaitConfig()
	if err != nil {
		panic("invalid Juicer adaptive wait: " + err.Error())
	}
	if juicerHoldCalibration && (awCfg.Enabled || manualKeys != nil) {
		panic("COCKROACH_JUICER_HOLD_CALIBRATION cannot run with COCKROACH_JUICER_AW or COCKROACH_JUICER_MANUAL_QUEUE_KEYS")
	}
	if mode := juicerRulesMode(); (mode == juicerRulesReadPass || mode == juicerRulesReadPassGuarded || mode == juicerRulesWritesReadPass || mode == juicerRulesWritesApplied) && (juicerFifo || juicerLatePolicy != "queue" ||
		juicerFixedWaitMillis != 0 || awCfg.Enabled || juicerHoldCalibration || juicerSkipQueueing ||
		juicerFilterDiagnostic != "off") {
		panic(fmt.Sprintf("%s requires sorted late-queue admission, zero fixed wait, AW off, calibration off, queueing enabled and FILTER_DIAGNOSTIC=off", mode))
	}
	log.Dev.Infof(context.Background(), "juicer: filter diagnostic = %s; filter K = %d; filter S = %d; typed markers = true",
		juicerFilterDiagnostic, juicerFilterK, juicerFilterS)
	if manualKeys != nil {
		log.Dev.Infof(context.Background(),
			"juicer: manual queue keys = on; skip tracking = true; manual key count = %d; manual keys = [%s]; filter K = %d and filter S = %d are parsed but unused in manual mode",
			len(manualKeys), strings.Join(manualKeys, ","), juicerFilterK, juicerFilterS)
	}
	opts := []grpc.ServerOption{
		grpc.JuicerSPIImpl(newCRDBJuicerSPI()),
		grpc.JuicerUnaryMethods(juicerUnaryBatchPaths),
		grpc.JuicerStreamMethods(juicerStreamBatchPaths),
		grpc.JuicerFilterK(juicerFilterK),
		grpc.JuicerFilterS(juicerFilterS),
		grpc.JuicerEpoch(time.Duration(juicerEpochMillis) * time.Millisecond),
		grpc.JuicerIdleEpochs(juicerIdleEpochs),
		grpc.JuicerLatePolicy(juicerLatePolicy),
		grpc.JuicerDiagnostics(juicerDiagnostics),
	}
	if juicerStageTiming {
		opts = append(opts, grpc.JuicerStageTiming(juicerStageTiming))
	}
	// Appended only when on, so the option list of every run without it is
	// unchanged.
	if awCfg.Enabled {
		opts = append(opts, grpc.JuicerAdaptiveWait(awCfg))
	}
	if juicerHoldCalibration {
		opts = append(opts, grpc.JuicerHoldCalibration(true))
	}
	// Manual mode: the fork skips both hot-key filters and creates a queue only
	// for the listed keys. Appended only when the variable is set, so the
	// option list of every unset run is exactly the one earlier campaigns ran.
	if manualKeys != nil {
		opts = append(opts,
			grpc.JuicerSkipTracking(true),
			grpc.JuicerManualQueueKeys(manualKeys))
	}
	// The fork uses an empty diagnostic option for normal queue/hold mode.
	// Passing the environment label "off" would select its direct path.
	if juicerFilterDiagnostic != "off" {
		opts = append(opts, grpc.JuicerFilterDiagnostic(juicerFilterDiagnostic))
	}
	if juicerSkipQueueing {
		opts = append(opts, grpc.JuicerSkipQueueing(true))
	}
	// FIFO release order: the queues, the holds and the TTL all stay, only the
	// min-heap goes away. Appended only when asked so that the default arm's
	// option list is byte-for-byte what earlier campaigns ran.
	if juicerFifo {
		opts = append(opts, grpc.JuicerFifoQueue(true))
	}
	// The fixed strategy is always selected: with a zero window it is the
	// pure-sorting behaviour, so one code path serves both "no wait" and
	// every FIXED_WAIT_MS sweep point.
	opts = append(opts,
		grpc.JuicerWaitStrategy(juicer.WaitStrategyFixed),
		grpc.JuicerFixedWait(millisToDuration(juicerFixedWaitMillis)))
	return opts
}

// juicerManualQueueKeysOrPanic returns nil when COCKROACH_JUICER_MANUAL_QUEUE_KEYS
// is unset, and otherwise the validated, normalized, non-empty key list. It
// panics on a malformed list, and when manual mode is combined with a mode
// that would bypass the queues it configures (a FILTER_DIAGNOSTIC direct path
// or SKIP_QUEUEING): the run would then not be the treatment its list claims.
func juicerManualQueueKeysOrPanic() []string {
	if !juicerManualQueueKeysSet {
		return nil
	}
	keys, err := parseJuicerManualQueueKeys(juicerManualQueueKeysRaw)
	if err != nil {
		panic("invalid COCKROACH_JUICER_MANUAL_QUEUE_KEYS: " + err.Error())
	}
	if juicerFilterDiagnostic != "off" {
		panic("COCKROACH_JUICER_MANUAL_QUEUE_KEYS requires COCKROACH_JUICER_FILTER_DIAGNOSTIC=off, got " + juicerFilterDiagnostic)
	}
	if juicerSkipQueueing {
		panic("COCKROACH_JUICER_MANUAL_QUEUE_KEYS is incompatible with COCKROACH_JUICER_SKIP_QUEUEING")
	}
	return keys
}

// crdbJuicerSPI adapts kvpb batch traffic to the Juicer SPI. Keys are hashed
// to int64 (fnv-64a over the roachpb.Key bytes); a hash collision merges two
// keys onto one sorting queue, which over-blocks conservatively but never
// under-blocks. The batch type is a routing hint; each data marker carries its
// own typed operation. Range operations have no exact point-key representation
// and are explicitly counted as uncovered instead of queuing a fabricated key.
type crdbJuicerSPI struct{}

var _ juicer.JuicerSPI[int64] = crdbJuicerSPI{}

func newCRDBJuicerSPI() crdbJuicerSPI { return crdbJuicerSPI{} }

func juicerKeyHash(k []byte) int64 {
	h := fnv.New64a()
	_, _ = h.Write(k)
	return int64(h.Sum64())
}

// juicerTxnID derives a non-zero int64 txn id from the txn UUID. Zero means
// "unbound" to the enforcer (a head that can never be admitted), so a real
// transaction must never map to it.
func juicerTxnID(ba *kvpb.BatchRequest) int64 {
	if ba == nil || ba.Txn == nil {
		return 0
	}
	return juicerTxnIDFromUUID(ba.Txn.ID)
}

// juicerTxnIDFromUUID is juicerTxnID's mapping from a transaction UUID, also
// applied to the transaction a ResolveIntent names (IntentTxn), so that an
// intent's resolution and the transaction's own messages carry the same id.
func juicerTxnIDFromUUID(u uuid.UUID) int64 {
	b := u.GetBytes()
	id := int64(binary.LittleEndian.Uint64(b[8:16]))
	if id == 0 {
		id = int64(binary.LittleEndian.Uint64(b[0:8]))
	}
	if id == 0 {
		id = 1
	}
	return id
}

func asBatchRequest(req interface{}) *kvpb.BatchRequest {
	ba, _ := req.(*kvpb.BatchRequest)
	return ba
}

// classifyBatch returns the batch-level operation type.
//
//   - Any write (incl. a 1PC [writes..., EndTxn] batch) -> OpSet. A 1PC batch
//     is deliberately NOT OpCommit: its commit-ness reaches the dependency
//     rules through the RespReturned release arm (crdbRules), because
//     participants never observe a separate commit message.
//   - Data-free EndTxn -> OpCommit / OpAbort by its Commit flag.
//   - ResolveIntent{,Range} -> OpCommit / OpAbort by intent status (async
//     resolution is the only commit signal non-anchor participants see).
//   - Locking Get (implicit SFU) -> OpGetForPut; plain Get -> OpGet.
//   - Refresh{,Range} -> OpGet. A refresh re-reads a span to check whether
//     anything wrote to it since the transaction's original read timestamp,
//     so it is a read for ordering purposes and must be sortable: it is the
//     single message whose outcome decides whether the transaction commits or
//     restarts, and leaving it unsorted meant the arrival order Juicer
//     arranged for the reads was not the order in which they were validated.
func classifyBatch(ba *kvpb.BatchRequest) juicer.OperationType {
	if ba == nil {
		return juicer.OpUnknown
	}
	var hasWrite, hasLockingGet, hasGet, hasRefresh bool
	var endTxn *kvpb.EndTxnRequest
	var resolve juicer.OperationType
	for i := range ba.Requests {
		switch r := ba.Requests[i].GetInner().(type) {
		case *kvpb.GetRequest:
			if r.KeyLockingStrength != lock.None {
				hasLockingGet = true
			} else {
				hasGet = true
			}
		case *kvpb.PutRequest, *kvpb.ConditionalPutRequest, *kvpb.IncrementRequest, *kvpb.DeleteRequest:
			hasWrite = true
		case *kvpb.RefreshRequest, *kvpb.RefreshRangeRequest:
			hasRefresh = true
		case *kvpb.EndTxnRequest:
			endTxn = r
		case *kvpb.ResolveIntentRequest:
			resolve = resolveStatusOp(r.Status, resolve)
		case *kvpb.ResolveIntentRangeRequest:
			resolve = resolveStatusOp(r.Status, resolve)
		}
	}
	switch {
	case hasWrite:
		return juicer.OpSet
	case hasLockingGet:
		return juicer.OpGetForPut
	case hasGet || hasRefresh:
		return juicer.OpGet
	case endTxn != nil && !endTxn.Commit:
		return juicer.OpAbort
	case endTxn != nil:
		return juicer.OpCommit
	case resolve != juicer.OpUnknown:
		return resolve
	default:
		return juicer.OpUnknown
	}
}

func resolveStatusOp(status roachpb.TransactionStatus, prev juicer.OperationType) juicer.OperationType {
	switch status {
	case roachpb.COMMITTED:
		return juicer.OpCommit
	case roachpb.ABORTED:
		if prev == juicer.OpCommit {
			return prev
		}
		return juicer.OpAbort
	default:
		return prev
	}
}

// sortableJuicerOp reports whether the batch-level op type enters the sorting
// queues (the interceptor sorts OpGet/OpGetForPut/OpSet).
func sortableJuicerOp(op juicer.OperationType) bool {
	return op == juicer.OpGet || op == juicer.OpGetForPut || op == juicer.OpSet
}

// juicerSortKey returns the sorting key of ba: the batch header's per-send
// clock reading, with the transaction's MinTimestamp as the fallback for an
// empty Now. See juicerTxnSortKey for what the key guarantees.
func juicerSortKey(ba *kvpb.BatchRequest) juicerTxnSortKey {
	// The header documents Now as optional, so an empty one falls back to
	// the transaction's MinTimestamp (assigned at creation, never empty in
	// practice) rather than collapsing every such batch onto key zero. In
	// practice everything arriving over gRPC came through a DistSender and
	// carries Now.
	if now := ba.Now; !now.IsEmpty() {
		return juicerTxnSortKey{wallTime: now.WallTime, logical: now.Logical}
	}
	ts := ba.Txn.MinTimestamp
	return juicerTxnSortKey{wallTime: ts.WallTime, logical: ts.Logical}
}

// SplitMarker emits one marker per point request of a transactional, sortable
// batch. Non-transactional batches return nil: they are never sorted, which
// also keeps txn id 0 (the enforcer's "unbound" sentinel) out of the queues.
//
// Span requests are counted but do not produce point markers. In particular,
// a span's start boundary does not stand for every data key in that span.
func (crdbJuicerSPI) SplitMarker(req interface{}) []juicer.Marker[int64] {
	juicerSplitCalls.Add(1)
	ba := asBatchRequest(req)
	if ba == nil || ba.Txn == nil {
		return nil
	}
	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *kvpb.ScanRequest, *kvpb.ReverseScanRequest, *kvpb.DeleteRangeRequest, *kvpb.RefreshRangeRequest:
			juicerSpanSkipped.Add(1)
		}
	}
	if !sortableJuicerOp(classifyBatch(ba)) {
		return nil
	}
	txnID := juicerTxnID(ba)
	sortKey := juicerSortKey(ba)
	markers := make([]juicer.Marker[int64], 0, len(ba.Requests))
	for i := range ba.Requests {
		var key []byte
		var opName string
		var typed juicer.OperationType
		switch r := ba.Requests[i].GetInner().(type) {
		case *kvpb.GetRequest:
			key = r.Key
			if r.KeyLockingStrength != lock.None {
				opName = "GetForUpdate"
				typed = juicer.OpGetForPut
			} else {
				opName = "Get"
				typed = juicer.OpGet
			}
		case *kvpb.PutRequest:
			key, opName = r.Key, "Put"
			typed = juicer.OpSet
		case *kvpb.ConditionalPutRequest:
			key, opName = r.Key, "CPut"
			typed = juicer.OpSet
		case *kvpb.IncrementRequest:
			key, opName = r.Key, "Increment"
			typed = juicer.OpSet
		case *kvpb.DeleteRequest:
			key, opName = r.Key, "Delete"
			typed = juicer.OpSet
		case *kvpb.RefreshRequest:
			key, opName = r.Key, "Refresh"
			typed = juicer.OpGet
		default:
			continue
		}
		markers = append(markers, juicer.Marker[int64]{
			Timestamp:   sortKey.wallTime,
			Logical:     int64(sortKey.logical),
			TxnId:       txnID,
			Key:         juicerKeyHash(key),
			OpIndex:     int64(i),
			OpType:      opName,
			TypedOpType: typed,
		})
	}
	return markers
}

func (crdbJuicerSPI) GetTxnId(req interface{}) int64 {
	return juicerTxnID(asBatchRequest(req))
}

func (crdbJuicerSPI) GetTimestamp(req interface{}) int64 {
	if ba := asBatchRequest(req); ba != nil && ba.Txn != nil {
		return juicerSortKey(ba).wallTime
	}
	return 0
}

func (crdbJuicerSPI) GetOperationCount(req interface{}) int32 {
	if ba := asBatchRequest(req); ba != nil {
		return int32(len(ba.Requests))
	}
	return 0
}

// GetOperationType classifies a batch. The sorting key is stateless (it is
// read straight off the batch header), so unlike the old pinned-key mode
// there is nothing to evict here.
func (s crdbJuicerSPI) GetOperationType(req interface{}) juicer.OperationType {
	return classifyBatch(asBatchRequest(req))
}

// GetKeysFromRequest hashes every keyed request in the batch, including
// EndTxn (anchor key) and ResolveIntent{,Range} (intent key) — the observe
// path uses these to deliver release events to the right queues.
func (crdbJuicerSPI) GetKeysFromRequest(req interface{}) []int64 {
	ba := asBatchRequest(req)
	if ba == nil {
		return nil
	}
	keys := make([]int64, 0, len(ba.Requests))
	for i := range ba.Requests {
		if h := ba.Requests[i].GetInner().Header(); len(h.Key) > 0 {
			keys = append(keys, juicerKeyHash(h.Key))
		}
	}
	return keys
}

var _ juicer.AbortSourceSPI[int64] = crdbJuicerSPI{}

// AbortedKeys implements the fork's AbortSourceSPI: the (key, transaction)
// pairs an abort message reports to the first level, whose keys are those
// GetKeysFromRequest returns. The transaction is the batch's own (an
// EndTxn(commit=false)), except for intent resolutions, which carry no
// transaction of their own and name the aborted one (IntentTxn): the
// first level counts one abort per transaction and key in an epoch
// (Juicer spec 9.4(d)), so a resolution must not count as "transaction 0".
func (crdbJuicerSPI) AbortedKeys(req interface{}) []juicer.ControlEvent[int64] {
	ba := asBatchRequest(req)
	if ba == nil {
		return nil
	}
	own := juicerTxnID(ba)
	out := make([]juicer.ControlEvent[int64], 0, len(ba.Requests))
	for i := range ba.Requests {
		inner := ba.Requests[i].GetInner()
		h := inner.Header()
		if len(h.Key) == 0 {
			continue
		}
		txn := own
		switch r := inner.(type) {
		case *kvpb.ResolveIntentRequest:
			txn = juicerTxnIDFromUUID(r.IntentTxn.ID)
		case *kvpb.ResolveIntentRangeRequest:
			txn = juicerTxnIDFromUUID(r.IntentTxn.ID)
		}
		out = append(out, juicer.ControlEvent[int64]{Key: juicerKeyHash(h.Key), TxnID: txn, OpType: juicer.OpAbort})
	}
	return out
}

func (s crdbJuicerSPI) IsGetRequest(req interface{}) bool {
	op := s.GetOperationType(req)
	return op == juicer.OpGet || op == juicer.OpGetForPut
}

func (s crdbJuicerSPI) IsSetRequest(req interface{}) bool {
	return s.GetOperationType(req) == juicer.OpSet
}

func (s crdbJuicerSPI) IsCommitRequest(req interface{}) bool {
	return s.GetOperationType(req) == juicer.OpCommit
}

func (s crdbJuicerSPI) IsAbortRequest(req interface{}) bool {
	return s.GetOperationType(req) == juicer.OpAbort
}

// IsPrepareRequest is always false: CRDB has no prepare message at this
// layer (parallel commits stage writes inside the commit batch instead).
func (crdbJuicerSPI) IsPrepareRequest(req interface{}) bool { return false }

func (crdbJuicerSPI) IsSelfAbortedResponse(resp interface{}) bool {
	juicerResponseCalls.Add(1)
	br, ok := resp.(*kvpb.BatchResponse)
	if !ok || br == nil || br.Error == nil {
		return false
	}
	juicerFailures.recordFailure(br.Error)
	return true
}

var _ juicer.ControlEventSPI[int64] = crdbJuicerSPI{}

// juicerAppliedOp maps a transaction status to the control operation it
// applies: COMMITTED -> OpCommit, ABORTED -> OpAbort, anything else (PENDING,
// STAGING, PREPARED) -> OpUnknown, i.e. nothing has been applied yet.
func juicerAppliedOp(status roachpb.TransactionStatus) juicer.OperationType {
	switch status {
	case roachpb.COMMITTED:
		return juicer.OpCommit
	case roachpb.ABORTED:
		return juicer.OpAbort
	}
	return juicer.OpUnknown
}

// AppliedControlEvents implements the fork's ControlEventSPI: after a batch's
// handler has returned, which transactions' commits or aborts it applied on
// this server, and on which keys. These end the control-class holds of the
// conflict-table relation (Juicer spec 5.4 (b), table 5-C):
//
//   - A transactional batch carrying an EndTxn whose response shows the
//     transaction COMMITTED or ABORTED applied that outcome for the batch's
//     own transaction: on its anchor key (the EndTxn's header key), on every
//     point key of its lock spans and in-flight writes — EndTxn resolves the
//     range-local ones synchronously, and no ResolveIntent ever comes for
//     them — and on the keys of the batch's other requests. That covers the
//     client's own commit or rollback, the parallel commit's explicit-commit
//     EndTxn, and a one-phase commit, whose [writes..., EndTxn] batch is
//     sortable and responds COMMITTED. A STAGING response (a parallel commit
//     not yet explicit) applies nothing. Lock-span keys on other ranges get
//     the event too when this server happens to hold a queue for them; the
//     transaction is decided by then, so a conflicting request that meets
//     its intent resolves it instead of aborting.
//   - A ResolveIntent request whose status is COMMITTED or ABORTED applied
//     that outcome for the transaction it names (IntentTxn, mapped like
//     juicerTxnID) on its key. Async intent resolution batches carry no
//     transaction of their own (juicerTxnID is 0 for them) and often the
//     intents of many transactions, which is why the events are per request.
//     Ranged resolutions name no point key and are skipped, like span
//     requests everywhere in this SPI.
//
// A failed batch applies nothing.
func (crdbJuicerSPI) AppliedControlEvents(
	req, resp interface{}, err error,
) []juicer.ControlEvent[int64] {
	if err != nil {
		return nil
	}
	ba := asBatchRequest(req)
	br, _ := resp.(*kvpb.BatchResponse)
	if ba == nil || br == nil || br.Error != nil {
		return nil
	}
	if ba.Txn != nil {
		// Only an EndTxn moves a transaction to COMMITTED or ABORTED, and a
		// transactional batch never carries ResolveIntent requests.
		if br.Txn == nil {
			return nil
		}
		op := juicerAppliedOp(br.Txn.Status)
		if op == juicer.OpUnknown {
			return nil
		}
		txnID := juicerTxnID(ba)
		var out []juicer.ControlEvent[int64]
		add := func(k roachpb.Key) {
			if len(k) > 0 {
				out = append(out, juicer.ControlEvent[int64]{Key: juicerKeyHash(k), TxnID: txnID, OpType: op})
			}
		}
		for i := range ba.Requests {
			inner := ba.Requests[i].GetInner()
			if et, ok := inner.(*kvpb.EndTxnRequest); ok {
				for _, sp := range et.LockSpans {
					if len(sp.EndKey) == 0 {
						add(sp.Key)
					}
				}
				for _, w := range et.InFlightWrites {
					add(w.Key)
				}
			}
			if h := inner.Header(); len(h.EndKey) == 0 {
				add(h.Key)
			}
		}
		return out
	}
	var out []juicer.ControlEvent[int64]
	for i := range ba.Requests {
		r, ok := ba.Requests[i].GetInner().(*kvpb.ResolveIntentRequest)
		if !ok {
			continue
		}
		if op := juicerAppliedOp(r.Status); op != juicer.OpUnknown && len(r.Key) > 0 {
			out = append(out, juicer.ControlEvent[int64]{
				Key: juicerKeyHash(r.Key), TxnID: juicerTxnIDFromUUID(r.IntentTxn.ID), OpType: op,
			})
		}
	}
	return out
}

// crdbJuicerLogBridge routes the fork's Juicer logs (queue events, pairing
// violations, block escapes) into CRDB's Dev channel so they appear in the
// normal node logs. Debug-level fork logs are per-operation and only flow
// when COCKROACH_JUICER_DEBUG=true.
type crdbJuicerLogBridge struct{}

func (crdbJuicerLogBridge) Debug(format string, v ...interface{}) {
	log.Dev.Infof(context.Background(), "juicer[debug]: %s", fmt.Sprintf(format, v...))
}
func (crdbJuicerLogBridge) Info(format string, v ...interface{}) {
	log.Dev.Infof(context.Background(), "juicer: %s", fmt.Sprintf(format, v...))
}
func (crdbJuicerLogBridge) Warning(format string, v ...interface{}) {
	log.Dev.Warningf(context.Background(), "juicer: %s", fmt.Sprintf(format, v...))
}
func (crdbJuicerLogBridge) Error(format string, v ...interface{}) {
	log.Dev.Errorf(context.Background(), "juicer: %s", fmt.Sprintf(format, v...))
}

var crdbJuicerLogger = juicer.NewLogger(
	"crdb", crdbJuicerLogBridge{}, "" /* logDir */, 0 /* serverID */, false /* hotkeyLogging */, juicerDebugLogs,
)

func (crdbJuicerSPI) ServerLogger() *juicer.Logger { return crdbJuicerLogger }

// The 2PC helper hooks below serve juicer-cc's manual dispatch modes only;
// the interceptor pipeline used for CRDB never calls them.
func (crdbJuicerSPI) GeneratePrepareRequest(req interface{}) interface{}         { return nil }
func (crdbJuicerSPI) GenerateCommitRequest(req interface{}) interface{}          { return nil }
func (crdbJuicerSPI) ExecuteCommitRequest(req interface{}) (interface{}, error)  { return nil, nil }
func (crdbJuicerSPI) ExecutePrepareRequest(req interface{}) (interface{}, error) { return nil, nil }
func (crdbJuicerSPI) ExecuteAbortRequest(req interface{}) (interface{}, error)   { return nil, nil }
func (crdbJuicerSPI) AbortCurrentRequest(req interface{}) (interface{}, error)   { return nil, nil }

// BuildRules declares CRDB's dependency relation to the enforcer, selected by
// COCKROACH_JUICER_RULES. The switch exists because Juicer does two separable
// things — it sorts requests into per-key arrival order, and it holds them
// against a dependency relation — and a single on/off flag cannot tell you
// which one produced an effect.
//
//	conflict-table  (default since 2026-10-19) The Juicer spec's relation, the
//	                same for every system: reads never block reads; a write
//	                (or locking read) blocks and is blocked by the other
//	                transactions' operations on its key; write and locking
//	                read holds last until their transaction's commit or abort
//	                has been applied here, read holds until their own
//	                response. See the case below.
//	gate            The completion gate, juicer.GateRules: after a message is
//	                released on a key, the next message on that key — whatever
//	                its operation type or transaction — waits for the released
//	                message's response (success or failure) or the MaxHold
//	                TTL. The standard configuration (campaigns #11/#12/#14);
//	                effective behaviour is ≤MaxHold-quantized serialization of
//	                each hot key, which pre-empts deadlock formation. ONLY safe
//	                on single-wave one-shot traffic: RMW / multi-wave workloads
//	                must select off (campaign #13 measured −73% throughput).
//	writes          The unified-rule CANDIDATE (validation pending): only
//	                cross-txn write-write pairs exclude; plain reads and
//	                locking reads pass untouched and hold nothing. Designed to
//	                keep the gate's single-wave wins (on pure-write traffic it
//	                is literally the gate), delete the read tax, and be
//	                neutral on RMW — one always-on relation with no
//	                workload-class caveat. See the case below for the
//	                fact-by-fact derivation.
//	writes-readpass The writes relation unchanged, with plain OpGet admission
//	                ahead of a blocked write queue head. Reads create no holds;
//	                write/write blocking remains independent of sorting HLC.
//	                Shares the readpass configuration restrictions below.
//	writegate       ABLATION PROBE (campaign #16), not a production candidate:
//	                writes plus one change — EVERY head (reads included) waits
//	                behind a live cross-txn write hold, but reads still hold
//	                nothing. Campaign #15 measured that the gate's mixed-load
//	                advantage over writes comes from read participation; this
//	                relation keeps only the read-waits-behind-writes half and
//	                drops the read-holds half, so the outcome attributes that
//	                advantage to ordering (≈gate) or to the admission throttle
//	                the read holds themselves imposed (≈writes).
//	readhold        ABLATION PROBE (campaign #17), decomposing the read-HOLD
//	                half that campaigns #16/#16b measured as owning the gate's
//	                mixed-load advantage: plain reads and writes both hold,
//	                but only WRITE heads wait (behind any cross-txn hold);
//	                read heads are not directly blocked, but queued reads can
//	                wait behind a blocked write. Splits read-holding into two
//	                sub-components — this keeps "reads block later writes"
//	                (write-anchored micro-batching: reads flow until a write
//	                arrives, the write drains the in-flight reads, reads pile
//	                behind it) and drops "reads block later reads" (pure
//	                pacing of the read flood). Locking reads hold nothing —
//	                the derivation gets that from Blocks — so the RMW hazard
//	                stays out by construction.
//	readhold-ordered Timestamp-directed admission CANDIDATE: readhold with
//	                the additional requirement that the held sorting HLC is
//	                strictly earlier than the head's (physical, then logical).
//	                Exact HLC ties do not block; TxnID is only an equality
//	                guard. Uses batch-now ordering, not MVCC timestamps; the
//	                release relation and TTL are readhold's unchanged. This
//	                local admission rule makes no global deadlock-freedom or
//	                performance claim.
//	readhold-ordered-readpass The ordered candidate plus marker-level OpGet
//	                admission ahead of a blocked write head, through the same
//	                enforcer with the same holds and releases. Includes point
//	                Refresh and nonlocking fragments of mixed RPCs; the RPC
//	                still waits for every key. Requires sorted late-queue
//	                admission with no buffer wait, AW or calibration.
//	readpass-guarded Read-ahead with strict sorting-HLC read/write conflicts:
//	                an earlier active write also blocks a plain read. Shares
//	                readpass restrictions and readhold releases and TTL.
//	readonly        ABLATION PROBE (campaign #19): the complement none of the
//	                #15-#17 probes ran — ONLY plain reads hold and ONLY plain
//	                read heads wait (cross-txn read-read pairs exclude;
//	                writes and locking reads pass untouched and hold
//	                nothing). The eight-round consensus review found that
//	                read-read pairs carry ~73% of the gate's injected wait
//	                yet were the common factor of every ablation, never the
//	                manipulated one; this relation makes them the manipulated
//	                variable. If read-read waves alone carry the gate's
//	                mixed-load gain, this rule keeps most of it; if the gain
//	                needs write participation (the superadditivity reading),
//	                this rule is pure read pacing with no conflict-channel
//	                benefit. RMW hazard is out by construction: writes and
//	                locking reads neither hold nor wait.
//	noreadread      ABLATION PROBE (campaign #19c): the readonly probe's
//	                complement — the gate verbatim with exactly one pair
//	                deleted. A plain-read head never waits behind a
//	                plain-read hold; every other (type, transaction) pair,
//	                same-transaction included, gates exactly as the
//	                completion gate does, and the hold population and
//	                release relation are the gate's unchanged. Campaign #19
//	                measured the isolated read-read pairing at 3/4 of the
//	                gate's m90 gain (sufficiency); this relation measures
//	                what the OTHER pairings are worth without read-read
//	                (the marginal / necessity direction).
//	typegate        ABLATION PROBE (campaign #20): read-read plus write-write
//	                only — each plain type gates itself, cross-type pairs
//	                pass, locking reads exempt. With #19 (read-read alone,
//	                +65%) and #19c (everything-but-read-read, -44%) in hand,
//	                the #20 trio decomposes the gate's margin over readonly
//	                one coupling at a time, each in the presence of the
//	                read-read pacing that #19c showed is the precondition.
//	readwait        ABLATION PROBE (campaign #20): read-read plus
//	                read-behind-write — only plain-read heads wait (behind
//	                cross-txn plain reads and writes); writes never wait;
//	                plain reads and writes both hold, locking reads exempt.
//	readgate        ABLATION PROBE (campaign #20): read-read plus
//	                write-behind-read — only plain-read holds block, and both
//	                plain types wait behind them; nothing waits behind a
//	                write; the writegate probe's mirror image.
//	readfirst       CAMPAIGN #29 PROBE: readhold minus the write-write pair.
//	                Only plain reads hold; only WRITE heads wait, and only
//	                behind cross-txn in-flight plain reads. Isolates "writes
//	                wait behind in-flight reads" — the MVCC intuition that a
//	                CockroachDB read conflicts only with a live intent at or
//	                below its timestamp, so putting in-flight reads ahead of a
//	                write keeps them off its intent and moves the cost to the
//	                writer's timestamp bump. Writes hold nothing (derived from
//	                Blocks); locking reads are excluded, keeping the campaign
//	                #13 RMW hazard out by construction.
//	full            Locking operations (SFU reads and writes) of different
//	                transactions mutually exclude. This is the relation as
//	                originally written, and the one that cost 85-98% of
//	                throughput while cutting aborts by 90-97%.
//	sfu             Only two cross-txn SFU *reads* exclude. Write-write
//	                exclusion is left entirely to CRDB's lock table, which
//	                already solves it below this layer; Juicer only adds the
//	                ordering guarantee CRDB does not provide, namely that a
//	                reader intending to write is not overtaken. Same release
//	                relation and MaxHold as full.
//	off             The zero-value DependencyRules. Enabled() is false, so
//	                QueueManager builds no enforcer and the per-key queues do
//	                pure timestamp sorting with no holds at all.
//
// COCKROACH_JUICER_FIFO is an axis of its own, ORTHOGONAL to every relation
// above. Juicer does two separable things; the relations decompose the
// holding, and this switch turns off the other one. With it set, each per-key
// queue releases in arrival order instead of sorting-key order — the hold
// population, the blocking relation, the release events and the MaxHold TTL
// are all exactly what the selected relation says they are, and only the order
// in which a key's queued messages reach the enforcer changes. So RULES=gate
// with FIFO on is "same holding, no reordering": the control arm that says how
// much of the gate's effect is the reordering, which no RULES value can
// isolate because every one of them holds in sorted order. The startup banner
// reports it as "order = fifo" / "order = sorted".
//
// Release relation for full/sfu, split by held-entry type (the 2026-08-13
// audit's self-release fix): a held WRITE releases when a same-txn write
// response returns — event delivery is key-scoped, so that means "this
// transaction's write covering THIS key landed" (1PC hot path: response
// return == committed) — or on commit/abort arrival, or on any failed
// response. A held LOCKING READ releases only on commit/abort arrival, a
// failed response, or the TTL — never on the transaction's own write
// response: the SFU claim is "a reader intending to write is not overtaken
// until the claim resolves", and the removed write-response arm released it
// mid-transaction (a multi-wave txn's write wave responds long before it
// commits), contradicting the hold-through-commit intent documented beside
// it. An OpGetForPut response also deliberately does NOT release its own
// hold: returning from the locking read is the beginning of the claim, not
// its resolution.
func (crdbJuicerSPI) BuildRules() juicer.DependencyRules {
	mode := juicerRulesMode()
	maxHold := time.Duration(juicerMaxHoldMillis) * time.Millisecond
	switch mode {
	case juicerRulesConflictTable:
		// Juicer spec section 5, the CockroachDB row of table 5-B: writes and
		// locking reads (SELECT FOR UPDATE, the read of a read-modify-write)
		// count as writes; a hold blocks a head of another transaction iff at
		// least one of the two writes, so reads never wait for reads. Writes
		// and locking reads leave an intent or a lock until their transaction
		// ends: control-class, their holds last until the transaction's
		// commit or abort has been applied on this server (AppliedControlEvents:
		// the anchor's EndTxn, this key's ResolveIntent, or a one-phase
		// commit's response) or their own request fails. Plain reads and
		// Refresh leave only the timestamp cache: request-class, their holds
		// end with their own response — a read-only transaction sends no
		// EndTxn, so anything longer would hold until MaxHold.
		lockingOp := func(o juicer.OperationType) bool {
			return o == juicer.OpSet || o == juicer.OpGetForPut
		}
		rules := juicer.ConflictTableRules(maxHold)
		controlRelease := rules.Releases
		rules.Releases = func(held juicer.OpRef, event juicer.Event) bool {
			return controlRelease(held, event) ||
				(!lockingOp(held.OpType) && juicer.ReleasedOnOwnResponse(held, event))
		}
		rules.HoldEnds = "Get: own response; Set/GetForPut: txn commit/abort applied; any: own request failed; TTL maxHold"
		return rules
	case juicerRulesOff:
		// Zero value: DependencyRules.Enabled() is false and the fork's
		// enforcerFactory returns nil, leaving the queues in sorting-only mode.
		return juicer.DependencyRules{}
	case juicerRulesGate:
		return crdbCompletionGate(maxHold)
	case juicerRulesNoReadRead:
		// The campaign #19c probe: gate minus the read-read pair. Blocks is
		// wrapped rather than rewritten so the derived hold population and
		// the release relation stay the gate's verbatim — reads still hold
		// (writes must still wait behind them); only the read-behind-read
		// wait is deleted, for any transaction pair.
		rules := crdbCompletionGate(maxHold)
		gateBlocks := rules.Blocks
		rules.Blocks = func(h, hd juicer.OpRef) bool {
			if h.OpType == juicer.OpGet && hd.OpType == juicer.OpGet {
				return false
			}
			return gateBlocks(h, hd)
		}
		return rules
	case juicerRulesTypeGate, juicerRulesReadWait, juicerRulesReadGate:
		// The campaign #20 trio: read-read plus exactly one write-involving
		// coupling each, cross-txn only, locking reads exempt throughout.
		// The three Blocks relations share the release arm of the readhold
		// probe verbatim (each hold ends at its own kind of response,
		// commit/abort arrival, a failed response, or the TTL); the held
		// population falls out of Blocks per relation.
		blocks := func(h, hd juicer.OpRef) bool { // typegate: same plain type
			return h.TxnID != hd.TxnID && h.OpType == hd.OpType &&
				(h.OpType == juicer.OpGet || h.OpType == juicer.OpSet)
		}
		switch mode {
		case juicerRulesReadWait: // read heads wait behind plain reads+writes
			blocks = func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID && hd.OpType == juicer.OpGet &&
					(h.OpType == juicer.OpGet || h.OpType == juicer.OpSet)
			}
		case juicerRulesReadGate: // both plain types wait behind read holds
			blocks = func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID && h.OpType == juicer.OpGet &&
					(hd.OpType == juicer.OpGet || hd.OpType == juicer.OpSet)
			}
		}
		return juicer.DependencyRules{
			Blocks: blocks,
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				if ev.Kind != juicer.RespReturned {
					return false
				}
				return ev.Failed || ev.OpType == h.OpType
			},
			MaxHold:      maxHold,
			AdmitUnbound: true,
		}
	case juicerRulesWritesApplied:
		return crdbWritesAppliedReadPassRules(maxHold)
	case juicerRulesWrites, juicerRulesWritesReadPass:
		// writes-readpass changes only queue read-ahead; predicates stay shared.
		// The unified-rule candidate: space concurrent WRITERS per hot key and
		// touch nothing else. Every clause is pinned to a measured fact —
		// cross-txn write-write spacing is where the single-wave wins came
		// from (campaigns #11/#12/#14); plain reads are exempt because holding
		// them was the one in-class cost (m50 read p50 +40%) and non-locking
		// reads cannot deadlock; locking reads are exempt because holding that
		// wave is where the RMW harm came from (campaign #13: the lock table
		// already owns that serialization; stacking a quantized hold on it was
		// −73%). If the do-no-harm prediction on RMW validates, this relation
		// replaces the gate as the single always-on rule and the
		// workload-class warning dies with the mode switch.
		return juicer.DependencyRules{
			Blocks: func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID &&
					h.OpType == juicer.OpSet && hd.OpType == juicer.OpSet
			},
			// Held entries are only ever writes here, so the release relation
			// is the write arm alone: own write response (key-scoped delivery;
			// 1PC hot path: response return == committed), commit/abort
			// arrival, or any failed response.
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				return ev.Kind == juicer.RespReturned &&
					(ev.Failed || ev.OpType == juicer.OpSet)
			},
			// Hold membership is derived from Blocks (fork -21): only writes
			// can block here, so only writes enter the in-flight set.
			MaxHold:                 maxHold,
			AdmitUnbound:            true,
			ReadAheadOfBlockedWrite: mode == juicerRulesWritesReadPass,
		}
	case juicerRulesWriteGate:
		// The campaign #16 ablation probe: identical to writes except that
		// Blocks drops its head-type condition, so reads and locking reads
		// wait behind a live cross-txn write hold instead of passing.
		// Releases is the writes relation verbatim, and the derived held
		// population is still writes only (only a held write can block) —
		// which is exactly the point: read-waiting without read-holding.
		// Same-txn heads stay exempt so an RMW transaction is never parked
		// behind its own write.
		return juicer.DependencyRules{
			Blocks: func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID && h.OpType == juicer.OpSet
			},
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				return ev.Kind == juicer.RespReturned &&
					(ev.Failed || ev.OpType == juicer.OpSet)
			},
			MaxHold:      maxHold,
			AdmitUnbound: true,
		}
	case juicerRulesReadPassGuarded:
		return crdbReadPassGuardedRules(maxHold)
	case juicerRulesReadHold, juicerRulesReadHoldOrdered, juicerRulesReadPass:
		// The campaign #17 ablation probe: writes plus one change on the
		// OTHER side of the relation than writegate moved — the held
		// population widens from writes to writes-and-plain-reads, while
		// the waiting population narrows back to write heads only. Read
		// heads are not directly blocked; queued reads can wait behind a
		// blocked write. A write waits behind any cross-txn in-flight read
		// or write. Held entries end at their own kind of response (a write
		// hold at the txn's write response, a read hold at the txn's read
		// response — key-scoped delivery as always), at commit/abort
		// arrival, at any failed response, or at the TTL. Locking reads
		// are not holdable (Blocks never matches them as held), keeping
		// the campaign #13 RMW hazard out by construction.
		// The ordered candidate adds only a strict sorting-HLC direction;
		// All three modes share this release relation and MaxHold backstop.
		rules := crdbReadHoldRules(maxHold, mode != juicerRulesReadHold)
		rules.ReadAheadOfBlockedWrite = mode == juicerRulesReadPass
		return rules
	case juicerRulesReadOnly:
		// The campaign #19 ablation probe: the exact complement of the
		// pairing grid's tested rows — cross-txn read-read pairs exclude and
		// nothing else does. Only plain reads can block, so the derived held
		// population is plain reads only; writes and locking reads neither
		// hold nor wait, which keeps the campaign #13 RMW hazard out by
		// construction. A read hold ends at its own transaction's read
		// response (key-scoped delivery), at commit/abort arrival, at any
		// failed response, or at the TTL — the read arm of the readhold
		// release relation verbatim.
		return juicer.DependencyRules{
			Blocks: func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID &&
					h.OpType == juicer.OpGet && hd.OpType == juicer.OpGet
			},
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				return ev.Kind == juicer.RespReturned &&
					(ev.Failed || ev.OpType == juicer.OpGet)
			},
			MaxHold:      maxHold,
			AdmitUnbound: true,
		}
	case juicerRulesReadFirst:
		// The campaign #29 probe: readhold with the write-write pair deleted,
		// leaving exactly one live clause — a WRITE head waits behind a
		// cross-txn in-flight PLAIN READ. Everything else passes: read heads
		// never wait, a write no longer waits behind another transaction's
		// write, and same-txn pairs stay exempt. Because only a held read can
		// block, the derived hold population narrows to plain reads — writes
		// hold nothing, and locking reads neither hold nor wait, which keeps
		// the campaign #13 RMW hazard out by construction. This isolates
		// "writes wait behind in-flight reads" from the write-write spacing
		// the readhold probe bundled with it: a CRDB read conflicts only with
		// a live intent at or below its timestamp, so letting the in-flight
		// reads land before the write keeps them off its intent and moves the
		// cost onto the writer's timestamp bump. The release relation is the
		// READ arm of the readhold release relation verbatim — a read hold
		// ends at its own transaction's read response (key-scoped delivery),
		// at commit/abort arrival, at any failed response, or at the TTL.
		return juicer.DependencyRules{
			Blocks: func(h, hd juicer.OpRef) bool {
				return h.TxnID != hd.TxnID &&
					h.OpType == juicer.OpGet && hd.OpType == juicer.OpSet
			},
			Releases: func(h juicer.OpRef, ev juicer.Event) bool {
				if ev.TxnID != h.TxnID {
					return false
				}
				if juicer.ReleasedOnCommitAbort(h, ev) {
					return true
				}
				if ev.Kind != juicer.RespReturned {
					return false
				}
				return ev.Failed || ev.OpType == h.OpType
			},
			MaxHold:      maxHold,
			AdmitUnbound: true,
		}
	}

	locking := func(o juicer.OperationType) bool {
		return o == juicer.OpGetForPut || o == juicer.OpSet
	}
	blocks := func(h, hd juicer.OpRef) bool {
		if h.TxnID == hd.TxnID {
			return false
		}
		if mode == juicerRulesSFU {
			return h.OpType == juicer.OpGetForPut && hd.OpType == juicer.OpGetForPut
		}
		return locking(h.OpType) && locking(hd.OpType)
	}
	return juicer.DependencyRules{
		Blocks: blocks,
		Releases: func(h juicer.OpRef, ev juicer.Event) bool {
			if ev.TxnID != h.TxnID {
				return false
			}
			if juicer.ReleasedOnCommitAbort(h, ev) {
				return true
			}
			if ev.Kind != juicer.RespReturned {
				return false
			}
			if ev.Failed {
				return true
			}
			// A write response ends a held write (its own batch, by key-scoped
			// delivery) but never a held locking read — see the relation note
			// above.
			return ev.OpType == juicer.OpSet && h.OpType == juicer.OpSet
		},
		// Hold membership is derived from Blocks (fork -21). Under full both
		// locking types hold; under sfu the derivation sheds the write dead
		// weight the old explicit declaration carried — a held write blocks
		// nothing there, so it no longer enters the set (sfu counter values
		// are not comparable with pre--21 rounds).
		MaxHold: maxHold,
	}
}

// crdbCompletionGate declares the legacy completion-gate diagnostic relation;
// the shared enforcer still owns all queueing and hold lifecycle behavior.
func crdbCompletionGate(maxHold time.Duration) juicer.DependencyRules {
	return juicer.DependencyRules{
		Blocks: func(juicer.OpRef, juicer.OpRef) bool { return true },
		Releases: func(held juicer.OpRef, event juicer.Event) bool {
			return event.Kind == juicer.RespReturned && event.TxnID == held.TxnID
		},
		MaxHold: maxHold, AdmitUnbound: true,
	}
}
