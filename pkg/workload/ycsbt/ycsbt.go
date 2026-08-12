// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package ycsbt implements a transactional YCSB-shaped workload in which every
// transaction is a single implicit-transaction SQL statement.
//
// It exists because the Juicer request-reordering layer only applies to a
// specific transaction class: one whose full key set is known before the
// transaction starts executing, so that the whole transaction can be issued as
// one message and sorted as a unit. Stock ycsb does not qualify — its
// read-modify-write does a SELECT, waits for the answer, then writes — and
// tpcc only qualifies after the --one-shot rewrite, which turned out to halve
// its throughput on CockroachDB for reasons of its own and therefore confounds
// every comparison made on it.
//
// ycsbt is what is left when both of those problems are removed. Each
// transaction:
//
//   - draws --ops-per-txn distinct keys from a Zipf distribution over
//     [0, --keys), on the client, before anything is sent;
//   - splits them into a read set and a write set by --read-pct;
//   - issues exactly one statement, whose read set is a SELECT and whose write
//     set is an UPDATE ... SET field0 = field0 + 1 ... RETURNING, both carried
//     as CTEs of the same statement.
//
// The increment is what makes the write set a genuine read-modify-write:
// concurrent transactions touching the same hot key conflict on real data, not
// merely on a lock, so the aborts and retries the reordering layer is supposed
// to prevent actually occur. The RETURNING is not decorative: a mutating CTE
// whose output nothing consumes may not be evaluated at all.
//
// The --blind mode trades that property away on purpose. Each transaction
// becomes one multi-row UPSERT that overwrites its keys with client-chosen
// values and reads nothing: on this table (primary index only, every column
// written) CockroachDB plans it as a blind put — an upsert fed by values,
// no scan — so the whole transaction reaches the KV layer as a single wave
// of writes committed together. An UPDATE could not do this: even with a
// constant SET it plans a locking scan first. This is the closest a SQL
// workload gets to the single-wave one-shot class the reordering layer's
// model assumes, at the cost of a different failure mix: with no read set a
// WriteTooOld error bumps the timestamp and commits without a refresh, so
// serialization restarts vanish and the abort channel narrows to lock
// cycles between transactions writing the same hot keys across ranges.
package ycsbt

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/pflag"
)

// usertableSchema is deliberately one narrow table with one counter column.
// The workload's subject is contention on a key, so anything that widens the
// row only adds bytes to move. field0 is NOT NULL because every row is loaded
// up front and the increment must never see a NULL.
const usertableSchema = `(
	ycsb_key BIGINT PRIMARY KEY NOT NULL,
	field0   INT NOT NULL,
	FAMILY (ycsb_key, field0)
)`

const (
	defaultKeys       = 1000
	defaultOpsPerTxn  = 5
	defaultReadPct    = 50
	defaultZipfTheta  = 0.99
	initialFieldValue = 0
)

// Histogram names. A run reports one line per name, so they double as the
// classification of what happened to a transaction.
const (
	// A completed transaction is recorded under the name that describes what
	// it did, so a --read-pct 100 run is not silently reported as
	// read-modify-write traffic. Exactly one of the three is ever used in a
	// given run, since the split (and --blind) is fixed for the run.
	readWriteOpName = `readModifyWrite`
	readOnlyOpName  = `readOnly`
	blindOpName     = `blindWrite`
	// retryErrOpName is a transaction the server gave up retrying and returned
	// a serialization failure for. It is recorded rather than propagated: on a
	// contended workload these are the signal, not a reason to stop the run.
	retryErrOpName = `serialization-err`
)

// RandomSeed makes a run reproducible; it is a Uint64 seed because this
// package uses math/rand/v2.
var RandomSeed = workload.NewUint64RandomSeed()

type ycsbt struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	keys      int
	opsPerTxn int
	readPct   int
	zipfTheta float64
	splits    int
	blind     bool
}

func init() {
	workload.Register(ycsbtMeta)
}

var ycsbtMeta = workload.Meta{
	Name:        `ycsbt`,
	Description: `Transactional YCSB: one implicit-transaction statement per transaction, Zipf keys known up front.`,
	Details: `
	Each transaction picks --ops-per-txn distinct keys from a Zipf(--zipf-theta)
	distribution over [0, --keys), splits them into reads and writes by
	--read-pct, and issues a single statement whose read set is a SELECT CTE and
	whose write set is an "UPDATE ... SET field0 = field0 + 1 ... RETURNING" CTE.

	The read/write split is fixed for a run, not drawn per transaction:
	reads = round(--ops-per-txn * --read-pct / 100). When the rounding would
	wipe out a side the caller asked for -- e.g. --ops-per-txn 5 --read-pct 95
	rounds to 5 reads and 0 writes -- it is clamped to leave one operation on
	that side, so a run that asked for writes always performs writes.

	With --blind (requires --read-pct 0) every transaction is instead one
	multi-row UPSERT that overwrites its keys without reading them, which
	CockroachDB plans as a blind put: the transaction reaches the KV layer as
	a single wave of writes.
	`,
	Version:    `1.0.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := &ycsbt{}
		g.flags.FlagSet = pflag.NewFlagSet(`ycsbt`, pflag.ContinueOnError)
		// Only --keys and --splits shape the loaded data; the rest describe
		// the transaction mix and are meaningless at init time.
		g.flags.Meta = map[string]workload.FlagMeta{
			`ops-per-txn`: {RuntimeOnly: true},
			`read-pct`:    {RuntimeOnly: true},
			`zipf-theta`:  {RuntimeOnly: true},
			`blind`:       {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.keys, `keys`, defaultKeys,
			`Number of rows loaded into usertable, and the support of the Zipf key distribution.`)
		g.flags.IntVar(&g.opsPerTxn, `ops-per-txn`, defaultOpsPerTxn,
			`Number of distinct keys touched by each transaction.`)
		g.flags.IntVar(&g.readPct, `read-pct`, defaultReadPct,
			`Percent (0-100) of each transaction's keys that are read rather than incremented.`)
		g.flags.Float64Var(&g.zipfTheta, `zipf-theta`, defaultZipfTheta,
			`Skew of the Zipf key distribution. Larger is more skewed; must be positive and not exactly 1.`)
		g.flags.IntVar(&g.splits, `splits`, 0,
			`Number of range splits to perform on usertable before the workload starts.`)
		g.flags.BoolVar(&g.blind, `blind`, false,
			`Replace the read-modify-write statement with one blind multi-row UPSERT (requires --read-pct 0).`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*ycsbt) Meta() workload.Meta { return ycsbtMeta }

// Flags implements the Flagser interface.
func (w *ycsbt) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *ycsbt) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *ycsbt) Hooks() workload.Hooks {
	return workload.Hooks{Validate: w.validateConfig}
}

func (w *ycsbt) validateConfig() error {
	if w.keys < 1 {
		return errors.Errorf("--keys (%d) must be at least 1", w.keys)
	}
	if w.opsPerTxn < 1 {
		return errors.Errorf("--ops-per-txn (%d) must be at least 1", w.opsPerTxn)
	}
	// Each transaction needs that many *distinct* keys, so the key space has
	// to be able to supply them. Without this the draw loop below would spin
	// forever looking for a key that does not exist.
	if w.opsPerTxn > w.keys {
		return errors.Errorf("--ops-per-txn (%d) must not exceed --keys (%d)", w.opsPerTxn, w.keys)
	}
	if w.readPct < 0 || w.readPct > 100 {
		return errors.Errorf("--read-pct (%d) must be between 0 and 100", w.readPct)
	}
	// Blind mode performs no reads by construction; a nonzero --read-pct
	// would silently measure a different transaction than the one asked for,
	// so it is rejected rather than ignored.
	if w.blind && w.readPct != 0 {
		return errors.Errorf("--blind performs no reads; it requires --read-pct 0, not %d", w.readPct)
	}
	// Matches workloadimpl.NewZipfGenerator's own precondition, checked here so
	// the failure is a flag error rather than a mid-run one.
	if w.zipfTheta <= 0 || w.zipfTheta == 1 {
		return errors.Errorf("--zipf-theta (%v) must be positive and not exactly 1", w.zipfTheta)
	}
	if w.splits < 0 {
		return errors.Errorf("--splits (%d) must not be negative", w.splits)
	}
	if w.splits >= w.keys {
		return errors.Errorf("--splits (%d) must be less than --keys (%d)", w.splits, w.keys)
	}
	return nil
}

// splitCounts returns how many of a transaction's keys are read and how many
// are incremented.
//
// The split is fixed for the whole run rather than drawn per transaction: the
// statement's placeholder layout depends on it, and a fixed layout means one
// prepared statement instead of one per possible split. Rounding is
// half-up, then clamped so that neither side is wiped out unless the caller
// asked for exactly that with --read-pct 0 or 100. Without the clamp,
// --ops-per-txn 5 --read-pct 95 would round to five reads and quietly measure
// a read-only workload.
func splitCounts(opsPerTxn, readPct int) (reads, writes int) {
	reads = (opsPerTxn*readPct + 50) / 100
	if readPct > 0 && reads == 0 {
		reads = 1
	}
	if readPct < 100 && reads == opsPerTxn {
		reads = opsPerTxn - 1
	}
	return reads, opsPerTxn - reads
}

// Tables implements the Generator interface.
func (w *ycsbt) Tables() []workload.Table {
	usertable := workload.Table{
		Name:   `usertable`,
		Schema: usertableSchema,
		// Keys are the Zipf ranks themselves, so the hot end of the
		// distribution is the low end of the key space and the split points
		// are evenly spaced over it.
		Splits: workload.Tuples(w.splits, func(splitIdx int) []interface{} {
			return []interface{}{int64((splitIdx + 1) * (w.keys / (w.splits + 1)))}
		}),
		InitialRows: workload.TypedTuples(
			w.keys,
			[]*types.T{types.Int, types.Int},
			func(rowIdx int) []interface{} {
				return []interface{}{int64(rowIdx), int64(initialFieldValue)}
			},
		),
	}
	return []workload.Table{usertable}
}

// buildStmt renders the one statement every transaction runs, for a given
// read/write split.
//
// Both sides live in the same statement as CTEs, so the whole transaction is a
// single implicit-transaction message: that is the property the Juicer sorting
// layer requires, and it also puts the statement on CockroachDB's server-side
// automatic retry path. Placeholders are positional: $1..$reads are the read
// keys and the rest are the write keys, which is why the split has to be fixed
// before the statement is prepared.
//
// The final SELECT counts both CTEs. Counting the write CTE is what forces it
// to be evaluated — a mutating CTE nothing reads from is not guaranteed to run
// — and both counts are checked by the caller against the key counts, which
// turns a silently-missing row into a loud failure rather than into throughput.
func buildStmt(reads, writes int) string {
	var b strings.Builder
	b.WriteString("WITH ")
	if reads > 0 {
		b.WriteString("r AS (SELECT ycsb_key, field0 FROM usertable WHERE ycsb_key IN (")
		writePlaceholders(&b, 1, reads)
		b.WriteString("))")
		if writes > 0 {
			b.WriteString(", ")
		}
	}
	if writes > 0 {
		b.WriteString("w AS (UPDATE usertable SET field0 = field0 + 1 WHERE ycsb_key IN (")
		writePlaceholders(&b, reads+1, writes)
		b.WriteString(") RETURNING ycsb_key)")
	}
	b.WriteString(" SELECT ")
	if reads > 0 {
		b.WriteString("(SELECT count(*) FROM r)")
	} else {
		b.WriteString("0")
	}
	b.WriteString(", ")
	if writes > 0 {
		b.WriteString("(SELECT count(*) FROM w)")
	} else {
		b.WriteString("0")
	}
	return b.String()
}

// buildBlindStmt renders the --blind statement: one UPSERT overwriting writes
// keys. Placeholders are (key, value) pairs in order — $1, $2 for the first
// row and so on. There is no RETURNING: an UPSERT is evaluated for its
// effect (unlike a mutating CTE), and the row count is checked from the
// command tag instead. Adding a RETURNING that touched existing row state
// would also force the read this statement exists to avoid.
func buildBlindStmt(writes int) string {
	var b strings.Builder
	b.WriteString("UPSERT INTO usertable (ycsb_key, field0) VALUES ")
	for i := 0; i < writes; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "($%d, $%d)", 2*i+1, 2*i+2)
	}
	return b.String()
}

// writePlaceholders emits `$first, $first+1, ... ` for n placeholders.
func writePlaceholders(b *strings.Builder, first, n int) {
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "$%d", first+i)
	}
}

// Ops implements the Opser interface.
func (w *ycsbt) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if err := w.validateConfig(); err != nil {
		return workload.QueryLoad{}, err
	}
	cfg := workload.NewMultiConnPoolCfgFromFlags(w.connFlags)
	cfg.MaxTotalConnections = w.connFlags.Concurrency + 1
	mcp, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	reads, writes := splitCounts(w.opsPerTxn, w.readPct)
	stmtStr := buildStmt(reads, writes)
	opName := readWriteOpName
	if writes == 0 {
		opName = readOnlyOpName
	}
	// argsLen is the placeholder count: one key per operation, plus one value
	// per operation in blind mode, whose rows are (key, value) pairs.
	argsLen := w.opsPerTxn
	if w.blind {
		stmtStr = buildBlindStmt(w.opsPerTxn)
		opName = blindOpName
		argsLen = 2 * w.opsPerTxn
	}

	var retryErrors atomic.Int64
	ql := workload.QueryLoad{ResultHist: opName}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		// Every worker draws from the same distribution but owns its own
		// generator and random stream. A single shared generator would
		// serialize every key draw in the workload behind one mutex, and the
		// draws are independent anyway.
		zipf, err := workloadimpl.NewZipfGenerator(
			rand.New(rand.NewPCG(RandomSeed.Seed(), uint64(i))),
			0 /* iMin */, uint64(w.keys-1), w.zipfTheta, false /* verbose */)
		if err != nil {
			return workload.QueryLoad{}, errors.Wrap(err, "building the key distribution")
		}
		op := &ycsbtOp{
			hists:       reg.GetHandle(),
			opName:      opName,
			reads:       reads,
			writes:      writes,
			keySpace:    int64(w.keys),
			zipf:        zipf,
			rng:         rand.New(rand.NewPCG(RandomSeed.Seed(), uint64(i)+uint64(w.connFlags.Concurrency))),
			keys:        make([]int64, w.opsPerTxn),
			args:        make([]interface{}, argsLen),
			retryErrors: &retryErrors,
		}
		op.stmt = op.sr.Define(stmtStr)
		if err := op.sr.Init(ctx, "ycsbt", mcp); err != nil {
			return workload.QueryLoad{}, err
		}
		runFn := op.run
		if w.blind {
			runFn = op.runBlind
		}
		ql.WorkerFns = append(ql.WorkerFns, runFn)
	}
	ql.Close = func(context.Context) error {
		if n := retryErrors.Load(); n != 0 {
			fmt.Printf("Transactions that returned a serialization failure: %d.\n", n)
		}
		return nil
	}
	return ql, nil
}

// ycsbtOp is one worker. Its scratch slices are reused across transactions:
// the worker is single-threaded and the driver copies the arguments out before
// run returns, so there is nothing to alias.
type ycsbtOp struct {
	hists  *histogram.Histograms
	sr     workload.SQLRunner
	stmt   workload.StmtHandle
	opName string

	reads, writes int
	// keySpace is --keys: the number of loaded rows, and the modulus of the
	// linear probe in drawKeys.
	keySpace int64
	zipf     *workloadimpl.ZipfGenerator
	rng      *rand.Rand

	keys        []int64
	args        []interface{}
	retryErrors *atomic.Int64
}

// maxDrawAttempts bounds rejection sampling for one key before drawKeys falls
// back to probing. A Zipf draw is cheap and collisions are common at the hot
// end, so the budget is generous; it exists only to keep the loop finite when
// --ops-per-txn approaches --keys, where pure rejection sampling degenerates
// into coupon collecting over a heavily skewed distribution.
const maxDrawAttempts = 32

// drawKeys fills o.keys with distinct keys drawn from the Zipf distribution,
// then shuffles them.
//
// Distinctness is what makes the transaction well formed: the same key twice
// in the write set would have one statement increment it through two CTE rows,
// and twice in the read set is simply wasted. The shuffle then decides which
// of them are reads and which are writes. Without it the assignment would
// inherit the draw order, and rejection sampling makes later draws slightly
// less likely to be the hottest keys -- which would systematically bias the
// hot end of the distribution towards the read set, the one thing this
// workload exists to measure.
func (o *ycsbtOp) drawKeys() {
	n := len(o.keys)
	for i := 0; i < n; i++ {
		var k int64
		for attempt := 0; ; attempt++ {
			// The generator's arithmetic is floating point, so clamp rather
			// than trust it: a key outside the loaded range would not exist,
			// and the row-count check in run would fail the whole run.
			k = int64(o.zipf.Uint64())
			if k >= o.keySpace {
				k = o.keySpace - 1
			}
			if !o.contains(k, i) {
				break
			}
			if attempt >= maxDrawAttempts {
				// Walk forward to the next free key. This distorts the
				// distribution slightly in favour of the keys just above a
				// contended one, which is preferable to an unbounded loop and
				// is unreachable at the intended --ops-per-txn << --keys.
				for o.contains(k, i) {
					k = (k + 1) % o.keySpace
				}
				break
			}
		}
		o.keys[i] = k
	}
	o.rng.Shuffle(n, func(i, j int) { o.keys[i], o.keys[j] = o.keys[j], o.keys[i] })
}

// contains reports whether k is among the first n keys already drawn.
func (o *ycsbtOp) contains(k int64, n int) bool {
	for j := 0; j < n; j++ {
		if o.keys[j] == k {
			return true
		}
	}
	return false
}

func (o *ycsbtOp) run(ctx context.Context) error {
	o.drawKeys()
	for i, k := range o.keys {
		o.args[i] = k
	}

	start := timeutil.Now()
	// int64 rather than int: count(*) comes back as INT8.
	var gotReads, gotWrites int64
	if err := o.stmt.QueryRow(ctx, o.args...).Scan(&gotReads, &gotWrites); err != nil {
		if o.recordIfSerializationFailure(err, start) {
			return nil
		}
		return errors.Wrap(err, "ycsbt transaction failed")
	}
	// Every key is loaded before the run, so a short count means the statement
	// silently touched fewer rows than the transaction asked for -- which would
	// show up as free throughput rather than as an error.
	if gotReads != int64(o.reads) || gotWrites != int64(o.writes) {
		return errors.Errorf("ycsbt transaction touched %d/%d rows, want %d reads and %d writes",
			gotReads, gotWrites, o.reads, o.writes)
	}
	o.hists.Get(o.opName).Record(timeutil.Since(start))
	return nil
}

// runBlind is the --blind worker: overwrite the drawn keys with one UPSERT.
// The values are drawn fresh per transaction, so successive overwrites of a
// key change the row rather than rewriting an identical one.
func (o *ycsbtOp) runBlind(ctx context.Context) error {
	o.drawKeys()
	for i, k := range o.keys {
		o.args[2*i] = k
		o.args[2*i+1] = o.rng.Int64N(1 << 30)
	}

	start := timeutil.Now()
	tag, err := o.stmt.Exec(ctx, o.args...)
	if err != nil {
		if o.recordIfSerializationFailure(err, start) {
			return nil
		}
		return errors.Wrap(err, "ycsbt blind transaction failed")
	}
	// The statement lists each key exactly once, so anything but an exact row
	// count means rows were silently dropped -- which would show up as free
	// throughput rather than as an error.
	if got := tag.RowsAffected(); got != int64(len(o.keys)) {
		return errors.Errorf("ycsbt blind transaction wrote %d rows, want %d", got, len(o.keys))
	}
	o.hists.Get(o.opName).Record(timeutil.Since(start))
	return nil
}

// recordIfSerializationFailure reports whether err is a serialization
// failure, meaning the server exhausted its automatic retries; if so it is
// counted and timed under retryErrOpName. On a contended workload these are
// the quantity being measured, so the caller swallows them rather than
// aborting the whole run.
func (o *ycsbtOp) recordIfSerializationFailure(err error, start time.Time) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
		o.retryErrors.Add(1)
		o.hists.Get(retryErrOpName).Record(timeutil.Since(start))
		return true
	}
	return false
}
