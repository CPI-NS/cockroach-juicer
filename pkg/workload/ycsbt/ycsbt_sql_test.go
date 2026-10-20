// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ycsbt

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// The workload's three statements, run against a real (single-node,
// in-process) server with a key repeated inside one transaction, which the
// default independent draw produces. This pins what the workload's row-count
// checks rely on and what the KV layer sees:
//
//   - the blind UPSERT accepts a repeated key (its fast path builds no
//     distinct-on, so there is no "cannot affect row a second time" error),
//     counts every input row, and the last value wins; the KV batch carries
//     one Put per input row, the repeated key twice;
//   - the read-only SELECT and the read-modify-write CTE touch a repeated
//     row once.
func TestYCSBTRepeatedKeysThroughSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	// The system tenant: the test reads the KV trace of its own statements.
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	exec := func(q string, args ...interface{}) int64 {
		t.Helper()
		res, err := conn.ExecContext(ctx, q, args...)
		require.NoError(t, err, q)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		return n
	}
	exec(`CREATE DATABASE ycsbt`)
	exec(`SET database = ycsbt`)
	exec(`CREATE TABLE usertable ` + usertableSchema)
	exec(`INSERT INTO usertable SELECT i, 0 FROM generate_series(0, 9) AS g(i)`)
	value := func(k int64) int64 {
		var v int64
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT field0 FROM usertable WHERE ycsb_key = $1`, k).Scan(&v))
		return v
	}

	// Blind UPSERT with key 1 three times and key 2 twice, KV-traced.
	exec(`SET tracing = on,kv`)
	n := exec(buildBlindStmt(5), int64(1), int64(11), int64(2), int64(22), int64(1), int64(33), int64(3), int64(44), int64(1), int64(55))
	exec(`SET tracing = off`)
	require.Equal(t, int64(5), n, "the blind UPSERT counts every input row, repeated keys included")
	require.Equal(t, int64(55), value(1), "last write of key 1 wins")
	require.Equal(t, int64(22), value(2))
	require.Equal(t, int64(44), value(3))
	rows, err := conn.QueryContext(ctx, `SELECT message FROM [SHOW KV TRACE FOR SESSION]`)
	require.NoError(t, err)
	var msgs []string
	for rows.Next() {
		var m string
		require.NoError(t, rows.Scan(&m))
		msgs = append(msgs, m)
	}
	require.NoError(t, rows.Err())
	putKey1 := regexp.MustCompile(`Put.*/1/1/0 ->`)
	puts := 0
	for _, m := range msgs {
		if putKey1.MatchString(m) {
			puts++
		}
	}
	t.Logf("kv trace of the blind UPSERT:\n%s", strings.Join(msgs, "\n"))
	require.Equal(t, 3, puts, "one Put of key 1 per input row")
	// Observation, not a contract: how the single-range transaction with a
	// repeated key was committed (a one-phase-commit attempt evaluates the
	// batch without a transaction, where the second write of a key at the
	// same timestamp is too old, and falls back to transactional evaluation).
	onePC, err := conn.QueryContext(ctx,
		`SELECT message FROM [SHOW TRACE FOR SESSION] WHERE message ILIKE '%1PC%' OR message ILIKE '%one phase%' OR message ILIKE '%WriteTooOld%'`)
	require.NoError(t, err)
	for onePC.Next() {
		var m string
		require.NoError(t, onePC.Scan(&m))
		t.Logf("commit path: %s", m)
	}
	require.NoError(t, onePC.Err())

	// Read-only statement: a repeated key is one row.
	var got int64
	require.NoError(t, conn.QueryRowContext(ctx, buildReadStmt(5, ""),
		int64(1), int64(1), int64(2), int64(3), int64(2)).Scan(&got))
	require.Equal(t, int64(distinctCount([]int64{1, 1, 2, 3, 2})), got)

	// Read-modify-write: reads {4, 4}, writes {5, 5, 6}; each row once.
	var gotReads, gotWrites int64
	require.NoError(t, conn.QueryRowContext(ctx, buildStmt(2, 3),
		int64(4), int64(4), int64(5), int64(5), int64(6)).Scan(&gotReads, &gotWrites))
	require.Equal(t, int64(1), gotReads)
	require.Equal(t, int64(2), gotWrites)
	require.Equal(t, int64(1), value(5), "a repeated write key is incremented once")
	require.Equal(t, int64(1), value(6))

}
