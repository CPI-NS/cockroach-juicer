// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func connect(ctx context.Context, url string) (*pgx.Conn, error) {
	c, e := pgx.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	c.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, c)
}
func ready(c config, timeout float64) error {
	if len(c.URLs) == 0 || timeout <= 0 {
		return errors.New("readiness requires URLs and positive timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), seconds(timeout))
	defer cancel()
	for _, url := range c.URLs {
		for {
			conn, e := connect(ctx, url)
			if e == nil {
				e = conn.Ping(ctx)
				conn.Close(ctx)
			}
			if e == nil {
				break
			}
			if !sleep(ctx, 100*time.Millisecond) {
				return fmt.Errorf("SQL readiness deadline: %w", ctx.Err())
			}
		}
	}
	return nil
}
func load(c config) error {
	if len(c.URLs) == 0 {
		return errors.New("load requires SQL URLs")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	conn, e := connect(ctx, c.URLs[0])
	if e != nil {
		return e
	}
	defer conn.Close(context.Background())
	// Fresh per-cell stores isolate parallel experiments; reset remains explicit
	// even if this operation is retried before any measurement starts.
	for _, sql := range []string{
		"DROP TABLE IF EXISTS usertable",
		"CREATE TABLE usertable (ycsb_key BIGINT PRIMARY KEY NOT NULL, field0 INT NOT NULL, FAMILY (ycsb_key,field0))",
		fmt.Sprintf("ALTER DATABASE defaultdb CONFIGURE ZONE USING num_replicas = %d", c.Replicas),
	} {
		if _, e = conn.Exec(ctx, sql); e != nil {
			return e
		}
	}
	for start := int64(0); start < c.Keys; start += c.LoadBatch {
		end := min(start+c.LoadBatch, c.Keys) - 1
		// Keep the data start identical across cells and independent of arrival RNG.
		sql := fmt.Sprintf("INSERT INTO usertable SELECT i, (i + %d) %% 1073741824 FROM generate_series(%d,%d) AS g(i)", c.Seed%(1<<30), start, end)
		if _, e = conn.Exec(ctx, sql); e != nil {
			return e
		}
		fmt.Fprintf(os.Stdout, "LOAD rows=%d total=%d\n", end+1, c.Keys)
	}
	if c.Splits > 0 {
		parts := make([]string, c.Splits)
		for i := range parts {
			parts[i] = fmt.Sprintf("(%d)", int64(i+1)*c.Keys/int64(c.Splits+1))
		}
		if _, e = conn.Exec(ctx, "ALTER TABLE usertable SPLIT AT VALUES "+strings.Join(parts, ",")); e != nil {
			return e
		}
	}
	var got int64
	if e = conn.QueryRow(ctx, "SELECT count(*) FROM usertable").Scan(&got); e != nil {
		return e
	}
	if got != c.Keys {
		return fmt.Errorf("loaded row count %d; want %d", got, c.Keys)
	}
	return writeJSON(c.Output+".load.json", map[string]any{"schema_version": 1, "rows": got, "seed": c.Seed, "splits": c.Splits, "replicas": c.Replicas, "reset": true})
}

// SQL statement construction and count validation preserve the audited pgx
// client and current ycsbt semantics: SELECT counts distinct rows; the blind
// fast-path UPSERT counts every input row, including repeated keys.
func execute(ctx context.Context, conn *pgx.Conn, e event) error {
	if e.Type == "readOnly" {
		placeholders := make([]string, len(e.Keys))
		args := make([]any, len(e.Keys))
		seen := map[int64]bool{}
		for i, k := range e.Keys {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = k
			seen[k] = true
		}
		var got int64
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM usertable WHERE ycsb_key IN ("+strings.Join(placeholders, ",")+")", args...).Scan(&got); err != nil {
			return err
		}
		if got != int64(len(seen)) {
			return fmt.Errorf("read row count %d; want %d", got, len(seen))
		}
		return nil
	}
	placeholders := make([]string, len(e.Keys))
	args := make([]any, 0, len(e.Keys)*2)
	for i, k := range e.Keys {
		placeholders[i] = fmt.Sprintf("($%d,$%d)", 2*i+1, 2*i+2)
		args = append(args, k, e.Values[i])
	}
	tag, err := conn.Exec(ctx, "UPSERT INTO usertable (ycsb_key,field0) VALUES "+strings.Join(placeholders, ","), args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(e.Keys)) {
		return fmt.Errorf("write row count %d; want %d", tag.RowsAffected(), len(e.Keys))
	}
	return nil
}
func state(err error) string {
	var p *pgconn.PgError
	if errors.As(err, &p) {
		return p.Code
	}
	return ""
}
func writeJSON(path string, value any) error {
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}
func seconds(s float64) time.Duration { return time.Duration(s * 1e9) }
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}
