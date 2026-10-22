// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

type result struct {
	ID          uint64  `json:"id"`
	Type        string  `json:"txn_type"`
	Start       int64   `json:"start_unix_ns"`
	Done        int64   `json:"complete_unix_ns"`
	StartOffset int64   `json:"start_offset_ns"`
	DoneOffset  int64   `json:"complete_offset_ns"`
	Latency     float64 `json:"latency_ms"`
	Terminal    string  `json:"terminal"`
	State       string  `json:"sqlstate,omitempty"`
	Error       string  `json:"error,omitempty"`
	Attempts    int     `json:"attempts"`
	Measured    bool    `json:"measured"`
}
type summary struct {
	AttemptStates  map[string]uint64 `json:"attempt_sqlstates"`
	Schema         int               `json:"schema_version"`
	Complete       bool              `json:"complete"`
	Start          int64             `json:"start_unix_ns"`
	End            int64             `json:"end_unix_ns"`
	Duration       float64           `json:"duration_seconds"`
	Config         config            `json:"config"`
	Commits        uint64            `json:"commits"`
	Attempts       uint64            `json:"attempts"`
	AttemptErrors  uint64            `json:"attempt_errors"`
	ClientRetries  uint64            `json:"client_retries"`
	Visible40001   uint64            `json:"visible_40001"`
	Errors         uint64            `json:"errors"`
	Dispatched     uint64            `json:"dispatched"`
	CapacityWait   uint64            `json:"capacity_wait_count"`
	CapacityWaitNS int64             `json:"capacity_wait_ns"`
	Dropped        int               `json:"dropped"`
	Counters       []snapshot        `json:"counter_snapshots"`
}

func run(c config) error {
	if len(c.URLs) == 0 || c.Output == "" {
		return errors.New("run requires URLs and output prefix")
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	setup, cancelSetup := context.WithTimeout(parent, seconds(c.RequestTimeout))
	defer cancelSetup()
	conns := make([]*pgx.Conn, 0, c.Slots)
	defer func() {
		for _, conn := range conns {
			conn.Close(context.Background())
		}
	}()
	for i := 0; i < c.Slots; i++ {
		conn, e := connect(setup, c.URLs[i%len(c.URLs)])
		if e != nil {
			return e
		}
		conns = append(conns, conn)
	}
	g := newGenerator(c)
	var genMu sync.Mutex
	var nextID uint64
	f, e := os.Create(c.Output + ".events.jsonl")
	if e != nil {
		return e
	}
	defer f.Close()
	results := make(chan result, max(2, c.Slots*2))
	writerDone := make(chan error, 1)
	go func() {
		w := bufio.NewWriterSize(f, 1<<20)
		enc := json.NewEncoder(w)
		var err error
		for row := range results {
			if err == nil {
				err = enc.Encode(row)
			}
		}
		if err == nil {
			err = w.Flush()
		}
		if err == nil {
			err = f.Sync()
		}
		writerDone <- err
	}()
	start := time.Now()
	measure := start.Add(seconds(c.Warmup))
	end := measure.Add(seconds(c.Duration))
	ctx, cancel := context.WithDeadline(parent, end)
	defer cancel()
	s := summary{AttemptStates: map[string]uint64{}, Schema: 1, Start: measure.UnixNano(), End: end.UnixNano(), Duration: c.Duration, Config: c}
	var mu sync.Mutex
	var fatal error
	var dispatched atomic.Uint64
	type counterResult struct {
		rows []snapshot
		err  error
	}
	counterDone := make(chan counterResult, 1)
	go func() {
		rows, err := observe(ctx, c.MetricsURLs, measure, end, c.Output+".metrics.jsonl")
		counterDone <- counterResult{rows, err}
	}()
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Fprintf(os.Stdout, "PROGRESS dispatched=%d elapsed_seconds=%.3f\n", dispatched.Load(), time.Since(start).Seconds())
			}
		}
	}()
	task := func(conn *pgx.Conn) {
		genMu.Lock()
		ev := g.next(nextID, time.Since(start).Nanoseconds())
		nextID++
		genMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		dispatched.Add(1)
		begin := time.Now()
		r := result{ID: ev.ID, Type: ev.Type, Start: begin.UnixNano(), StartOffset: begin.Sub(start).Nanoseconds(), Terminal: "committed"}
		req, finish := context.WithTimeout(ctx, seconds(c.RequestTimeout))
		defer finish()
		var err error
		for attempt := 0; ; attempt++ {
			attemptStart := time.Now()
			r.Attempts++
			err = execute(req, conn, ev)
			attemptEnd := time.Now()
			if !attemptStart.Before(measure) && attemptEnd.Before(end) {
				mu.Lock()
				s.Attempts++
				if err != nil {
					s.AttemptErrors++
					code := state(err)
					if code == "" {
						code = "non_sql_error"
					}
					s.AttemptStates[code]++
					if state(err) == "40001" {
						s.Visible40001++
					}
				}
				mu.Unlock()
			}
			if err == nil || state(err) != "40001" || attempt >= c.MaxRetries || req.Err() != nil {
				break
			}
			mu.Lock()
			if !time.Now().Before(measure) && time.Now().Before(end) {
				s.ClientRetries++
			}
			mu.Unlock()
			if !sleep(req, seconds(c.RetryBackoff)) {
				err = req.Err()
				break
			}
		}
		done := time.Now()
		r.Done = done.UnixNano()
		r.DoneOffset = done.Sub(start).Nanoseconds()
		r.Latency = float64(done.Sub(begin)) / 1e6
		r.Measured = !begin.Before(measure) && done.Before(end)
		if err != nil {
			r.State = state(err)
			r.Error = err.Error()
			r.Terminal = "error"
		}
		if !done.Before(end) || ctx.Err() != nil {
			r.Terminal = "unfinished"
			r.Measured = false
		}
		mu.Lock()
		if r.Measured {
			if err == nil {
				s.Commits++
			} else {
				s.Errors++
			}
		}
		if err != nil && state(err) != "40001" && ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			fatal = err
			cancel()
		}
		mu.Unlock()
		results <- r
	}
	var wg sync.WaitGroup
	if c.Mode == "closed" {
		for _, conn := range conns {
			wg.Add(1)
			go func(conn *pgx.Conn) {
				defer wg.Done()
				for ctx.Err() == nil {
					task(conn)
				}
			}(conn)
		}
	} else {
		available := make(chan *pgx.Conn, c.Slots)
		for _, conn := range conns {
			available <- conn
		}
		r := rand.New(rand.NewPCG(c.Seed, 1))
		due := start
	schedule:
		for sleep(ctx, time.Until(due)) {
			var conn *pgx.Conn
			select {
			case conn = <-available:
			default:
				wait := time.Now()
				s.CapacityWait++
				select {
				case conn = <-available:
				case <-ctx.Done():
					s.CapacityWaitNS += time.Since(wait).Nanoseconds()
					break schedule
				}
				s.CapacityWaitNS += time.Since(wait).Nanoseconds()
			}
			if ctx.Err() != nil {
				break
			}
			// Re-anchor every interval to actual admission. Capacity waits create no
			// backlog, overflow rejection, or catch-up bursts.
			due = time.Now().Add(seconds(interval(c, r)))
			wg.Add(1)
			go func(conn *pgx.Conn) { defer wg.Done(); task(conn); available <- conn }(conn)
		}
	}
	wg.Wait()
	close(results)
	writeErr := <-writerDone
	counter := <-counterDone
	s.Counters = counter.rows
	<-progressDone
	s.Dispatched = dispatched.Load()
	s.Complete = parent.Err() == nil && fatal == nil && counter.err == nil && writeErr == nil && !time.Now().Before(end)
	if e := writeJSON(c.Output+".summary.json", s); e != nil {
		return e
	}
	if writeErr != nil {
		return writeErr
	}
	if counter.err != nil {
		return counter.err
	}
	if fatal != nil {
		return fatal
	}
	if !s.Complete {
		return errors.New("measurement interrupted")
	}
	return nil
}
