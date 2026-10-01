package main

import (
	"fmt"
	"sort"
	"time"
)

func validateStartReady(start, ready time.Time) error {
	if !ready.Before(start) {
		return fmt.Errorf("client became ready at %s after start deadline %s", ready.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano))
	}
	return nil
}

type window struct{ Start, T0, T1, DrainEnd time.Time }

func makeWindow(start time.Time, warmup, measure, drain time.Duration) window {
	return window{start, start.Add(warmup), start.Add(warmup + measure), start.Add(warmup + measure + drain)}
}
func (w window) phase(t time.Time) string {
	if t.Before(w.T0) {
		return "warmup"
	}
	if t.Before(w.T1) {
		return "measure"
	}
	return "drain"
}

// activeAt uses closed completion boundaries: a transaction completing exactly
// at a boundary is part of the active carry set at that boundary.
func activeAt(start, end, boundary time.Time) bool {
	return start.Before(boundary) && !end.Before(boundary)
}

// observationTailWait keeps the otherwise idle client process alive long
// enough for an external process-level observer to take a sample after T1.
// Transaction execution, drain, and summary generation have already finished.
func observationTailWait(now, t1 time.Time, tail time.Duration) time.Duration {
	if wait := t1.Add(tail).Sub(now); wait > 0 {
		return wait
	}
	return 0
}

type latencySummary struct {
	Count int64 `json:"count"`
	Min   int64 `json:"min"`
	P50   int64 `json:"p50"`
	P95   int64 `json:"p95"`
	P99   int64 `json:"p99"`
	P999  int64 `json:"p999"`
	Max   int64 `json:"max"`
	Sum   int64 `json:"sum"`
}

func summarize(v []int64) latencySummary {
	if len(v) == 0 {
		return latencySummary{}
	}
	x := append([]int64(nil), v...)
	sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
	q := func(p float64) int64 { n := int(float64(len(x)-1)*p + .5); return x[n] }
	var sum int64
	for _, n := range x {
		sum += n
	}
	return latencySummary{int64(len(x)), x[0], q(.5), q(.95), q(.99), q(.999), x[len(x)-1], sum}
}
