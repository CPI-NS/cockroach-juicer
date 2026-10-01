package main

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBernoulliMixAndRetryStableSpec(t *testing.T) {
	g, err := newSpecGenerator(1_000_000, 5, 90, .99, 7, "/x/", true)
	require.NoError(t, err)
	var reads, total int
	for i := 0; i < 10000; i++ {
		s := g.next()
		seen := map[string]bool{}
		for j, k := range s.Keys {
			require.False(t, seen[string(k)])
			seen[string(k)] = true
			if s.Reads[j] {
				reads++
			}
			total++
		}
	}
	require.InDelta(t, .9, float64(reads)/float64(total), .01)
}
func TestWindowCompletionClassification(t *testing.T) {
	s := time.Unix(100, 0)
	w := makeWindow(s, 30*time.Second, 60*time.Second, 30*time.Second)
	require.Equal(t, "warmup", w.phase(s.Add(29*time.Second)))
	require.Equal(t, "measure", w.phase(s.Add(30*time.Second)))
	require.Equal(t, "drain", w.phase(s.Add(90*time.Second)))
}
func TestStartDeadlineRequiresClientReadyBeforeStart(t *testing.T) {
	start := time.Unix(100, 0)
	require.NoError(t, validateStartReady(start, start.Add(-time.Nanosecond)))
	require.Error(t, validateStartReady(start, start))
	require.Error(t, validateStartReady(start, start.Add(time.Nanosecond)))
}
func TestObservationTailWaitIncludesDrainTime(t *testing.T) {
	t1 := time.Unix(100, 0)
	require.Equal(t, 3*time.Second, observationTailWait(t1, t1, 3*time.Second))
	require.Equal(t, time.Second, observationTailWait(t1.Add(2*time.Second), t1, 3*time.Second))
	require.Zero(t, observationTailWait(t1.Add(3*time.Second), t1, 3*time.Second))
	require.Zero(t, observationTailWait(t1.Add(10*time.Second), t1, 3*time.Second))
}
func TestWindowIntervalBoundariesAndIdentity(t *testing.T) {
	t0 := time.Unix(100, 0)
	t1 := t0.Add(time.Minute)
	type interval struct {
		start, end time.Time
		retried    bool
		failed     bool
	}
	events := []interval{
		{start: t0.Add(-time.Second), end: t0},                      // carry-in at exact T0
		{start: t0.Add(-time.Second), end: t0.Add(2 * time.Second)}, // carry-in completed in window
		{t0, t0.Add(3 * time.Second), true, false},                  // retried success
		{start: t0.Add(time.Second), end: t0.Add(4 * time.Second)},  // first-attempt success
		{t0.Add(2 * time.Second), t0.Add(5 * time.Second), false, true},
		{start: t0.Add(3 * time.Second), end: t1}, // exact T1 remains active for the identity
	}
	var started, activeT0, commits, failures, activeT1, first, retried int
	for _, e := range events {
		if !e.start.Before(t0) && e.start.Before(t1) {
			started++
		}
		if activeAt(e.start, e.end, t0) {
			activeT0++
		}
		if activeAt(e.start, e.end, t1) {
			activeT1++
		}
		if !e.end.Before(t0) && e.end.Before(t1) {
			if e.failed {
				failures++
			} else {
				commits++
				if e.retried {
					retried++
				} else {
					first++
				}
			}
		}
	}
	require.Equal(t, started+activeT0, commits+failures+activeT1)
	require.Equal(t, commits, first+retried)
	require.True(t, activeAt(t0.Add(-time.Second), t0, t0))
	require.True(t, activeAt(t0.Add(time.Second), t1, t1))
}
func TestScrambleBijection(t *testing.T) {
	a := scrambleMultiplier(1000)
	seen := map[int64]bool{}
	for r := int64(0); r < 1000; r++ {
		seen[(r*a)%1000] = true
	}
	require.Len(t, seen, 1000)
}

func TestMillionKeyScrambleMatchesYCSBT(t *testing.T) {
	require.Equal(t, int64(732999), scrambleMultiplier(1_000_000))
	require.Equal(t, int64(732999), (int64(1)*scrambleMultiplier(1_000_000))%1_000_000)
	require.Equal(t, int64(465998), (int64(2)*scrambleMultiplier(1_000_000))%1_000_000)
}
