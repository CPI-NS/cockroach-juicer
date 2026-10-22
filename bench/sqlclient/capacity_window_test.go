package main

import (
	"testing"
	"time"
)

func TestCapacityWaitCountersUseMeasurementWindow(t *testing.T) {
	origin := time.Unix(100, 0)
	measure, end := origin.Add(time.Second), origin.Add(2*time.Second)
	s := summary{}
	addCapacityWait(&s, origin, origin.Add(500*time.Millisecond), measure, end)
	if s.CapacityWait != 0 || s.CapacityWaitNS != 0 {
		t.Fatal("warmup-only wait marked measured capacity limited")
	}
	addCapacityWait(&s, origin.Add(900*time.Millisecond), origin.Add(1100*time.Millisecond), measure, end)
	if s.CapacityWait != 1 || s.CapacityWaitNS != int64(100*time.Millisecond) {
		t.Fatalf("straddling wait must retain measured intersection: %+v", s)
	}
	addCapacityWait(&s, origin.Add(1800*time.Millisecond), origin.Add(2200*time.Millisecond), measure, end)
	if s.CapacityWait != 2 || s.CapacityWaitNS != int64(300*time.Millisecond) {
		t.Fatalf("end must clip to measurement deadline: %+v", s)
	}
}
