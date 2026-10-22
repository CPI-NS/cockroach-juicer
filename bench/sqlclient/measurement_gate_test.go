package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeMeasurementGateAndActualWindow(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "client")
	t.Setenv("EXPERIMENT_MEASUREMENT_PREFIX", prefix)
	started := make(chan time.Time, 1)
	failure := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		start, err := measurementStart(ctx)
		if err != nil {
			failure <- err
			return
		}
		started <- start
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(prefix + ".ready"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native ready absent")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-started:
		t.Fatal("client started before release")
	default:
	}
	target := time.Now().Add(20 * time.Millisecond)
	if err := os.WriteFile(prefix+".start", []byte(fmt.Sprint(target.UnixNano())), 0600); err != nil {
		t.Fatal(err)
	}
	var start time.Time
	select {
	case start = <-started:
	case err := <-failure:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if start.Before(target) {
		t.Fatal("native start precedes common target")
	}
	time.Sleep(20 * time.Millisecond)
	if err := measurementWindow(start, 0, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(prefix + ".window")
	if err != nil {
		t.Fatal(err)
	}
	var begin, end, complete int64
	if _, err = fmt.Fscan(strings.NewReader(string(data)), &begin, &end, &complete); err != nil {
		t.Fatal(err)
	}
	if begin != start.UnixNano() || end-begin != int64(20*time.Millisecond) || complete < end {
		t.Fatalf("incorrect actual native window: %s", data)
	}
}
