package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// measurementStart waits after native initialization, before the warmup clock.
// The common supervisor bounds lifetime and owns cancellation/child cleanup.
func measurementStart(ctx context.Context) (time.Time, error) {
	prefix := os.Getenv("EXPERIMENT_MEASUREMENT_PREFIX")
	if prefix == "" {
		return time.Now(), nil
	}
	if err := os.WriteFile(prefix+".ready", []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		return time.Time{}, err
	}
	for {
		data, err := os.ReadFile(prefix + ".start")
		if err == nil {
			target, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				return time.Time{}, err
			}
			timer := time.NewTimer(time.Until(time.Unix(0, target)))
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return time.Time{}, ctx.Err()
			case <-timer.C:
				return time.Now(), nil
			}
		}
		if !os.IsNotExist(err) {
			return time.Time{}, err
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Window endpoints are the actual native scheduler's boundaries, not process
// launch timestamps. Completion is recorded only after the native run finishes.
func measurementWindow(origin time.Time, warmup, duration time.Duration) error {
	prefix := os.Getenv("EXPERIMENT_MEASUREMENT_PREFIX")
	if prefix == "" {
		return nil
	}
	start := origin.Add(warmup)
	data := fmt.Sprintf("%d %d %d\n", start.UnixNano(), start.Add(duration).UnixNano(), time.Now().UnixNano())
	return os.WriteFile(prefix+".window", []byte(data), 0600)
}
