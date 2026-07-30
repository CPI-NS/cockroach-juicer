// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestJuicerKVBaselineObservation checks the mode-independent counter path:
// ObserveKVResponse must bucket a serializable retry into the baseline retry
// counters without needing Juicer to be enabled, must count clean responses as
// observed-but-not-failed, and must tolerate a nil response.
//
// Deltas are compared rather than absolutes because the counters are
// package-level and other tests in this package may also drive them.
func TestJuicerKVBaselineObservation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	beforeResp := kvBaselineResponses.Load()
	before := kvBaselineFailures.snapshot()

	// The metric the campaign turns on: a KV-layer serializable retry, counted
	// with no interceptor in the picture.
	retryResp := &kvpb.BatchResponse{}
	retryResp.Error = kvpb.NewError(&kvpb.TransactionRetryError{Reason: kvpb.RETRY_SERIALIZABLE})
	ObserveKVResponse(retryResp)

	// A clean response is observed but must not be counted as failed.
	ObserveKVResponse(&kvpb.BatchResponse{})

	// A WriteTooOldError must not leak into the retry bucket.
	wtoResp := &kvpb.BatchResponse{}
	wtoResp.Error = kvpb.NewError(&kvpb.WriteTooOldError{})
	ObserveKVResponse(wtoResp)

	// A nil response must be a no-op rather than a panic: Node.Batch is the
	// caller and its contract permits neither.
	ObserveKVResponse(nil)

	afterResp := kvBaselineResponses.Load()
	after := kvBaselineFailures.snapshot()

	for _, tc := range []struct {
		name          string
		expectedDelta uint64
		got           uint64
	}{
		{name: "observed", expectedDelta: 3, got: afterResp - beforeResp},
		{name: "total", expectedDelta: 2, got: after.total - before.total},
		{name: "retry", expectedDelta: 1, got: after.retry - before.retry},
		{name: "retrySerializable", expectedDelta: 1, got: after.retrySerializable - before.retrySerializable},
		{name: "writeTooOld", expectedDelta: 1, got: after.writeTooOld - before.writeTooOld},
		{name: "aborted", expectedDelta: 0, got: after.aborted - before.aborted},
		{name: "lockConflict", expectedDelta: 0, got: after.lockConflict - before.lockConflict},
		{name: "uncertainty", expectedDelta: 0, got: after.uncertainty - before.uncertainty},
		{name: "other", expectedDelta: 0, got: after.other - before.other},
	} {
		if tc.got != tc.expectedDelta {
			t.Errorf("baseline %s delta = %d, want %d", tc.name, tc.got, tc.expectedDelta)
		}
	}

	// The baseline set must be independent of the juicer set: nothing above went
	// through IsSelfAbortedResponse, so the interceptor counters must be untouched.
	juicerBefore := juicerFailures.snapshot()
	ObserveKVResponse(retryResp)
	if juicerAfter := juicerFailures.snapshot(); juicerAfter.total != juicerBefore.total {
		t.Errorf("ObserveKVResponse bumped the juicer counters (total %d -> %d); the two sets must not alias",
			juicerBefore.total, juicerAfter.total)
	}
}
