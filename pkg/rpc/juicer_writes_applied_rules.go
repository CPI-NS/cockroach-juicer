// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"time"

	"google.golang.org/grpc/juicer"
)

const juicerWritesAppliedReadPassName = "writes-applied-readpass"

// crdbWritesAppliedReadPassRules keeps writes-readpass admission, but waits for
// the SPI's observed finalized control outcome instead of a successful Set
// response or control arrival. Such an event does not certify that every
// physical intent on the named keys has been removed. Own failure, cancellation,
// eviction and the existing hold TTL remain independent release paths.
func crdbWritesAppliedReadPassRules(maxHold time.Duration) juicer.DependencyRules {
	return juicer.DependencyRules{
		Name:     juicerWritesAppliedReadPassName,
		HoldEnds: "control(put,cput,inc,del):observed_final(endtxn|point-resolve-intent|finalized-reply)|own_failure(token)|ttl;get,refresh,locking-get:unheld",
		Blocks: func(held, head juicer.OpRef) bool {
			return held.TxnID != head.TxnID && held.OpType == juicer.OpSet && head.OpType == juicer.OpSet
		},
		Releases: func(held juicer.OpRef, event juicer.Event) bool {
			return juicer.ReleasedOnCommitAbortApplied(held, event) || juicer.ReleasedOnOwnFailure(held, event)
		},
		MaxHold:                 maxHold,
		AdmitUnbound:            true,
		ObserveControlApplied:   true,
		ReadAheadOfBlockedWrite: true,
	}
}
