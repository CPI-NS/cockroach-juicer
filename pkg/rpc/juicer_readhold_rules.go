// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"time"

	"google.golang.org/grpc/juicer"
)

// crdbReadHoldRules shares hold membership, releases and TTL between readhold
// and its timestamp-directed admission candidate. ordered adds only strict
// batch-now HLC direction to Blocks; it does not order by MVCC timestamp.
func crdbReadHoldRules(maxHold time.Duration, ordered bool) juicer.DependencyRules {
	return juicer.DependencyRules{
		Blocks: func(h, hd juicer.OpRef) bool {
			return h.TxnID != hd.TxnID && hd.OpType == juicer.OpSet &&
				(h.OpType == juicer.OpSet || h.OpType == juicer.OpGet) &&
				(!ordered || h.Timestamp < hd.Timestamp ||
					(h.Timestamp == hd.Timestamp && h.Logical < hd.Logical))
		},
		Releases: func(h juicer.OpRef, ev juicer.Event) bool {
			if ev.TxnID != h.TxnID {
				return false
			}
			if juicer.ReleasedOnCommitAbort(h, ev) {
				return true
			}
			if ev.Kind != juicer.RespReturned {
				return false
			}
			return ev.Failed || ev.OpType == h.OpType
		},
		MaxHold:      maxHold,
		AdmitUnbound: true,
	}
}

// crdbReadPassGuardedRules lets plain reads pass blocked writes only when
// no earlier active write holds them back, using the sorting HLC, not MVCC.
func crdbReadPassGuardedRules(maxHold time.Duration) juicer.DependencyRules {
	rules := crdbReadHoldRules(maxHold, true)
	rules.Blocks = func(h, hd juicer.OpRef) bool {
		return h.TxnID != hd.TxnID &&
			(h.OpType == juicer.OpGet || h.OpType == juicer.OpSet) &&
			(hd.OpType == juicer.OpGet || hd.OpType == juicer.OpSet) &&
			(h.OpType == juicer.OpSet || hd.OpType == juicer.OpSet) &&
			(h.Timestamp < hd.Timestamp || (h.Timestamp == hd.Timestamp && h.Logical < hd.Logical))
	}
	rules.ReadAheadOfBlockedWrite = true
	return rules
}
