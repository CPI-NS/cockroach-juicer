// Copyright 2026 The Cockroach Authors.
// Use of this software is governed by the CockroachDB Software License.

package rpc

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/juicertrace"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc/juicer"
)

var juicerTraceReporterOnce sync.Once

func (crdbJuicerSPI) OnJuicerHoldReleased(e juicer.HoldReleaseEvent) {
	juicertrace.HoldReleased(e.TxnID, e.Key, e.OpType.String(), e.CreatedAt, e.ReleasedAt, string(e.Reason))
}

func startJuicerTraceReporter() {
	if !juicertrace.Enabled() {
		return
	}
	juicerTraceReporterOnce.Do(func() {
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for range t.C {
				b, err := json.Marshal(juicertrace.Snapshot())
				if err == nil {
					log.Dev.Infof(context.Background(), "juicer-native-release: %s", b)
				}
			}
		}()
	})
}
