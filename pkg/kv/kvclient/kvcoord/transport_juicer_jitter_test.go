// Copyright 2026 The Cockroach Authors.
// Use of this software is governed by the CockroachDB Software License.

package kvcoord

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

type jitterSpyClient struct {
	*mockInternalClient
	calls int
	nows  []hlc.ClockTimestamp
}

func (s *jitterSpyClient) Batch(
	_ context.Context, in *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	s.calls++
	s.nows = append(s.nows, in.Now)
	return in.CreateReply(), nil
}

// TestJuicerJitterTransportIntegration uses a subprocess because the jitter
// configuration is intentionally read once at package initialization. This
// exercises the real sendBatch -> rpc.JuicerDataRPCDelay path without a test
// hook in production code.
func TestJuicerJitterTransportIntegration(t *testing.T) {
	if mode := os.Getenv("COCKROACH_JUICER_JITTER_TEST_CHILD"); mode != "" {
		runJuicerJitterTransportChild(t, mode)
		return
	}
	for _, mode := range []string{"off", "on"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestJuicerJitterTransportIntegration$", "-test.v")
			cmd.Env = append(os.Environ(), "COCKROACH_JUICER_JITTER_TEST_CHILD="+mode)
			if mode == "on" {
				cmd.Env = append(cmd.Env,
					"COCKROACH_JUICER_DATA_JITTER_MIN_US=20",
					"COCKROACH_JUICER_DATA_JITTER_MAX_US=20")
			} else {
				cmd.Env = append(cmd.Env,
					"COCKROACH_JUICER_DATA_JITTER_MIN_US=0",
					"COCKROACH_JUICER_DATA_JITTER_MAX_US=0")
			}
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}
}

func runJuicerJitterTransportChild(t *testing.T, mode string) {
	metrics := MakeDistSenderMetrics(roachpb.Locality{})
	gt := grpcTransport{opts: SendOptions{metrics: &metrics}}
	server := &jitterSpyClient{mockInternalClient: &mockInternalClient{}}
	now := hlc.ClockTimestamp{WallTime: 987654321, Logical: 7}
	ba := &kvpb.BatchRequest{}
	ba.Txn, ba.Now = &roachpb.Transaction{}, now
	ba.Add(kvpb.NewGet(roachpb.Key("jitter-key")))
	before := rpc.JuicerDataRPCJitterSnapshot()

	for i := 0; i < 2; i++ {
		_, err := gt.sendBatch(context.Background(), roachpb.NodeID(1), server, ba)
		require.NoError(t, err)
	}
	after := rpc.JuicerDataRPCJitterSnapshot()
	require.Equal(t, 2, server.calls)
	require.Equal(t, []hlc.ClockTimestamp{now, now}, server.nows)
	require.Equal(t, now, ba.Now, "transport jitter mutated the stamped batch timestamp")
	if mode == "on" {
		require.Equal(t, uint64(2), after.Scheduled-before.Scheduled)
		require.Equal(t, uint64(2), after.Ready-before.Ready)
	} else {
		require.Equal(t, before.Scheduled, after.Scheduled)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := gt.sendBatch(cancelled, roachpb.NodeID(1), server, ba)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, server.calls, "cancelled context reached Batch")
}
