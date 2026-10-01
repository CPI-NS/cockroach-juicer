package main

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type clientConfig struct {
	Addrs                 []string
	Insecure              bool
	CertsDir, ClusterName string
}
type kvClient struct {
	db      *kv.DB
	stopper *stop.Stopper
	metrics kvcoord.TxnMetrics
	wire    *wireCounters
}

type wireCounters struct{ batches, mixed, withEnd atomic.Int64 }
type txnMetricSnapshot struct {
	Commits      int64 `json:"commits"`
	Aborts       int64 `json:"aborts"`
	OnePC        int64 `json:"commits_1pc"`
	Parallel     int64 `json:"parallel_commits"`
	WriteTooOld  int64 `json:"restart_write_too_old"`
	Serializable int64 `json:"restart_serializable"`
	AsyncWrite   int64 `json:"restart_async_write"`
	Deadline     int64 `json:"restart_deadline"`
	Uncertainty  int64 `json:"restart_uncertainty"`
	Exclusion    int64 `json:"restart_exclusion"`
	TxnAborted   int64 `json:"restart_txn_aborted"`
	TxnPush      int64 `json:"restart_txn_push"`
	Unknown      int64 `json:"restart_unknown"`
}

func (c *kvClient) metricSnapshot() txnMetricSnapshot {
	m := c.metrics
	return txnMetricSnapshot{m.Commits.Count(), m.Aborts.Count(), m.Commits1PC.Count(), m.ParallelCommits.Count(), m.RestartsWriteTooOld.Count(), m.RestartsSerializable.Count(), m.RestartsAsyncWriteFailure.Count(), m.RestartsCommitDeadlineExceeded.Count(), m.RestartsReadWithinUncertainty.Count(), m.RestartsExclusionViolation.Count(), m.RestartsTxnAborted.Count(), m.RestartsTxnPush.Count(), m.RestartsUnknown.Count()}
}

func newClient(ctx context.Context, c clientConfig) (*kvClient, error) {
	if len(c.Addrs) == 0 {
		return nil, errors.New("at least one --addrs entry is required")
	}
	st := cluster.MakeTestingClusterSettings()
	tracer := tracing.NewTracer()
	ambient := log.AmbientContext{Tracer: tracer}
	rpcCtx, stopper := rpc.NewClientContext(ctx, rpc.ClientConnConfig{Insecure: c.Insecure, SSLCertsDir: c.CertsDir, ClusterName: c.ClusterName, User: username.RootUserName(), Settings: st, Tracer: tracer, RPCHeartbeatInterval: base.PingInterval, RPCHeartbeatTimeout: base.DefaultRPCHeartbeatTimeout, HistogramWindowInterval: base.DefaultHistogramWindowInterval()})
	clusterID := &base.ClusterIDContainer{}
	nodeID := &base.NodeIDContainer{}
	g := gossip.New(ambient, clusterID, nodeID, stopper, metric.NewRegistry(), roachpb.Locality{})
	boots := make([]util.UnresolvedAddr, len(c.Addrs))
	for i, a := range c.Addrs {
		boots[i] = util.MakeUnresolvedAddr("tcp", a)
	}
	dummy, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	g.Start(dummy, boots, rpcCtx)
	select {
	case <-g.Connected:
	case <-ctx.Done():
		stopper.Stop(context.Background())
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		stopper.Stop(context.Background())
		return nil, errors.New("gossip bootstrap timeout")
	}
	resolver := func(id roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		d, e := g.GetNodeDescriptor(id)
		if e != nil {
			return nil, roachpb.Locality{}, e
		}
		return &d.Address, d.Locality, nil
	}
	dialer := nodedialer.New(rpcCtx, resolver)
	clock := hlc.NewClockWithSystemTimeSource(500*time.Millisecond, 500*time.Millisecond, hlc.PanicLogger)
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	latency := func(id roachpb.NodeID) (time.Duration, bool) {
		if rpcCtx.RemoteClocks == nil {
			return 0, false
		}
		return rpcCtx.RemoteClocks.Latency(id)
	}
	ds := kvcoord.NewDistSender(ctx, kvcoord.DistSenderConfig{AmbientCtx: ambient, Settings: st, Clock: clock, NodeDescs: g, NodeIDGetter: func() roachpb.NodeID { return 0 }, RPCRetryOptions: &retryOpts, Stopper: stopper, LatencyFunc: latency, TransportFactory: kvcoord.GRPCTransportFactory(dialer), FirstRangeProvider: g})
	wire := &wireCounters{}
	observed := kv.SenderFunc(func(sendCtx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		wire.batches.Add(1)
		var hasGet, hasPut, hasEnd bool
		for _, ru := range ba.Requests {
			switch ru.GetInner().Method() {
			case kvpb.Get:
				hasGet = true
			case kvpb.Put:
				hasPut = true
			case kvpb.EndTxn:
				hasEnd = true
			}
		}
		if hasGet && hasPut {
			wire.mixed.Add(1)
		}
		if hasEnd {
			wire.withEnd.Add(1)
		}
		return ds.Send(sendCtx, ba)
	})
	tm := kvcoord.MakeTxnMetrics(base.DefaultHistogramWindowInterval())
	factory := kvcoord.NewTxnCoordSenderFactory(kvcoord.TxnCoordSenderFactoryConfig{AmbientCtx: ambient, Settings: st, Clock: clock, Stopper: stopper, HeartbeatInterval: base.DefaultTxnHeartbeatInterval, Metrics: tm}, observed)
	dbc := kv.DefaultDBContext(st, stopper)
	dbc.NodeID = &base.SQLIDContainer{}
	return &kvClient{kv.NewDBWithContext(ambient, factory, clock, dbc), stopper, tm, wire}, nil
}
func (c *kvClient) close() { c.stopper.Stop(context.Background()) }
