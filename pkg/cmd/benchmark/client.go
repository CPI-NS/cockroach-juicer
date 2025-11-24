// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package benchmark

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// Config holds the configuration for creating a standalone KV client.
type Config struct {
	// BootstrapAddrs is a list of addresses to bootstrap the gossip network.
	// At least one address must be provided.
	BootstrapAddrs []string

	// Insecure specifies whether to use an insecure connection.
	Insecure bool

	// SSLCertsDir is the directory containing SSL certificates (if not insecure).
	SSLCertsDir string

	// ClusterName is the name of the cluster to connect to.
	ClusterName string

	// User is the username to use for authentication.
	User username.SQLUsername
}

// NewClient creates a new standalone KV client that can connect to a
// CockroachDB cluster. It returns a *kv.DB that can be used to perform
// KV operations.
func NewClient(ctx context.Context, cfg Config) (*kv.DB, *stop.Stopper, error) {
	if len(cfg.BootstrapAddrs) == 0 {
		return nil, nil, errors.New("at least one bootstrap address is required")
	}

	// Create tracer
	tracer := tracing.NewTracer()

	// Create stopper
	stopper := stop.NewStopper(stop.WithTracer(tracer))
	defer func() {
		if stopper != nil {
			stopper.Stop(context.Background())
		}
	}()

	// Create ambient context
	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()

	// Create RPC Context
	rpcCfg := rpc.ClientConnConfig{
		Insecure:                       cfg.Insecure,
		SSLCertsDir:                    cfg.SSLCertsDir,
		ClusterName:                    cfg.ClusterName,
		User:                           cfg.User,
		Settings:                       cluster.MakeTestingClusterSettings(),
		Tracer:                         tracer,
		RPCHeartbeatInterval:           base.PingInterval,
		RPCHeartbeatTimeout:            base.DefaultRPCHeartbeatTimeout,
		HistogramWindowInterval:        base.DefaultHistogramWindowInterval(),
		DisableClusterNameVerification: false,
	}

	rpcCtx, rpcStopper := rpc.NewClientContext(ctx, rpcCfg)
	if rpcStopper != stopper {
		// If NewClientContext created a new stopper, we need to manage it
		stopper.AddCloser(stop.CloserFn(func() {
			rpcStopper.Stop(context.Background())
		}))
	}

	// Create ClusterID and NodeID containers
	clusterID := &base.ClusterIDContainer{}
	nodeID := &base.NodeIDContainer{}

	// Create Gossip
	registry := metric.NewRegistry()
	g := gossip.New(ambientCtx, clusterID, nodeID, stopper, registry, roachpb.Locality{})

	// Convert bootstrap addresses to UnresolvedAddr
	bootstrapAddrs := make([]util.UnresolvedAddr, len(cfg.BootstrapAddrs))
	for i, addr := range cfg.BootstrapAddrs {
		bootstrapAddrs[i] = util.MakeUnresolvedAddr("tcp", addr)
	}

	// Start Gossip with a dummy advertise address (clients don't advertise)
	dummyAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create dummy address")
	}
	g.Start(dummyAddr, bootstrapAddrs, rpcCtx)

	// Wait for gossip to connect
	select {
	case <-g.Connected:
		// Connected successfully
	case <-ctx.Done():
		return nil, nil, errors.Wrap(ctx.Err(), "context canceled while waiting for gossip connection")
	case <-time.After(30 * time.Second):
		return nil, nil, errors.New("timeout waiting for gossip connection")
	}

	// Create Node Dialer
	// Gossip implements NodeDescStore, so we can use it as an AddressResolver
	addressResolver := func(nodeID roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		nd, err := g.GetNodeDescriptor(nodeID)
		if err != nil {
			return nil, roachpb.Locality{}, err
		}
		return &nd.Address, nd.Locality, nil
	}
	nodeDialer := nodedialer.New(rpcCtx, addressResolver)

	// Create HLC Clock
	clock := hlc.NewClockForTesting(nil)

	// Create DistSender
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()

	dsCfg := kvcoord.DistSenderConfig{
		AmbientCtx:        ambientCtx,
		Settings:          cluster.MakeTestingClusterSettings(),
		Clock:             clock,
		NodeDescs:         g,
		NodeIDGetter:      func() roachpb.NodeID { return 0 }, // Clients don't have a NodeID
		RPCRetryOptions:   &retryOpts,
		Stopper:           stopper,
		LatencyFunc:       rpcCtx.RemoteClocks.Latency,
		TransportFactory:  kvcoord.GRPCTransportFactory(nodeDialer),
		FirstRangeProvider: g,
		Locality:          roachpb.Locality{},
	}

	ds := kvcoord.NewDistSender(dsCfg)

	// Create TxnCoordSenderFactory
	txnMetrics := kvcoord.MakeTxnMetrics(base.DefaultHistogramWindowInterval())
	tcsFactoryCfg := kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx:        ambientCtx,
		Settings:          cluster.MakeTestingClusterSettings(),
		Clock:             clock,
		Stopper:           stopper,
		HeartbeatInterval: base.DefaultTxnHeartbeatInterval,
		Linearizable:      false,
		Metrics:           txnMetrics,
	}

	tcsFactory := kvcoord.NewTxnCoordSenderFactory(tcsFactoryCfg, ds)

	// Create DB Context
	dbCtx := kv.DefaultDBContext(cluster.MakeTestingClusterSettings(), stopper)
	// For external clients, NodeID is not required
	dbCtx.NodeID = &base.SQLIDContainer{}

	// Create DB
	db := kv.NewDBWithContext(ambientCtx, tcsFactory, clock, dbCtx)

	// Clear the defer so we don't stop the stopper
	stopper = nil

	return db, rpcStopper, nil
}

