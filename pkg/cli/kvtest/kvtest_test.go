// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"

	"github.com/cockroachdb/errors"
)

const (
	numShards  = 3 // 3个shard节点（使用3个LocalTestCluster实例）
	numClients = 1 // 2个客户端

	defaultKeyRange = 1000
	defaultZipfS    = 1.1
	defaultZipfV    = 1.0
)

// TestKVOperations 测试3个shard和2个客户端的KV操作
func TestKVOperations(t *testing.T) {
	defer log.Scope(t).Close(t)

	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	t.Logf("启动 %d 个shard 和 %d 个客户端进行KV操作测试...", numShards, numClients)

	// 创建测试集群，使用系统租户（禁用租户隔离）以便直接访问KV层
	// 设置 ReplicationManual 禁用自动复制队列
	cluster := serverutils.StartCluster(t, numShards, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer cluster.Stopper().Stop(ctx)

	t.Logf("✓ 集群已启动（模拟 %d 个shard）", numShards)

	t.Log("✓ 集群已准备就绪")

	clientDBs := make([]*kv.DB, numClients)
	for i := 0; i < numClients; i++ {
		clientDBs[i] = cluster.Server(i).DB()
		t.Logf("✓ 客户端 %d 的RPC地址: %v", i+1, cluster.Server(i).RPCAddr())
		t.Logf("✓ 客户端 %d 已连接到集群", i+1)
	}

	db := clientDBs[0]
	runCtx, cancelRun := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRun()
	runClient(runCtx, t, 0, db)
}

// isContextError 检查错误是否是由上下文取消或超时引起的
func isContextError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		strings.Contains(errStr, "context deadline exceeded") ||
		strings.Contains(errStr, "context canceled")
}

// runClient 运行一个客户端的KV操作测试
func runClient(ctx context.Context, t *testing.T, clientID int, db *kv.DB) {
	keyRange := defaultKeyRange

	zipfS := defaultZipfS

	zipfV := defaultZipfV

	const workerCount = 10
	var workerWG sync.WaitGroup
	workerWG.Add(workerCount)

	var totalOps atomic.Int64
	var totalLatency atomic.Int64
	var abortCount atomic.Int64
	keyNumber := 10

	for workerID := 0; workerID < workerCount; workerID++ {
		go func(id int) {
			defer workerWG.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID*1000+id*10)))
			zipf := rand.NewZipf(rng, zipfS, zipfV, uint64(keyRange-1))
			ops := 0
			for {
				select {
				case <-ctx.Done():
					t.Logf("Client %d Worker %d: Stop，Finish RMW: %d", clientID, id, ops)
					return
				default:
				}

				txn := db.NewJuicerTxn(ctx, "juicer_benchmark")
				txn.SetDebugName("juicer_benchmark")

				start := time.Now()
				keys := make([]roachpb.Key, 0, keyNumber)
				seen := make(map[int]struct{}, keyNumber)
				for len(keys) < keyNumber {

					k := int(zipf.Uint64()) + 1
					if _, ok := seen[k]; ok {
						continue
					}
					seen[k] = struct{}{}
					keys = append(keys, roachpb.Key(fmt.Sprintf("rmw-key-%04d", k)))
				}

				getBatch := txn.NewBatch()
				for _, key := range keys {
					getBatch.GetForUpdate(key, kvpb.BestEffort)
				}
				if err := txn.Run(ctx, getBatch); err != nil {
					if !isContextError(err) {
						t.Logf("Client %d Worker %d: Get batch failed: %v", clientID, id, err)
					}
					abortCount.Add(1)
					_ = txn.Rollback(ctx)
					continue
				}
				prevValues := make([]string, len(keys))
				for i, res := range getBatch.Results {
					if len(res.Rows) > 0 && res.Rows[0].Value != nil {
						prevValues[i] = string(res.Rows[0].ValueBytes())
					} else {
						prevValues[i] = ""
					}
				}

				putBatch := txn.NewBatch()
				for i, key := range keys {
					newValue := fmt.Sprintf("client-%d-worker-%d-key-%s-op-%d-prev-%s",
						clientID, id, key, ops, prevValues[i])
					putBatch.Put(key, newValue)
				}
				if err := txn.Run(ctx, putBatch); err != nil {
					if !isContextError(err) {
						t.Logf("Client %d Worker %d: Put batch failed: %v", clientID, id, err)
					}
					abortCount.Add(1)
					_ = txn.Rollback(ctx)
					continue
				}

				if err := txn.Commit(ctx); err != nil {
					if !isContextError(err) {
						t.Logf("Client %d Worker %d: Commit failed: %v", clientID, id, err)
					}
					abortCount.Add(1)
					continue
				}
				totalOps.Add(1)
				totalLatency.Add(time.Since(start).Nanoseconds())
				ops++
			}
		}(workerID)
	}

	<-ctx.Done()
	workerWG.Wait()
	ops := totalOps.Load()
	aborts := abortCount.Load()
	avgLatency := time.Duration(0)

	if ops > 0 {
		avgLatency = time.Duration(totalLatency.Load() / ops)
	}
	totalAttempts := ops + aborts
	abortRate := 0.0
	if totalAttempts > 0 {
		abortRate = float64(aborts) / float64(totalAttempts) * 100
	}
	t.Logf("Client %d: All RMW workers stopped (Success=%d, Abort=%d, Average latency=%s, Abort rate=%.2f%%)",
		clientID, ops, aborts, avgLatency, abortRate)

	// Get and print TxnMetrics
	printTxnMetrics(t, db)
}

// printTxnMetrics retrieves and prints TxnMetrics from the DB's TxnCoordSenderFactory
func printTxnMetrics(t *testing.T, db *kv.DB) {
	factory := db.GetFactory()
	tcsFactory, ok := factory.(*kvcoord.TxnCoordSenderFactory)
	if !ok {
		t.Logf("Warning: DB factory is not a TxnCoordSenderFactory, cannot get TxnMetrics")
		return
	}

	metrics := tcsFactory.Metrics()

	// Get histogram snapshots
	durationSnapshot := metrics.Durations.CumulativeSnapshot()
	restartSnapshot := metrics.Restarts.CumulativeSnapshot()

	durationTotal, _ := durationSnapshot.Total()
	restartTotal, _ := restartSnapshot.Total()

	// Print metrics
	t.Logf("=== TxnMetrics ===")
	t.Logf("Commits: %d", metrics.Commits.Count())
	t.Logf("Commits1PC: %d", metrics.Commits1PC.Count())
	t.Logf("CommitsReadOnly: %d", metrics.CommitsReadOnly.Count())
	t.Logf("ParallelCommits: %d", metrics.ParallelCommits.Count())
	t.Logf("ParallelCommitAutoRetries: %d", metrics.ParallelCommitAutoRetries.Count())
	t.Logf("Aborts: %d", metrics.Aborts.Count())
	t.Logf("Prepares: %d", metrics.Prepares.Count())
	t.Logf("CommitWaits: %d", metrics.CommitWaits.Count())
	t.Logf("Durations (total): %d", durationTotal)
	t.Logf("Restarts (total): %d", restartTotal)
	t.Logf("ClientRefreshSuccess: %d", metrics.ClientRefreshSuccess.Count())
	t.Logf("ClientRefreshFail: %d", metrics.ClientRefreshFail.Count())
	t.Logf("ClientRefreshAutoRetries: %d", metrics.ClientRefreshAutoRetries.Count())
	t.Logf("ServerRefreshSuccess: %d", metrics.ServerRefreshSuccess.Count())
	t.Logf("TxnsWithCondensedIntents: %d", metrics.TxnsWithCondensedIntents.Count())
	t.Logf("TxnsRejectedByLockSpanBudget: %d", metrics.TxnsRejectedByLockSpanBudget.Count())
	t.Logf("TxnsRejectedByCountLimit: %d", metrics.TxnsRejectedByCountLimit.Count())
	t.Logf("RestartsWriteTooOld: %d", metrics.RestartsWriteTooOld.Count())
	t.Logf("RestartsSerializable: %d", metrics.RestartsSerializable.Count())
	t.Logf("RestartsAsyncWriteFailure: %d", metrics.RestartsAsyncWriteFailure.Count())
	t.Logf("RestartsReadWithinUncertainty: %d", metrics.RestartsReadWithinUncertainty.Count())
	t.Logf("==================")
}
