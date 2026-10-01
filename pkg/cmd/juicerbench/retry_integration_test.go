package main

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestNativeTxnClosureRetriesOnRealWriteConflict exercises the same native
// TxnWithAdmissionControl retry loop used by the benchmark against an actual
// in-process KV server. The first attempt establishes a read timestamp and a
// non-transactional write then makes that timestamp stale. CockroachDB must
// re-enter the closure; this is not a mocked Sender or synthetic retry error.
func TestNativeTxnClosureRetriesOnRealWriteConflict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(context.Background())

	key := "juicerbench-retry-integration"
	require.NoError(t, db.Put(ctx, key, "initial"))
	type attemptState struct {
		id    string
		epoch int32
	}
	var attempts []attemptState
	err := db.TxnWithAdmissionControl(ctx, kvpb.AdmissionHeader_FROM_SQL, admissionpb.NormalPri, kv.SteppingEnabled, func(ctx context.Context, txn *kv.Txn) error {
		proto := txn.TestingCloneTxn()
		attempts = append(attempts, attemptState{id: proto.ID.String(), epoch: int32(proto.Epoch)})
		txn.SetBufferedWritesEnabled(false)
		if _, err := txn.Get(ctx, key); err != nil {
			return err
		}
		if len(attempts) == 1 {
			// Advance the key after the transaction fixed its read timestamp.
			if err := db.Put(ctx, key, "conflict"); err != nil {
				return err
			}
		}
		return txn.Put(ctx, key, "committed")
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(attempts), 2, "real write conflict must re-enter transaction closure")
	first, second := attempts[0], attempts[1]
	require.Equal(t, first.id, second.id, "write-too-old restart should preserve transaction identity")
	require.Greater(t, second.epoch, first.epoch, "write-too-old restart must advance its epoch: %+v", attempts)
	value, err := db.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "committed", string(value.ValueBytes()))
}
