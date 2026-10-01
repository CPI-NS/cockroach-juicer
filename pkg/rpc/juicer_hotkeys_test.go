// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// hotKeyRecord is one ycsbt row's identity at every layer the experiment
// crosses: the SQL primary key, the KV key a point request for it carries,
// and the Juicer queue key the fork builds from that request.
type hotKeyRecord struct {
	YCSBKey      int64  `json:"ycsb_key"`
	PrettyKey    string `json:"pretty_key"`
	FamilyKeyHex string `json:"family_key_hex"`
	RowKeyHex    string `json:"row_key_hex"`
	QueueKey     string `json:"queue_key"`
	// MarkerQueueKeys are the queue keys the real SplitMarker path produced
	// for a transactional point Get and a point Put of the same row.
	MarkerQueueKeyGet string `json:"marker_queue_key_get"`
	MarkerQueueKeyPut string `json:"marker_queue_key_put"`
}

// TestJuicerYCSBTQueueKeys derives the queue key of every row of the ycsbt
// usertable (table 106, primary index 1, one BIGINT key column, one column
// family) with CockroachDB's own key-encoding functions and the production
// juicerKeyHash, checks it against the independently computed values the
// experiment froze, and checks that the real SplitMarker path produces the
// same queue key for a point Get and a point Put. The complete table is
// written to the test's undeclared outputs for the experiment record.
func TestJuicerYCSBTQueueKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const tableID, indexID, rows = 106, 1, 1000

	// Frozen by the experiment's main-session computation (ranks 0-9 under
	// --key-scramble with A = 733): key -> {family-key hex, queue key}.
	want := map[int64][2]string{
		0:   {"f2898888", "1817670077699909844"},
		733: {"f289f702dd88", "6960948776747566164"},
		466: {"f289f701d288", "4712501373694096178"},
		199: {"f289f6c788", "797559098763748673"},
		932: {"f289f703a488", "6276501688851137378"},
		665: {"f289f7029988", "6896165551626241344"},
		398: {"f289f7018e88", "4754889745976560750"},
		131: {"f289f68388", "862870089466614773"},
		864: {"f289f7036088", "6096947041953342534"},
		597: {"f289f7025588", "6830836968737323868"},
	}

	records := make([]hotKeyRecord, 0, rows)
	byQueueKey := make(map[string]int64, rows)
	for k := int64(0); k < rows; k++ {
		row := keys.SystemSQLCodec.IndexPrefix(tableID, indexID)
		row = encoding.EncodeVarintAscending(row, k)
		fam := roachpb.Key(keys.MakeFamilyKey(append([]byte(nil), row...), 0))

		if got, exp := fam.String(), fmt.Sprintf("/Table/%d/%d/%d/0", tableID, indexID, k); got != exp {
			t.Fatalf("pretty key of row %d = %s, want %s", k, got, exp)
		}
		queueKey := fmt.Sprint(juicerKeyHash(fam))

		txn := juicerTestTxn(1)
		get := crdbJuicerSPI{}.SplitMarker(juicerBatch(txn,
			&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: fam}}))
		putBatch := crdbJuicerSPI{}.SplitMarker(juicerBatch(txn,
			&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: fam}}, endTxn(true)))
		if len(get) != 1 || len(putBatch) != 1 {
			t.Fatalf("row %d: got %d Get markers and %d Put markers, want one each", k, len(get), len(putBatch))
		}
		rec := hotKeyRecord{
			YCSBKey:           k,
			PrettyKey:         fam.String(),
			FamilyKeyHex:      hex.EncodeToString(fam),
			RowKeyHex:         hex.EncodeToString(row),
			QueueKey:          queueKey,
			MarkerQueueKeyGet: fmt.Sprint(get[0].Key),
			MarkerQueueKeyPut: fmt.Sprint(putBatch[0].Key),
		}
		if rec.MarkerQueueKeyGet != queueKey || rec.MarkerQueueKeyPut != queueKey {
			t.Fatalf("row %d: marker queue keys %s/%s differ from %s",
				k, rec.MarkerQueueKeyGet, rec.MarkerQueueKeyPut, queueKey)
		}
		if other, dup := byQueueKey[queueKey]; dup {
			t.Fatalf("rows %d and %d share queue key %s", other, k, queueKey)
		}
		byQueueKey[queueKey] = k
		if w, ok := want[k]; ok {
			if rec.FamilyKeyHex != w[0] || rec.QueueKey != w[1] {
				t.Fatalf("row %d: got hex %s queue key %s, frozen value hex %s queue key %s",
					k, rec.FamilyKeyHex, rec.QueueKey, w[0], w[1])
			}
			t.Logf("row %4d  %-18s %-14s queue key %s", k, rec.PrettyKey, rec.FamilyKeyHex, rec.QueueKey)
		}
		records = append(records, rec)
	}
	t.Logf("%d rows -> %d distinct queue keys", rows, len(byQueueKey))

	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		data, err := json.MarshalIndent(records, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "ycsbt-queue-keys.json"), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
