// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
)

// This file implements the --one-shot variants of the five TPC-C
// transactions. Each variant issues its whole transaction as a single
// implicit-transaction SQL statement: every read dependency that the standard
// implementations resolve with a client round trip (read stock, compute, write
// stock; read district next order id, then insert) is instead expressed inside
// one statement with common table expressions. Client-side transaction-mix
// logic, random-input generation and audit accounting are identical to the
// standard implementations.
//
// Why this exists: the interactive implementations hold a transaction open
// across several client round trips, so no RPC-level view of a single batch
// can observe or order the whole transaction. The one-shot forms make each
// transaction a single batch, which also places it on CockroachDB's automatic
// server-side retry path (nothing has been returned to the client when a
// retryable error occurs).
//
// Semantic deltas versus the standard implementations, all deliberate:
//   - newOrder's brand/generic display flag (i_data/s_data inspection) is not
//     computed; it is display-only output and is never written or audited.
//   - The simulated-user-error rollback (2.4.1.4) surfaces as a forced
//     division-by-zero error, which aborts the single statement; statement
//     atomicity provides the rollback that the interactive form gets from
//     ROLLBACK. The client recognises the error code and accounts it exactly
//     like the interactive form accounts errSimulated.
//   - Row sets that the interactive forms return to the terminal (order-status
//     item lists) are reduced to aggregates over the same rows: the reads
//     happen, the display payload does not cross the wire.

// Guard expressions: several statements below embed
// `CASE WHEN <expected> THEN ... ELSE 1/0 END`. A false condition aborts the
// whole statement with a division-by-zero error, and statement atomicity rolls
// back every CTE mutation — used where the interactive implementations return
// an error (or errSimulated) mid-transaction.

// isForcedRollbackErr reports whether err is the division-by-zero error
// produced by a one-shot guard expression.
func isForcedRollbackErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgcode.MakeCode(pgErr.Code) == pgcode.DivisionByZero
}

// ---------------------------------------------------------------------------
// newOrder
// ---------------------------------------------------------------------------

type newOrderOneShot struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	stmt workload.StmtHandle
}

var _ tpccTx = &newOrderOneShot{}

func createNewOrderOneShot(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	n := &newOrderOneShot{
		config: config,
		mcp:    mcp,
	}

	// One statement for the whole transaction. Input arrays are zipped by
	// unnest; the district CTE both reads and advances d_next_o_id; the stock
	// CTE applies the quantity/ytd/order_cnt/remote_cnt updates in place. The
	// guard aborts the statement when an item id does not exist (the simulated
	// user error, 1% of orders) or an expected stock row is missing, matching
	// the interactive implementation's rollback/error behaviour.
	n.stmt = n.sr.Define(`
		WITH
		input AS (
			SELECT * FROM unnest($7::INT[], $8::INT[], $9::INT[], $10::INT[], $11::INT[])
				AS t(ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_remote)
		),
		dist AS (
			UPDATE district SET d_next_o_id = d_next_o_id + 1
			WHERE d_w_id = $1 AND d_id = $2
			RETURNING d_tax, d_next_o_id - 1 AS o_id
		),
		wh AS (SELECT w_tax FROM warehouse WHERE w_id = $1),
		cust AS (
			SELECT c_discount, c_last, c_credit FROM customer
			WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
		),
		item_info AS (
			SELECT t.ol_number, t.ol_i_id, t.ol_supply_w_id, t.ol_quantity, i.i_price
			FROM input t JOIN item i ON i.i_id = t.ol_i_id
		),
		stock_upd AS (
			UPDATE stock SET
				s_quantity = CASE WHEN s_quantity < t.ol_quantity + 10
					THEN s_quantity - t.ol_quantity + 91
					ELSE s_quantity - t.ol_quantity END,
				s_ytd = s_ytd + t.ol_quantity,
				s_order_cnt = s_order_cnt + 1,
				s_remote_cnt = s_remote_cnt + t.ol_remote
			FROM input t
			WHERE s_i_id = t.ol_i_id AND s_w_id = t.ol_supply_w_id
			RETURNING s_i_id, s_w_id,
				CASE $2::INT
					WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02
					WHEN 3 THEN s_dist_03 WHEN 4 THEN s_dist_04
					WHEN 5 THEN s_dist_05 WHEN 6 THEN s_dist_06
					WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08
					WHEN 9 THEN s_dist_09 ELSE s_dist_10
				END AS dist_info
		),
		ord AS (
			INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
			SELECT d.o_id, $2, $1, $3, $6::TIMESTAMP, $4, $5 FROM dist d
			RETURNING o_id
		),
		no_ins AS (
			INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
			SELECT d.o_id, $2, $1 FROM dist d
			RETURNING no_o_id
		),
		ol_ins AS (
			INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
				ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
			SELECT d.o_id, $2, $1, ii.ol_number, ii.ol_i_id, ii.ol_supply_w_id,
				ii.ol_quantity, ii.ol_quantity * ii.i_price, su.dist_info
			FROM dist d, item_info ii
			JOIN stock_upd su ON su.s_i_id = ii.ol_i_id AND su.s_w_id = ii.ol_supply_w_id
			RETURNING ol_amount
		)
		SELECT
			CASE WHEN (SELECT count(*) FROM item_info) = $4::INT
				AND (SELECT count(*) FROM ol_ins) = $4::INT
				THEN 0 ELSE 1/0 END,
			(SELECT sum(ol_amount) FROM ol_ins)
				* (1 - c.c_discount) * (1 + w.w_tax + d.d_tax),
			c.c_last, c.c_credit, d.o_id
		FROM cust c, wh w, dist d`,
	)

	if err := n.sr.Init(ctx, "new-order-one-shot", mcp); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *newOrderOneShot) run(ctx context.Context, wID int) (interface{}, time.Duration, error) {
	n.config.auditor.newOrderTransactions.Add(1)

	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	d := newOrderData{
		wID:    wID,
		dID:    int(randInt(rng, 1, 10)),
		cID:    n.config.randCustomerID(rng),
		oOlCnt: int(randInt(rng, 5, 15)),
	}
	d.items = make([]orderItem, d.oOlCnt)

	n.config.auditor.Lock()
	n.config.auditor.orderLinesFreq[d.oOlCnt]++
	n.config.auditor.Unlock()
	n.config.auditor.totalOrderLines.Add(uint64(d.oOlCnt))

	itemIDs := make(map[int]struct{})

	// 2.4.1.4: A fixed 1% of the New-Order transactions are chosen at random to
	// simulate user data entry errors and exercise the performance of rolling
	// back update transactions.
	rollback := rng.IntN(100) == 0

	allLocal := 1
	for i := 0; i < d.oOlCnt; i++ {
		item := orderItem{
			olNumber:   i + 1,
			olQuantity: rng.IntN(10) + 1,
		}
		if rollback && i == d.oOlCnt-1 {
			item.olIID = -1
		} else {
			for {
				item.olIID = n.config.randItemID(rng)
				if _, ok := itemIDs[item.olIID]; !ok {
					itemIDs[item.olIID] = struct{}{}
					break
				}
			}
		}
		if n.config.localWarehouses {
			item.remoteWarehouse = false
		} else {
			item.remoteWarehouse = rng.IntN(100) == 0
		}
		item.olSupplyWID = wID
		if item.remoteWarehouse && n.config.activeWarehouses > 1 {
			allLocal = 0
			if len(n.config.multiRegionCfg.regions) > 0 {
				item.olSupplyWID = n.config.wMRPart.randActive(rng)
				for item.olSupplyWID == wID {
					item.olSupplyWID = n.config.wMRPart.randActive(rng)
				}
			} else {
				item.olSupplyWID = n.config.wPart.randActive(rng)
				for item.olSupplyWID == wID {
					item.olSupplyWID = n.config.wPart.randActive(rng)
				}
			}
			n.config.auditor.Lock()
			n.config.auditor.orderLineRemoteWarehouseFreq[item.olSupplyWID]++
			n.config.auditor.Unlock()
		} else {
			item.olSupplyWID = wID
		}
		d.items[i] = item
	}

	sort.Slice(d.items, func(i, j int) bool {
		return d.items[i].olIID < d.items[j].olIID
	})

	d.oEntryD = timeutil.Now()

	olNumbers := make([]int, d.oOlCnt)
	olItemIDs := make([]int, d.oOlCnt)
	olSupplyWIDs := make([]int, d.oOlCnt)
	olQuantities := make([]int, d.oOlCnt)
	olRemotes := make([]int, d.oOlCnt)
	for i, item := range d.items {
		olNumbers[i] = item.olNumber
		olItemIDs[i] = item.olIID
		olSupplyWIDs[i] = item.olSupplyWID
		olQuantities[i] = item.olQuantity
		if item.remoteWarehouse {
			olRemotes[i] = 1
		}
	}

	var guard int
	err := n.stmt.QueryRow(ctx,
		wID, d.dID, d.cID, d.oOlCnt, allLocal, d.oEntryD.Format("2006-01-02 15:04:05"),
		olNumbers, olItemIDs, olSupplyWIDs, olQuantities, olRemotes,
	).Scan(&guard, &d.totalAmount, &d.cLast, &d.cCredit, &d.oID)
	if err != nil {
		if rollback && isForcedRollbackErr(err) {
			// 2.4.2.3: the guard aborted the statement because the invalid item
			// id found no row; statement atomicity rolled everything back.
			n.config.auditor.newOrderRollbacks.Add(1)
			return d, 0, nil
		}
		return nil, 0, errors.Wrap(err, "one-shot new-order failed")
	}
	return d, 0, nil
}

// ---------------------------------------------------------------------------
// payment
// ---------------------------------------------------------------------------

type paymentOneShot struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	stmt workload.StmtHandle

	a bufalloc.ByteAllocator
}

var _ tpccTx = &paymentOneShot{}

func createPaymentOneShot(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	p := &paymentOneShot{
		config: config,
		mcp:    mcp,
	}

	// The by-last-name customer selection (2.5.2.2 case 2: middle row, rounded
	// up, ordered by first name) happens in the pick CTE with window functions;
	// row_number rn (1-based) = (count+1)/2 selects the same row as the
	// interactive form's zero-based (len-1)/2. The guard aborts if the
	// customer update matched no row.
	p.stmt = p.sr.Define(`
		WITH
		wh AS (
			UPDATE warehouse SET w_ytd = w_ytd + ($1:::FLOAT)::DECIMAL
			WHERE w_id = $2
			RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip
		),
		dst AS (
			UPDATE district SET d_ytd = d_ytd + ($1:::FLOAT)::DECIMAL
			WHERE d_w_id = $2 AND d_id = $3
			RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip
		),
		pick AS (
			SELECT CASE WHEN $6::INT != 0 THEN $6::INT ELSE (
				SELECT c_id FROM (
					SELECT c_id, row_number() OVER (ORDER BY c_first) AS rn,
						count(*) OVER () AS cnt
					FROM customer
					WHERE c_w_id = $4 AND c_d_id = $5 AND c_last = $7
				) WHERE rn = (cnt + 1) / 2
			) END AS c_id
		),
		cust AS (
			UPDATE customer SET (c_balance, c_ytd_payment, c_payment_cnt, c_data) =
				(c_balance - ($1:::FLOAT)::DECIMAL,
				 c_ytd_payment + ($1:::FLOAT)::DECIMAL,
				 c_payment_cnt + 1,
				 CASE c_credit WHEN 'BC' THEN
					left(c_id::TEXT || c_d_id::TEXT || c_w_id::TEXT
						|| $3::TEXT || $2::TEXT || ($1:::FLOAT)::TEXT || c_data, 500)
				 ELSE c_data END)
			WHERE c_w_id = $4 AND c_d_id = $5 AND c_id = (SELECT c_id FROM pick)
			RETURNING c_id, c_first, c_middle, c_last, c_street_1, c_street_2,
				c_city, c_state, c_zip, c_phone, c_since, c_credit,
				c_credit_lim, c_discount, c_balance,
				CASE c_credit WHEN 'BC' THEN left(c_data, 200) ELSE '' END AS c_data_out
		),
		hist AS (
			INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
				h_amount, h_date, h_data)
			SELECT c.c_id, $5, $4, $3, $2, ($1:::FLOAT)::DECIMAL, $8::TIMESTAMP,
				w.w_name || '    ' || d.d_name
			FROM cust c, wh w, dst d
			RETURNING h_c_id
		)
		SELECT
			CASE WHEN (SELECT count(*) FROM cust) = 1 THEN 0 ELSE 1/0 END,
			c.c_id, c.c_first, c.c_middle, c.c_last, c.c_street_1, c.c_street_2,
			c.c_city, c.c_state, c.c_zip, c.c_phone, c.c_since, c.c_credit,
			c.c_credit_lim, c.c_discount, c.c_balance, c.c_data_out,
			w.w_street_1, w.w_street_2, w.w_city, w.w_state, w.w_zip,
			d.d_street_1, d.d_street_2, d.d_city, d.d_state, d.d_zip
		FROM cust c, wh w, dst d`,
	)

	if err := p.sr.Init(ctx, "payment-one-shot", mcp); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *paymentOneShot) run(ctx context.Context, wID int) (interface{}, time.Duration, error) {
	p.config.auditor.paymentTransactions.Add(1)

	rng := rand.New(rand.NewPCG(uint64(timeutil.Now().UnixNano()), 1))

	d := paymentData{
		dID:     rng.IntN(10) + 1,
		hAmount: float64(randInt(rng, 100, 500000)) / float64(100.0),
		hDate:   timeutil.Now(),
	}

	// 2.5.1.2: 85% home warehouse, else remote (see the interactive form for
	// the multi-region variants).
	if p.config.localWarehouses || rng.IntN(100) < 85 {
		d.cWID = wID
		d.cDID = d.dID
	} else {
		if len(p.config.multiRegionCfg.regions) > 0 {
			d.cWID = p.config.wMRPart.randActive(rng)
			for d.cWID == wID && p.config.activeWarehouses > 1 {
				d.cWID = p.config.wMRPart.randActive(rng)
			}
		} else {
			d.cWID = p.config.wPart.randActive(rng)
			for d.cWID == wID && p.config.activeWarehouses > 1 {
				d.cWID = p.config.wPart.randActive(rng)
			}
		}
		p.config.auditor.Lock()
		p.config.auditor.paymentRemoteWarehouseFreq[d.cWID]++
		p.config.auditor.Unlock()
		d.cDID = rng.IntN(10) + 1
	}

	// 2.5.1.2: 60% by last name, 40% by customer id.
	if rng.IntN(100) < 60 {
		d.cLast = string(p.config.randCLast(rng, &p.a))
		p.config.auditor.paymentsByLastName.Add(1)
	} else {
		d.cID = p.config.randCustomerID(rng)
	}

	var guard int
	err := p.stmt.QueryRow(ctx,
		d.hAmount, wID, d.dID, d.cWID, d.cDID, d.cID, d.cLast,
		d.hDate.Format("2006-01-02 15:04:05"),
	).Scan(&guard, &d.cID, &d.cFirst, &d.cMiddle, &d.cLast, &d.cStreet1, &d.cStreet2,
		&d.cCity, &d.cState, &d.cZip, &d.cPhone, &d.cSince, &d.cCredit,
		&d.cCreditLim, &d.cDiscount, &d.cBalance, &d.cData,
		&d.wStreet1, &d.wStreet2, &d.wCity, &d.wState, &d.wZip,
		&d.dStreet1, &d.dStreet2, &d.dCity, &d.dState, &d.dZip)
	if err != nil {
		return nil, 0, errors.Wrap(err, "one-shot payment failed")
	}
	return d, 0, nil
}

// ---------------------------------------------------------------------------
// orderStatus
// ---------------------------------------------------------------------------

type orderStatusOneShot struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	stmt workload.StmtHandle

	a bufalloc.ByteAllocator
}

var _ tpccTx = &orderStatusOneShot{}

func createOrderStatusOneShot(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	o := &orderStatusOneShot{
		config: config,
		mcp:    mcp,
	}

	// Read-only. The order-line rows the interactive form ships to the
	// terminal are reduced to aggregates over the same rows; the scan itself
	// is preserved.
	o.stmt = o.sr.Define(`
		WITH
		pick AS (
			SELECT CASE WHEN $3::INT != 0 THEN $3::INT ELSE (
				SELECT c_id FROM (
					SELECT c_id, row_number() OVER (ORDER BY c_first) AS rn,
						count(*) OVER () AS cnt
					FROM customer
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $4
				) WHERE rn = (cnt + 1) / 2
			) END AS c_id
		),
		cust AS (
			SELECT c_id, c_balance, c_first, c_middle, c_last FROM customer
			WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = (SELECT c_id FROM pick)
		),
		ord AS (
			SELECT o_id, o_entry_d, o_carrier_id FROM "order"
			WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = (SELECT c_id FROM pick)
			ORDER BY o_id DESC LIMIT 1
		)
		SELECT c.c_id, c.c_balance, c.c_first, c.c_middle, c.c_last,
			o.o_id, o.o_entry_d, o.o_carrier_id,
			(SELECT count(*) FROM order_line, ord
				WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = ord.o_id),
			(SELECT COALESCE(sum(ol_amount), 0) FROM order_line, ord
				WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = ord.o_id)
		FROM cust c, ord o`,
	)

	if err := o.sr.Init(ctx, "order-status-one-shot", mcp); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *orderStatusOneShot) run(ctx context.Context, wID int) (interface{}, time.Duration, error) {
	o.config.auditor.orderStatusTransactions.Add(1)

	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	d := orderStatusData{
		dID: rng.IntN(10) + 1,
	}

	// 2.6.1.2: 60% by last name, 40% by customer id.
	if rng.IntN(100) < 60 {
		d.cLast = string(o.config.randCLast(rng, &o.a))
		o.config.auditor.orderStatusByLastName.Add(1)
	} else {
		d.cID = o.config.randCustomerID(rng)
	}

	var itemCount int
	var amountSum float64
	err := o.stmt.QueryRow(ctx, wID, d.dID, d.cID, d.cLast).Scan(
		&d.cID, &d.cBalance, &d.cFirst, &d.cMiddle, &d.cLast,
		&d.oID, &d.oEntryD, &d.oCarrierID, &itemCount, &amountSum)
	if err != nil {
		return nil, 0, errors.Wrap(err, "one-shot order-status failed")
	}
	_ = itemCount
	_ = amountSum
	return d, 0, nil
}

// ---------------------------------------------------------------------------
// delivery
// ---------------------------------------------------------------------------

type deliveryOneShot struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	stmt workload.StmtHandle
}

var _ tpccTx = &deliveryOneShot{}

func createDeliveryOneShot(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	del := &deliveryOneShot{
		config: config,
		mcp:    mcp,
	}

	// All ten districts in one statement: cand picks each district's oldest
	// undelivered order (2.7.4.2); districts with no undelivered order simply
	// produce no cand row and are skipped, accounted client-side. Concurrent
	// deliveries against the same warehouse are serialised by SERIALIZABLE
	// isolation itself (both would delete the same new_order row), which is
	// what the interactive form's SELECT ... FOR UPDATE achieved. The guard
	// aborts on any order/customer/new_order fan-out mismatch, mirroring the
	// interactive form's checkSameKeys and NULL-sum failures.
	del.stmt = del.sr.Define(`
		WITH
		cand AS (
			SELECT no_d_id AS d_id, min(no_o_id) AS o_id FROM new_order
			WHERE no_w_id = $1 GROUP BY no_d_id
		),
		sums AS (
			SELECT c.d_id, c.o_id, sum(ol.ol_amount) AS total
			FROM cand c JOIN order_line ol
				ON ol.ol_w_id = $1 AND ol.ol_d_id = c.d_id AND ol.ol_o_id = c.o_id
			GROUP BY c.d_id, c.o_id
		),
		ord AS (
			UPDATE "order" SET o_carrier_id = $2 FROM cand c
			WHERE o_w_id = $1 AND o_d_id = c.d_id AND "order".o_id = c.o_id
			RETURNING o_d_id, o_c_id
		),
		cust AS (
			UPDATE customer SET c_delivery_cnt = c_delivery_cnt + 1,
				c_balance = c_balance + s.total
			FROM ord o JOIN sums s ON s.d_id = o.o_d_id
			WHERE c_w_id = $1 AND c_d_id = o.o_d_id AND c_id = o.o_c_id
			RETURNING 1
		),
		del AS (
			DELETE FROM new_order WHERE no_w_id = $1
				AND (no_d_id, no_o_id) IN (SELECT d_id, o_id FROM cand)
			RETURNING 1
		),
		ol_upd AS (
			UPDATE order_line SET ol_delivery_d = $3::TIMESTAMP
			WHERE ol_w_id = $1 AND (ol_d_id, ol_o_id) IN (SELECT d_id, o_id FROM cand)
			RETURNING 1
		)
		SELECT (SELECT count(*) FROM cand),
			CASE WHEN (SELECT count(*) FROM ord) = (SELECT count(*) FROM cand)
				AND (SELECT count(*) FROM cust) = (SELECT count(*) FROM cand)
				AND (SELECT count(*) FROM del) = (SELECT count(*) FROM cand)
				THEN (SELECT count(*) FROM ol_upd) ELSE 1/0 END`,
	)

	if err := del.sr.Init(ctx, "delivery-one-shot", mcp); err != nil {
		return nil, err
	}
	return del, nil
}

func (del *deliveryOneShot) run(ctx context.Context, wID int) (interface{}, time.Duration, error) {
	del.config.auditor.deliveryTransactions.Add(1)

	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	oCarrierID := rng.IntN(10) + 1
	olDeliveryD := timeutil.Now()

	var delivered, olUpdated int
	err := del.stmt.QueryRow(ctx,
		wID, oCarrierID, olDeliveryD.Format("2006-01-02 15:04:05"),
	).Scan(&delivered, &olUpdated)
	if err != nil {
		return nil, 0, errors.Wrap(err, "one-shot delivery failed")
	}
	_ = olUpdated
	if skipped := 10 - delivered; skipped > 0 {
		del.config.auditor.skippedDelivieries.Add(uint64(skipped))
	}
	return nil, 0, nil
}

// ---------------------------------------------------------------------------
// stockLevel
// ---------------------------------------------------------------------------

type stockLevelOneShot struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	stmt workload.StmtHandle
}

var _ tpccTx = &stockLevelOneShot{}

func createStockLevelOneShot(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	s := &stockLevelOneShot{
		config: config,
		mcp:    mcp,
	}

	s.stmt = s.sr.Define(`
		WITH d AS (
			SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2
		)
		SELECT count(DISTINCT s_i_id)
		FROM d, order_line JOIN stock ON s_w_id = $1 AND s_i_id = ol_i_id
		WHERE ol_w_id = $1
			AND ol_d_id = $2
			AND ol_o_id BETWEEN d.d_next_o_id - 20 AND d.d_next_o_id - 1
			AND s_quantity < $3`,
	)

	if err := s.sr.Init(ctx, "stock-level-one-shot", mcp); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *stockLevelOneShot) run(ctx context.Context, wID int) (interface{}, time.Duration, error) {
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	d := stockLevelData{
		threshold: int(randInt(rng, 10, 20)),
		dID:       rng.IntN(10) + 1,
	}

	if err := s.stmt.QueryRow(ctx, wID, d.dID, d.threshold).Scan(&d.lowStock); err != nil {
		return nil, 0, errors.Wrap(err, "one-shot stock-level failed")
	}
	return d, 0, nil
}
