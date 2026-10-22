// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

type hotKey struct {
	Rank       int    `json:"rank"`
	Key        int64  `json:"key"`
	TargetNode string `json:"target_node"`
	RangeID    int64  `json:"range_id"`
	LeaseStore int64  `json:"lease_store"`
}
type desiredMap struct {
	Hot    []hotKey `json:"hot_keys"`
	Splits []int64  `json:"split_keys"`
}

func desiredLayout(c config) (desiredMap, error) {
	var p desiredMap
	if c.Layout == nil || c.Layout.HotRanks < 1 || int64(c.Layout.HotRanks) > c.Keys || len(c.Layout.LeaseNodes) == 0 || len(c.NodeIDs) != len(c.URLs) || len(c.NodeIDs) != c.Replicas || !c.Layout.DisableRebalancing {
		return p, errors.New("invalid fixed hot-rank layout")
	}
	ids := map[string]bool{}
	for _, id := range c.NodeIDs {
		if ids[id] || id == "" {
			return p, errors.New("duplicate/empty node identity")
		}
		ids[id] = true
	}
	targets := map[string]bool{}
	for _, id := range c.Layout.LeaseNodes {
		if !ids[id] || targets[id] {
			return p, errors.New("unknown/duplicate lease target")
		}
		targets[id] = true
	}
	if len(targets) != len(ids) {
		return p, errors.New("lease targets must include every configured voter")
	}
	multiplier := int64(1)
	if c.Scramble {
		multiplier = max(1, c.Keys*733/1000)
		for multiplier > 1 && gcd(multiplier, c.Keys) != 1 {
			multiplier--
		}
	}
	splits := map[int64]bool{}
	for rank := 0; rank < c.Layout.HotRanks; rank++ {
		key := int64(rank) * multiplier % c.Keys
		p.Hot = append(p.Hot, hotKey{Rank: rank, Key: key, TargetNode: c.Layout.LeaseNodes[rank%len(c.Layout.LeaseNodes)]})
		splits[key] = true
		splits[key+1] = true
	}
	for key := range splits {
		p.Splits = append(p.Splits, key)
	}
	sort.Slice(p.Splits, func(i, j int) bool { return p.Splits[i] < p.Splits[j] })
	return p, nil
}

type layoutNode struct {
	Name    string `json:"name"`
	NodeID  int64  `json:"node_id"`
	StoreID int64  `json:"store_id"`
	SQLURL  string `json:"sql_url"`
}
type layoutRange struct {
	ID           int64   `json:"range_id"`
	Start        string  `json:"start_hex"`
	End          string  `json:"end_hex"`
	LogicalStart string  `json:"logical_start_hex"`
	LogicalEnd   string  `json:"logical_end_hex"`
	Replicas     []int64 `json:"replicas"`
	Voters       []int64 `json:"voting_replicas"`
	NonVoters    []int64 `json:"non_voting_replicas"`
	Learners     []int64 `json:"learner_replicas"`
	Lease        int64   `json:"lease_holder"`
}
type layoutReceipt struct {
	SQL    string `json:"sql"`
	Start  int64  `json:"start_unix_ns"`
	End    int64  `json:"end_unix_ns"`
	Result any    `json:"result"`
}
type layoutProof struct {
	Schema       int               `json:"schema_version"`
	AdminSession string            `json:"admin_session"`
	Valid        bool              `json:"valid"`
	Started      int64             `json:"start_unix_ns"`
	Ended        int64             `json:"end_unix_ns"`
	Desired      desiredMap        `json:"desired"`
	Nodes        []layoutNode      `json:"nodes"`
	Ranges       []layoutRange     `json:"ranges"`
	Settings     map[string]string `json:"settings"`
	Receipts     []layoutReceipt   `json:"receipts"`
}

const rangeSQL = "SELECT range_id, encode(raw_start_key,'hex'), encode(raw_end_key,'hex'), replicas, voting_replicas, non_voting_replicas, learner_replicas, coalesce(lease_holder,0) FROM [SHOW RANGES FROM TABLE usertable WITH KEYS, DETAILS] ORDER BY raw_start_key"
const loadRebalance = "kv.allocator.load_based_rebalancing"
const leaseQueue = "kv.lease_queue.enabled"

// Catalog inspection is enabled only on these administrative connections;
// workload connections retain their normal session defaults.
func layoutConnect(ctx context.Context, url string) (*pgx.Conn, error) {
	conn, err := connect(ctx, url)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Exec(ctx, "SET allow_unsafe_internals=true"); err != nil {
		conn.Close(context.Background())
		return nil, err
	}
	return conn, nil
}
func resolveNodes(ctx context.Context, c config) ([]layoutNode, error) {
	var nodes []layoutNode
	stores := map[int64]bool{}
	ids := map[int64]bool{}
	for i, url := range c.URLs {
		conn, err := layoutConnect(ctx, url)
		if err != nil {
			return nil, err
		}
		node := layoutNode{Name: c.NodeIDs[i], SQLURL: url}
		err = conn.QueryRow(ctx, "SELECT crdb_internal.node_id()").Scan(&node.NodeID)
		if err != nil {
			conn.Close(context.Background())
			return nil, err
		}
		for {
			var count int
			err = conn.QueryRow(ctx, "SELECT count(*), coalesce(min(store_id),0) FROM crdb_internal.kv_store_status WHERE node_id=$1", node.NodeID).Scan(&count, &node.StoreID)
			if err != nil || count > 1 {
				conn.Close(context.Background())
				if err != nil {
					return nil, err
				}
				return nil, errors.New("fixed layout expects exactly one owned store per node")
			}
			if count == 1 {
				break
			}
			if !sleep(ctx, 100*time.Millisecond) {
				conn.Close(context.Background())
				return nil, ctx.Err()
			}
		}
		conn.Close(context.Background())
		if ids[node.NodeID] || stores[node.StoreID] {
			return nil, errors.New("duplicate gateway node/store identity")
		}
		ids[node.NodeID] = true
		stores[node.StoreID] = true
		nodes = append(nodes, node)
	}
	return nodes, nil
}
func readRanges(ctx context.Context, conn *pgx.Conn, p *layoutProof) ([]layoutRange, error) {
	started := time.Now().UnixNano()
	rows, err := conn.Query(ctx, rangeSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ranges []layoutRange
	for rows.Next() {
		var r layoutRange
		if err = rows.Scan(&r.ID, &r.Start, &r.End, &r.Replicas, &r.Voters, &r.NonVoters, &r.Learners, &r.Lease); err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	var tableStart, tableEnd string
	err = conn.QueryRow(ctx, "SELECT encode((crdb_internal.table_span(table_id))[1],'hex'),encode((crdb_internal.table_span(table_id))[2],'hex') FROM crdb_internal.tables WHERE database_name=current_database() AND schema_name='public' AND name='usertable'").Scan(&tableStart, &tableEnd)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, errors.New("empty table range map")
	}
	for i := range ranges {
		r := &ranges[i]
		r.LogicalStart = max(r.Start, tableStart)
		r.LogicalEnd = min(r.End, tableEnd)
		if r.LogicalStart >= r.LogicalEnd || (i > 0 && ranges[i-1].LogicalEnd != r.LogicalStart) {
			return nil, errors.New("table range map has gaps or overlapping intervals")
		}
	}
	if ranges[0].LogicalStart != tableStart || ranges[len(ranges)-1].LogicalEnd != tableEnd {
		return nil, errors.New("range map does not cover table span")
	}
	if p != nil {
		p.Receipts = append(p.Receipts, layoutReceipt{SQL: rangeSQL, Start: started, End: time.Now().UnixNano(), Result: ranges})
	}
	return ranges, nil
}
func completeReplicas(ranges []layoutRange, nodes []layoutNode) (bool, error) {
	stores := map[int64]bool{}
	for _, n := range nodes {
		stores[n.StoreID] = true
	}
	complete := true
	for _, r := range ranges {
		if len(r.Replicas) != len(nodes) || len(r.Voters) != len(nodes) || len(r.NonVoters) != 0 || len(r.Learners) != 0 {
			complete = false
		}
		for _, list := range [][]int64{r.Replicas, r.Voters, r.NonVoters, r.Learners} {
			seen := map[int64]bool{}
			for _, id := range list {
				if !stores[id] || seen[id] {
					return false, errors.New("range map has unknown/duplicate store")
				}
				seen[id] = true
			}
		}
		if r.Lease == 0 {
			complete = false
		} else if !stores[r.Lease] {
			return false, errors.New("range map has foreign lease holder")
		}
	}
	return complete, nil
}
func setLayoutSetting(
	ctx context.Context, conn *pgx.Conn, p *layoutProof, name, value string,
) error {
	var before, after string
	started := time.Now().UnixNano()
	if err := conn.QueryRow(ctx, "SHOW CLUSTER SETTING "+name).Scan(&before); err != nil {
		return err
	}
	sql := "SET CLUSTER SETTING " + name + " = " + value
	if _, err := conn.Exec(ctx, sql); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, "SHOW CLUSTER SETTING "+name).Scan(&after); err != nil {
		return err
	}
	if (name == loadRebalance && after != "off") || (name == leaseQueue && after != "false" && after != "f") {
		return errors.New("automatic lease control did not take effect")
	}
	p.Settings[name] = after
	p.Receipts = append(p.Receipts, layoutReceipt{SQL: sql, Start: started, End: time.Now().UnixNano(), Result: map[string]string{"before": before, "after": after}})
	return nil
}
func leaseStore(nodes []layoutNode, name string) int64 {
	for _, n := range nodes {
		if n.Name == name {
			return n.StoreID
		}
	}
	return 0
}
func hotRanges(
	ctx context.Context, conn *pgx.Conn, desired desiredMap, ranges []layoutRange,
) ([]hotKey, error) {
	result := append([]hotKey(nil), desired.Hot...)
	seen := map[int64]bool{}
	for i := range result {
		r := &result[i]
		if err := conn.QueryRow(ctx, "SELECT range_id, lease_holder FROM [SHOW RANGE FROM TABLE usertable FOR ROW ($1)]", r.Key).Scan(&r.RangeID, &r.LeaseStore); err != nil {
			return nil, err
		}
		if seen[r.RangeID] {
			return nil, errors.New("hot ranks share a range after splitting")
		}
		seen[r.RangeID] = true
		present := false
		for _, rangeRow := range ranges {
			if rangeRow.ID == r.RangeID && rangeRow.Lease == r.LeaseStore {
				present = true
			}
		}
		if !present {
			return nil, errors.New("key/range map disagrees with complete table map")
		}
	}
	return result, nil
}
func relocate(ctx context.Context, conn *pgx.Conn, p *layoutProof, rangeID, store int64) error {
	sql := fmt.Sprintf("ALTER RANGE %d RELOCATE LEASE TO %d", rangeID, store)
	started := time.Now().UnixNano()
	var id int64
	var pretty, result string
	if err := conn.QueryRow(ctx, sql).Scan(&id, &pretty, &result); err != nil {
		return err
	}
	p.Receipts = append(p.Receipts, layoutReceipt{SQL: sql, Start: started, End: time.Now().UnixNano(), Result: map[string]any{"range_id": id, "pretty": pretty, "result": result}})
	if id != rangeID || result != "ok" {
		return fmt.Errorf("lease relocation failed: range=%d store=%d result=%s", id, store, result)
	}
	return nil
}
func layout(c config, timeout float64) (err error) {
	desired, err := desiredLayout(c)
	if err != nil {
		return err
	}
	p := layoutProof{Schema: 1, AdminSession: "allow_unsafe_internals=true (catalog-inspection connections only)", Started: time.Now().UnixNano(), Desired: desired, Settings: map[string]string{}}
	defer func() {
		p.Ended = time.Now().UnixNano()
		writeErr := writeJSON(c.Output+".layout.json", p)
		if err == nil {
			err = writeErr
		}
	}()
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, seconds(timeout))
	defer cancel()
	p.Nodes, err = resolveNodes(ctx, c)
	if err != nil {
		return err
	}
	conn, err := layoutConnect(ctx, c.URLs[0])
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	parts := make([]string, len(desired.Splits))
	for i, key := range desired.Splits {
		parts[i] = fmt.Sprintf("(%d)", key)
	}
	splitSQL := "ALTER TABLE usertable SPLIT AT VALUES " + strings.Join(parts, ",")
	started := time.Now().UnixNano()
	if _, err = conn.Exec(ctx, splitSQL); err != nil {
		return err
	}
	p.Receipts = append(p.Receipts, layoutReceipt{SQL: splitSQL, Start: started, End: time.Now().UnixNano()})
	var ranges []layoutRange
	for {
		ranges, err = readRanges(ctx, conn, &p)
		if err != nil {
			return err
		}
		complete, e := completeReplicas(ranges, p.Nodes)
		if e != nil {
			return e
		}
		if complete {
			break
		}
		if !sleep(ctx, 100*time.Millisecond) {
			return ctx.Err()
		}
	}
	if err = setLayoutSetting(ctx, conn, &p, loadRebalance, "'off'"); err != nil {
		return err
	}
	if err = setLayoutSetting(ctx, conn, &p, leaseQueue, "false"); err != nil {
		return err
	}
	hot, err := hotRanges(ctx, conn, desired, ranges)
	if err != nil {
		return err
	}
	targets := map[int64]int64{}
	counts := map[string]int{}
	for _, r := range hot {
		targets[r.RangeID] = leaseStore(p.Nodes, r.TargetNode)
		counts[r.TargetNode]++
	}
	for _, r := range ranges {
		if _, exists := targets[r.ID]; exists {
			continue
		}
		target := c.Layout.LeaseNodes[0]
		for _, n := range c.Layout.LeaseNodes {
			if counts[n] < counts[target] {
				target = n
			}
		}
		targets[r.ID] = leaseStore(p.Nodes, target)
		counts[target]++
	}
	for _, r := range ranges {
		if r.Lease != targets[r.ID] {
			if err = relocate(ctx, conn, &p, r.ID, targets[r.ID]); err != nil {
				return err
			}
		}
	}
	p.Ranges, err = readRanges(ctx, conn, &p)
	if err != nil {
		return err
	}
	p.Desired.Hot, err = hotRanges(ctx, conn, desired, p.Ranges)
	if err != nil {
		return err
	}
	for _, r := range p.Desired.Hot {
		if r.LeaseStore != leaseStore(p.Nodes, r.TargetNode) {
			return errors.New("observed hot-rank lease differs from target")
		}
	}
	for _, r := range p.Ranges {
		if r.Lease != targets[r.ID] {
			return errors.New("complete lease map differs from target")
		}
	}
	p.Valid = true
	return nil
}

func sameLayout(expected, observed []layoutRange) bool {
	if len(expected) != len(observed) {
		return false
	}
	for i, now := range observed {
		before := expected[i]
		if now.LogicalStart != before.LogicalStart || now.LogicalEnd != before.LogicalEnd || now.Lease != before.Lease {
			return false
		}
		for _, pair := range [][2][]int64{{now.Replicas, before.Replicas}, {now.Voters, before.Voters}} {
			a, b := append([]int64(nil), pair[0]...), append([]int64(nil), pair[1]...)
			sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
			sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
			if len(a) != len(b) {
				return false
			}
			for j := range a {
				if a[j] != b[j] {
					return false
				}
			}
		}
		if len(now.NonVoters) != 0 || len(now.Learners) != 0 {
			return false
		}
	}
	return true
}
func checkLayout(ctx context.Context, c config, p layoutProof) (map[string]any, error) {
	started := time.Now().UnixNano()
	conn, err := layoutConnect(ctx, c.URLs[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	ranges, err := readRanges(ctx, conn, nil)
	if err != nil {
		return nil, err
	}
	complete, err := completeReplicas(ranges, p.Nodes)
	if err != nil {
		return nil, err
	}
	hot, err := hotRanges(ctx, conn, p.Desired, ranges)
	if err != nil {
		return nil, err
	}
	settings := map[string]string{}
	for _, name := range []string{loadRebalance, leaseQueue} {
		var value string
		if err = conn.QueryRow(ctx, "SHOW CLUSTER SETTING "+name).Scan(&value); err != nil {
			return nil, err
		}
		settings[name] = value
	}
	valid := p.Valid && complete && sameLayout(p.Ranges, ranges) && settings[loadRebalance] == "off" && (settings[leaseQueue] == "false" || settings[leaseQueue] == "f")
	for _, r := range hot {
		if r.LeaseStore != leaseStore(p.Nodes, r.TargetNode) {
			valid = false
		}
	}
	return map[string]any{"schema_version": 1, "valid": valid, "start_unix_ns": started, "end_unix_ns": time.Now().UnixNano(), "ranges": ranges, "hot_keys": hot, "settings": settings}, nil
}
func inspectLayout(c config, watch bool) error {
	raw, err := os.ReadFile(c.Output + ".layout.json")
	if err != nil {
		return err
	}
	var proof layoutProof
	if err = json.Unmarshal(raw, &proof); err != nil {
		return err
	}
	if !proof.Valid {
		return errors.New("initial layout is invalid")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if !watch {
		request, cancel := context.WithTimeout(ctx, seconds(c.RequestTimeout))
		defer cancel()
		row, e := checkLayout(request, c, proof)
		if e != nil {
			return e
		}
		if e = writeJSON(c.Output+".layout-final.json", row); e != nil {
			return e
		}
		if row["valid"] != true {
			return errors.New("final table/lease map changed")
		}
		return nil
	}
	f, err := os.Create(c.Output + ".layout-monitor.jsonl")
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	for ctx.Err() == nil {
		request, cancel := context.WithTimeout(ctx, seconds(c.RequestTimeout))
		row, e := checkLayout(request, c, proof)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if e != nil {
			return e
		}
		if e = encoder.Encode(row); e != nil {
			return e
		}
		if e = f.Sync(); e != nil {
			return e
		}
		if row["valid"] != true {
			return errors.New("runtime table/lease map changed")
		}
		wait := c.Layout.MonitorInterval
		if wait == 0 {
			wait = 1
		}
		if !sleep(ctx, seconds(wait)) {
			return nil
		}
	}
	return nil
}
