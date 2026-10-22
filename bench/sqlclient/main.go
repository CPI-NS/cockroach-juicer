// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
)

type layoutConfig struct {
	HotRanks           int      `json:"hot_ranks"`
	LeaseNodes         []string `json:"lease_nodes"`
	DisableRebalancing bool     `json:"disable_rebalancing"`
	MonitorInterval    float64  `json:"monitor_interval_seconds"`
}

type config struct {
	Keys            int64             `json:"keys"`
	Ops             int               `json:"ops_per_txn"`
	Theta           float64           `json:"zipf_theta"`
	Seed            uint64            `json:"seed"`
	ReadPct         float64           `json:"read_txn_pct"`
	Duplicates      bool              `json:"duplicates"`
	Scramble        bool              `json:"scramble"`
	Mode            string            `json:"mode"`
	Arrival         string            `json:"arrival"`
	Rate            float64           `json:"rate"`
	Slots           int               `json:"slots"`
	Warmup          float64           `json:"warmup_seconds"`
	Duration        float64           `json:"duration_seconds"`
	NodeIDs         []string          `json:"node_ids"`
	Layout          *layoutConfig     `json:"layout"`
	HoldCalibration bool              `json:"hold_calibration"`
	URLs            []string          `json:"urls"`
	MetricsURLs     []string          `json:"metrics_urls"`
	Output          string            `json:"output"`
	RequestTimeout  float64           `json:"request_timeout_seconds"`
	MaxRetries      int               `json:"max_retries"`
	RetryBackoff    float64           `json:"retry_backoff_seconds"`
	LoadBatch       int64             `json:"load_batch_rows"`
	Replicas        int               `json:"replicas"`
	Splits          int               `json:"splits"`
	ArtifactPaths   map[string]string `json:"artifact_paths"`
	ArtifactHashes  map[string]string `json:"artifact_sha256"`
	SourceRevision  string            `json:"source_revision"`
	SourceManifest  string            `json:"source_manifest"`
	LogDir          string            `json:"log_dir"`
	JuicerEnabled   bool              `json:"juicer_enabled"`
}

func readConfig(path string) (config, error) {
	var c config
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return c, errors.New("config must contain one JSON object")
	}
	if c.Keys < 1 || c.Keys > 1e9 || c.Ops < 1 || c.Ops > 10000 || (!c.Duplicates && int64(c.Ops) > c.Keys) || c.Theta < 0 || c.Theta >= 1 || math.IsNaN(c.Theta) || c.ReadPct < 0 || c.ReadPct > 100 {
		return c, errors.New("invalid workload parameters")
	}
	if c.Slots < 1 || c.Duration <= 0 || c.Warmup < 0 || c.MaxRetries < 0 || c.RequestTimeout < 0 || c.RetryBackoff < 0 {
		return c, errors.New("invalid scheduler parameters")
	}
	if c.Mode != "open" && c.Mode != "closed" {
		return c, errors.New("mode must be open or closed")
	}
	if c.Mode == "open" && (c.Rate <= 0 || math.IsInf(c.Rate, 0) || math.IsNaN(c.Rate) || (c.Arrival != "regular" && c.Arrival != "poisson")) {
		return c, errors.New("invalid open-loop arrival parameters")
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 30
	}
	if c.LoadBatch == 0 {
		c.LoadBatch = 10000
	}
	if c.LoadBatch < 1 || c.Splits < 0 || int64(c.Splits) >= c.Keys {
		return c, errors.New("invalid load parameters")
	}
	return c, nil
}

// Gray/YCSB Zipf transform migrated from the audited SQL trace client.
// The normalizer is computed once per process, rather than per SQL slot.
type generator struct {
	c                      config
	rng                    *rand.Rand
	zeta, eta, alpha, half float64
	multiplier             int64
}

func newGenerator(c config) *generator {
	g := &generator{c: c, rng: rand.New(rand.NewPCG(c.Seed, 0)), multiplier: 1}
	if c.Theta > 0 && c.Keys > 1 {
		for i := int64(1); i <= c.Keys; i++ {
			g.zeta += 1 / math.Pow(float64(i), c.Theta)
		}
		z2 := 1 + math.Pow(.5, c.Theta)
		g.alpha = 1 / (1 - c.Theta)
		g.half = z2
		if c.Keys > 2 {
			g.eta = (1 - math.Pow(2/float64(c.Keys), 1-c.Theta)) / (1 - z2/g.zeta)
		}
	}
	if c.Scramble && c.Keys > 1 {
		g.multiplier = max(1, c.Keys*733/1000)
		for g.multiplier > 1 && gcd(g.multiplier, c.Keys) != 1 {
			g.multiplier--
		}
	}
	return g
}
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
func (g *generator) key() int64 {
	var k int64
	if g.c.Theta == 0 || g.c.Keys == 1 {
		k = g.rng.Int64N(g.c.Keys)
	} else {
		u := g.rng.Float64()
		uz := u * g.zeta
		switch {
		case uz < 1:
			k = 0
		case uz < g.half:
			k = 1
		default:
			k = int64(float64(g.c.Keys) * math.Pow(g.eta*u-g.eta+1, g.alpha))
			if k >= g.c.Keys {
				k = g.c.Keys - 1
			}
		}
	}
	return k
}

type event struct {
	ID     uint64  `json:"id"`
	Offset int64   `json:"offset_ns"`
	Type   string  `json:"txn_type"`
	Keys   []int64 `json:"keys"`
	Values []int64 `json:"values,omitempty"`
}

func (g *generator) next(id uint64, offset int64) event {
	e := event{ID: id, Offset: offset, Type: "blindWrite", Keys: make([]int64, g.c.Ops)}
	if g.rng.Float64()*100 < g.c.ReadPct {
		e.Type = "readOnly"
	} else {
		e.Values = make([]int64, g.c.Ops)
	}
	seen := map[int64]bool{}
	for i := range e.Keys {
		k := g.key()
		if !g.c.Duplicates {
			for attempts := 0; seen[k]; attempts++ {
				if attempts < 32 {
					k = g.key()
				} else {
					k = (k + 1) % g.c.Keys
				}
			}
		}
		e.Keys[i] = k
		seen[k] = true
	}
	for i, k := range e.Keys {
		e.Keys[i] = (k * g.multiplier) % g.c.Keys
	}
	g.rng.Shuffle(len(e.Keys), func(i, j int) { e.Keys[i], e.Keys[j] = e.Keys[j], e.Keys[i] })
	for i := range e.Values {
		e.Values[i] = g.rng.Int64N(1 << 30)
	}
	return e
}
func interval(c config, r *rand.Rand) float64 {
	if c.Arrival == "poisson" {
		return r.ExpFloat64() / c.Rate
	}
	return 1 / c.Rate
}
func main() {
	if e := entry(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func entry(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: crdb-sql-client trace|ready|load|run|layout-plan|layout|watch-layout|layout-check|verify|evidence --config FILE")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	path := fs.String("config", "", "validated JSON config")
	count := fs.Int("count", 10, "trace events")
	timeout := fs.Float64("timeout", 60, "readiness deadline seconds")
	if e := fs.Parse(args[1:]); e != nil {
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	c, e := readConfig(*path)
	if e != nil {
		return e
	}
	switch args[0] {
	case "layout":
		return layout(c, *timeout)
	case "watch-layout":
		return inspectLayout(c, true)
	case "layout-check":
		return inspectLayout(c, false)
	case "layout-plan":
		p, e := desiredLayout(c)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(p)
	case "trace":
		if *count < 1 {
			return errors.New("count must be positive")
		}
		g := newGenerator(c)
		r := rand.New(rand.NewPCG(c.Seed, 1))
		enc := json.NewEncoder(os.Stdout)
		var offset float64
		for i := 0; i < *count; i++ {
			if e := enc.Encode(g.next(uint64(i), int64(math.Round(offset*1e9)))); e != nil {
				return e
			}
			if c.Mode == "open" {
				offset += interval(c, r)
			}
		}
		return nil
	case "verify":
		return verify(c)
	case "evidence":
		return evidence(c)
	case "ready":
		return ready(c, *timeout)
	case "load":
		return load(c)
	case "run":
		return run(c)
	default:
		return errors.New("unknown operation: " + args[0])
	}
}
