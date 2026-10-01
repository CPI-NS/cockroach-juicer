package main

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"

	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
)

type txnSpec struct {
	ID     uint64
	Keys   [][]byte
	Reads  []bool
	Values [][]byte
}

type specGenerator struct {
	keys            int64
	ops             int
	readProbability float64
	prefix          []byte
	scramble        int64
	zipf            *workloadimpl.ZipfGenerator
	rng             *rand.Rand
	nextID          uint64
}

func newSpecGenerator(keys int64, ops int, readPct, theta float64, seed uint64, prefix string, scramble bool) (*specGenerator, error) {
	if keys < 1 || ops < 1 || int64(ops) > keys {
		return nil, fmt.Errorf("invalid keys/ops: %d/%d", keys, ops)
	}
	if readPct < 0 || readPct > 100 {
		return nil, fmt.Errorf("read-pct must be in [0,100]")
	}
	z, err := workloadimpl.NewZipfGenerator(rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), 0, uint64(keys-1), theta, false)
	if err != nil {
		return nil, err
	}
	g := &specGenerator{keys: keys, ops: ops, readProbability: readPct / 100, prefix: []byte(prefix), zipf: z, rng: rand.New(rand.NewPCG(seed^0xd1b54a32d192ed03, seed+1))}
	if scramble {
		g.scramble = scrambleMultiplier(keys)
	}
	return g, nil
}

func (g *specGenerator) next() txnSpec {
	g.nextID++
	ranks := make([]int64, 0, g.ops)
	for len(ranks) < g.ops {
		r := int64(g.zipf.Uint64())
		if r >= g.keys {
			r = g.keys - 1
		}
		found := false
		for _, old := range ranks {
			if old == r {
				found = true
				break
			}
		}
		if found {
			continue
		}
		ranks = append(ranks, r)
	}
	g.rng.Shuffle(len(ranks), func(i, j int) { ranks[i], ranks[j] = ranks[j], ranks[i] })
	nReads := 0
	for i := 0; i < g.ops; i++ {
		if g.rng.Float64() < g.readProbability {
			nReads++
		}
	}
	s := txnSpec{ID: g.nextID, Keys: make([][]byte, g.ops), Reads: make([]bool, g.ops), Values: make([][]byte, g.ops)}
	for i, r := range ranks {
		if g.scramble != 0 {
			r = (r * g.scramble) % g.keys
		}
		s.Keys[i] = encodeKey(g.prefix, r)
		s.Reads[i] = i < nReads
		v := make([]byte, 16)
		binary.BigEndian.PutUint64(v, g.nextID)
		binary.BigEndian.PutUint64(v[8:], g.rng.Uint64())
		s.Values[i] = v
	}
	return s
}

func encodeKey(prefix []byte, n int64) []byte {
	b := make([]byte, len(prefix)+8)
	copy(b, prefix)
	binary.BigEndian.PutUint64(b[len(prefix):], uint64(n))
	return b
}
func prefixEnd(prefix []byte) []byte {
	b := append([]byte(nil), prefix...)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return b[:i+1]
		}
	}
	return append(b, 0)
}
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
func scrambleMultiplier(keys int64) int64 {
	for a := keys * 733 / 1000; a > 1; a-- {
		if gcd(a, keys) == 1 {
			return a
		}
	}
	return 1
}
