package load

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/client"
)

// Mix holds per-operation weights. Zero total selects a read-heavy
// default (90% get, 9% set, 1% del).
type Mix struct {
	Get, Set, Del int
}

func (m Mix) total() int { return m.Get + m.Set + m.Del }

func (m Mix) withDefault() Mix {
	if m.total() == 0 {
		return Mix{Get: 90, Set: 9, Del: 1}
	}
	return m
}

// Config drives one load run.
type Config struct {
	Nodes []client.NodeAddr
	// Client behavior.
	MaxRetries int
	// RPCTimeout bounds each RPC. Zero selects the tree-standard 5s:
	// passing 0 through would trip Client.Do's unconditional deadline
	// (now+0 expires immediately despite the "0 = no timeout" doc).
	RPCTimeout time.Duration
	// Load shape.
	Workers     int
	TotalOps    int64 // stop after this many ops (0 = run until ctx ends)
	Duration    time.Duration
	KeySpace    int     // distinct cold keys
	HotKeys     int     // distinct hot keys (rotated when HotFraction > 0)
	HotFraction float64 // share of ops aimed at the hot set
	ValueSize   int     // set payload bytes
	Mix         Mix     // op weights
	Seed        uint64  // worker streams derive from Seed+worker
}

// Result is the client-observed outcome of a run.
type Result struct {
	Gets, Sets, Dels Histogram
	Errors           atomic.Int64
	FirstErr         atomic.Value // stores error
	Ops              int64
	Elapsed          time.Duration
}

// Throughput returns completed ops per second.
func (r *Result) Throughput() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Ops) / r.Elapsed.Seconds()
}

func (r *Result) recordErr(err error) {
	r.Errors.Add(1)
	r.FirstErr.CompareAndSwap(nil, err)
}

// validate fills defaults and rejects nonsense before any IO.
func (c *Config) validate() (Config, error) {
	out := *c
	out.Mix = c.Mix.withDefault()
	if len(out.Nodes) == 0 {
		return out, fmt.Errorf("load: no target nodes")
	}
	if out.RPCTimeout <= 0 {
		out.RPCTimeout = 5 * time.Second
	}
	if out.Workers <= 0 {
		out.Workers = 1
	}
	if out.TotalOps < 0 {
		return out, fmt.Errorf("load: negative TotalOps")
	}
	if out.KeySpace <= 0 {
		out.KeySpace = 10000
	}
	if out.HotKeys < 0 || out.HotFraction < 0 || out.HotFraction > 1 {
		return out, fmt.Errorf("load: bad hot-key config")
	}
	if out.ValueSize < 0 {
		return out, fmt.Errorf("load: negative ValueSize")
	}
	return out, nil
}

// Run drives cfg against a live cluster until TotalOps completes, the
// deadline passes, or ctx ends. Workers share one Cluster (safe for
// concurrent use); each keeps private histograms merged at the end.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	cl, err := client.NewCluster(client.ClusterConfig{
		Nodes: cfg.Nodes, MaxRetries: cfg.MaxRetries, RPCTimeout: cfg.RPCTimeout,
	})
	if err != nil {
		return nil, err
	}

	defer cl.Close()
	res := &Result{}
	start := time.Now()

	var done, claimed atomic.Int64
	type workerHists struct{ gets, sets, dels Histogram }
	hists := make([]workerHists, cfg.Workers)
	var wg sync.WaitGroup
	for w := range cfg.Workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.Seed, uint64(w)))
			value := make([]byte, cfg.ValueSize)
			for ctx.Err() == nil {
				if cfg.TotalOps > 0 && claimed.Add(1) > cfg.TotalOps {
					break
				}

				op := pickOp(rng, cfg.Mix)
				key := pickKey(rng, cfg)
				t := time.Now()

				var oerr error
				switch op {
				case opGet:
					_, oerr = cl.Get(key)
				case opSet:
					_, oerr = cl.Set(key, value)
				default:
					_, oerr = cl.Del(key)
				}

				el := time.Since(t).Nanoseconds()
				switch op {
				case opGet:
					hists[w].gets.Record(el)
				case opSet:
					hists[w].sets.Record(el)
				default:
					hists[w].dels.Record(el)
				}
				if oerr != nil {
					res.recordErr(oerr)
				}
				done.Add(1)
			}
		}(w)
	}
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-ctx.Done():
	case <-finished:
	}

	wg.Wait()
	for i := range hists {
		res.Gets.Merge(&hists[i].gets)
		res.Sets.Merge(&hists[i].sets)
		res.Dels.Merge(&hists[i].dels)
	}

	res.Elapsed = time.Since(start)
	res.Ops = done.Load()
	return res, nil
}

type opKind int

const (
	opGet opKind = iota
	opSet
	opDel
)

// pickOp draws an operation by weight.
func pickOp(rng *rand.Rand, m Mix) opKind {
	n := rng.IntN(m.total())
	if n < m.Get {
		return opGet
	}
	if n < m.Get+m.Set {
		return opSet
	}
	return opDel
}

// pickKey draws from the hot set with HotFraction probability, else the
// cold keyspace. Keys are "k-<n>" so every worker addresses the same
// key universe (hot keys collide across workers by design).
func pickKey(rng *rand.Rand, cfg Config) string {
	if cfg.HotKeys > 0 && rng.Float64() < cfg.HotFraction {
		return fmt.Sprintf("hot-%d", rng.IntN(cfg.HotKeys))
	}
	return fmt.Sprintf("k-%d", rng.IntN(cfg.KeySpace))
}
