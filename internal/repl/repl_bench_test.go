package repl

import (
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

// Coordinator cost only: testkit answers inline under a mutex, so these
// measure fan-out/reduction/drain, not network I/O.

func BenchmarkWriteQuorum(b *testing.B) {
	for _, replicas := range [][]string{nil, {"n2"}, {"n2", "n3"}, {"n2", "n3", "n4", "n5"}} {
		b.Run(fmt.Sprintf("rf%d", len(replicas)+1), func(b *testing.B) {
			ft := testkit.NewStores("n2", "n3", "n4", "n5")
			m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
			b.ReportAllocs()
			for b.Loop() {
				if err := m.Write("k", []byte("v"), 0, replicas, Quorum, 0, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadMiss(b *testing.B) {
	ft := testkit.NewStores("n2")
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := m.Read("ghost", []string{"n2"}, 2, 0, 0); err == nil {
			b.Fatal("expected miss")
		}
	}
}

func BenchmarkReadRepair(b *testing.B) {
	ft := testkit.NewStores("n2")
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	if err := m.Local().Put("k", []byte("old")); err != nil {
		b.Fatal(err)
	}
	le, _ := m.Local().GetVersioned("k")
	le.Value = []byte("new")
	le.Version += 10
	if err := ft.Stores["n2"].PutVersioned("k", le); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		// Re-diverge every iteration so each read repairs: decision
		// only, the repair fan-out drains in the background.
		le, _ := ft.Stores["n2"].GetVersioned("k")
		le.Version++
		if err := ft.Stores["n2"].PutVersioned("k", le); err != nil {
			b.Fatal(err)
		}
		if _, err := m.Read("k", []string{"n2"}, 2, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplayBatch256(b *testing.B) {
	ft := testkit.NewStores("n2")
	ft.Fail["n2"] = true // all replays fail fast: snapshot + fan-out cost only
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	exp := expiry.At(time.Now().Add(time.Hour))
	for i := range ReplayBatch {
		m.HintQueue().Push(Hint{
			Target:    "n2",
			Req:       protocol.Request{Opcode: protocol.OpReplicate, Key: fmt.Appendf(nil, "k%d", i), Value: []byte("v"), Version: uint64(i + 1), WriterNodeHash: 1},
			ExpiresAt: exp,
		})
	}
	b.ReportAllocs()
	for b.Loop() {
		// Nothing is ever removed (all fail), so every round replays
		// the same full batch.
		m.ReplayHints()
	}
}
