package hashring

import (
	"fmt"
	"testing"
)

// BenchmarkRingGetNode measures steady-state lookup across four nodes and
// 1024 rotating keys at vnode counts 50, 150, and 300.
func BenchmarkRingGetNode(b *testing.B) {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("ring-benchmark-key-%d", i)
	}
	for _, vnodes := range []int{50, 150, 300} {
		b.Run(fmt.Sprintf("vnodes=%d", vnodes), func(b *testing.B) {
			r, err := New(vnodes, 0)
			if err != nil {
				b.Fatal(err)
			}
			if err := r.SetNodes([]string{"node1", "node2", "node3", "node4"}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := range b.N {
				_ = r.GetNode(keys[i%len(keys)])
			}
		})
	}
}

// BenchmarkRingGetNodeScale measures lookup cost as the number of ring points
// grows. The default production setting is 150 vnodes per physical node.
func BenchmarkRingGetNodeScale(b *testing.B) {
	keys := benchmarkKeys(1024)
	for _, nodes := range []int{2, 3, 4, 32, 256} {
		for _, vnodes := range []int{50, 150, 300} {
			b.Run(fmt.Sprintf("nodes=%d/vnodes=%d", nodes, vnodes), func(b *testing.B) {
				ring := benchmarkRing(b, nodes, vnodes, 0)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					_ = ring.GetNode(keys[i%len(keys)])
				}
			})
		}
	}
}

// BenchmarkRingGetNodeParallel measures concurrent, read-only lookups against
// one immutable snapshot. It is intended to expose contention or cache effects.
func BenchmarkRingGetNodeParallel(b *testing.B) {
	keys := benchmarkKeys(1024)
	for _, nodes := range []int{2, 3, 4, 32, 256} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			ring := benchmarkRing(b, nodes, DefaultVNodes, 0)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var i uint64
				for pb.Next() {
					_ = ring.GetNode(keys[i%uint64(len(keys))])
					i++
				}
			})
		})
	}
}

// BenchmarkRingGetNodes measures RF=2 and RF=3 replica-set resolution.
func BenchmarkRingGetNodes(b *testing.B) {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("ring-benchmark-key-%d", i)
	}
	for _, rf := range []int{2, 3} {
		b.Run(fmt.Sprintf("rf=%d", rf), func(b *testing.B) {
			r, err := New(DefaultVNodes, 0)
			if err != nil {
				b.Fatal(err)
			}
			if err := r.SetNodes([]string{"node1", "node2", "node3", "node4"}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := range b.N {
				if _, err := r.GetNodes(keys[i%len(keys)], ReplicaCount(rf)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRingGetNodesScale measures the replica traversal at larger ring
// sizes and replication factors, including the maximum supported factor.
func BenchmarkRingGetNodesScale(b *testing.B) {
	keys := benchmarkKeys(1024)
	for _, nodes := range []int{4, 32, 256} {
		rfs := []int{2, 3}
		if nodes >= MaxReplicas {
			rfs = append(rfs, 8, MaxReplicas)
		}
		for _, rf := range rfs {
			b.Run(fmt.Sprintf("nodes=%d/rf=%d", nodes, rf), func(b *testing.B) {
				ring := benchmarkRing(b, nodes, DefaultVNodes, 0)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if _, err := ring.GetNodes(keys[i%len(keys)], ReplicaCount(rf)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkRingGetNodesUnderReplicated captures the bounded but full-ring walk
// when configured RF exceeds the number of currently alive physical nodes.
func BenchmarkRingGetNodesUnderReplicated(b *testing.B) {
	keys := benchmarkKeys(1024)
	for _, nodes := range []int{2, 3, 4} {
		b.Run(fmt.Sprintf("alive=%d/rf=%d", nodes, MaxReplicas), func(b *testing.B) {
			ring := benchmarkRing(b, nodes, DefaultVNodes, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := ring.GetNodes(keys[i%len(keys)], ReplicaCount(MaxReplicas)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRingSetNodesRebuild measures a full membership replacement that
// changes one survivor and adds one new node per operation. Inputs are built
// outside the timed region; rebuilding, hashing, sorting, and allocation are
// all included.
func BenchmarkRingSetNodesRebuild(b *testing.B) {
	for _, nodes := range []int{4, 32, 256, 1024} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			base := benchmarkNodeIDs(nodes)
			a := append([]string(nil), base...)
			z := append([]string(nil), base...)
			a[len(a)-1] = "replacement-a"
			z[len(z)-1] = "replacement-z"
			ring := benchmarkRing(b, nodes, DefaultVNodes, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				ids := a
				if i%2 != 0 {
					ids = z
				}
				if err := ring.SetNodes(ids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRingSetNodesNoop measures repeated membership notifications whose
// deduplicated membership is unchanged, as can happen during gossip updates.
func BenchmarkRingSetNodesNoop(b *testing.B) {
	for _, nodes := range []int{4, 32, 256, 1024} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			ids := benchmarkNodeIDs(nodes)
			ring := benchmarkRing(b, nodes, DefaultVNodes, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := ring.SetNodes(ids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRingAddRemove measures incremental writer cost as a pair of
// mutations. ReportMetric makes the number of AddNode/RemoveNode calls clear.
func BenchmarkRingAddRemove(b *testing.B) {
	for _, nodes := range []int{4, 32, 256} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			ring := benchmarkRing(b, nodes, DefaultVNodes, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ring.AddNode("temporary")
				ring.RemoveNode("temporary")
			}
			b.ReportMetric(2, "mutations/op")
		})
	}
}

// BenchmarkRingGetNodeBounded compares idle, eligible, and saturated load
// states. With zero load and factor 1, the current implementation scans the
// ring before falling back to the preferred owner.
func BenchmarkRingGetNodeBounded(b *testing.B) {
	keys := benchmarkKeys(1024)
	for _, nodes := range []int{4, 32} {
		for _, state := range []string{"idle", "eligible", "saturated"} {
			b.Run(fmt.Sprintf("nodes=%d/%s", nodes, state), func(b *testing.B) {
				factor := 1.0
				if state == "eligible" {
					factor = 2
				}
				ring := benchmarkRing(b, nodes, DefaultVNodes, factor)
				switch state {
				case "eligible":
					ring.IncrementLoad(ring.Nodes()[0])
				case "saturated":
					for _, id := range ring.Nodes() {
						ring.IncrementLoad(id)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					_ = ring.GetNodeBounded(keys[i%len(keys)])
				}
			})
		}
	}
}

func benchmarkRing(b *testing.B, nodes, vnodes int, loadFactor float64) *Ring {
	b.Helper()
	ring, err := New(vnodes, loadFactor)
	if err != nil {
		b.Fatal(err)
	}
	if err := ring.SetNodes(benchmarkNodeIDs(nodes)); err != nil {
		b.Fatal(err)
	}
	return ring
}

func benchmarkNodeIDs(n int) []string {
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	return ids
}

func benchmarkKeys(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("ring-benchmark-key-%d", i)
	}
	return keys
}
