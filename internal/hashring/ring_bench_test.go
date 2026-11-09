package hashring

import (
	"fmt"
	"testing"
)

// BenchmarkRingGetNode mirrors bmRingGetNode: 4 nodes, 1024-key rotation,
// vnode counts 50/150/300.
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
			for i := 0; i < b.N; i++ {
				_ = r.GetNode(keys[i%len(keys)])
			}
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
			for i := 0; i < b.N; i++ {
				if _, err := r.GetNodes(keys[i%len(keys)], rf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
