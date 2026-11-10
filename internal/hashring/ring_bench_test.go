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
				if _, err := r.GetNodes(keys[i%len(keys)], rf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
