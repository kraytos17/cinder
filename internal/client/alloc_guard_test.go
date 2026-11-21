package client

import "testing"

// Owner routing is per-request: both the ring-lookup miss path and the
// learned-owner hit path must stay allocation-free.
func TestAllocsPreferredNodeZero(t *testing.T) {
	c, err := NewCluster(ClusterConfig{Nodes: []NodeAddr{{ID: "n1"}, {ID: "n2"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if n := testing.AllocsPerRun(200, func() {
		_ = c.preferredNode("k")
	}); n != 0 {
		t.Fatalf("preferredNode miss allocs = %v, want 0", n)
	}
	c.learnOwner("k", "n1")
	if n := testing.AllocsPerRun(200, func() {
		_ = c.preferredNode("k")
	}); n != 0 {
		t.Fatalf("preferredNode hit allocs = %v, want 0", n)
	}
}
