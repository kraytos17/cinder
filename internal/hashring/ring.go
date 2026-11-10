// Package hashring implements a consistent-hash ring with virtual nodes.
// XXH3 hashes keys and vnode identities. Ring reads use immutable snapshots;
// mutations serialize and publish new snapshots atomically. Per-node load
// counters are shared between snapshots so in-flight counts survive
// membership changes.
package hashring

import (
	"cmp"
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

const (
	// MaxPhysicalNodes caps membership: node indices ride uint16 slots.
	MaxPhysicalNodes = 65_536
	// MaxReplicas bounds GetNodes fan-out.
	MaxReplicas = 32
	// DefaultVNodes is the default virtual-node count and is reported by the
	// admin ring response.
	DefaultVNodes = 150
)

// ReplicaCount is the number of distinct ring owners requested for a key.
// Valid values are in [1, MaxReplicas].
type ReplicaCount int

// NewReplicaCount validates and returns a replica count supported by Ring.
func NewReplicaCount(n int) (ReplicaCount, error) {
	if n < 1 || n > MaxReplicas {
		return 0, ErrInvalidReplicaCount
	}
	return ReplicaCount(n), nil
}

// ErrInvalidReplicaCount rejects GetNodes counts outside [1, 32].
var ErrInvalidReplicaCount = errors.New("hashring: replica count must be between 1 and 32")

// ErrTooManyNodes rejects memberships the uint16 index cannot address.
var ErrTooManyNodes = errors.New("hashring: consistent hash ring exceeds node index capacity")

type snapshot struct {
	hashes []uint64
	nodeIx []uint16 // parallel to hashes: index into nodes
	nodes  []string
	loads  []*atomic.Uint64 // parallel to nodes, shared across snapshots
}

// Ring is a consistent-hash ring. The zero value is unusable; use New.
type Ring struct {
	vnodes     int
	loadFactor float64
	snap       atomic.Pointer[snapshot]
	mu         sync.Mutex // mutators only
}

// New returns an empty ring. vnodes must be positive; loadFactor must be
// finite and non-negative (0 disables bounded-load spillover).
func New(vnodes int, loadFactor float64) (*Ring, error) {
	if vnodes <= 0 {
		return nil, errors.New("hashring: vnodes_per_node must be positive")
	}
	if math.IsNaN(loadFactor) || math.IsInf(loadFactor, 0) || loadFactor < 0 {
		return nil, errors.New("hashring: bounded_load_factor must be finite and non-negative")
	}
	r := &Ring{vnodes: vnodes, loadFactor: loadFactor}
	r.snap.Store(&snapshot{})
	return r, nil
}

func hashVnode(node string, index int) uint64 {
	return xxh3.HashString(node + "-" + strconv.Itoa(index))
}

func hashKey(key string) uint64 {
	return xxh3.HashString(key)
}

func (r *Ring) cloneForWrite() *snapshot {
	old := r.snap.Load()
	out := &snapshot{
		hashes: append([]uint64(nil), old.hashes...),
		nodeIx: append([]uint16(nil), old.nodeIx...),
		nodes:  append([]string(nil), old.nodes...),
		loads:  append([]*atomic.Uint64(nil), old.loads...),
	}
	return out
}

// AddNode inserts id with a full vnode set; duplicates are ignored.
func (r *Ring) AddNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		old := r.snap.Load()
		if slices.Contains(old.nodes, id) {
			return
		}
		if len(old.nodes) >= MaxPhysicalNodes {
			return
		}
		snap := r.cloneForWrite()
		phys := uint16(len(snap.nodes))
		snap.nodes = append(snap.nodes, id)
		snap.loads = append(snap.loads, &atomic.Uint64{})
		for i := range r.vnodes {
			snap.hashes = append(snap.hashes, hashVnode(id, i))
			snap.nodeIx = append(snap.nodeIx, phys)
		}
		sortRing(snap)
		if r.snap.CompareAndSwap(old, snap) {
			return
		}
	}
}

// RemoveNode drops id and all its vnodes; unknown ids are ignored.
func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		old := r.snap.Load()
		phys := -1
		for i, n := range old.nodes {
			if n == id {
				phys = i
				break
			}
		}
		if phys < 0 {
			return
		}
		snap := &snapshot{}
		for i, n := range old.nodes {
			if i != phys {
				snap.nodes = append(snap.nodes, n)
				snap.loads = append(snap.loads, old.loads[i])
			}
		}
		remap := make([]uint16, len(old.nodes))
		next := uint16(0)
		for i := range old.nodes {
			if i == phys {
				remap[i] = 0
				continue
			}
			remap[i] = next
			next++
		}
		for i, h := range old.hashes {
			if int(old.nodeIx[i]) == phys {
				continue
			}
			snap.hashes = append(snap.hashes, h)
			snap.nodeIx = append(snap.nodeIx, remap[old.nodeIx[i]])
		}
		// Removal preserves the relative vnode order, so no re-sort needed.
		if r.snap.CompareAndSwap(old, snap) {
			return
		}
	}
}

// SetNodes replaces membership in one rebuild. Survivors keep their order
// and load cells; genuinely new ids append in first-occurrence input
// order; duplicates collapse. An identical set is a no-op.
func (r *Ring) SetNodes(ids []string) error {
	dedup := ids[:0:0]
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		dedup = append(dedup, id)
	}
	if len(dedup) > MaxPhysicalNodes {
		return ErrTooManyNodes
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		old := r.snap.Load()
		if sameSet(old.nodes, dedup) {
			return nil
		}
		snap := &snapshot{}
		for _, id := range dedup {
			if idx := indexOf(old.nodes, id); idx >= 0 {
				snap.nodes = append(snap.nodes, id)
				snap.loads = append(snap.loads, old.loads[idx])
			} else {
				snap.nodes = append(snap.nodes, id)
				snap.loads = append(snap.loads, &atomic.Uint64{})
			}
		}
		for pi, id := range snap.nodes {
			for i := range r.vnodes {
				snap.hashes = append(snap.hashes, hashVnode(id, i))
				snap.nodeIx = append(snap.nodeIx, uint16(pi))
			}
		}
		sortRing(snap)
		if r.snap.CompareAndSwap(old, snap) {
			return nil
		}
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if indexOf(b, x) < 0 {
			return false
		}
	}
	return true
}

func indexOf(nodes []string, id string) int {
	for i, n := range nodes {
		if n == id {
			return i
		}
	}
	return -1
}

func sortRing(snap *snapshot) {
	order := make([]int, len(snap.hashes))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return cmp.Compare(snap.hashes[a], snap.hashes[b]) })
	hashes := make([]uint64, len(order))
	ix := make([]uint16, len(order))
	for i, o := range order {
		hashes[i] = snap.hashes[o]
		ix[i] = snap.nodeIx[o]
	}
	snap.hashes, snap.nodeIx = hashes, ix
}

// GetNode returns the owner of key, or "" on an empty ring.
func (r *Ring) GetNode(key string) string {
	snap := r.snap.Load()
	if len(snap.hashes) == 0 {
		return ""
	}
	h := hashKey(key)
	idx, _ := slices.BinarySearch(snap.hashes, h)
	if idx == len(snap.hashes) {
		idx = 0
	}
	return snap.nodes[snap.nodeIx[idx]]
}

// GetNodes returns up to n distinct owners walking clockwise from key's
// position, wrapping at most once. It returns fewer than n when the ring
// contains fewer nodes.
func (r *Ring) GetNodes(key string, n ReplicaCount) ([]string, error) {
	if n <= 0 || n > MaxReplicas {
		return nil, ErrInvalidReplicaCount
	}
	snap := r.snap.Load()
	if len(snap.hashes) == 0 {
		return nil, nil
	}
	h := hashKey(key)
	start, _ := slices.BinarySearch(snap.hashes, h)
	if start == len(snap.hashes) {
		start = 0
	}
	var seen [MaxReplicas]uint16
	var out []string
	cur := start
	for len(out) < int(n) {
		ix := snap.nodeIx[cur]
		dup := slices.Contains(seen[:len(out)], ix)
		if !dup {
			seen[len(out)] = ix
			out = append(out, snap.nodes[ix])
		}
		cur++
		if cur == len(snap.hashes) {
			cur = 0
		}
		if cur == start {
			break
		}
	}
	return out, nil
}

// GetNodeBounded spills to the next ring position when the preferred node
// is overloaded (in-flight load above ceil(avg * factor)), wrapping at
// most once. A non-positive factor (or a single node) behaves like GetNode.
func (r *Ring) GetNodeBounded(key string) string {
	if r.loadFactor <= 0 {
		return r.GetNode(key)
	}
	snap := r.snap.Load()
	if len(snap.hashes) == 0 {
		return ""
	}
	h := hashKey(key)
	idx, _ := slices.BinarySearch(snap.hashes, h)
	if idx == len(snap.hashes) {
		idx = 0
	}
	if len(snap.nodes) <= 1 {
		return snap.nodes[snap.nodeIx[idx]]
	}
	var total uint64
	for _, c := range snap.loads {
		total += c.Load()
	}
	avg := float64(total) / float64(len(snap.nodes))
	maxLoad := uint64(math.Ceil(avg * r.loadFactor))
	start := idx
	for {
		phys := snap.nodeIx[idx]
		if snap.loads[phys].Load() < maxLoad {
			return snap.nodes[phys]
		}
		idx++
		if idx == len(snap.hashes) {
			idx = 0
		}
		if idx == start {
			return snap.nodes[snap.nodeIx[start]]
		}
	}
}

// IncrementLoad records one in-flight request against node. Unknown nodes
// are ignored.
func (r *Ring) IncrementLoad(node string) {
	snap := r.snap.Load()
	if idx := indexOf(snap.nodes, node); idx >= 0 {
		snap.loads[idx].Add(1)
	}
}

// DecrementLoad releases one in-flight slot; unknown nodes are ignored.
func (r *Ring) DecrementLoad(node string) {
	snap := r.snap.Load()
	if idx := indexOf(snap.nodes, node); idx >= 0 {
		snap.loads[idx].Add(^uint64(0))
	}
}

// CurrentLoad returns node's in-flight count, or 0 when unknown.
func (r *Ring) CurrentLoad(node string) uint64 {
	snap := r.snap.Load()
	if idx := indexOf(snap.nodes, node); idx >= 0 {
		return snap.loads[idx].Load()
	}
	return 0
}

// TotalLoad sums every node's in-flight count.
func (r *Ring) TotalLoad() uint64 {
	var total uint64
	for _, c := range r.snap.Load().loads {
		total += c.Load()
	}
	return total
}

// LoadFactor returns total in-flight load over node count (0 when empty).
func (r *Ring) LoadFactor() float64 {
	snap := r.snap.Load()
	if len(snap.loads) == 0 {
		return 0
	}
	var total uint64
	for _, c := range snap.loads {
		total += c.Load()
	}
	return float64(total) / float64(len(snap.loads))
}

// Nodes returns the physical membership in ring order.
func (r *Ring) Nodes() []string {
	return append([]string(nil), r.snap.Load().nodes...)
}
