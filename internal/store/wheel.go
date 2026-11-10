package store

import (
	"container/heap"
)

// Wheel schedules expirations with 256 short-term slots and a min-heap
// for longer deadlines. Tick drains due slot IDs before invoking callbacks,
// so callbacks may safely schedule work for a later tick. Remove eagerly
// deletes heap entries, so the heap does not retain canceled IDs.
//
// Scheduling is by opaque integer id (the store's node index). All linkage
// is index-addressed: no pointers, nothing for the GC to scan.
type Wheel struct {
	slots  [wheelSlots]map[int]struct{}
	cursor int
	ticks  uint64
	heap   []wheelItem
	pos    map[int]int // id -> heap index, for O(1) removal
	batch  []int       // reused drain buffer
}

const wheelSlots = 256

type wheelItem struct {
	fireAt uint64
	id     int
}

// NewWheel returns an empty wheel.
func NewWheel() *Wheel {
	return &Wheel{pos: make(map[int]int)}
}

// Insert schedules id to fire after ttlTicks ticks, replacing any existing
// schedule for it.
func (w *Wheel) Insert(id int, ttlTicks uint64) {
	w.Remove(id)
	if ttlTicks <= wheelSlots {
		slot := (uint64(w.cursor) + ttlTicks) % wheelSlots
		m := w.slots[slot]
		if m == nil {
			m = make(map[int]struct{})
			w.slots[slot] = m
		}
		m[id] = struct{}{}
		return
	}
	heap.Push(w, wheelItem{fireAt: w.ticks + ttlTicks, id: id})
}

// Remove cancels any schedule for id.
func (w *Wheel) Remove(id int) {
	for s := range w.slots {
		if m := w.slots[s]; m != nil {
			if _, ok := m[id]; ok {
				delete(m, id)
				if len(m) == 0 {
					w.slots[s] = nil
				}
				break
			}
		}
	}
	if idx, ok := w.pos[id]; ok {
		heap.Remove(w, idx)
	}
}

// Tick advances one tick and fires onExpired for every due id. Slot ids are
// drained before firing so a callback that re-inserts (even into the same
// slot) lands in the wheel for a future tick and termination is guaranteed.
func (w *Wheel) Tick(onExpired func(id int)) {
	w.cursor = (w.cursor + 1) % wheelSlots
	w.ticks++
	m := w.slots[w.cursor]
	w.slots[w.cursor] = nil
	w.batch = w.batch[:0]
	for id := range m {
		w.batch = append(w.batch, id)
	}
	for _, id := range w.batch {
		onExpired(id)
	}
	for len(w.heap) > 0 && w.heap[0].fireAt <= w.ticks {
		item, ok := heap.Pop(w).(wheelItem)
		if !ok {
			panic("store: heap held a non-wheelItem")
		}
		onExpired(item.id)
	}
}

// TickCount returns the number of ticks elapsed.
func (w *Wheel) TickCount() uint64 { return w.ticks }

// Push implements heap.Interface (exported only for container/heap).
func (w *Wheel) Push(x any) {
	it, ok := x.(wheelItem)
	if !ok {
		panic("store: Push called with non-wheelItem")
	}
	w.pos[it.id] = len(w.heap)
	w.heap = append(w.heap, it)
}

// Pop implements heap.Interface (exported only for container/heap).
func (w *Wheel) Pop() any {
	n := len(w.heap)
	it := w.heap[n-1]
	w.heap = w.heap[:n-1]
	delete(w.pos, it.id)
	return it
}

// Swap implements heap.Interface and tracks positions.
func (w *Wheel) Swap(i, j int) {
	w.heap[i], w.heap[j] = w.heap[j], w.heap[i]
	w.pos[w.heap[i].id] = i
	w.pos[w.heap[j].id] = j
}

// Len implements heap.Interface.
func (w *Wheel) Len() int { return len(w.heap) }

// Less implements heap.Interface.
func (w *Wheel) Less(i, j int) bool { return w.heap[i].fireAt < w.heap[j].fireAt }
