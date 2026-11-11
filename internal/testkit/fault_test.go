package testkit

import (
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

func faultPair() (*Stores, *FaultTransport) {
	st := NewStores("n2")
	return st, NewFaultTransport(st, 42)
}

// waitQueued blocks until n sends are parked in the queue. FaultTransport
// only moves queued events, so every Deliver must be gated on the sends
// having arrived — otherwise the test races the sender goroutines.
func waitQueued(t *testing.T, ft *FaultTransport, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for ft.Pending() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d queued sends (have %d)", n, ft.Pending())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFaultDelayNeedsAdvance(t *testing.T) {
	st, ft := faultPair()
	ft.SetDelay(10 * time.Millisecond)
	var wg sync.WaitGroup
	var res protocol.Response
	var rerr error
	done := make(chan struct{})
	wg.Go(func() {
		res, rerr = ft.Send("n2", protocol.Request{
			Opcode: protocol.OpReplicate, Key: []byte("k"), Value: []byte("v"),
			Version: 1, WriterNodeHash: 1,
		})
		close(done)
	})
	// Not yet due: nothing delivered.
	waitQueued(t, ft, 1)
	ft.Deliver()
	select {
	case <-done:
		t.Fatal("delivered before its time")
	default:
	}
	if got := ft.Pending(); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
	ft.Advance(10 * time.Millisecond)
	<-done
	wg.Wait()
	if rerr != nil || res.Status != protocol.StatusOK {
		t.Fatalf("res = %+v, err = %v", res, rerr)
	}
	if got := ft.Delivered(); got != 1 {
		t.Fatalf("delivered = %d, want 1", got)
	}
	if _, ok := st.Stores["n2"].GetVersioned("k"); !ok {
		t.Fatal("peer missing delivered write")
	}
}

func TestFaultLossDropsAtDelivery(t *testing.T) {
	_, ft := faultPair()
	ft.SetLossRate(1.0)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Go(func() {
		_, err := ft.Send("n2", protocol.Request{Opcode: protocol.OpPing})
		errCh <- err
	})
	waitQueued(t, ft, 1)
	ft.Deliver()
	wg.Wait()
	if err := <-errCh; err == nil {
		t.Fatal("lossy send succeeded")
	}
	if got := ft.Dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	if got := ft.Delivered(); got != 0 {
		t.Fatalf("delivered = %d, want 0", got)
	}
}

func TestFaultDownRejectsAtRoute(t *testing.T) {
	_, ft := faultPair()
	ft.SetNodeDown("n2")
	if _, err := ft.Send("n2", protocol.Request{Opcode: protocol.OpPing}); err == nil {
		t.Fatal("send to down node succeeded")
	}
	if got := ft.Pending(); got != 0 {
		t.Fatalf("pending = %d, want 0 (rejected, not queued)", got)
	}
	ft.SetNodeUp("n2")
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Go(func() {
		_, err := ft.Send("n2", protocol.Request{Opcode: protocol.OpPing})
		errCh <- err
	})
	waitQueued(t, ft, 1)
	ft.Deliver()
	wg.Wait()
	if err := <-errCh; err != nil {
		t.Fatalf("send after re-up: %v", err)
	}
}

func TestFaultReorderStillAppliesBoth(t *testing.T) {
	st := NewStores("n2")
	ft := NewFaultTransport(st, 7)
	ft.SetReorder(true)
	var wg sync.WaitGroup
	for i, k := range []string{"a", "b"} {
		wg.Add(1)
		go func(k string, v uint64) {
			defer wg.Done()
			_, _ = ft.Send("n2", protocol.Request{
				Opcode: protocol.OpReplicate, Key: []byte(k), Value: []byte("v"),
				Version: v, WriterNodeHash: 1,
			})
		}(k, uint64(i+1))
	}
	// Both due immediately; reorder swaps the pair. Distinct keys, so
	// both apply regardless of arrival order.
	waitQueued(t, ft, 2)
	ft.Deliver()
	wg.Wait()
	a, oka := st.Stores["n2"].GetVersioned("a")
	b, okb := st.Stores["n2"].GetVersioned("b")
	if !oka || !okb || a.Version != 1 || b.Version != 2 {
		t.Fatalf("peer state: a=%+v b=%+v", a, b)
	}
}

func TestFaultSeededDeterminism(t *testing.T) {
	run := func() (uint64, uint64) {
		st := NewStores("n2")
		ft := NewFaultTransport(st, 99)
		ft.SetLossRate(0.5)
		ft.SetJitter(5 * time.Millisecond)
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				_, _ = ft.Send("n2", protocol.Request{Opcode: protocol.OpPing})
			})
		}
		waitQueued(t, ft, 20)
		ft.Advance(10 * time.Millisecond)
		wg.Wait()
		return ft.Delivered(), ft.Dropped()
	}
	d1, x1 := run()
	d2, x2 := run()
	if d1 != d2 || x1 != x2 || d1+x1 != 20 {
		t.Fatalf("nondeterministic: (%d,%d) vs (%d,%d)", d1, x1, d2, x2)
	}
}

func TestFaultVersionedWriteEndToEnd(t *testing.T) {
	st := NewStores("n2")
	ft := NewFaultTransport(st, 1)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Go(func() {
		_, err := ft.Send("n2", protocol.Request{
			Opcode: protocol.OpReplicate, Key: []byte("k"), Value: []byte("v"),
			Version: 3, WriterNodeHash: 7,
		})
		errCh <- err
	})
	waitQueued(t, ft, 1)
	ft.Deliver()
	wg.Wait()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	e, ok := st.Stores["n2"].GetVersioned("k")
	if !ok || string(e.Value) != "v" || e.Version != 3 || e.WriterNodeHash != 7 {
		t.Fatalf("peer state = %+v %v", e, ok)
	}
}
