package obs

import (
	"sync/atomic"
	"testing"
)

func TestIfPresentAppliesWhenInstalled(t *testing.T) {
	var p atomic.Pointer[int]
	var got *int
	IfPresent(&p, func(v *int) { got = v })
	if got != nil {
		t.Fatal("unset pointer must not apply")
	}
	v := 7
	p.Store(&v)
	IfPresent(&p, func(w *int) { *w = 42 })
	if v != 42 {
		t.Fatalf("got %d", v)
	}
}

func TestRetryMaxKeepsMaximum(t *testing.T) {
	var a atomic.Uint64
	RetryMax(a.Load, a.CompareAndSwap, 5)
	RetryMax(a.Load, a.CompareAndSwap, 3)
	if got := a.Load(); got != 5 {
		t.Fatalf("got %d", got)
	}
	RetryMax(a.Load, a.CompareAndSwap, 9)
	if got := a.Load(); got != 9 {
		t.Fatalf("got %d", got)
	}
}

func TestRetryMaxConcurrent(t *testing.T) {
	var a atomic.Uint64
	done := make(chan struct{})
	for i := uint64(1); i <= 8; i++ {
		go func(v uint64) {
			defer func() { done <- struct{}{} }()
			for j := range uint64(100) {
				RetryMax(a.Load, a.CompareAndSwap, v*100+j)
			}
		}(i)
	}
	for range 8 {
		<-done
	}
	if got := a.Load(); got != 899 {
		t.Fatalf("got %d", got)
	}
}
