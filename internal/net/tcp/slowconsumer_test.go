package tcp

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

func TestSlowConsumerDropsOnWriteTimeout(t *testing.T) {
	st := store.NewLRUStore(1<<26, nil)
	big := make([]byte, 1<<20)
	if err := st.Put("big", big); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(ServerConfig{NodeID: "n1", Store: st})
	srv.SetWriteTimeout(100 * time.Millisecond)

	cc, sc := net.Pipe()
	srv.sem <- struct{}{}
	srv.wg.Add(1)
	done := make(chan struct{})
	go func() { defer close(done); srv.handle(sc) }()

	frame, err := protocol.EncodeRequest(protocol.Request{Opcode: protocol.OpGet, Key: []byte("big")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Write(frame); err != nil {
		t.Fatal(err)
	}
	// Never read the 1 MiB reply: the server's write must stall on the
	// rendezvous pipe until the write deadline fires and closes us.
	// (Reading even one byte would unblock the server, so observe the
	// close instead of the bytes.)
	start := time.Now()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler goroutine leaked past the write timeout")
	}
	elapsed := time.Since(start)
	if _, err := io.ReadAtLeast(cc, make([]byte, 1), 1); err == nil {
		t.Fatal("expected EOF on the dropped connection")
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("dropped after %v, before the write deadline", elapsed)
	}
	if got := srv.met.SlowConsumerDrops.Load(); got != 1 {
		t.Fatalf("slow drops = %d, want 1", got)
	}
	if got := srv.met.WriteFailures.Load(); got != 0 {
		t.Fatalf("write failures = %d, want 0 (drop, not failure)", got)
	}
	cc.Close()
}

// A healthy connection is unaffected by the split: reads keep the idle
// budget, writes succeed immediately.
func TestHealthyConnectionUnaffectedByWriteTimeout(t *testing.T) {
	st := store.NewLRUStore(1<<20, nil)
	if err := st.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(ServerConfig{NodeID: "n1", Store: st})
	srv.SetWriteTimeout(100 * time.Millisecond)

	cc, sc := net.Pipe()
	srv.sem <- struct{}{}
	srv.wg.Add(1)
	done := make(chan struct{})
	go func() { defer close(done); srv.handle(sc) }()
	defer func() {
		cc.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not exit after client close")
		}
	}()

	frame, err := protocol.EncodeRequest(protocol.Request{Opcode: protocol.OpGet, Key: []byte("k")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Write(frame); err != nil {
		t.Fatal(err)
	}
	var header [protocol.FrameHeaderSize]byte
	if _, err := io.ReadFull(cc, header[:]); err != nil {
		t.Fatal(err)
	}
	if got := srv.met.SlowConsumerDrops.Load(); got != 0 {
		t.Fatalf("healthy conn counted as slow: %d", got)
	}
}
