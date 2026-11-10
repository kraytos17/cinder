package tcp_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

// BenchmarkRPCSetGet measures sequential Set+Get round trips to one server
// over loopback TCP.
func BenchmarkRPCSetGet(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := tcp.NewServer("node1", l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<26, nil), nil)
	go srv.Serve(l)
	defer srv.Shutdown()
	c, err := client.Dial(l.Addr().String(), 5*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	val := make([]byte, 256)
	b.SetBytes(int64(256 * 2))
	// Prebuilt keys: formatting is harness overhead, not transport cost.
	benchKeys := make([][]byte, 1024)
	for i := range benchKeys {
		benchKeys[i] = fmt.Appendf(nil, "tput-key-%d", i)
	}

	for i := 0; b.Loop(); i++ {
		k := benchKeys[i%len(benchKeys)]
		if res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: k, Value: val}); err != nil || res.Status != protocol.StatusOK {
			b.Fatalf("set: %+v %v", res, err)
		}
		if res, err := c.Do(protocol.Request{Opcode: protocol.OpGet, Key: k}); err != nil || res.Status != protocol.StatusOK {
			b.Fatalf("get: %+v %v", res, err)
		}
	}
}

// BenchmarkRPCSetParallel saturates the server with parallel clients.
func BenchmarkRPCSetParallel(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := tcp.NewServer("node1", l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<26, nil), nil)
	go srv.Serve(l)
	defer srv.Shutdown()
	val := make([]byte, 256)
	b.SetBytes(256)
	benchPKeys := make([][]byte, 4096)
	for i := range benchPKeys {
		benchPKeys[i] = fmt.Appendf(nil, "tput-p-%d", i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := client.Dial(l.Addr().String(), 5*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		defer c.Close()
		i := 0
		for pb.Next() {
			i++
			k := benchPKeys[i%len(benchPKeys)]
			if res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: k, Value: val}); err != nil || res.Status != protocol.StatusOK {
				b.Fatalf("set: %+v %v", res, err)
			}
		}
	})
}
