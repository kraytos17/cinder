package tcp_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

// BenchmarkRPCSetGet measures sequential Set+Get round trips to one server
// over loopback TCP.
func BenchmarkRPCSetGet(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<26, nil)})
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

// BenchmarkRPCPing isolates one minimal request/response round trip over
// loopback TCP: framing, connection read/write, dispatch, and scheduling.
func BenchmarkRPCPing(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<26, nil)})
	go srv.Serve(l)
	defer srv.Shutdown()
	c, err := client.Dial(l.Addr().String(), 5*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := c.Ping()
		if err != nil || res.Status != protocol.StatusOK {
			b.Fatalf("ping: %+v %v", res, err)
		}
	}
}

// BenchmarkRPCSetOnly measures one 256 B Set round trip, without a paired Get.
func BenchmarkRPCSetOnly(b *testing.B) {
	l, c, stop := benchmarkServerClient(b)
	_ = l
	defer stop()
	key, value := []byte("set-only-key"), make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: key, Value: value})
		if err != nil || res.Status != protocol.StatusOK {
			b.Fatalf("set: %+v %v", res, err)
		}
	}
}

// BenchmarkRPCGetOnly measures one 256 B Get round trip from a resident key.
func BenchmarkRPCGetOnly(b *testing.B) {
	_, c, stop := benchmarkServerClient(b)
	defer stop()
	key := []byte("get-only-key")
	if res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: key, Value: make([]byte, 256)}); err != nil || res.Status != protocol.StatusOK {
		b.Fatalf("seed set: %+v %v", res, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := c.Do(protocol.Request{Opcode: protocol.OpGet, Key: key})
		if err != nil || res.Status != protocol.StatusOK {
			b.Fatalf("get: %+v %v", res, err)
		}
	}
}

// BenchmarkDispatchSet bypasses TCP framing and syscalls, isolating the
// server's in-process dispatch plus store mutation cost.
func BenchmarkDispatchSet(b *testing.B) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<26, nil)})
	key, value := []byte("dispatch-set-key"), make([]byte, 256)
	b.ReportAllocs()
	for b.Loop() {
		res := srv.Dispatch(protocol.Request{Opcode: protocol.OpSet, Key: key, Value: value})
		if res.Status != protocol.StatusOK {
			b.Fatalf("set: %+v", res)
		}
	}
}

// BenchmarkDispatchGet bypasses TCP framing and syscalls, isolating the
// server's in-process dispatch plus store read/copy cost.
func BenchmarkDispatchGet(b *testing.B) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<26, nil)})
	key := []byte("dispatch-get-key")
	if res := srv.Dispatch(protocol.Request{Opcode: protocol.OpSet, Key: key, Value: make([]byte, 256)}); res.Status != protocol.StatusOK {
		b.Fatalf("seed set: %+v", res)
	}
	b.ReportAllocs()
	for b.Loop() {
		res := srv.Dispatch(protocol.Request{Opcode: protocol.OpGet, Key: key})
		if res.Status != protocol.StatusOK {
			b.Fatalf("get: %+v", res)
		}
	}
}

func benchmarkServerClient(b *testing.B) (net.Listener, *client.Client, func()) {
	b.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<26, nil)})
	go srv.Serve(l)
	c, err := client.Dial(l.Addr().String(), 5*time.Second)
	if err != nil {
		srv.Shutdown()
		b.Fatal(err)
	}
	return l, c, func() {
		c.Close()
		srv.Shutdown()
	}
}

// BenchmarkRPCSetParallel saturates the server with parallel clients.
func BenchmarkRPCSetParallel(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<26, nil)})
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

// BenchmarkRPCWorkers measures how independent client connections scale for
// minimal Ping and store Set/Get round trips. Each worker has its own Client
// and key, separating server concurrency from client mutex and hot-key costs.
func BenchmarkRPCWorkers(b *testing.B) {
	for _, op := range []string{"ping", "set", "get"} {
		for _, workers := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("%s/workers=%d", op, workers), func(b *testing.B) {
				b.SetParallelism(1)
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				srv := tcp.NewServer(tcp.ServerConfig{
					NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port),
					Store: store.NewLRUStore(1<<26, nil),
				})
				go srv.Serve(l)
				defer srv.Shutdown()

				clients := make([]*client.Client, workers)
				keys := make([][]byte, workers)
				value := make([]byte, 256)
				for i := range workers {
					clients[i], err = client.Dial(l.Addr().String(), 5*time.Second)
					if err != nil {
						b.Fatal(err)
					}
					defer clients[i].Close()
					keys[i] = fmt.Appendf(nil, "workers-%s-%d", op, i)
					if op == "get" {
						res, err := clients[i].Do(protocol.Request{Opcode: protocol.OpSet, Key: keys[i], Value: value})
						if err != nil || res.Status != protocol.StatusOK {
							b.Fatalf("seed Get worker %d: %+v %v", i, res, err)
						}
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				start := time.Now()
				var wg sync.WaitGroup
				errCh := make(chan error, workers)
				for worker := range workers {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						for i := worker; i < b.N; i += workers {
							var res protocol.Response
							var err error
							switch op {
							case "ping":
								res, err = clients[worker].Ping()
							case "set":
								res, err = clients[worker].Do(protocol.Request{Opcode: protocol.OpSet, Key: keys[worker], Value: value})
							case "get":
								res, err = clients[worker].Do(protocol.Request{Opcode: protocol.OpGet, Key: keys[worker]})
							}
							if err != nil {
								errCh <- err
								return
							}
							if res.Status != protocol.StatusOK {
								errCh <- fmt.Errorf("%s status=%s", op, res.Status)
								return
							}
						}
					}(worker)
				}
				wg.Wait()
				b.StopTimer()
				b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "ops/s")
				close(errCh)
				for err := range errCh {
					b.Error(err)
				}
			})
		}
	}
}

// BenchmarkDispatchWorkers is the no-socket control for RPCWorkers. It uses
// one key per worker to isolate dispatch/store parallelism from same-key lock
// contention.
func BenchmarkDispatchWorkers(b *testing.B) {
	for _, op := range []string{"set", "get"} {
		for _, workers := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("%s/workers=%d", op, workers), func(b *testing.B) {
				srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<26, nil)})
				keys := make([][]byte, workers)
				value := make([]byte, 256)
				for worker := range workers {
					keys[worker] = fmt.Appendf(nil, "dispatch-workers-%s-%d", op, worker)
					if op == "get" {
						res := srv.Dispatch(protocol.Request{Opcode: protocol.OpSet, Key: keys[worker], Value: value})
						if res.Status != protocol.StatusOK {
							b.Fatalf("seed Get worker %d: %+v", worker, res)
						}
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				start := time.Now()
				var wg sync.WaitGroup
				errCh := make(chan error, workers)
				for worker := range workers {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						opcode := protocol.OpSet
						if op == "get" {
							opcode = protocol.OpGet
						}
						for i := worker; i < b.N; i += workers {
							res := srv.Dispatch(protocol.Request{Opcode: opcode, Key: keys[worker], Value: value})
							if res.Status != protocol.StatusOK {
								errCh <- fmt.Errorf("%s status=%s", op, res.Status)
								return
							}
						}
					}(worker)
				}
				wg.Wait()
				b.StopTimer()
				b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "ops/s")
				close(errCh)
				for err := range errCh {
					b.Error(err)
				}
			})
		}
	}
}
