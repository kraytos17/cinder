package grpcgw_test

import (
	"context"
	"fmt"
	"testing"

	gen "github.com/kraytos17/cinder/internal/grpcgw/gen"
)

var (
	benchGetResponse       *gen.GetResponse
	benchSetResponse       *gen.SetResponse
	benchDeleteResponse    *gen.DeleteResponse
	benchVersionedResponse *gen.GetVersionedResponse
	benchPingResponse      *gen.PingResponse
	benchCounterAdjust     *gen.CounterAdjustResponse
	benchCounterGet        *gen.CounterGetResponse
	benchMultiGet          *gen.MultiGetResponse
	benchMultiSet          *gen.MultiSetResponse
)

// Gateway benchmarks run RPCs over loopback gRPC against an in-process
// node (same shape as BenchmarkRPCSetGet in net/tcp).
func BenchmarkGatewaySetGet(b *testing.B) {
	c, _ := startGateway(b, "node1")
	ctx := context.Background()
	req := &gen.SetRequest{Key: "bench-key", Value: "v"}
	getReq := &gen.GetRequest{Key: req.Key}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.Set(ctx, req); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(ctx, getReq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGatewayCounterAdjustGet(b *testing.B) {
	c, _ := startGateway(b, "node1")
	ctx := context.Background()
	adjustReq := &gen.CounterAdjustRequest{Key: "bench-c", Delta: 1}
	getReq := &gen.CounterGetRequest{Key: adjustReq.Key}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.CounterAdjust(ctx, adjustReq); err != nil {
			b.Fatal(err)
		}
		if _, err := c.CounterGet(ctx, getReq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGatewayAuthedSetGet(b *testing.B) {
	c := startAuthedGateway(b, "s3cr3t")
	ctx := bearerCtx("s3cr3t")
	setReq := &gen.SetRequest{Key: "bench-key", Value: "v"}
	getReq := &gen.GetRequest{Key: setReq.Key}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.Set(ctx, setReq); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(ctx, getReq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGatewayRPCs(b *testing.B) {
	for _, tc := range []struct {
		name string
		run  func(*testing.B, gen.CinderCacheServiceClient, context.Context)
	}{
		{"ping", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			req := &gen.PingRequest{}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchPingResponse, err = c.Ping(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"get-hit", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			if _, err := c.Set(ctx, &gen.SetRequest{Key: "bench-hit", Value: "value-256-bytes"}); err != nil {
				b.Fatal(err)
			}
			req := &gen.GetRequest{Key: "bench-hit"}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchGetResponse, err = c.Get(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"get-miss", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			req := &gen.GetRequest{Key: "bench-missing"}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchGetResponse, err = c.Get(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"set-overwrite", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			req := &gen.SetRequest{Key: "bench-overwrite", Value: "value-256-bytes"}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchSetResponse, err = c.Set(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"set-ttl-overwrite", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			req := &gen.SetRequest{Key: "bench-ttl", Value: "value-256-bytes", TtlMs: 60_000}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchSetResponse, err = c.Set(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"delete-cycle", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			req := &gen.DeleteRequest{Key: "bench-delete"}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := c.Set(ctx, &gen.SetRequest{Key: req.Key, Value: "v"}); err != nil {
					b.Fatal(err)
				}
				var err error
				benchDeleteResponse, err = c.Delete(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"get-versioned", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			if _, err := c.Set(ctx, &gen.SetRequest{Key: "bench-versioned", Value: "value-256-bytes"}); err != nil {
				b.Fatal(err)
			}
			req := &gen.GetVersionedRequest{Key: "bench-versioned"}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchVersionedResponse, err = c.GetVersioned(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"counter-adjust", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			req := &gen.CounterAdjustRequest{Key: "bench-counter", Delta: 1}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchCounterAdjust, err = c.CounterAdjust(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
		{"counter-get", func(b *testing.B, c gen.CinderCacheServiceClient, ctx context.Context) {
			if _, err := c.CounterAdjust(ctx, &gen.CounterAdjustRequest{Key: "bench-counter-get", Delta: 1}); err != nil {
				b.Fatal(err)
			}
			req := &gen.CounterGetRequest{Key: "bench-counter-get"}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchCounterGet, err = c.CounterGet(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c, _ := startGateway(b, "node1")
			tc.run(b, c, context.Background())
		})
	}
}

func BenchmarkGatewayBatch(b *testing.B) {
	for _, batchSize := range []int{1, 16, 128, 512} {
		b.Run(fmt.Sprintf("size=%d/multiget-hit", batchSize), func(b *testing.B) {
			c, _ := startGateway(b, "node1")
			ctx := context.Background()
			keys := make([]string, batchSize)
			for i := range keys {
				keys[i] = fmt.Sprintf("multi-%04d", i)
				if _, err := c.Set(ctx, &gen.SetRequest{Key: keys[i], Value: "v"}); err != nil {
					b.Fatal(err)
				}
			}
			req := &gen.MultiGetRequest{Keys: keys}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				benchMultiGet, err = c.MultiGet(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("size=%d/multiget-miss", batchSize), func(b *testing.B) {
			c, _ := startGateway(b, "node1")
			ctx := context.Background()
			keys := make([]string, batchSize)
			for i := range keys {
				keys[i] = fmt.Sprintf("missing-%04d", i)
			}
			req := &gen.MultiGetRequest{Keys: keys}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				benchMultiGet, err = c.MultiGet(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("size=%d/multiset", batchSize), func(b *testing.B) {
			c, _ := startGateway(b, "node1")
			ctx := context.Background()
			requests := make([]*gen.SetRequest, batchSize)
			for i := range requests {
				requests[i] = &gen.SetRequest{Key: fmt.Sprintf("multi-set-%04d", i), Value: "v"}
			}
			req := &gen.MultiSetRequest{Requests: requests}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				benchMultiSet, err = c.MultiSet(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGatewaySetValueSize measures unary Set across payload sizes. Input
// messages and payloads are built before timing; bytes/op and allocations
// include only the RPC/gateway/server path.
func BenchmarkGatewaySetValueSize(b *testing.B) {
	for _, size := range []int{0, 16, 256, 4096, 64 << 10} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			c, _ := startGateway(b, "node1")
			ctx := context.Background()
			req := &gen.SetRequest{Key: "bench-sized", Value: string(make([]byte, size))}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				benchSetResponse, err = c.Set(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGatewayPingParallel(b *testing.B) {
	c, _ := startGateway(b, "node1")
	ctx := context.Background()
	req := &gen.PingRequest{}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.Ping(ctx, req); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGatewayTLSAndAuth(b *testing.B) {
	for _, secret := range []string{"", "s3cr3t"} {
		name := "tls-open"
		ctx := context.Background()
		if secret != "" {
			name = "tls-bearer"
			ctx = bearerCtx(secret)
		}
		b.Run(name, func(b *testing.B) {
			c := startTLSGateway(b, secret) //nolint:contextcheck // benchmark helper owns gateway and server lifecycle
			req := &gen.PingRequest{}
			b.ReportAllocs()
			for b.Loop() {
				var err error
				benchPingResponse, err = c.Ping(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
