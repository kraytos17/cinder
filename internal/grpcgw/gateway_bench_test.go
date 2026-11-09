package grpcgw_test

import (
	"context"
	"testing"

	gen "github.com/kraytos17/cinder/internal/grpcgw/gen"
)

// Gateway benchmarks run RPCs over loopback gRPC against an in-process
// node (same shape as BenchmarkRPCSetGet in net/tcp).
func BenchmarkGatewaySetGet(b *testing.B) {
	c, _ := startGateway(b, "node1")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := "bench-key"
		if _, err := c.Set(ctx, &gen.SetRequest{Key: key, Value: "v"}); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(ctx, &gen.GetRequest{Key: key}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGatewayCounterAdjustGet(b *testing.B) {
	c, _ := startGateway(b, "node1")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := c.CounterAdjust(ctx, &gen.CounterAdjustRequest{Key: "bench-c", Delta: 1}); err != nil {
			b.Fatal(err)
		}
		if _, err := c.CounterGet(ctx, &gen.CounterGetRequest{Key: "bench-c"}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGatewayAuthedSetGet(b *testing.B) {
	c := startAuthedGateway(b, "s3cr3t")
	ctx := bearerCtx("s3cr3t")
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := c.Set(ctx, &gen.SetRequest{Key: "bench-key", Value: "v"}); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(ctx, &gen.GetRequest{Key: "bench-key"}); err != nil {
			b.Fatal(err)
		}
	}
}
