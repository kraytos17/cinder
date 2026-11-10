package tcp_test

import (
	"encoding/json"
	"testing"

	"github.com/kraytos17/cinder/internal/netport"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

func TestAdminInfoReportsEvictionPolicy(t *testing.T) {
	for _, tc := range []struct {
		name string
		st   *store.Store
		want string
	}{
		{"lru", store.NewLRUStore(1<<20, nil), "lru"},
		{"lfu", store.NewLFUStore(1<<20, nil), "lfu"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Port: netport.Port(0), Store: tc.st})
			res := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminInfo})
			if res.Status != protocol.StatusOK {
				t.Fatalf("status = %v", res.Status)
			}
			var info struct {
				EvictionPolicy string `json:"eviction_policy"`
			}
			if err := json.Unmarshal(res.Value, &info); err != nil {
				t.Fatal(err)
			}
			if info.EvictionPolicy != tc.want {
				t.Fatalf("eviction_policy=%q, want %q", info.EvictionPolicy, tc.want)
			}
		})
	}
}
