package tcp

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

func testReloadServer(t *testing.T) *Server {
	t.Helper()
	srv := NewServer(ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestReloadAppliesAllFields(t *testing.T) {
	srv := testReloadServer(t)
	applied, err := srv.Reload(ReloadableConfig{
		LogLevel:         "debug",
		GossipIntervalMs: 200,
		PingIntervalMs:   200,
		SuspectTimeoutMs: 600,
		AEIntervalMs:     5000,
		Capacity:         1 << 21,
		TombstoneGCMs:    60000,
		MaxHintBytes:     1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != len(reloadableKeys) {
		t.Fatalf("applied = %v, want all %d reloadable keys", applied, len(reloadableKeys))
	}
	srv.ringMu.RLock()
	defer srv.ringMu.RUnlock()
	if srv.gossipInterval != 200*time.Millisecond || srv.probeInterval != 200*time.Millisecond || srv.suspectTimeout != 600*time.Millisecond {
		t.Fatalf("intervals not applied: %+v", srv)
	}
	srv.aeMu.RLock()
	defer srv.aeMu.RUnlock()
	if srv.aeInterval != 5*time.Second {
		t.Fatalf("ae interval = %v", srv.aeInterval)
	}
	if got := srv.store.Capacity(); got != 1<<21 {
		t.Fatalf("capacity = %d", got)
	}
}

func TestReloadValidationLeavesStateUntouched(t *testing.T) {
	srv := testReloadServer(t)
	before := srv.gossipInterval
	for _, rc := range []ReloadableConfig{
		{LogLevel: "nope", Capacity: 1 << 20},
		{LogLevel: "info", GossipIntervalMs: -1, Capacity: 1 << 20},
		{LogLevel: "info", Capacity: 0},
		{LogLevel: "info", Capacity: 1 << 20, TLSCertFile: "/nonexistent/cert.pem"},
		{LogLevel: "info", Capacity: 1 << 20, TLSCertFile: "only-cert-no-key"},
	} {
		if _, err := srv.Reload(rc); err == nil {
			t.Fatalf("Reload(%+v) succeeded, want error", rc)
		}
	}
	srv.ringMu.RLock()
	defer srv.ringMu.RUnlock()
	if srv.gossipInterval != before {
		t.Fatal("failed reload mutated server state")
	}
}
