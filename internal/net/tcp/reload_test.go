package tcp

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/auth"
	"github.com/kraytos17/cinder/internal/net/protocol"
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
		MaxConnections:   10000,
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
		{
			LogLevel: "nope", Capacity: 1 << 20,
			MaxConnections: 10000,
		},
		{
			LogLevel: "info", GossipIntervalMs: -1, Capacity: 1 << 20,
			MaxConnections: 10000,
		},
		{LogLevel: "info", Capacity: 0, MaxConnections: 10000},
		{
			LogLevel: "info", Capacity: 1 << 20,
			MaxConnections: 10000, TLSCertFile: "/nonexistent/cert.pem",
		},
		{
			LogLevel: "info", Capacity: 1 << 20,
			MaxConnections: 10000, TLSCertFile: "only-cert-no-key",
		},
		{
			LogLevel: "info", Capacity: 1 << 20,
			MaxConnections: 10000, PingIntervalMs: 500, SuspectTimeoutMs: 500,
		},
		{
			LogLevel: "info", Capacity: 1 << 20,
			MaxConnections: 10000, PingIntervalMs: 500, SuspectTimeoutMs: 200,
		},
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

func TestReloadRotatesSecrets(t *testing.T) {
	newTLS := func(t *testing.T) *Server {
		t.Helper()
		srv := NewServer(ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
		if err := srv.ConfigureTLS("../../../tests/fixtures/server.pem", "../../../tests/fixtures/server-key.pem", ""); err != nil {
			t.Fatal(err)
		}
		return srv
	}
	base := func() ReloadableConfig {
		return ReloadableConfig{
			LogLevel: "info", GossipIntervalMs: 1000, PingIntervalMs: 1000,
			SuspectTimeoutMs: 3000, AEIntervalMs: 30000, Capacity: 1 << 20,
			MaxConnections: 10000,
			TombstoneGCMs:  60000, MaxHintBytes: 1024,
		}
	}
	// Secrets require TLS: enabling them on a plaintext server must fail
	// closed and leave existing (empty) secrets untouched.
	plain := testReloadServer(t)
	rc := base()
	rc.SharedSecret, rc.ClientSecret = "s-new", "c-new"
	if _, err := plain.Reload(rc); err == nil {
		t.Fatal("secret reload without TLS must fail")
	}

	// Full rotation without restart: install, rotate with previous, drop.
	srv := newTLS(t)
	rc = base()
	rc.SharedSecret, rc.ClientSecret, rc.AdminSecret = "s-new", "c-new", "a-new"
	if _, err := srv.Reload(rc); err != nil {
		t.Fatal(err)
	}
	ping := protocol.Request{Opcode: protocol.OpPing}
	if res := srv.DispatchWithAuth(ping, auth.ClientToken("c-new")); res.Status != protocol.StatusOK {
		t.Fatalf("fresh secret: %+v", res)
	}
	rc.SharedSecret, rc.ClientSecret, rc.AdminSecret = "s-new2", "c-new2", "a-new2"
	rc.SharedSecretPrev, rc.ClientSecretPrev, rc.AdminSecretPrev = "s-new", "c-new", "a-new"
	if _, err := srv.Reload(rc); err != nil {
		t.Fatal(err)
	}
	for _, tok := range [][]byte{auth.ClientToken("c-new2"), auth.ClientToken("c-new")} {
		if res := srv.DispatchWithAuth(ping, tok); res.Status != protocol.StatusOK {
			t.Fatalf("rotation window token: %+v", res)
		}
	}
	rc.SharedSecretPrev, rc.ClientSecretPrev, rc.AdminSecretPrev = "", "", ""
	if _, err := srv.Reload(rc); err != nil {
		t.Fatal(err)
	}
	if res := srv.DispatchWithAuth(ping, auth.ClientToken("c-new")); res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("dropped secret must be denied: %+v", res)
	}
	if res := srv.DispatchWithAuth(ping, auth.ClientToken("c-new2")); res.Status != protocol.StatusOK {
		t.Fatalf("current secret: %+v", res)
	}
}
