package main

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestResolveAdvertiseHost(t *testing.T) {
	lookup := func(host string) ([]string, error) {
		if host == "cache.example.test" {
			return []string{"192.0.2.10"}, nil
		}
		return nil, errors.New("not found")
	}
	tests := []struct {
		name, bind, advertise, want string
		wantErr                     string
	}{
		{name: "default bind is advertised", bind: "127.0.0.1", want: "127.0.0.1"},
		{name: "bind hostname is advertised", bind: "cache.example.test", want: "cache.example.test"},
		{name: "explicit advertise overrides bind", bind: "0.0.0.0", advertise: "cache.example.test", want: "cache.example.test"},
		{name: "wildcard needs advertise", bind: "::", wantErr: "--advertise-host is required"},
		{name: "wildcard advertise rejected", bind: "0.0.0.0", advertise: "::", wantErr: "wildcard address cannot be advertised"},
		{name: "bad bind hostname", bind: "missing.example.test", wantErr: "hostname does not resolve"},
		{name: "bad advertise hostname", bind: "127.0.0.1", advertise: "missing.example.test", wantErr: "hostname does not resolve"},
		{name: "empty bind", advertise: "cache.example.test", wantErr: "host must be non-empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAdvertiseHostWithLookup(tc.bind, tc.advertise, lookup)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("resolveAdvertiseHost() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestValidateBindHost(t *testing.T) {
	lookup := func(string) ([]string, error) { return []string{"192.0.2.1"}, nil }
	for _, tc := range []struct {
		host    string
		wantErr bool
	}{{"127.0.0.1", false}, {"::", false}, {"cache.example.test", false}, {"bad host", true}, {"", true}} {
		err := validateHost(tc.host, true, lookup)
		if (err != nil) != tc.wantErr {
			t.Errorf("validateHost(%q) error = %v, wantErr=%v", tc.host, err, tc.wantErr)
		}
	}
}

func TestValidateCoreFlags(t *testing.T) {
	for _, tc := range []struct {
		name                string
		port, buckets, tomb int
		wantErr             bool
	}{
		{name: "valid", port: 7000, buckets: 256},
		{name: "port zero allowed", port: 0, buckets: 1},
		{name: "port too low", port: -1, buckets: 1, wantErr: true},
		{name: "port too high", port: 65536, buckets: 1, wantErr: true},
		{name: "zero buckets", port: 7000, buckets: 0, wantErr: true},
		{name: "negative tombstone", port: 7000, buckets: 1, tomb: -1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := validateCoreFlags(tc.port, tc.buckets, tc.tomb)
			if (len(errs) != 0) != tc.wantErr {
				t.Fatalf("validateCoreFlags() errors = %v, wantErr=%v", errs, tc.wantErr)
			}
		})
	}
}

func TestValidateClusterPort(t *testing.T) {
	for _, tc := range []struct {
		name      string
		port      int
		peers     int
		wantError bool
	}{
		{name: "fixed standalone", port: 7000},
		{name: "fixed clustered", port: 7000, peers: 2},
		{name: "ephemeral standalone", port: 0},
		{name: "ephemeral cluster refused", port: 0, peers: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateClusterPort(tc.port, tc.peers)
			if (err != nil) != tc.wantError {
				t.Fatalf("validateClusterPort() error = %v, wantError=%v", err, tc.wantError)
			}
		})
	}
}

func TestClusterRPCTimeout(t *testing.T) {
	tests := []struct {
		name                   string
		probe, suspect, gossip int
		rpc                    int
		want                   time.Duration
		wantErr                string
	}{
		{name: "automatic caps at one second", probe: 1500, suspect: 3000, gossip: 1000, want: time.Second},
		{name: "automatic follows short probe", probe: 200, suspect: 600, gossip: 1000, want: 200 * time.Millisecond},
		{name: "explicit shorter timeout", probe: 500, suspect: 1500, gossip: 1000, rpc: 100, want: 100 * time.Millisecond},
		{name: "nonpositive probe", probe: 0, suspect: 600, gossip: 1000, wantErr: "--ping-interval-ms"},
		{name: "suspect must exceed probe", probe: 600, suspect: 600, gossip: 1000, wantErr: "greater than --ping-interval-ms"},
		{name: "nonpositive gossip", probe: 200, suspect: 600, gossip: 0, wantErr: "--gossip-interval-ms"},
		{name: "negative rpc timeout", probe: 200, suspect: 600, gossip: 1000, rpc: -1, wantErr: "--gossip-rpc-timeout-ms"},
		{name: "rpc timeout cannot exceed probe interval", probe: 100, suspect: 600, gossip: 1000, rpc: 101, wantErr: "must not exceed"},
		{name: "rpc timeout below suspect timeout", probe: 100, suspect: 100, gossip: 1000, rpc: 100, wantErr: "must be greater"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := clusterRPCTimeout(tc.probe, tc.suspect, tc.gossip, tc.rpc)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("clusterRPCTimeout() = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestValidateGRPCTLS(t *testing.T) {
	for _, tc := range []struct {
		name    string
		port    int
		tls     bool
		secret  string
		wantErr string
	}{
		{name: "grpc plaintext without auth allowed", port: 9000},
		{name: "tls without auth allowed", port: 9000, tls: true},
		{name: "tls with auth allowed", port: 9000, tls: true, secret: "secret"},
		{name: "secret without tls rejected", port: 9000, secret: "secret", wantErr: "requires --tls"},
		{name: "disabled grpc secret does not gate tcp", port: 0, secret: "secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateGRPCTLS(tc.port, tc.tls, tc.secret)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestValidateListenerHosts(t *testing.T) {
	tests := []struct {
		name, bind, advertise, metrics, grpc string
		want, wantErr                        string
	}{
		{name: "loopback defaults", bind: "127.0.0.1", metrics: "127.0.0.1", grpc: "127.0.0.1", want: "127.0.0.1"},
		{name: "wildcard with advertise", bind: "0.0.0.0", advertise: "192.0.2.8", metrics: "127.0.0.1", grpc: "127.0.0.1", want: "192.0.2.8"},
		{name: "wildcard missing advertise", bind: "::", metrics: "127.0.0.1", grpc: "127.0.0.1", wantErr: "--advertise-host is required"},
		{name: "bad metrics bind", bind: "127.0.0.1", metrics: "bad host", grpc: "127.0.0.1", wantErr: "--metrics-bind"},
		{name: "bad grpc bind", bind: "127.0.0.1", metrics: "127.0.0.1", grpc: "bad host", wantErr: "--grpc-bind"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateListenerHostsWithLookup(tc.bind, tc.advertise, tc.metrics, tc.grpc, net.LookupHost)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("validateListenerHosts() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
