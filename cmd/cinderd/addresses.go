package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func resolveAdvertiseHostWithLookup(bindHost, advertiseHost string, lookup func(string) ([]string, error)) (string, error) {
	if err := validateHost(bindHost, true, lookup); err != nil {
		return "", fmt.Errorf("invalid --bind host %q: %w", bindHost, err)
	}
	if advertiseHost == "" {
		if isWildcardHost(bindHost) {
			return "", fmt.Errorf("--advertise-host is required when --bind is wildcard (%q)", bindHost)
		}
		return bindHost, nil
	}
	if err := validateHost(advertiseHost, false, lookup); err != nil {
		return "", fmt.Errorf("invalid --advertise-host %q: %w", advertiseHost, err)
	}
	return advertiseHost, nil
}

func validateListenerHosts(bindHost, advertiseHost, metricsHost, grpcHost string) (string, error) {
	return validateListenerHostsWithLookup(bindHost, advertiseHost, metricsHost, grpcHost, net.LookupHost)
}

func validateListenerHostsWithLookup(bindHost, advertiseHost, metricsHost, grpcHost string, lookup func(string) ([]string, error)) (string, error) {
	selfHost, err := resolveAdvertiseHostWithLookup(bindHost, advertiseHost, lookup)
	if err != nil {
		return "", err
	}
	for _, endpoint := range []struct{ name, host string }{{"--metrics-bind", metricsHost}, {"--grpc-bind", grpcHost}} {
		if err := validateHost(endpoint.host, true, lookup); err != nil {
			return "", fmt.Errorf("%s: %w", endpoint.name, err)
		}
	}
	return selfHost, nil
}

func validateCoreFlags(port, aeBuckets, tombstoneMs int) error {
	if tombstoneMs < 0 {
		return fmt.Errorf("--tombstone-gc-ms must be >= 0")
	}
	if port < 0 || port > 65535 {
		return fmt.Errorf("--port must be between 0 and 65535")
	}
	if aeBuckets <= 0 {
		return fmt.Errorf("--anti-entropy-buckets must be positive")
	}
	return nil
}

func validateClusterPort(port, peerCount int) error {
	if port == 0 && peerCount > 0 {
		return fmt.Errorf("--port 0 cannot be used with --peers; choose a fixed port so it can be advertised before cluster startup")
	}
	return nil
}

func clusterRPCTimeout(probeMs, suspectMs, gossipMs, rpcTimeoutMs int) (time.Duration, error) {
	if probeMs <= 0 {
		return 0, fmt.Errorf("--ping-interval-ms must be positive")
	}
	if suspectMs <= probeMs {
		return 0, fmt.Errorf("--suspect-timeout-ms must be greater than --ping-interval-ms")
	}
	if gossipMs <= 0 {
		return 0, fmt.Errorf("--gossip-interval-ms must be positive")
	}
	if rpcTimeoutMs < 0 {
		return 0, fmt.Errorf("--gossip-rpc-timeout-ms must be >= 0 (0 selects the automatic timeout)")
	}
	if rpcTimeoutMs == 0 {
		rpcTimeoutMs = min(probeMs, 1000)
	}
	if rpcTimeoutMs > probeMs {
		return 0, fmt.Errorf("--gossip-rpc-timeout-ms must not exceed --ping-interval-ms")
	}
	if rpcTimeoutMs >= suspectMs {
		return 0, fmt.Errorf("--gossip-rpc-timeout-ms must be less than --suspect-timeout-ms")
	}
	return time.Duration(rpcTimeoutMs) * time.Millisecond, nil
}

func validateGRPCTLS(grpcPort int, tlsEnabled bool, clientSecret string) error {
	if grpcPort > 0 && clientSecret != "" && !tlsEnabled {
		return fmt.Errorf("--client-shared-secret requires --tls when --grpc-port is enabled")
	}
	return nil
}

func configuredSecret(flagValue, envName string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv(envName)
}

func validateHost(host string, allowWildcard bool, lookup func(string) ([]string, error)) error {
	if host == "" || strings.TrimSpace(host) != host {
		return fmt.Errorf("host must be non-empty and contain no surrounding whitespace")
	}
	if ip := net.ParseIP(strings.Split(host, "%")[0]); ip != nil {
		if ip.IsUnspecified() && !allowWildcard {
			return fmt.Errorf("wildcard address cannot be advertised")
		}
		return nil
	}
	if strings.ContainsAny(host, "/[]: ") {
		return fmt.Errorf("expected an IP address or DNS hostname")
	}
	if _, err := lookup(host); err != nil {
		return fmt.Errorf("hostname does not resolve: %w", err)
	}
	return nil
}

func isWildcardHost(host string) bool {
	ip := net.ParseIP(strings.Split(host, "%")[0])
	return ip != nil && ip.IsUnspecified()
}
