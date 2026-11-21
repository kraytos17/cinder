package main

import (
	"cmp"
	"errors"
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

func validateCoreFlags(port, aeBuckets, tombstoneMs int) []error {
	var errs []error
	if tombstoneMs < 0 {
		errs = append(errs, errors.New("--tombstone-gc-ms must be >= 0"))
	}
	if port < 0 || port > 65535 {
		errs = append(errs, errors.New("--port must be between 0 and 65535"))
	}
	if aeBuckets <= 0 {
		errs = append(errs, errors.New("--anti-entropy-buckets must be positive"))
	}
	return errs
}

func validateAuxPortRange(metricsPort, grpcPort int) error {
	if metricsPort < 0 || metricsPort > 65535 || grpcPort < 0 || grpcPort > 65535 {
		return errors.New("--metrics-port and --grpc-port must be between 0 and 65535")
	}
	return nil
}

func validateNonNegativeIntervals(aeIntervalMs, quarantineIntervalMs, snapshotIntervalS, rpcTimeoutMs, aeFetchBytes int) error {
	for _, iv := range []struct {
		name  string
		value int
	}{
		{"--anti-entropy-interval-ms", aeIntervalMs},
		{"--quarantine-interval-ms", quarantineIntervalMs},
		{"--snapshot-interval-s", snapshotIntervalS},
		{"--rpc-timeout-ms", rpcTimeoutMs},
		{"--anti-entropy-fetch-bytes", aeFetchBytes},
	} {
		if iv.value < 0 {
			return fmt.Errorf("%s must be >= 0", iv.name)
		}
	}
	return nil
}

func validateCapacity(capacity uint64) error {
	if capacity == 0 {
		return errors.New("--capacity must be positive")
	}
	return nil
}

func validateListenerPortCollisions(port, grpcPort, metricsPort int) error {
	if grpcPort > 0 && grpcPort == port {
		return errors.New("--grpc-port must not equal --port")
	}
	if metricsPort > 0 && metricsPort == port {
		return errors.New("--metrics-port must not equal --port")
	}
	if grpcPort > 0 && metricsPort > 0 && grpcPort == metricsPort {
		return errors.New("--grpc-port must not equal --metrics-port")
	}
	return nil
}

func validateClusterPort(port, peerCount int) error {
	if port == 0 && peerCount > 0 {
		return errors.New("--port 0 cannot be used with --peers; choose a fixed port so it can be advertised before cluster startup")
	}
	return nil
}

func clusterRPCTimeout(probeMs, suspectMs, gossipMs, rpcTimeoutMs int) (time.Duration, error) {
	if probeMs <= 0 {
		return 0, errors.New("--ping-interval-ms must be positive")
	}
	if suspectMs <= probeMs {
		return 0, errors.New("--suspect-timeout-ms must be greater than --ping-interval-ms")
	}
	if gossipMs <= 0 {
		return 0, errors.New("--gossip-interval-ms must be positive")
	}
	if rpcTimeoutMs < 0 {
		return 0, errors.New("--gossip-rpc-timeout-ms must be >= 0 (0 selects the automatic timeout)")
	}
	if rpcTimeoutMs == 0 {
		rpcTimeoutMs = min(probeMs, 1000)
	}
	if rpcTimeoutMs > probeMs {
		return 0, errors.New("--gossip-rpc-timeout-ms must not exceed --ping-interval-ms")
	}
	if rpcTimeoutMs >= suspectMs {
		return 0, errors.New("--gossip-rpc-timeout-ms must be less than --suspect-timeout-ms")
	}
	return time.Duration(rpcTimeoutMs) * time.Millisecond, nil
}

func validateGRPCTLS(grpcPort int, tlsEnabled bool, clientSecret, adminSecret string) error {
	if grpcPort > 0 && (clientSecret != "" || adminSecret != "") && !tlsEnabled {
		return errors.New("--client-shared-secret/--client-secret-admin requires --tls when --grpc-port is enabled")
	}
	return nil
}

func configuredSecret(flagValue, envName string) string {
	return cmp.Or(flagValue, os.Getenv(envName))
}

func validateHost(host string, allowWildcard bool, lookup func(string) ([]string, error)) error {
	if host == "" || strings.TrimSpace(host) != host {
		return errors.New("host must be non-empty and contain no surrounding whitespace")
	}

	zone, _, _ := strings.Cut(host, "%")
	if ip := net.ParseIP(zone); ip != nil {
		if ip.IsUnspecified() && !allowWildcard {
			return errors.New("wildcard address cannot be advertised")
		}
		return nil
	}
	if strings.ContainsAny(host, "/[]: ") {
		return errors.New("expected an IP address or DNS hostname")
	}
	if _, err := lookup(host); err != nil {
		return fmt.Errorf("hostname does not resolve: %w", err)
	}
	return nil
}

func isWildcardHost(host string) bool {
	zone, _, _ := strings.Cut(host, "%")
	ip := net.ParseIP(zone)
	return ip != nil && ip.IsUnspecified()
}
