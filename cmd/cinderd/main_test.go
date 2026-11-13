package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urfave/cli/v3"
)

func TestCinderdHelpAndVersion(t *testing.T) {
	var out, errOut strings.Builder
	if code := run([]string{"--help"}, &out, &errOut); code != 0 {
		t.Fatalf("help exit=%d stderr=%q", code, errOut.String())
	}
	for _, want := range []string{"--bind", "--advertise-host", "--grpc-bind", "--metrics-bind", "--tls", "--gossip-rpc-timeout-ms"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("help missing %q", want)
		}
	}
	out.Reset()
	errOut.Reset()
	if code := run([]string{"--version"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "cinderd (go)") {
		t.Fatalf("version exit=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func TestReadDaemonConfigDefaultsAndOverrides(t *testing.T) {
	cfg := defaultDaemonConfig()
	cmd := &cli.Command{Flags: cfg.flags()}
	if err := cmd.Run(context.Background(), []string{"cinderd", "--bind", "0.0.0.0", "--advertise-host", "cache.example.test", "--port", "7100", "--tls", "--grpc-port", "7200"}); err != nil {
		t.Fatal(err)
	}
	got := readDaemonConfig(cmd)
	if got.Port != 7100 || got.BindHost != "0.0.0.0" || got.AdvertiseHost != "cache.example.test" || !got.TLS || got.GRPCPort != 7200 {
		t.Fatalf("read config = %+v", got)
	}
	defaults := readDaemonConfig(&cli.Command{Flags: defaultDaemonConfig().flags()})
	if defaults.Port != 7000 || defaults.BindHost != "127.0.0.1" || defaults.GRPCPort != 0 || defaults.ReplicationFactor != 1 {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
}

func TestOverlayConfigFileV2StrictAndMerge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cinderd.json")
	if err := os.WriteFile(path, []byte(`{"port":7100,"capacity":12345,"tls":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := overlayConfigFile(defaultDaemonConfig(), path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port != 7100 || got.Capacity != 12345 || !got.TLS {
		t.Fatalf("file values not applied: %+v", got)
	}
	if got.BindHost != "127.0.0.1" || got.ReplicationFactor != 1 || got.LogLevel != "info" {
		t.Fatalf("omitted fields did not retain defaults: %+v", got)
	}

	if err := os.WriteFile(path, []byte(`{"port":7001,"porrt":7002}`), 0o600); err != nil {
		t.Fatal(err)
	}
	unchanged := defaultDaemonConfig()
	got, err = overlayConfigFile(unchanged, path)
	if err == nil || !strings.Contains(err.Error(), "porrt") {
		t.Fatalf("unknown member error = %v, want unknown key porrt", err)
	}
	if got.Port != unchanged.Port {
		t.Fatalf("failed overlay changed base: port=%d want %d", got.Port, unchanged.Port)
	}
}

func TestCinderdFlagValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "wildcard bind needs advertise", args: []string{"--bind", "0.0.0.0"}, want: "--advertise-host is required"},
		{name: "ephemeral cluster port refused", args: []string{"--port", "0", "--peers", "n2@127.0.0.1:7001"}, want: "--port 0 cannot be used"},
		{name: "probe interval positive", args: []string{"--ping-interval-ms", "0"}, want: "--ping-interval-ms must be positive"},
		{name: "suspect exceeds probe", args: []string{"--suspect-timeout-ms", "1000"}, want: "greater than --ping-interval-ms"},
		{name: "secret requires TLS for grpc", args: []string{"--grpc-port", "9000", "--client-shared-secret", "s"}, want: "requires --tls"},
		{name: "invalid replica count", args: []string{"--replication-factor", "33"}, want: "replication-factor"},
		{name: "persistence needs directory", args: []string{"--enable-persistence"}, want: "--data-dir is required"},
		{name: "invalid consistency at RF=1", args: []string{"--consistency", "linearizable"}, want: "--consistency must be async|quorum"},
		{name: "invalid consistency at RF=2", args: []string{"--replication-factor", "2", "--consistency", "bogus"}, want: "--consistency must be async|quorum"},
		{name: "negative snapshot interval", args: []string{"--snapshot-interval-s", "-1"}, want: "--snapshot-interval-s must be >= 0"},
		{name: "zero capacity", args: []string{"--capacity", "0"}, want: "--capacity must be positive"},
		{name: "negative quarantine", args: []string{"--quarantine-interval-ms", "-5"}, want: "--quarantine-interval-ms must be >= 0"},
		{name: "negative rpc timeout", args: []string{"--rpc-timeout-ms", "-1"}, want: "--rpc-timeout-ms must be >= 0"},
		{name: "negative fetch budget", args: []string{"--anti-entropy-fetch-bytes", "-1"}, want: "--anti-entropy-fetch-bytes must be >= 0"},
		{name: "negative shutdown timeout", args: []string{"--shutdown-timeout-ms", "-1"}, want: "--shutdown-timeout-ms must be >= 0"},
		{name: "negative ae interval", args: []string{"--anti-entropy-interval-ms", "-1"}, want: "--anti-entropy-interval-ms must be >= 0"},
		{name: "grpc collides with data port", args: []string{"--port", "7000", "--grpc-port", "7000"}, want: "--grpc-port must not equal --port"},
		{name: "metrics collides with data port", args: []string{"--port", "7000", "--metrics-port", "7000"}, want: "--metrics-port must not equal --port"},
		{name: "grpc collides with metrics port", args: []string{"--grpc-port", "9000", "--metrics-port", "9000"}, want: "--grpc-port must not equal --metrics-port"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			code := run(tc.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("exit=%d, want 2; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr=%q, want containing %q", stderr.String(), tc.want)
			}
		})
	}
}

func TestValidateDaemonConfigReportsAllErrors(t *testing.T) {
	cfg := defaultDaemonConfig()
	cfg.Port = 99999
	cfg.AEBuckets = 0
	cfg.SnapshotIntervalS = -1
	cfg.Capacity = 0
	cfg.Consistency = "bogus"
	_, errs := validateDaemonConfig(cfg)
	if len(errs) < 5 {
		t.Fatalf("got %d errors, want at least 5: %v", len(errs), errs)
	}
	joined := ""
	for _, err := range errs {
		joined += err.Error() + "\n"
	}
	for _, want := range []string{"--port", "--anti-entropy-buckets", "--snapshot-interval-s", "--capacity", "--consistency"} {
		if !strings.Contains(joined, want) {
			t.Errorf("aggregated errors missing %q:\n%s", want, joined)
		}
	}
}

func TestValidateDaemonConfigWarns(t *testing.T) {
	cfg := defaultDaemonConfig()
	cfg.Consistency = "quorum"
	cfg.MaxHintBytes = 1024
	_, errs := validateDaemonConfig(cfg)
	if len(errs) != 0 {
		t.Fatalf("valid config reported errors: %v", errs)
	}
	vc, _ := validateDaemonConfig(cfg)
	joined := strings.Join(vc.warns, "\n")
	for _, want := range []string{"--consistency is ignored at replication-factor 1", "--max-hint-bytes has no effect at replication-factor 1"} {
		if !strings.Contains(joined, want) {
			t.Errorf("warnings missing %q: %q", want, joined)
		}
	}

	cfg.ReplicationFactor = 2
	vc, errs = validateDaemonConfig(cfg)
	if len(errs) != 0 {
		t.Fatalf("RF=2 config reported errors: %v", errs)
	}
	if len(vc.warns) != 0 {
		t.Errorf("RF=2 config warned unexpectedly: %v", vc.warns)
	}
}
