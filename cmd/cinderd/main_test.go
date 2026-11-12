package main

import (
	"context"
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
	if got.port != 7100 || got.bindHost != "0.0.0.0" || got.advertiseHost != "cache.example.test" || !got.tls || got.grpcPort != 7200 {
		t.Fatalf("read config = %+v", got)
	}
	defaults := readDaemonConfig(&cli.Command{Flags: defaultDaemonConfig().flags()})
	if defaults.port != 7000 || defaults.bindHost != "127.0.0.1" || defaults.grpcPort != 0 || defaults.replicationFactor != 1 {
		t.Fatalf("unexpected defaults: %+v", defaults)
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
