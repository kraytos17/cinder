// Command cinderd runs a Cinder cache node and its configured cluster services.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/persist"
)

var version = "dev"

func buildVersion() string {
	if version != "dev" {
		return version
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 7 {
			return "dev-" + s.Value[:7]
		}
	}
	return version
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg := defaultDaemonConfig()
	root := &cli.Command{
		Name:                   "cinderd",
		Usage:                  "run a Cinder cache node",
		Version:                buildVersion(),
		HideVersion:            true,
		Suggest:                false,
		Writer:                 stdout,
		ErrWriter:              stderr,
		HideHelpCommand:        true,
		UseShortOptionHandling: true,
		ExitErrHandler:         func(context.Context, *cli.Command, error) {},
		Flags:                  cfg.flags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.String("verify-data-dir") != "" {
				for _, name := range []string{"config", "enable-persistence", "data-dir", "peers", "port", "node-id"} {
					if cmd.IsSet(name) {
						return daemonUsageError(fmt.Errorf("--verify-data-dir cannot be combined with --%s", name))
					}
				}
				return verifyDataDir(cmd.String("verify-data-dir"), stdout, stderr)
			}
			if cmd.Bool("version") {
				fmt.Fprintf(stdout, "cinderd (go) %s\n", buildVersion())
				return nil
			}

			cfg, err := resolveDaemonConfig(cmd, cmd.String("config"))
			if err != nil {
				return daemonUsageError(err)
			}
			return runDaemon(ctx, cfg, stderr, func() (tcp.ReloadableConfig, error) {
				fresh, err := resolveDaemonConfig(cmd, cmd.String("config"))
				if err != nil {
					return tcp.ReloadableConfig{}, err
				}
				return ReloadableFromDaemon(fresh), nil
			})
		},
	}

	err := root.Run(context.Background(), append([]string{"cinderd"}, args...))
	if err != nil {
		fmt.Fprintln(stderr, err)
		if ec, ok := errors.AsType[cli.ExitCoder](err); ok {
			return ec.ExitCode()
		}
		return 2
	}
	return 0
}

func verifyDataDir(dir string, stdout, stderr io.Writer) error {
	report, err := persist.VerifyDataDir(dir)
	if err != nil {
		return daemonRuntimeError(fmt.Errorf("verify data directory: %w", err))
	}

	fmt.Fprintf(stdout, "directory: %s\n", report.Directory)
	fmt.Fprintf(stdout, "files: %d  kv_records: %d  counter_records: %d\n", len(report.Files), report.KVRecords, report.CounterRecords)
	for _, file := range report.Files {
		state := "ok"
		if file.TornTail {
			state = fmt.Sprintf("torn-tail valid-bytes=%d", file.ValidBytes)
		}
		if file.Error != "" {
			state = file.Error
		}
		fmt.Fprintf(stdout, "%s %s format=%s bytes=%d kv=%d counters=%d %s\n", file.Kind, file.Path, file.Format, file.Bytes, file.KVRecords, file.CounterRecords, state)
	}
	if len(report.Errors) > 0 {
		for _, verifyErr := range report.Errors {
			fmt.Fprintln(stderr, "cinderd: verify:", verifyErr)
		}
		return cli.Exit("cinderd: data directory contains corruption", 1)
	}
	if len(report.TornTails) > 0 {
		fmt.Fprintf(stdout, "warning: %d WAL file(s) have recoverable torn tails; verification did not modify files\n", len(report.TornTails))
	}
	return nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
