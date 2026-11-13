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

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
