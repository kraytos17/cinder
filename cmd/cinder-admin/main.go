// Command cinder-admin sends administrative operations to a Cinder server.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v3"
)

var version = "dev"

func run(args []string, stdout, stderr io.Writer) int {
	root := &cli.Command{
		Name:                   "cinder-admin",
		Usage:                  "administer a Cinder server",
		Version:                version,
		HideVersion:            true,
		Suggest:                false,
		Writer:                 stdout,
		ErrWriter:              stderr,
		UseShortOptionHandling: true,
		ExitErrHandler:         func(context.Context, *cli.Command, error) {},
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "host", Value: "127.0.0.1", Usage: "Server host"},
			&cli.IntFlag{Name: "port", Aliases: []string{"p"}, Value: 7000, Usage: "Server port"},
			&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Enable verbose (debug) logging"},
			&cli.BoolFlag{Name: "tls", Usage: "Enable TLS encryption"},
			&cli.StringFlag{Name: "tls-cert", Usage: "Path to TLS certificate chain (PEM)"},
			&cli.StringFlag{Name: "tls-key", Usage: "Path to TLS private key (PEM)"},
			&cli.StringFlag{Name: "tls-ca", Usage: "Path to CA certificate for peer verification (PEM)"},
			&cli.StringFlag{Name: "client-secret", Usage: "Shared secret for client authentication (or CINDER_CLIENT_SECRET)"},
			&cli.BoolFlag{Name: "version", Usage: "print version and exit"},
		},
		Commands: []*cli.Command{
			{Name: "info", Usage: "show node information", Action: adminAction(adminInfo, stdout, stderr)},
			{Name: "cluster", Usage: "show cluster membership", Action: adminAction(adminCluster, stdout, stderr)},
			{Name: "ring", Usage: "show ring information", Action: adminAction(adminRing, stdout, stderr)},
			{Name: "compact", Usage: "compact persistent data", Action: adminAction(adminCompact, stdout, stderr)},
			{Name: "config-reload", Usage: "reload node configuration", Action: adminAction(adminConfigReload, stdout, stderr)},
			{Name: "shutdown", Usage: "shut down the node", Action: adminAction(adminShutdown, stdout, stderr)},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.Bool("version") {
				fmt.Fprintf(stdout, "cinder-admin (go) %s\n", version)
				return nil
			}
			if cmd.Args().Len() > 0 {
				fmt.Fprintf(stderr, "unknown command: %s\n", cmd.Args().First())
				fmt.Fprintln(stderr, "available commands: info, cluster, ring, compact, config-reload, shutdown")
				return cli.Exit("", 1)
			}

			fmt.Fprintln(stderr, "error: command required: info|cluster|ring|compact|config-reload|shutdown")
			return cli.Exit("", 2)
		},
	}

	err := root.Run(context.Background(), append([]string{"cinder-admin"}, args...))
	if err != nil {
		if ec, ok := errors.AsType[cli.ExitCoder](err); ok {
			return ec.ExitCode()
		}

		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
