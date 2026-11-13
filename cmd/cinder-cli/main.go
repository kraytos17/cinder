// Command cinder-cli sends cache and counter operations to a Cinder server.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

var version = "dev"

type operation string

const (
	opGet          operation = "get"
	opGetVersioned operation = "get-versioned"
	opSet          operation = "set"
	opDel          operation = "del"
	opPing         operation = "ping"
	opInfo         operation = "info"
	opCluster      operation = "cluster"
	opRing         operation = "ring"
	opMultiGet     operation = "mget"
	opMultiSet     operation = "mset"
	opCounterIncr  operation = "counter-incr"
	opCounterDecr  operation = "counter-decr"
	opCounterGet   operation = "counter-get"
)

func statusString(s protocol.Status) string {
	switch s {
	case protocol.StatusOK:
		return "OK"
	case protocol.StatusNotFound:
		return "(not found)"
	case protocol.StatusCapacityExceeded:
		return "(capacity exceeded)"
	case protocol.StatusInvalidArgument:
		return "(invalid argument)"
	case protocol.StatusTTLExpired:
		return "(ttl expired)"
	case protocol.StatusNotSupported:
		return "(not supported)"
	case protocol.StatusInternalError:
		return "(internal error)"
	case protocol.StatusTimeout:
		return "(timeout)"
	case protocol.StatusNotReady:
		return "(not ready)"
	case protocol.StatusPermissionDenied:
		return "(permission denied)"
	case protocol.StatusCorruptData:
		return "(corrupt data)"
	default:
		return "(unknown)"
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	args, trailingTTL, err := extractTTL(args)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}

	root := &cli.Command{
		Name:                   "cinder-cli",
		Usage:                  "send cache and counter operations to a Cinder server",
		Version:                version,
		HideVersion:            true,
		Suggest:                false,
		Writer:                 stdout,
		ErrWriter:              stderr,
		HideHelpCommand:        true,
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
			&cli.IntFlag{Name: "ttl", Usage: "TTL in ms (set/mset only)"},
			&cli.BoolFlag{Name: "version", Usage: "print version and exit"},
		},
		Commands: []*cli.Command{
			{Name: string(opGet), Usage: "get a value", Action: cliCommandAction(opGet, trailingTTL, stdout, stderr)},
			{Name: string(opGetVersioned), Usage: "get a value with LWW metadata", Action: cliCommandAction(opGetVersioned, trailingTTL, stdout, stderr)},
			{Name: string(opSet), Usage: "set a value", Action: cliCommandAction(opSet, trailingTTL, stdout, stderr)},
			{Name: string(opDel), Usage: "delete a key", Action: cliCommandAction(opDel, trailingTTL, stdout, stderr)},
			{Name: string(opPing), Usage: "ping the server", Action: cliCommandAction(opPing, trailingTTL, stdout, stderr)},
			{Name: string(opInfo), Usage: "show node information", Action: cliCommandAction(opInfo, trailingTTL, stdout, stderr)},
			{Name: string(opCluster), Usage: "show cluster membership", Action: cliCommandAction(opCluster, trailingTTL, stdout, stderr)},
			{Name: string(opRing), Usage: "show ring information", Action: cliCommandAction(opRing, trailingTTL, stdout, stderr)},
			{Name: string(opMultiGet), Usage: "get multiple values", Action: cliCommandAction(opMultiGet, trailingTTL, stdout, stderr)},
			{Name: string(opMultiSet), Usage: "set multiple values", Action: cliCommandAction(opMultiSet, trailingTTL, stdout, stderr)},
			{Name: string(opCounterIncr), Usage: "increment a counter", Action: cliCommandAction(opCounterIncr, trailingTTL, stdout, stderr)},
			{Name: string(opCounterDecr), Usage: "decrement a counter", Action: cliCommandAction(opCounterDecr, trailingTTL, stdout, stderr)},
			{Name: string(opCounterGet), Usage: "read a counter", Action: cliCommandAction(opCounterGet, trailingTTL, stdout, stderr)},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.NArg() > 0 {
				fmt.Fprintf(stderr, "error: unknown command: %s\n", cmd.Args().First())
				return cli.Exit("", 1)
			}
			if cmd.Bool("version") {
				fmt.Fprintf(stdout, "cinder-cli (go) %s\n", version)
				return nil
			}

			fmt.Fprintln(stderr, "error: command required: get|set|del|ping|info|cluster|ring|mget|mset|get-versioned|counter-incr|counter-decr|counter-get")
			return cli.Exit("", 2)
		},
	}

	err = root.Run(context.Background(), append([]string{"cinder-cli"}, args...))
	if err != nil {
		if ec, ok := errors.AsType[cli.ExitCoder](err); ok {
			return ec.ExitCode()
		}

		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	return 0
}

func cliCommandAction(op operation, trailingTTL string, stdout, stderr io.Writer) cli.ActionFunc {
	return func(_ context.Context, cmd *cli.Command) error {
		return executeCommand(op, readConnectionOptions(cmd), cmd.Args().Slice(), cmd.Int("ttl"), trailingTTL, stdout, stderr)
	}
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
