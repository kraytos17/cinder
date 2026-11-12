// Command cinder-cli sends cache and counter operations to a Cinder server.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/tlscfg"
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

type connectionOptions struct {
	host, tlsCert, tlsKey, tlsCA, clientSecret string
	port                                       int
	useTLS                                     bool
}

func exitWithCode(code int) error { return cli.Exit("", code) }

type keyValuePairs struct {
	keys, values []string
}

func parseKeyValuePairs(args []string) (keyValuePairs, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return keyValuePairs{}, errors.New("mset requires key/value pairs")
	}

	out := keyValuePairs{
		keys:   make([]string, 0, len(args)/2),
		values: make([]string, 0, len(args)/2),
	}
	for i := 0; i < len(args); i += 2 {
		out.keys = append(out.keys, args[i])
		out.values = append(out.values, args[i+1])
	}
	return out, nil
}

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

func readConnectionOptions(cmd *cli.Command) connectionOptions {
	secret := cmd.String("client-secret")
	if secret == "" {
		secret = os.Getenv("CINDER_CLIENT_SECRET")
	}
	return connectionOptions{
		host: cmd.String("host"), port: cmd.Int("port"), useTLS: cmd.Bool("tls"),
		tlsCert: cmd.String("tls-cert"), tlsKey: cmd.String("tls-key"), tlsCA: cmd.String("tls-ca"), clientSecret: secret,
	}
}

func executeCommand(op operation, conn connectionOptions, args []string, ttlMs int, trailingTTL string, stdout, stderr io.Writer) error {
	if trailingTTL != "" && ttlMs == 0 {
		n, err := strconv.Atoi(trailingTTL)
		if err != nil || n < 0 {
			fmt.Fprintln(stderr, "error: invalid --ttl value")
			return exitWithCode(2)
		}
		ttlMs = n
	}

	var tlsCfg *tls.Config
	if conn.useTLS {
		var err error
		tlsCfg, err = tlscfg.Client(conn.tlsCert, conn.tlsKey, conn.tlsCA)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return exitWithCode(1)
		}
	}

	addr := net.JoinHostPort(conn.host, strconv.Itoa(conn.port))
	auth := client.Secrets{Client: conn.clientSecret}
	dial := func() (*client.Client, error) {
		var c *client.Client
		var err error
		if tlsCfg != nil {
			c, err = client.DialTLS(addr, 5*time.Second, tlsCfg)
		} else {
			c, err = client.Dial(addr, 5*time.Second)
		}

		if err != nil {
			return nil, err
		}

		c.SetSecrets(auth)
		return c, nil
	}

	switch op {
	case opMultiGet:
		if len(args) == 0 {
			fmt.Fprintln(stderr, "error: mget requires at least one key")
			return exitWithCode(1)
		}
		return codeError(runMultiGet(dial, args, stdout, stderr))
	case opMultiSet:
		pairs, err := parseKeyValuePairs(args)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return exitWithCode(1)
		}
		return codeError(runMultiSet(dial, pairs.keys, pairs.values, ttlMs, stdout, stderr))
	case opGet, opGetVersioned, opSet, opDel, opPing, opInfo, opCluster, opRing:
		key, value := "", ""
		if len(args) > 0 {
			key = args[0]
		}
		if len(args) > 1 {
			value = args[1]
		}
		return codeError(runSingle(dial, string(op), key, value, ttlMs, stdout, stderr))
	case opCounterIncr, opCounterDecr:
		if len(args) == 0 {
			fmt.Fprintf(stderr, "error: %s requires a key\n", op)
			return exitWithCode(2)
		}

		delta := uint64(1)
		if len(args) > 1 {
			parsed, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				fmt.Fprintln(stderr, "error: invalid delta (want 0..2^64-1)")
				return exitWithCode(2)
			}
			delta = parsed
		}
		return codeError(runCounterAdjust(dial, args[0], delta, op == opCounterDecr, stdout, stderr))
	case opCounterGet:
		if len(args) == 0 {
			fmt.Fprintln(stderr, "error: counter-get requires a key")
			return exitWithCode(2)
		}
		return codeError(runCounterGet(dial, args[0], stdout, stderr))
	default:
		fmt.Fprintf(stderr, "error: unknown command: %s\n", op)
		return exitWithCode(1)
	}
}

func codeError(code int) error {
	if code == 0 {
		return nil
	}
	return exitWithCode(code)
}

// extractTTL preserves the established `mset KEY VALUE --ttl MS` spelling,
// since command flags otherwise parse before positional arguments.
func extractTTL(args []string) ([]string, string, error) {
	var out []string
	ttl := ""
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--ttl" {
			if i+1 >= len(args) {
				return nil, "", fmt.Errorf("flag --ttl requires an argument")
			}

			ttl = args[i+1]
			i++
			continue
		}
		if v, ok := strings.CutPrefix(a, "--ttl="); ok {
			ttl = v
			continue
		}
		out = append(out, a)
	}
	return out, ttl, nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
