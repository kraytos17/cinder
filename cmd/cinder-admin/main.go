// Command cinder-admin sends administrative operations to a Cinder server.
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
	"time"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

var version = "dev"

type connectionOptions struct {
	host, tlsCert, tlsKey, tlsCA, clientSecret string
	port                                       int
	useTLS                                     bool
}

type adminOperation uint8

const (
	adminInfo adminOperation = iota
	adminCluster
	adminRing
	adminCompact
	adminConfigReload
	adminShutdown
)

func (op adminOperation) wireOpcode() protocol.Opcode {
	switch op {
	case adminInfo:
		return protocol.OpAdminInfo
	case adminCluster:
		return protocol.OpAdminCluster
	case adminRing:
		return protocol.OpAdminRing
	case adminCompact:
		return protocol.OpAdminCompact
	case adminConfigReload:
		return protocol.OpAdminConfigReload
	case adminShutdown:
		return protocol.OpAdminShutdown
	default:
		return 0
	}
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

func adminAction(op adminOperation, stdout, stderr io.Writer) cli.ActionFunc {
	return func(_ context.Context, cmd *cli.Command) error {
		return executeAdmin(op, readConnectionOptions(cmd), stdout, stderr)
	}
}

func executeAdmin(op adminOperation, options connectionOptions, stdout, stderr io.Writer) error {
	var tlsCfg *tls.Config
	if options.useTLS {
		var err error
		tlsCfg, err = tlscfg.Client(options.tlsCert, options.tlsKey, options.tlsCA)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return cli.Exit("", 1)
		}
	}

	addr := net.JoinHostPort(options.host, strconv.Itoa(options.port))
	var c *client.Client
	var err error
	if tlsCfg != nil {
		c, err = client.DialTLS(addr, 5*time.Second, tlsCfg)
	} else {
		c, err = client.Dial(addr, 5*time.Second)
	}

	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return cli.Exit("", 1)
	}

	defer c.Close()
	c.SetSecrets(client.Secrets{Client: options.clientSecret})
	res, err := c.Do(protocol.Request{Opcode: op.wireOpcode()})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return cli.Exit("", 1)
	}
	switch {
	case res.Status == protocol.StatusPermissionDenied:
		fmt.Fprintln(stderr, "error: permission denied")
		return cli.Exit("", 1)
	case res.HasValue:
		fmt.Fprintln(stdout, string(res.Value))
	case res.Status == protocol.StatusOK:
		fmt.Fprintln(stdout, "OK")
	default:
		fmt.Fprintln(stdout, statusString(res.Status))
	}
	return nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
