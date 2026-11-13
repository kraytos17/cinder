package main

import (
	"context"
	"crypto/tls"
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
