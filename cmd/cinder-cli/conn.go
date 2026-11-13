package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

type connectionOptions struct {
	host, tlsCert, tlsKey, tlsCA, clientSecret string
	port                                       int
	useTLS                                     bool
}

func exitWithCode(code int) error { return cli.Exit("", code) }

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
