package main

import (
	"context"
	"crypto/tls"
	"encoding/json/v2"
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

func dialAdmin(options connectionOptions, stderr io.Writer) (*client.Client, error) {
	var tlsCfg *tls.Config
	if options.useTLS {
		var err error
		tlsCfg, err = tlscfg.Client(options.tlsCert, options.tlsKey, options.tlsCA)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return nil, cli.Exit("", 1)
		}
	}

	addr := net.JoinHostPort(options.host, strconv.Itoa(options.port))
	if tlsCfg != nil {
		c, err := client.DialTLS(addr, 5*time.Second, tlsCfg)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return nil, cli.Exit("", 1)
		}
		return c, nil
	}

	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return nil, cli.Exit("", 1)
	}
	return c, nil
}

func adminAction(op adminOperation, stdout, stderr io.Writer) cli.ActionFunc {
	return func(_ context.Context, cmd *cli.Command) error {
		return executeAdmin(op, readConnectionOptions(cmd), stdout, stderr)
	}
}

func drainAction(stdout, stderr io.Writer) cli.ActionFunc {
	return func(_ context.Context, cmd *cli.Command) error {
		return executeDrain(readConnectionOptions(cmd), int64(cmd.Int("timeout-ms")), stdout, stderr)
	}
}

func executeAdmin(op adminOperation, options connectionOptions, stdout, stderr io.Writer) error {
	c, err := dialAdmin(options, stderr)
	if err != nil {
		return err
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

// drainRequest is the OpAdminDrain start payload.
type drainRequest struct {
	TimeoutMs int64 `json:"timeout_ms"`
}

// drainStatus mirrors the server's drain progress body.
type drainStatus struct {
	State         string `json:"state"`
	RemainingKeys int    `json:"remaining_keys"`
}

// executeDrain starts a graceful drain (idempotent when one is running) and
// polls progress until it completes. A transport failure on the start
// request is probed: a reachable server that cannot parse the opcode
// predates drain support, which is reported instead of killing the node.
func executeDrain(options connectionOptions, timeoutMs int64, stdout, stderr io.Writer) error {
	c, err := dialAdmin(options, stderr)
	if err != nil {
		return err
	}

	defer c.Close()
	c.SetSecrets(client.Secrets{Client: options.clientSecret})
	body, err := json.Marshal(drainRequest{TimeoutMs: timeoutMs})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return cli.Exit("", 1)
	}

	res, err := c.Do(protocol.Request{Opcode: protocol.OpAdminDrain, Value: body})
	if err != nil {
		if pingable(options) {
			fmt.Fprintln(stderr, "error: server does not support drain (needs a build with OpAdminDrain)")
			return cli.Exit("", 1)
		}

		fmt.Fprintln(stderr, "error:", err)
		return cli.Exit("", 1)
	}

	st, ok := drainOutcome(res, stdout, stderr)
	if !ok {
		return cli.Exit("", 1)
	}
	if st == "drained" {
		return nil
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond).Add(30 * time.Second)
	last := ""
	for {
		if time.Now().After(deadline) {
			fmt.Fprintln(stderr, "error: drain poll timed out")
			return cli.Exit("", 1)
		}

		time.Sleep(time.Second)
		res, err := c.Do(protocol.Request{Opcode: protocol.OpAdminDrain})
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return cli.Exit("", 1)
		}

		st, ok := drainOutcome(res, stdout, stderr)
		if !ok {
			return cli.Exit("", 1)
		}
		if st == last {
			continue
		}

		last = st
		if st == "drained" {
			return nil
		}
		if st == "timed-out" {
			fmt.Fprintln(stderr, "error: drain timed out server-side; retry with a larger --timeout-ms")
			return cli.Exit("", 1)
		}
	}
}

// drainOutcome prints one status body and reports whether polling should
// continue. A bare OK (no body) means the server predates drain support.
func drainOutcome(res protocol.Response, stdout, stderr io.Writer) (string, bool) {
	if res.Status == protocol.StatusPermissionDenied {
		fmt.Fprintln(stderr, "error: permission denied")
		return "", false
	}
	if !res.HasValue {
		fmt.Fprintln(stderr, "error: server does not support drain (got bare OK)")
		return "", false
	}

	var st drainStatus
	if err := json.Unmarshal(res.Value, &st); err != nil || st.State == "" {
		var serverErr struct {
			Err string `json:"error"`
		}
		if uerr := json.Unmarshal(res.Value, &serverErr); uerr == nil && serverErr.Err != "" {
			fmt.Fprintln(stderr, "error:", serverErr.Err)
		} else {
			fmt.Fprintln(stderr, "error: unparsable drain status")
		}
		return "", false
	}
	if st.State == "drained" {
		// Remaining keys, if any, landed after the final migration
		// pass; the data is safe on the new holders, so shut down to
		// finish.
		fmt.Fprintln(stdout, "drain drained")
		return st.State, true
	}

	fmt.Fprintf(stdout, "drain %s: %d keys remaining\n", st.State, st.RemainingKeys)
	return st.State, true
}

// pingable reports whether the node answers at all, distinguishing a
// server that rejects the drain opcode from one that is simply down.
func pingable(options connectionOptions) bool {
	c, err := dialAdmin(options, io.Discard)
	if err != nil {
		return false
	}

	defer c.Close()
	c.SetSecrets(client.Secrets{Client: options.clientSecret})
	res, err := c.Do(protocol.Request{Opcode: protocol.OpPing})
	return err == nil && res.Status == protocol.StatusOK
}
