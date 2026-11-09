// Command cinder-cli is the Cinder command-line client (Go port).
//
// Usage: cinder-cli [--host H] [-p PORT] <command> [key] [value] [extra...]
//
//	[--ttl MS] [--client-secret S]
//
// Commands mirror tools/cinder_cli.cpp: get|set|del|ping|info|cluster|ring|
// mget|mset|get-versioned, plus counter-incr|counter-decr|counter-get
// (new surface: the retired C++ CLI never had counter commands; these
// follow the existing output contract). Output formats are byte-identical
// by design (tests assert them): values print raw, misses print
// "(not found)", get-versioned appends LWW metadata, mget/mset print
// "key: value" lines, counter-get prints the int64 value.
// TLS flags are parsed for compatibility but rejected until the TLS
// milestone lands.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

var version = "dev"

// statusString mirrors cinder::toString(Errc): lowercase parenthesized
// words, not the Go Status.String() CamelCase used in logs.
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
	fs := flag.NewFlagSet("cinder-cli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		host         string
		port         int
		verbose      bool
		useTLS       bool
		tlsCert      string
		tlsKey       string
		tlsCA        string
		clientSecret string
		ttlMs        int
	)

	fs.StringVar(&host, "host", "127.0.0.1", "Server host")
	fs.IntVar(&port, "port", 7000, "Server port")
	fs.IntVar(&port, "p", 7000, "Server port (shorthand)")
	fs.BoolVar(&verbose, "verbose", false, "Enable verbose (debug) logging")
	fs.BoolVar(&verbose, "v", false, "Enable verbose (debug) logging (shorthand)")
	fs.BoolVar(&useTLS, "tls", false, "Enable TLS encryption")
	fs.StringVar(&tlsCert, "tls-cert", "", "Path to TLS certificate chain (PEM)")
	fs.StringVar(&tlsKey, "tls-key", "", "Path to TLS private key (PEM)")
	fs.StringVar(&tlsCA, "tls-ca", "", "Path to CA certificate for peer verification (PEM)")
	fs.StringVar(&clientSecret, "client-secret", "", "Shared secret for client authentication (or CINDER_CLIENT_SECRET)")
	fs.IntVar(&ttlMs, "ttl", 0, "TTL in ms (set/mset only)")
	showVersion := fs.Bool("version", false, "print version and exit")
	// Go's flag package stops parsing at the first positional, but
	// `mset c 3 --ttl 60000` places the flag last (like the C++ CLI
	// tests). Pre-extract --ttl/--ttl= so positionals can follow it.
	args, ttlStr, ttlErr := extractTTL(args)
	if ttlErr != nil {
		fmt.Fprintln(stderr, "error:", ttlErr)
		return 2
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if ttlStr != "" && ttlMs == 0 {
		n, err := strconv.Atoi(ttlStr)
		if err != nil || n < 0 {
			fmt.Fprintln(stderr, "error: invalid --ttl value")
			return 2
		}
		ttlMs = n
	}
	if *showVersion {
		fmt.Fprintf(stdout, "cinder-cli (go) %s\n", version)
		return 0
	}

	var tlsCfg *tls.Config
	if useTLS {
		var err error
		tlsCfg, err = tlscfg.Client(tlsCert, tlsKey, tlsCA)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
	}
	if clientSecret == "" {
		clientSecret = os.Getenv("CINDER_CLIENT_SECRET")
	}

	pos := fs.Args()
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "error: command required: get|set|del|ping|info|cluster|ring|mget|mset|get-versioned|counter-incr|counter-decr|counter-get")
		return 2
	}

	cmd, rest := pos[0], pos[1:]
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	auth := client.Secrets{Client: clientSecret}
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

	switch cmd {
	case "mget":
		if len(rest) == 0 {
			fmt.Fprintln(stderr, "error: mget requires at least one key")
			return 1
		}
		return runMultiGet(dial, rest, stdout, stderr)
	case "mset":
		if len(rest) == 0 || len(rest)%2 != 0 {
			fmt.Fprintln(stderr, "error: mset requires key/value pairs")
			return 1
		}

		var keys, values []string
		for i := 0; i < len(rest); i += 2 {
			keys = append(keys, rest[i])
			values = append(values, rest[i+1])
		}
		return runMultiSet(dial, keys, values, ttlMs, stdout, stderr)
	case "get", "get-versioned", "set", "del", "ping", "info", "cluster", "ring":
		key, value := "", ""
		if len(rest) > 0 {
			key = rest[0]
		}
		if len(rest) > 1 {
			value = rest[1]
		}
		return runSingle(dial, cmd, key, value, ttlMs, stdout, stderr)
	case "counter-incr", "counter-decr":
		if len(rest) == 0 {
			fmt.Fprintln(stderr, "error: "+cmd+" requires a key")
			return 2
		}
		var delta uint64 = 1
		if len(rest) > 1 {
			var err error
			delta, err = strconv.ParseUint(rest[1], 10, 64)
			if err != nil {
				fmt.Fprintln(stderr, "error: invalid delta (want 0..2^64-1)")
				return 2
			}
		}
		return runCounterAdjust(dial, rest[0], delta, cmd == "counter-decr", stdout, stderr)
	case "counter-get":
		if len(rest) == 0 {
			fmt.Fprintln(stderr, "error: counter-get requires a key")
			return 2
		}
		return runCounterGet(dial, rest[0], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "error: unknown command: %s\n", cmd)
		return 1
	}
}

// extractTTL pulls a trailing/embedded --ttl out of raw args so flags can
// follow positionals. Returns the remaining args and the raw ttl string.
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

func runSingle(
	dial func() (*client.Client, error),
	cmd, key, value string, ttlMs int,
	stdout, stderr io.Writer,
) int {
	var req protocol.Request
	switch cmd {
	case "get":
		req = protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)}
	case "get-versioned":
		req = protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key)}
	case "set":
		req = protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(value)}
		if ttlMs > 0 {
			req.HasTTL = true
			req.TTL = time.Duration(ttlMs) * time.Millisecond
		}
	case "del":
		req = protocol.Request{Opcode: protocol.OpDel, Key: []byte(key)}
	case "ping":
		req = protocol.Request{Opcode: protocol.OpPing}
	case "info":
		req = protocol.Request{Opcode: protocol.OpAdminInfo}
	case "cluster":
		req = protocol.Request{Opcode: protocol.OpAdminCluster}
	case "ring":
		req = protocol.Request{Opcode: protocol.OpAdminRing}
	}

	c, err := dial()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	defer c.Close()
	res, err := c.Do(req)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	if res.Status == protocol.StatusPermissionDenied {
		fmt.Fprintln(stderr, "error: permission denied")
		return 1
	}
	if cmd == "ping" {
		fmt.Fprintln(stdout, "pong")
		return 0
	}
	if res.Tombstone {
		fmt.Fprintln(stdout, "tombstone")
		fmt.Fprintln(stdout, "version:", res.Version)
		fmt.Fprintln(stdout, "writer:", res.WriterNodeHash)
		return 0
	}
	if res.HasValue {
		fmt.Fprintln(stdout, string(res.Value))
		if cmd == "get-versioned" {
			fmt.Fprintln(stdout, "version:", res.Version)
			fmt.Fprintln(stdout, "writer:", res.WriterNodeHash)
			if res.HasExpiresAt {
				fmt.Fprintln(stdout, "expires_at:", res.ExpiresAt.Unix())
			}
		}
		return 0
	}

	fmt.Fprintln(stdout, statusString(res.Status))
	return 0
}

// runCounterAdjust applies one increment/decrement. The server answers
// plain OK (no state), so output mirrors set: "OK" or a status word.
func runCounterAdjust(
	dial func() (*client.Client, error),
	key string, delta uint64, decrement bool,
	stdout, stderr io.Writer,
) int {
	c, err := dial()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer c.Close()
	res, err := c.CounterAdjust(key, delta, decrement)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	if res.Status == protocol.StatusPermissionDenied {
		fmt.Fprintln(stderr, "error: permission denied")
		return 1
	}
	fmt.Fprintln(stdout, statusString(res.Status))
	return 0
}

// runCounterGet decodes the PN-counter state and prints the int64 value;
// a miss prints "(not found)" like get.
func runCounterGet(
	dial func() (*client.Client, error),
	key string,
	stdout, stderr io.Writer,
) int {
	c, err := dial()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer c.Close()
	res, err := c.CounterGet(key)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	if res.Status == protocol.StatusPermissionDenied {
		fmt.Fprintln(stderr, "error: permission denied")
		return 1
	}
	if res.Status == protocol.StatusOK && res.HasValue {
		st, err := counter.DecodePNCounter(res.Value)
		if err != nil {
			fmt.Fprintln(stderr, "error: corrupt counter state")
			return 1
		}
		fmt.Fprintln(stdout, st.Value())
		return 0
	}
	fmt.Fprintln(stdout, statusString(res.Status))
	return 0
}

func runMultiGet(dial func() (*client.Client, error),
	keys []string,
	stdout, stderr io.Writer,
) int {
	c, err := dial()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	defer c.Close()
	failed := false
	for _, k := range keys {
		res, err := c.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(k)})
		switch {
		case err != nil:
			fmt.Fprintf(stdout, "%s: %s\n", k, err)
			failed = true
		case res.HasValue:
			fmt.Fprintf(stdout, "%s: %s\n", k, res.Value)
		default:
			fmt.Fprintf(stdout, "%s: %s\n", k, statusString(res.Status))
			failed = true
		}
	}
	if failed {
		return 1
	}
	return 0
}

func runMultiSet(
	dial func() (*client.Client, error),
	keys, values []string, ttlMs int,
	stdout, stderr io.Writer,
) int {
	c, err := dial()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	defer c.Close()
	failed := false
	for i := range keys {
		req := protocol.Request{Opcode: protocol.OpSet, Key: []byte(keys[i]), Value: []byte(values[i])}
		if ttlMs > 0 {
			req.HasTTL = true
			req.TTL = time.Duration(ttlMs) * time.Millisecond
		}

		res, err := c.Do(req)
		switch {
		case err != nil:
			fmt.Fprintf(stdout, "%s: %s\n", keys[i], err)
			failed = true
		case res.Status == protocol.StatusOK:
			fmt.Fprintf(stdout, "%s: OK\n", keys[i])
		default:
			fmt.Fprintf(stdout, "%s: %s\n", keys[i], statusString(res.Status))
			failed = true
		}
	}
	if failed {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
