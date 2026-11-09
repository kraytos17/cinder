// Command cinder-admin is the Cinder admin CLI (Go port).
//
// Usage: cinder-admin [--host H] [-p PORT] <command>
// Commands mirror tools/cinder_admin.cpp:
// info|cluster|ring|compact|config-reload|shutdown. A valued response
// prints raw; a valueless OK prints "OK"; other statuses print the
// lowercase C++ status word.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

var version = "dev"

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
	fs := flag.NewFlagSet("cinder-admin", flag.ContinueOnError)
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
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "cinder-admin (go) %s\n", version)
		return 0
	}
	_ = verbose
	_ = tlsCert
	_ = tlsKey
	_ = tlsCA
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
	_ = clientSecret // accepted for compat; not enforced yet
	pos := fs.Args()
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "error: command required: info|cluster|ring|compact|config-reload|shutdown")
		return 2
	}
	cmd := pos[0]
	var op protocol.Opcode
	switch cmd {
	case "info":
		op = protocol.OpAdminInfo
	case "cluster":
		op = protocol.OpAdminCluster
	case "ring":
		op = protocol.OpAdminRing
	case "compact":
		op = protocol.OpAdminCompact
	case "config-reload":
		op = protocol.OpAdminConfigReload
	case "shutdown":
		op = protocol.OpAdminShutdown
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", cmd)
		fmt.Fprintln(stderr, "available commands: info, cluster, ring, compact, config-reload, shutdown")
		return 1
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dial := func() (*client.Client, error) {
		if tlsCfg != nil {
			return client.DialTLS(addr, 5*time.Second, tlsCfg)
		}
		return client.Dial(addr, 5*time.Second)
	}
	c, err := dial()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer c.Close()
	res, err := c.Do(protocol.Request{Opcode: op})
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	if res.Status == protocol.StatusPermissionDenied {
		fmt.Fprintln(stderr, "error: permission denied")
		return 1
	}
	if res.HasValue {
		fmt.Fprintln(stdout, string(res.Value))
	} else if res.Status == protocol.StatusOK {
		fmt.Fprintln(stdout, "OK")
	} else {
		fmt.Fprintln(stdout, statusString(res.Status))
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
