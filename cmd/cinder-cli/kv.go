package main

// Single-key verbs: get, set, del, ping, info, cluster, ring, and
// get-versioned. Values print raw; misses print "(not found)". CLI tests
// cover the output format.

import (
	"fmt"
	"io"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// runSingle executes one single-key command and formats its response.
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
			if exp, ok := res.ExpiresAt.Time(); ok {
				fmt.Fprintln(stdout, "expires_at:", exp.Unix())
			}
		}
		return 0
	}

	fmt.Fprintln(stdout, statusString(res.Status))
	return 0
}

// runCounterAdjust applies one increment/decrement. The server answers
// plain OK (no state), so output follows the single-key command format.
