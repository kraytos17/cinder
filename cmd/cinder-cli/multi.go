package main

// Multi-key verbs: mget prints "key: value" lines, mset applies
// batches. Output formats are byte-identical by design.

import (
	"fmt"
	"io"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// statusString mirrors cinder::toString(Errc): lowercase parenthesized
// words, not the Go Status.String() CamelCase used in logs.

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
