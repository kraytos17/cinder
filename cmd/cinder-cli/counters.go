package main

// Counter verbs: counter-incr, counter-decr, and counter-get.

import (
	"fmt"
	"io"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// runCounterAdjust applies one increment or decrement and prints the
// operation status.
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

// runCounterGet decodes and prints the counter value, or prints the status
// when the key is absent or the operation fails.
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
