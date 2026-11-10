package main

// Counter verbs: counter-incr, counter-decr, counter-get. New
// surface (the retired C++ CLI never had counter commands); output
// follows the existing contract, counter-get prints the int64 value.

import (
	"fmt"
	"io"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// statusString mirrors cinder::toString(Errc): lowercase parenthesized
// words, not the Go Status.String() CamelCase used in logs.

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
