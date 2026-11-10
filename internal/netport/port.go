// Package netport is the single validated port type shared by gossip
// membership, the client pool, peer parsing, and redirects. Ports were
// three spellings (int, uint16, and range-checked strings) with
// conversions at every boundary; one type with one parser removes the
// int/uint16 friction and the tripled range checks. Wire formats are
// untouched: gossip still renders %d text, admin JSON still a number.
package netport

import (
	"strconv"
)

// Port is a validated TCP port (1-65535). The zero value means absent,
// matching every HasAddress-style check in the tree.
type Port uint16

// ParsePort parses a decimal port, rejecting empty, non-numeric, zero,
// and out-of-range values (one check replacing the gossip, redirect,
// and --peers validators). Only ASCII digits are accepted: no signs,
// spaces, or prefixes.
func ParsePort(s string) (Port, error) {
	if s == "" {
		return 0, &ParseError{Input: s}
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, &ParseError{Input: s}
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 || n > 65535 {
		return 0, &ParseError{Input: s}
	}
	return Port(n), nil
}

// ParseError reports a rejected port literal.
type ParseError struct {
	Input string
}

func (e *ParseError) Error() string { return "netport: bad port " + strconv.Quote(e.Input) }

// String renders the decimal form for JoinHostPort and gossip views.
func (p Port) String() string { return strconv.Itoa(int(p)) }
