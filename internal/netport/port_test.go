package netport

import (
	"strings"
	"testing"
)

func TestParsePort(t *testing.T) {
	for _, s := range []string{"1", "80", "7002", "65535"} {
		p, err := ParsePort(s)
		if err != nil || p.String() != s {
			t.Fatalf("ParsePort(%q) = %v, %v", s, p, err)
		}
	}
	for _, s := range []string{"", "0", "-1", "65536", "abc", " 80", "+80", "0x50", "[::1]:80"} {
		if _, err := ParsePort(s); err == nil {
			t.Fatalf("ParsePort(%q) accepted", s)
		}
	}
	var zero Port
	if zero.String() != "0" || zero != 0 {
		t.Fatalf("zero Port = %q", zero.String())
	}
	if !strings.Contains((&ParseError{Input: "x"}).Error(), "x") {
		t.Fatal("ParseError must name the input")
	}
}
