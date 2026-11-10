package cluster

import (
	"strings"
	"testing"
)

// Seed views: valid representatives plus truncation and garbage edges.
// `go test` runs these as unit tests; `go test -fuzz=.` explores from them.
var fuzzViewSeeds = []string{
	"",
	"n1@127.0.0.1:7000:alive:3",
	"n1@127.0.0.1:7000:alive:3;n2@10.0.0.1:7001:suspect:7",
	"n1@127.0.0.1:7000:alive:3;",
	";n1@127.0.0.1:7000:alive:3;;garbage",
	"n1@h:1:dead:0",
	"@@@@::::",
	"n@h:notaport:alive:3",
	"n@h:70000:alive:3",
	"n@h:1:zombie:3",
	"n@h:1:alive:notanum",
	"n@h:1:alive:18446744073709551616",
	"a:b@c:d:e:f:g",
}

// FuzzGossipView feeds arbitrary strings to the decoder. Decoding must not
// panic, and every accepted entry must remain stable through
// decode-encode-decode because EncodeView emits a canonical representation.
func FuzzGossipView(f *testing.F) {
	for _, seed := range fuzzViewSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		view := DecodeView(value)
		if len(view) == 0 {
			return
		}
		again := EncodeView(view)
		back := DecodeView(again)
		if len(back) != len(view) {
			t.Fatalf("not a fixed point: %q -> %d nodes -> %q -> %d nodes", value, len(view), again, len(back))
		}
		for i := range view {
			if back[i] != view[i] {
				t.Fatalf("node %d differs:\n%+v\n%+v", i, view[i], back[i])
			}
		}
		if strings.Count(again, ";")+1 != len(view) {
			t.Fatalf("separator count wrong in %q", again)
		}
	})
}
