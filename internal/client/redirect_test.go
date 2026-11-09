package client

import "testing"

func TestParseRedirect(t *testing.T) {
	if got := ParseRedirect("moved to node2"); got != "node2" {
		t.Fatalf("got %q", got)
	}
	if got := ParseRedirect("moved to node2@127.0.0.1:7002"); got != "node2" {
		t.Fatalf("id-only, got %q", got)
	}
	for _, bad := range []string{"", "moved node2", "OK", "moved to "} {
		if got := ParseRedirect(bad); got != "" {
			t.Fatalf("%q: got %q", bad, got)
		}
	}
}

func TestParseRedirectTarget(t *testing.T) {
	tg, ok := ParseRedirectTarget("moved to node2@127.0.0.1:7002")
	if !ok || tg.ID != "node2" || tg.Host != "127.0.0.1" || tg.Port != 7002 || !tg.HasAddress() {
		t.Fatalf("got %+v, %v", tg, ok)
	}
	tg, ok = ParseRedirectTarget("moved to node2")
	if !ok || tg.ID != "node2" || tg.HasAddress() {
		t.Fatalf("id-only: got %+v, %v", tg, ok)
	}
	// Unparsable addresses degrade to id-only rather than failing.
	for _, v := range []string{"moved to n@:7002", "moved to n@h:", "moved to n@h:0", "moved to n@h:99999", "moved to n@h:abc"} {
		tg, ok = ParseRedirectTarget(v)
		if !ok || tg.ID != "n" || tg.HasAddress() {
			t.Fatalf("%q: got %+v, %v", v, tg, ok)
		}
	}
	if _, ok := ParseRedirectTarget("nope"); ok {
		t.Fatal("non-redirect accepted")
	}
}
