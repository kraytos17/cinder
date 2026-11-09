package auth

import (
	"testing"
	"time"
)

func TestNodeTokenRoundTrip(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	tok := NodeToken("secret", "node1", at)
	if len(tok) != 32 {
		t.Fatalf("token len = %d", len(tok))
	}
	if !VerifyNodeToken("secret", "node1", tok, at) {
		t.Fatal("own token rejected")
	}
}

func TestNodeTokenEmptySecretFailsClosed(t *testing.T) {
	at := time.Now()
	if tok := NodeToken("", "node1", at); tok != nil {
		t.Fatal("empty secret must not issue a token")
	}
	if VerifyNodeToken("", "node1", make([]byte, 32), at) {
		t.Fatal("empty secret must never verify")
	}
}

func TestNodeTokenWindow(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	tok := NodeToken("secret", "node1", at)
	// Inside the 30s acceptance span (plus rounding): still valid.
	if !VerifyNodeToken("secret", "node1", tok, at.Add(25*time.Second)) {
		t.Fatal("token within window rejected")
	}
	// Well past it: invalid.
	if VerifyNodeToken("secret", "node1", tok, at.Add(120*time.Second)) {
		t.Fatal("stale token accepted")
	}
}

func TestNodeTokenWrongSecretNodeLength(t *testing.T) {
	at := time.Now()
	tok := NodeToken("secret-a", "node1", at)
	if VerifyNodeToken("secret-b", "node1", tok, at) {
		t.Fatal("wrong secret accepted")
	}
	if VerifyNodeToken("secret-a", "node2", tok, at) {
		t.Fatal("wrong node accepted")
	}
	if VerifyNodeToken("secret-a", "node1", nil, at) {
		t.Fatal("empty token accepted")
	}
	if VerifyNodeToken("secret-a", "node1", []byte("short"), at) {
		t.Fatal("short token accepted")
	}
	long := make([]byte, 64)
	if VerifyNodeToken("secret-a", "node1", long, at) {
		t.Fatal("long token accepted")
	}
}

func TestClientTokenRoundTrip(t *testing.T) {
	tok := ClientToken("client-secret")
	if len(tok) != 32 {
		t.Fatalf("token len = %d", len(tok))
	}
	if tok2 := ClientToken("client-secret"); string(tok) != string(tok2) {
		t.Fatal("client token must be deterministic")
	}
	if !VerifyClientToken("client-secret", tok) {
		t.Fatal("own token rejected")
	}
	if VerifyClientToken("other", tok) {
		t.Fatal("wrong secret accepted")
	}
	if VerifyClientToken("", tok) {
		t.Fatal("empty secret accepted")
	}
	if ClientToken("") != nil {
		t.Fatal("empty secret must not issue a token")
	}
}

func TestCrossTypeConfusion(t *testing.T) {
	at := time.Now()
	nodeTok := NodeToken("same-secret", "node1", at)
	if VerifyClientToken("same-secret", nodeTok) {
		t.Fatal("node token accepted as client token")
	}
	if VerifyNodeToken("same-secret", "node1", ClientToken("same-secret"), at) {
		t.Fatal("client token accepted as node token")
	}
}
