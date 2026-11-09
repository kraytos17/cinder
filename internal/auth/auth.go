// Package auth implements Cinder's credential scheme (Go port of
// cinder/common/hmac.hpp, byte-compatible by construction).
//
// Two credentials, two scopes. Node tokens authenticate cluster-internal
// traffic: HMAC-SHA256(secret, nodeID + ":" + unixSeconds/10), i.e. bound
// to the sender and to a 10-second window for clock-skew tolerance.
// Verification accepts the windows covering the last 30 seconds.
// Client tokens authenticate front-door traffic: HMAC-SHA256(secret,
// "cinder-client-v1"), static and therefore replayable by design —
// only usable over TLS (the server refuses to enable it otherwise).
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"strconv"
	"time"
)

// WindowSeconds bounds node-token acceptance: windows covering this span
// plus one for rounding, mirroring verifyAuthToken's default.
const WindowSeconds = 30

// windowSeconds is the token time quantum.
const windowSeconds = 10

// clientDomain separates the static client token from node tokens.
const clientDomain = "cinder-client-v1"

// NodeToken issues the credential for nodeID at time at: raw 32-byte
// HMAC-SHA256, empty when the secret is empty. Like the C++ build without
// CINDER_ENABLE_CRYPTO, an empty secret fails closed (no token issued).
func NodeToken(secret, nodeID string, at time.Time) []byte {
	if secret == "" {
		return nil
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(nodeID + ":" + strconv.FormatInt(at.Unix()/windowSeconds, 10)))
	return mac.Sum(nil)
}

// VerifyNodeToken reports whether token is a NodeToken for nodeID valid
// around at: the current 10-second window plus the WindowSeconds span
// behind it (plus one window for rounding), mirroring verifyAuthToken.
// Rejects empty secrets, empty tokens, and non-32-byte tokens.
func VerifyNodeToken(secret, nodeID string, token []byte, at time.Time) bool {
	if secret == "" || len(token) != sha256.Size {
		return false
	}
	base := at.Unix() / windowSeconds
	for i := int64(0); i <= WindowSeconds/windowSeconds+1; i++ {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(nodeID + ":" + strconv.FormatInt(base-i, 10)))
		if subtle.ConstantTimeCompare(token, mac.Sum(nil)) == 1 {
			return true
		}
	}
	return false
}

// ClientToken issues the static front-door credential: raw 32-byte
// HMAC-SHA256, empty when the secret is empty (fail closed).
func ClientToken(secret string) []byte {
	if secret == "" {
		return nil
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(clientDomain))
	return mac.Sum(nil)
}

// VerifyClientToken reports whether token equals ClientToken(secret) in
// constant time. Empty secrets never verify.
func VerifyClientToken(secret string, token []byte) bool {
	if secret == "" {
		return false
	}
	return subtle.ConstantTimeCompare(token, ClientToken(secret)) == 1
}
