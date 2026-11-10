// Package auth issues and verifies the bearer tokens used by cluster nodes
// and external clients. Node tokens are sender-bound and time-windowed;
// client tokens are static, domain-separated credentials and must be sent
// over TLS by the server and gateway configuration.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"strconv"
	"time"
)

// WindowSeconds is the node-token acceptance look-back, in seconds.
const WindowSeconds = 30

// windowSeconds is the token time quantum.
const windowSeconds = 10

// clientDomain separates the static client token from node tokens.
const clientDomain = "cinder-client-v1"

// NodeToken returns the raw 32-byte HMAC-SHA256 token for nodeID at time at.
// An empty secret disables token issuance and returns nil.
func NodeToken(secret, nodeID string, at time.Time) []byte {
	if secret == "" {
		return nil
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(nodeID + ":" + strconv.FormatInt(at.Unix()/windowSeconds, 10)))
	return mac.Sum(nil)
}

// VerifyNodeToken reports whether token is valid for nodeID near at. It
// checks the current 10-second time bucket and prior buckets covering
// WindowSeconds plus one bucket for boundary rounding. Empty secrets and
// tokens with the wrong length are rejected.
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
