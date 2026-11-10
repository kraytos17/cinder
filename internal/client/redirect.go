package client

import (
	"net"
	"net/netip"
	"strings"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

// RedirectTarget is a parsed ownership redirect: the owner id plus, when
// the server knew it, a reachable address for nodes outside the client's
// configured set. Mirrors parseRedirectTarget in cache_client.hpp.
type RedirectTarget struct {
	ID   string
	Host string
	Port netport.Port
}

// HasAddress reports whether the server attached a usable address.
func (t RedirectTarget) HasAddress() bool {
	return t.Port != 0 && t.Host != ""
}

// ParseRedirect extracts the owner id from "moved to <id>[@host:port]",
// or "" when value is not a redirect. Mirrors parseRedirect.
func ParseRedirect(value string) string {
	const prefix = "moved to "
	if !strings.HasPrefix(value, prefix) {
		return ""
	}
	rest := value[len(prefix):]
	if at := strings.Index(rest, "@"); at >= 0 {
		rest = rest[:at]
	}
	return rest
}

// ParseRedirectTarget parses the full redirect form, degrading id-only
// when the address part is unparsable. Mirrors parseRedirectTarget.
func ParseRedirectTarget(value string) (RedirectTarget, bool) {
	id := ParseRedirect(value)
	if id == "" {
		return RedirectTarget{}, false
	}
	target := RedirectTarget{ID: id}
	rest := value[len("moved to "):]
	_, after, ok := strings.Cut(rest, "@")
	if !ok {
		return target, true
	}
	addr := after
	// Prefer SplitHostPort (handles bracketed "[::1]:7002", which the
	// server now emits for v6 peers); fall back to a last-colon split
	// for the bare "host:port" and unbracketed-v6 forms the gossip
	// wire format also produces.
	host, portStr := func() (string, string) {
		if h, p, err := net.SplitHostPort(addr); err == nil {
			return h, p
		}
		if colon := strings.LastIndex(addr, ":"); colon > 0 {
			return addr[:colon], addr[colon+1:]
		}
		return "", ""
	}()
	if host == "" {
		return target, true
	}
	port, err := netport.ParsePort(portStr)
	if err != nil {
		return target, true
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.String()
	}
	target.Host = host
	target.Port = port
	return target, true
}

// IsNotReady reports whether res is a followable ownership redirect.
func IsNotReady(res protocol.Response) bool {
	return res.Status == protocol.StatusNotReady
}
