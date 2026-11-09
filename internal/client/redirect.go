package client

import (
	"strings"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

// RedirectTarget is a parsed ownership redirect: the owner id plus, when
// the server knew it, a reachable address for nodes outside the client's
// configured set. Mirrors parseRedirectTarget in cache_client.hpp.
type RedirectTarget struct {
	ID   string
	Host string
	Port int
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
	colon := strings.LastIndex(addr, ":")
	if colon <= 0 {
		return target, true
	}
	var port int
	for _, d := range addr[colon+1:] {
		if d < '0' || d > '9' {
			return target, true
		}
		port = port*10 + int(d-'0')
		if port > 65535 {
			return target, true
		}
	}
	if port == 0 {
		return target, true
	}
	target.Host = addr[:colon]
	target.Port = port
	return target, true
}

// IsNotReady reports whether res is a followable ownership redirect.
func IsNotReady(res protocol.Response) bool {
	return res.Status == protocol.StatusNotReady
}
