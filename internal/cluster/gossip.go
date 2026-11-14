package cluster

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/obs"
)

// Sender delivers one request; *client.Pool satisfies it.
type Sender interface {
	Send(node string, req protocol.Request) (protocol.Response, error)
}

// GossipManager disseminates this node's view (;-delimited text in a
// Gossip request) to a random peer per tick, and applies incoming views
// via incarnation-guarded rumors.
type GossipManager struct {
	clock     func() time.Time
	transport Sender
	table     *MembershipTable
	self      string
	interval  time.Duration

	mu     sync.Mutex
	peers  []string
	met    atomic.Pointer[metrics.Collector]
	logger atomic.Pointer[zerolog.Logger]
}

// NewGossipManager builds a gossip manager; Start loads peers and wires
// dynamic arrivals.
func NewGossipManager(clock func() time.Time, t Sender, table *MembershipTable, self string, interval time.Duration) *GossipManager {
	if clock == nil {
		clock = time.Now
	}
	return &GossipManager{clock: clock, transport: t, table: table, self: self, interval: interval}
}

// Start loads peers and rebuilds them on every membership change.
func (g *GossipManager) Start() {
	g.mu.Lock()
	g.rebuildLocked()
	g.mu.Unlock()
	g.table.OnChange(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.rebuildLocked()
	})
}

// SetMetrics wires the observability collector (nil disables).
func (g *GossipManager) SetMetrics(m *metrics.Collector) {
	g.met.Store(m)
}

// SetLogger installs the operational logger (nil selects Nop).
func (g *GossipManager) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	g.logger.Store(l)
}

func (g *GossipManager) log() *zerolog.Logger {
	if l := g.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// Tick sends this node's view to a random peer.
func (g *GossipManager) Tick() {
	g.mu.Lock()
	if len(g.peers) == 0 {
		g.mu.Unlock()
		return
	}

	target := g.peers[rand.IntN(len(g.peers))]
	g.mu.Unlock()
	obs.IfPresent(&g.met, func(m *metrics.Collector) { m.GossipRounds.Add(1) })
	if e := g.log().Debug(); e != nil {
		e.Str("target", target).Msg("gossip")
	}

	req := protocol.Request{Opcode: protocol.OpGossip, Key: nil, Value: []byte(EncodeView(g.table.Snapshot()))}
	g.transport.Send(target, req)
}

// HandleMessage applies one incoming gossip view. The sender id is
// best-effort (the connection layer knows only its own node id); debug
// logs tag it for trace correlation.
func (g *GossipManager) HandleMessage(from string, value []byte) {
	if e := g.log().Debug(); e != nil {
		e.Str("from", from).Int("bytes", len(value)).Msg("gossip received")
	}
	g.table.ApplyRumors(DecodeView(string(value)))
}

// Leave bumps self to Dead (overriding stale Alive rumors) and broadcasts
// the view to every peer so they don't suspect during teardown.
func (g *GossipManager) Leave() {
	g.table.MarkDead(g.self)
	g.mu.Lock()
	targets := append([]string(nil), g.peers...)
	g.mu.Unlock()
	g.log().Info().Msg("leaving cluster")
	req := protocol.Request{Opcode: protocol.OpGossip, Value: []byte(EncodeView(g.table.Snapshot()))}
	for _, peer := range targets {
		g.transport.Send(peer, req)
	}
}

// Interval returns the gossip interval.
func (g *GossipManager) Interval() time.Duration { return g.interval }

// SetInterval changes the gossip interval at runtime.
func (g *GossipManager) SetInterval(d time.Duration) { g.interval = d }

func (g *GossipManager) rebuildLocked() {
	g.peers = g.peers[:0]
	for _, m := range g.table.Snapshot() {
		if m.ID != g.self {
			g.peers = append(g.peers, m.ID)
		}
	}
	slices.Sort(g.peers)
}

// EncodeView renders "id@host:port:state:incarnation" entries joined by ';'.
// The output bytes are the frozen wire format (see TestInteropGossipWithCpp);
// optimizations here must not change a single byte.
func EncodeView(view []NodeInfo) string {
	var b strings.Builder
	if len(view) > 0 {
		b.Grow(len(view) * 64)
	}

	var numbuf [20]byte
	for i, m := range view {
		if i > 0 {
			b.WriteByte(';')
		}

		b.WriteString(m.ID)
		b.WriteByte('@')
		b.WriteString(m.Host)
		b.WriteByte(':')
		b.Write(strconv.AppendUint(numbuf[:0], uint64(m.Port), 10))
		b.WriteByte(':')
		b.WriteString(m.State.String())
		b.WriteByte(':')
		b.Write(strconv.AppendUint(numbuf[:0], m.Incarnation, 10))
	}
	return b.String()
}

// ParseNodeInfo parses one membership entry of the form
// id@host:port:state:incarnation. Malformed fields return false.
func ParseNodeInfo(entry string) (NodeInfo, bool) {
	var out NodeInfo
	id, rest, ok := strings.Cut(entry, "@")
	if !ok {
		return out, false
	}

	out.ID = id
	host, rest, ok := strings.Cut(rest, ":")
	if !ok {
		return out, false
	}

	out.Host = host
	portStr, rest, ok := strings.Cut(rest, ":")
	if !ok {
		return out, false
	}

	port, err := strconv.Atoi(portStr)
	// Membership entries permit port 0 to represent a peer whose address
	// is not yet known. Dial and redirect inputs use strict netport parsing.
	if err != nil || port < 0 || port > 65535 {
		return out, false
	}

	out.Port = netport.Port(port)
	state, incStr, ok := strings.Cut(rest, ":")
	if !ok {
		return out, false
	}
	switch state {
	case "alive":
		out.State = Alive
	case "suspect":
		out.State = Suspect
	case "dead":
		out.State = Dead
	default:
		return out, false
	}

	inc, err := strconv.ParseUint(incStr, 10, 64)
	if err != nil {
		return out, false
	}

	out.Incarnation = inc
	return out, true
}

// DecodeView splits a view, skipping empty and malformed entries.
func DecodeView(value string) []NodeInfo {
	var out []NodeInfo
	for e := range strings.SplitSeq(value, ";") {
		if e == "" {
			continue
		}
		if info, ok := ParseNodeInfo(e); ok {
			out = append(out, info)
		}
	}
	return out
}
