package cluster

import (
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kraytos17/cinder/go/internal/net/protocol"
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

	mu    sync.Mutex
	peers []string
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

// Tick sends this node's view to a random peer.
func (g *GossipManager) Tick() {
	g.mu.Lock()
	if len(g.peers) == 0 {
		g.mu.Unlock()
		return
	}
	target := g.peers[rand.IntN(len(g.peers))]
	g.mu.Unlock()
	req := protocol.Request{Opcode: protocol.OpGossip, Key: nil, Value: []byte(EncodeView(g.table.Snapshot()))}
	g.transport.Send(target, req)
}

// HandleMessage applies one incoming gossip view.
func (g *GossipManager) HandleMessage(from string, value []byte) {
	for _, rumor := range DecodeView(string(value)) {
		g.table.ApplyRumor(rumor)
	}
}

// Leave bumps self to Dead (overriding stale Alive rumors) and broadcasts
// the view to every peer so they don't suspect during teardown.
func (g *GossipManager) Leave() {
	g.table.MarkDead(g.self)
	g.mu.Lock()
	targets := append([]string(nil), g.peers...)
	g.mu.Unlock()
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
	sort.Strings(g.peers)
}

// EncodeView renders "id@host:port:state:incarnation" entries joined by ';'.
func EncodeView(view []NodeInfo) string {
	var b strings.Builder
	for i, m := range view {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(m.ID)
		b.WriteByte('@')
		b.WriteString(m.Host)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(int(m.Port)))
		b.WriteByte(':')
		b.WriteString(m.State.String())
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(m.Incarnation, 10))
	}
	return b.String()
}

// ParseEntry parses one view entry; malformed input fails. The split
// points mirror C++ parseEntry exactly: first '@', then sequential ':'.
func ParseEntry(entry string, out *NodeInfo) bool {
	idx := strings.Index(entry, "@")
	if idx < 0 {
		return false
	}
	c1rel := strings.Index(entry[idx:], ":")
	if c1rel < 0 {
		return false
	}
	c1 := idx + c1rel
	c2rel := strings.Index(entry[c1+1:], ":")
	if c2rel < 0 {
		return false
	}
	c2 := c1 + 1 + c2rel
	c3rel := strings.Index(entry[c2+1:], ":")
	if c3rel < 0 {
		return false
	}
	c3 := c2 + 1 + c3rel
	out.ID = entry[:idx]
	out.Host = entry[idx+1 : c1]
	port, err := strconv.Atoi(entry[c1+1 : c2])
	if err != nil || port < 0 || port > 65535 {
		return false
	}
	out.Port = uint16(port)
	switch entry[c2+1 : c3] {
	case "alive":
		out.State = Alive
	case "suspect":
		out.State = Suspect
	case "dead":
		out.State = Dead
	default:
		return false
	}
	inc, err := strconv.ParseUint(entry[c3+1:], 10, 64)
	if err != nil {
		return false
	}
	out.Incarnation = inc
	return true
}

// DecodeView splits a view, skipping empty and malformed entries.
func DecodeView(value string) []NodeInfo {
	var out []NodeInfo
	for e := range strings.SplitSeq(value, ";") {
		if e == "" {
			continue
		}
		var info NodeInfo
		if ParseEntry(e, &info) {
			out = append(out, info)
		}
	}
	return out
}
