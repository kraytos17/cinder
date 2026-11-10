package client

import (
	"crypto/tls"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// Routing bounds limit the number of cached owner overrides and redirect
// hops per call. A redirect loop therefore terminates instead of
// repeatedly spending the retry budget.
const (
	maxLearnedOwners = 4096
	maxRedirectHops  = 4
)

// ClusterConfig configures a multi-node client.
type ClusterConfig struct {
	Nodes         []NodeAddr
	MaxRetries    int           // attempts beyond the first (0 = no retry)
	BaseBackoffMs int           // exponential: base * 2^attempt, ±20% jitter
	RPCTimeout    time.Duration // per-RPC deadline (0 = no timeout)
	TLS           *tls.Config   // nil = plaintext; tlscfg.Client otherwise
	ClientSecret  string        // client auth credential (empty = open)
}

// Cluster routes requests to ring owners, follows ownership redirects,
// and retries Timeout/NotReady responses with backoff. It is safe for
// concurrent use. Redirects share the retry budget and are bounded by a
// hop limit and visited-node set. Transport failures discard the learned
// owner so the next attempt resolves through the ring.
type Cluster struct {
	ring        *hashring.Ring
	pool        *Pool
	addrs       map[string]NodeAddr
	maxRetries  int
	baseBackoff time.Duration

	mu      sync.Mutex
	learned map[string]string
}

// NewCluster builds a client over nodes. The ring contains every supplied
// node ID; unknown IDs fail at the pool and follow the configured retry
// behavior.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		return nil, err
	}
	addrs := make(map[string]NodeAddr, len(cfg.Nodes))
	ids := make([]string, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		addrs[n.ID] = n
		ids = append(ids, n.ID)
	}
	if err := ring.SetNodes(ids); err != nil {
		return nil, err
	}
	pool := newPoolTLS(cfg.Nodes, cfg.RPCTimeout, cfg.TLS)
	if cfg.ClientSecret != "" {
		pool.SetSecrets(Secrets{Client: cfg.ClientSecret})
	}
	return &Cluster{
		ring:        ring,
		pool:        pool,
		addrs:       addrs,
		maxRetries:  cfg.MaxRetries,
		baseBackoff: time.Duration(cfg.BaseBackoffMs) * time.Millisecond,
		learned:     make(map[string]string),
	}, nil
}

// Close drops all pooled connections.
func (c *Cluster) Close() {
	c.pool.Close()
}

func (c *Cluster) preferredNode(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if node, ok := c.learned[key]; ok {
		return node
	}
	return c.ring.GetNode(key)
}

func (c *Cluster) learnOwner(key, node string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.learned) >= maxLearnedOwners {
		c.learned = make(map[string]string)
	}
	c.learned[key] = node
}

func (c *Cluster) forgetOwner(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.learned, key)
}

// tryFollowRedirect registers a followable ownership redirect: the target
// must be fresh (not the node just asked, not already visited) and the
// hop budget must hold. Learned addresses register first so nodes outside
// the client config stay reachable.
func (c *Cluster) tryFollowRedirect(key string, res protocol.Response, node string, visited []string, hops int) (string, bool) {
	if res.Status != protocol.StatusNotReady {
		return "", false
	}
	target, ok := ParseRedirectTarget(string(res.Value))
	if !ok || target.ID == node || hops >= maxRedirectHops {
		return "", false
	}
	if slices.Contains(visited, target.ID) {
		return "", false
	}
	if target.HasAddress() {
		c.pool.AddAddr(target.ID, target.Host, target.Port)
	}
	c.learnOwner(key, target.ID)
	return target.ID, true
}

func retryBackoff(attempt, baseMs int) int {
	delay := int64(baseMs)
	for range attempt {
		delay *= 2
		if delay > int64(^uint(0)>>1) {
			return int(^uint(0) >> 1)
		}
	}
	return int(delay)
}

func jitterBackoff(delayMs int) int {
	if delayMs <= 0 {
		return delayMs
	}
	jitter := 0.8 + rand.Float64()*0.4
	d := int(float64(delayMs) * jitter)
	if d < 1 {
		return 1
	}
	return d
}

// sendToOwner routes one request, following redirects within the hop
// budget and retrying Timeout/NotReady/transport errors with backoff.
// Transport errors forget the learned owner so the next attempt
// re-resolves via the ring.
func (c *Cluster) sendToOwner(key string, req protocol.Request) (protocol.Response, error) {
	node := c.preferredNode(key)
	var visited []string
	hops := 0
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		res, err := c.pool.Send(node, req)
		if err == nil {
			if next, ok := c.tryFollowRedirect(key, res, node, visited, hops); ok {
				visited = append(visited, node)
				node = next
				hops++
				attempt-- // follows reuse this attempt's budget
				continue
			}
			if res.Status == protocol.StatusTimeout || res.Status == protocol.StatusNotReady {
				if attempt == c.maxRetries {
					return res, nil
				}
				time.Sleep(time.Duration(jitterBackoff(retryBackoff(attempt, int(c.baseBackoff.Milliseconds())))) * time.Millisecond)
				continue
			}
			return res, nil
		}
		lastErr = err
		node = c.preferredNodeAfterForget(key)
		if attempt == c.maxRetries {
			break
		}
		time.Sleep(time.Duration(jitterBackoff(retryBackoff(attempt, int(c.baseBackoff.Milliseconds())))) * time.Millisecond)
	}
	return protocol.Response{}, lastErr
}

func (c *Cluster) preferredNodeAfterForget(key string) string {
	c.forgetOwner(key)
	return c.preferredNode(key)
}

// Set stores value under key at its ring owner.
func (c *Cluster) Set(key string, value []byte) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: value})
}

// SetWithTTL stores value with a relative TTL at its ring owner.
func (c *Cluster) SetWithTTL(key string, value []byte, ttl time.Duration) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: value, TTL: ttl})
}

// Get fetches the live value from its ring owner.
func (c *Cluster) Get(key string) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)})
}

// Del deletes key at its ring owner via a versioned tombstone.
func (c *Cluster) Del(key string) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpDel, Key: []byte(key)})
}

// GetVersioned fetches the entry with LWW metadata.
func (c *Cluster) GetVersioned(key string) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key)})
}

// CounterAdjust increments/decrements key's PN-counter at its ring owner
// (redirect-following comes free: adjusts are owner-routed NotReady
// redirects, exactly like writes).
func (c *Cluster) CounterAdjust(key string, delta uint64, decrement bool) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte(key), CounterDelta: delta, CounterDecrement: decrement})
}

// CounterGet fetches key's PN-counter state via the owner (served from any
// local copy when present, redirected on miss, like reads).
func (c *Cluster) CounterGet(key string) (protocol.Response, error) {
	return c.sendToOwner(key, protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte(key)})
}

// Ping pings a specific node by id (no routing).
func (c *Cluster) Ping(node string) (protocol.Response, error) {
	return c.pool.Send(node, protocol.Request{Opcode: protocol.OpPing})
}
