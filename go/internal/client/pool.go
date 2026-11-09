package client

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/kraytos17/cinder/go/internal/net/protocol"
)

// NodeAddr is a reachable cluster member.
type NodeAddr struct {
	ID   string
	Host string
	Port int
}

func (a NodeAddr) dialAddr() string {
	return fmt.Sprintf("%s:%d", a.Host, a.Port)
}

// Pool holds one persistent connection per node, dialed lazily. Transport
// failures drop the connection so the next send redials; the cluster
// client decides whether the operation itself is retried.
type Pool struct {
	mu      sync.Mutex
	conns   map[string]*Client
	addrs   map[string]NodeAddr
	timeout time.Duration
	tlsCfg  *tls.Config
}

// NewPool returns a pool dialing the given nodes with per-RPC timeout.
func NewPool(addrs []NodeAddr, timeout time.Duration) *Pool {
	p := &Pool{
		conns:   make(map[string]*Client),
		addrs:   make(map[string]NodeAddr, len(addrs)),
		timeout: timeout,
	}
	for _, a := range addrs {
		p.addrs[a.ID] = a
	}
	return p
}

// newPoolTLS builds a pool with an optional shared TLS config.
func newPoolTLS(addrs []NodeAddr, timeout time.Duration, tlsCfg *tls.Config) *Pool {
	p := NewPool(addrs, timeout)
	if tlsCfg != nil {
		p.SetTLS(tlsCfg)
	}
	return p
}

// AddAddr registers (or replaces) a node's reachable address, then drops
// any existing connection so the next send redials the new address.
// Mirrors ConnectionPool::addAddr: redirect-learned members outside the
// client config become reachable.
func (p *Pool) AddAddr(id, host string, port int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addrs[id] = NodeAddr{ID: id, Host: host, Port: port}
	if c, ok := p.conns[id]; ok {
		delete(p.conns, id)
		go c.Close()
	}
}

// SetTLS enables TLS for subsequently dialed connections and drops
// existing plaintext ones, mirroring a cluster-wide move to --tls. The
// config must come from tlscfg.Client; it is shared read-only.
func (p *Pool) SetTLS(cfg *tls.Config) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tlsCfg = cfg
	for id, c := range p.conns {
		delete(p.conns, id)
		go c.Close()
	}
}

// Send delivers one request to node, dialing on first use. A transport
// error drops the connection and surfaces the error; the caller owns
// retry policy.
func (p *Pool) Send(node string, req protocol.Request) (protocol.Response, error) {
	c, err := p.connFor(node)
	if err != nil {
		return protocol.Response{}, err
	}
	res, err := c.Do(req)
	if err != nil {
		p.mu.Lock()
		if cur, ok := p.conns[node]; ok && cur == c {
			delete(p.conns, node)
		}
		p.mu.Unlock()
		return protocol.Response{}, err
	}
	return res, nil
}

func (p *Pool) connFor(node string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[node]; ok {
		return c, nil
	}
	a, ok := p.addrs[node]
	if !ok {
		return nil, fmt.Errorf("client: unknown node %q", node)
	}
	var c *Client
	var err error
	if p.tlsCfg != nil {
		c, err = DialTLS(a.dialAddr(), p.timeout, p.tlsCfg)
	} else {
		c, err = Dial(a.dialAddr(), p.timeout)
	}
	if err != nil {
		return nil, err
	}
	p.conns[node] = c
	return c, nil
}

// Close drops every pooled connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, c := range p.conns {
		delete(p.conns, id)
		go c.Close()
	}
}
