// Package tcp implements the Cinder v6 TCP server, including frame
// handling, request dispatch, connection lifecycle, and cluster wiring.
// Each connection processes one request-response exchange at a time.
package tcp

import (
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/shard"
	"github.com/kraytos17/cinder/internal/store"

	"github.com/rs/zerolog"
)

const (
	// connBufferSize bounds the header-plus-payload staging allowed for one
	// connection. Larger frames are rejected; bulk transfer is not supported.
	connBufferSize = 1 << 20
	// maxConnections caps concurrent clients; beyond it new sockets are
	// rejected instead of buffering unbounded descriptors.
	maxConnections = 10_000
	// idleTimeout closes connections with no completed request/response.
	// It bounds reads only; writes get their own shorter budget below so
	// one slow reader cannot park a handler for the full idle window.
	idleTimeout = 30 * time.Second
	// writeTimeout bounds a single response write. Exceeding it means the
	// peer is not reading: the connection closes and counts a slow
	// consumer drop instead of lingering until the idle deadline.
	writeTimeout = 10 * time.Second
	// maxReusePayload bounds the retained per-connection staging buffer:
	// payloads above it still work, but allocate fresh each time instead
	// of pinning a ~1 MiB slab per connection after one large value.
	maxReusePayload = 64 << 10
	// compactTickInterval is the auto-compact poll cadence. The persist
	// manager itself decides whether the snapshot interval elapsed or the
	// WAL passed its byte budget, so this only bounds check latency.
	compactTickInterval = time.Second
)

// noDelayListener sets TCP_NODELAY on accepted connections. It must wrap
// the raw listener before any TLS wrapper (which hides the TCP socket),
// and it also covers plain listeners so the accept loop needs no
// per-connection type switch of its own.
type noDelayListener struct {
	net.Listener
}

// Accept implements net.Listener.
func (l noDelayListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// Best-effort like the client side: a socket that refuses the option
	// still works, just with Nagle enabled.
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return c, nil
}

// nopLogger is the shared disabled logger for servers constructed
// without SetLogger. Disabled level: every event is nil, nothing emits.
var nopLogger = log.Nop()

// Server accepts v6 TCP requests and dispatches them to the local store or
// configured cluster managers. Without ConfigureCluster it is standalone;
// with membership configured, writes are ownership-routed and non-owners
// return NotReady redirects. Authentication is disabled when no secrets
// are configured.
type Server struct {
	store  *store.Store
	nodeID string
	port   netport.Port
	// advertiseHost is the externally reachable host shared with cluster
	// peers. It is independent from the local listener's bind address.
	advertiseHost string
	clock         store.Clock
	// counters holds CRDT counter state separately from the LWW cache. Its
	// initial capacity is one quarter of the LWW store's capacity.
	counters *counter.Store
	// met records observability data regardless of whether the /metrics
	// endpoint is enabled. It is wired into configured subsystem managers.
	met *metrics.Collector
	// logger is the operational logger (default Nop; cinderd installs
	// the --log-level logger via SetLogger). Request lines carry the
	// wire trace/span IDs for cross-node correlation.
	logger atomic.Pointer[zerolog.Logger]

	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	sem      chan struct{}
	shut     atomic.Bool
	wg       sync.WaitGroup
	trace    atomic.Uint64

	ringMu sync.RWMutex
	ring   *hashring.Ring
	peers  []Peer
	vnodes int

	aeMu       sync.RWMutex
	ae         *ae.Manager
	aeInterval time.Duration

	// Membership and liveness. The ring always reflects the alive set:
	// every membership change rebuilds it, so redirects, replica sets,
	// and the pool follow failures without a restart.
	table             *cluster.MembershipTable
	gossip            *cluster.GossipManager
	fd                *cluster.FailureDetector
	pool              *client.Pool
	clusterConfigured bool
	clusterStarted    bool
	stopBg            chan struct{}

	gossipInterval     time.Duration
	probeInterval      time.Duration
	suspectTimeout     time.Duration
	gossipRPCTimeoutNs atomic.Int64
	// writeTimeoutNs bounds a single response write in nanoseconds; 0
	// selects the writeTimeout default. Atomic so the hot read path
	// takes no lock (unlike the tick-interval knobs nearby).
	writeTimeoutNs atomic.Int64
	// replayInterval is the base hint-retry delay; consecutive all-failed
	// rounds back off from here toward replayMax.
	replayInterval time.Duration
	// replayMax caps the hint-retry backoff.
	replayMax time.Duration

	replMu sync.RWMutex
	repl   *repl.Manager
	rf     hashring.ReplicaCount
	mode   repl.Mode

	tlsMu  sync.RWMutex
	tlsCfg *tls.Config
	// TLS file paths for handshake-time certificate loading. The chain is
	// re-read on every handshake so Reload rotates without restarts.
	tlsCertFile, tlsKeyFile, tlsCAFile string

	authMu             sync.RWMutex
	sharedSecret       string
	clientSharedSecret string

	// Rebalance on membership change. Every ring rebuild schedules one
	// debounced scan; quarantine deferrals and migration failures
	// reschedule through the same debounce gate.
	shardMu            sync.Mutex
	shard              *shard.Manager
	quarantineInterval time.Duration
	rebalancePending   bool
	rebalanceTimer     *time.Timer
	quarantineTimer    *time.Timer
	migrationPending   bool
	migrationTimer     *time.Timer

	persistMu sync.RWMutex
	persist   *persist.Manager
}

// ServerConfig configures a TCP server. A zero Port allows the listener to
// choose an available port; AdvertiseHost is the reachable cluster host
// (empty defaults to loopback); a nil Clock selects the system clock.
type ServerConfig struct {
	NodeID        string
	Port          netport.Port
	AdvertiseHost string
	VNodes        int
	Store         *store.Store
	Clock         store.Clock
}

// NewServer returns a server configured with cfg. The counter store is
// initialized with one quarter of the cache store's capacity.
func NewServer(cfg ServerConfig) *Server {
	clock := cfg.Clock
	if clock == nil {
		clock = store.SystemClock{}
	}

	vnodes := cfg.VNodes
	if vnodes <= 0 {
		vnodes = hashring.DefaultVNodes
	}

	// The counter store starts with one quarter of the LWW capacity. Its
	// shard count is clamped to at least one, including for zero capacity.
	counterCap := cfg.Store.Capacity() / 4
	srv := &Server{
		store:          cfg.Store,
		counters:       counter.NewStore(counterCap),
		met:            metrics.NewCollector(),
		nodeID:         cfg.NodeID,
		port:           cfg.Port,
		advertiseHost:  cfg.AdvertiseHost,
		vnodes:         vnodes,
		clock:          clock,
		conns:          make(map[net.Conn]struct{}),
		sem:            make(chan struct{}, maxConnections),
		stopBg:         make(chan struct{}),
		gossipInterval: time.Second,
		probeInterval:  time.Second,
		suspectTimeout: 3 * time.Second,
		replayInterval: time.Second,
		replayMax:      30 * time.Second,
		aeInterval:     30 * time.Second,
	}

	cfg.Store.SetMetrics(srv.met)
	nop := log.Nop()
	srv.logger.Store(&nop)
	if srv.advertiseHost == "" {
		srv.advertiseHost = "127.0.0.1"
	}

	srv.gossipRPCTimeoutNs.Store(int64(time.Second))
	return srv
}

func (s *Server) Serve(l net.Listener) error {
	s.tlsMu.RLock()
	tlsCfg := s.tlsCfg
	s.tlsMu.RUnlock()
	// NoDelay first: the TLS wrapper below hides the TCP socket, so the
	// option must be set on the raw connection before wrapping. (See
	// setNoDelay in the client for why sequential frames want it off.)
	l = noDelayListener{l}
	if tlsCfg != nil {
		l = tls.NewListener(l, tlsCfg)
	}

	s.mu.Lock()
	s.listener = l
	if addr, ok := l.Addr().(*net.TCPAddr); ok {
		// Port 0 is useful for tests and supervised deployments. Advertise
		// the actual bound port, not the requested zero.
		s.port = netport.Port(addr.Port)
	}

	shut := s.shut.Load()
	advertiseHost := s.advertiseHost
	port := s.port
	s.mu.Unlock()
	s.ringMu.RLock()
	table := s.table
	s.ringMu.RUnlock()
	if table != nil {
		table.SetSelfAddress(advertiseHost, port)
	}
	if shut {
		// Shut down before Serve started: never serve, just release.
		l.Close()
		return nil
	}
	if err := s.startCluster(); err != nil {
		l.Close()
		return err
	}

	s.ringMu.RLock()
	gossip, fd := s.gossip, s.fd
	s.ringMu.RUnlock()
	s.aeMu.RLock()
	mgr := s.ae
	s.aeMu.RUnlock()
	s.replMu.RLock()
	replMgr := s.repl
	s.replMu.RUnlock()
	s.persistMu.RLock()
	pm := s.persist
	s.persistMu.RUnlock()

	var bgWG sync.WaitGroup
	if gossip != nil && fd != nil {
		bgWG.Add(2)
		go s.runGossipLoop(&bgWG, gossip)
		go s.runProbeLoop(&bgWG, fd)
	}
	if mgr != nil {
		bgWG.Go(func() { s.runAELoop(mgr) })
	}
	if replMgr != nil {
		bgWG.Go(func() { s.runReplayLoop(replMgr) })
	}
	if pm != nil {
		bgWG.Go(func() { s.runCompactLoop(pm) })
	}

	defer func() {
		close(s.stopBg)
		bgWG.Wait()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			if s.shut.Load() {
				return nil
			}
			continue
		}
		select {
		case s.sem <- struct{}{}:
		default:
			c.Close()
			continue
		}

		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		s.met.ConnsOpened.Add(1)
		go s.handle(c)
	}
}

// Shutdown stops accepting, broadcasts a leave (best-effort Dead
// self-rumor so peers don't suspect during teardown), and closes all
// connections.
func (s *Server) Shutdown() {
	if s.shut.Swap(true) {
		return
	}

	s.ringMu.RLock()
	gossip := s.gossip
	s.ringMu.RUnlock()
	if gossip != nil {
		gossip.Leave()
	}

	s.persistMu.RLock()
	pm := s.persist
	s.persistMu.RUnlock()
	if pm != nil {
		pm.Close()
	}

	s.stopRebalanceTimers()
	s.closeAll()
}

// Kill is a crash simulation: it closes the listener and connections
// without a leave broadcast, so peers keep routing to this node until
// failure detection marks it dead. Tests use it to exercise hints and
// quorum fail-closed paths deterministically.
func (s *Server) Kill() {
	if s.shut.Swap(true) {
		return
	}

	s.stopRebalanceTimers()
	s.closeAll()
}

func (s *Server) closeAll() {
	s.mu.Lock()
	l := s.listener
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}

	s.mu.Unlock()
	if l != nil {
		l.Close()
	}
	for _, c := range conns {
		c.Close()
	}
	s.wg.Wait()
}

func (s *Server) done(c net.Conn) {
	c.Close()
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	s.met.ConnsClosed.Add(1)
	<-s.sem
	s.wg.Done()
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) advertisedAddress() (string, netport.Port) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.advertiseHost, s.port
}

func (s *Server) AntiEntropy() *ae.Manager {
	s.aeMu.RLock()
	defer s.aeMu.RUnlock()
	return s.ae
}

// Store exposes the underlying store for introspection and tests.
func (s *Server) Store() *store.Store {
	return s.store
}

// CounterStore exposes the CRDT counter store for introspection and tests.
func (s *Server) CounterStore() *counter.Store {
	return s.counters
}

// SetLogger installs the operational logger (nil selects Nop) and fans
// it out to every configured manager, so call order with Configure*
// does not matter. Like SetMetrics: every site goes through log(), so
// unconfigured servers pay nothing.
func (s *Server) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}

	s.logger.Store(l)
	s.aeMu.RLock()
	aeMgr := s.ae
	s.aeMu.RUnlock()
	if aeMgr != nil {
		aeMgr.SetLogger(l)
	}

	s.replMu.RLock()
	replMgr := s.repl
	s.replMu.RUnlock()
	if replMgr != nil {
		replMgr.SetLogger(l)
	}

	s.ringMu.RLock()
	table, gossip, fd := s.table, s.gossip, s.fd
	s.ringMu.RUnlock()
	if table != nil {
		table.SetLogger(l)
	}
	if gossip != nil {
		gossip.SetLogger(l)
	}
	if fd != nil {
		fd.SetLogger(l)
	}

	s.shardMu.Lock()
	shardMgr := s.shard
	s.shardMu.Unlock()
	if shardMgr != nil {
		shardMgr.SetLogger(l)
	}

	s.persistMu.RLock()
	pm := s.persist
	s.persistMu.RUnlock()
	if pm != nil {
		pm.SetLogger(l)
	}
}

// log returns the installed logger (Nop when unset, so direct
// constructions stay silent without a SetLogger call).
func (s *Server) log() *zerolog.Logger {
	if l := s.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// Metrics exposes the observability collector.
func (s *Server) Metrics() *metrics.Collector {
	return s.met
}

// MetricGauges snapshots live store sizes for a Prometheus scrape.
func (s *Server) MetricGauges() metrics.StoreGauges {
	b, e := s.store.Stats()
	g := metrics.StoreGauges{Bytes: b, Entries: e, Capacity: s.store.Capacity()}
	s.replMu.RLock()
	m := s.repl
	s.replMu.RUnlock()
	if m != nil {
		g.HintEntries = uint64(m.HintCount())
		g.HintBytes = m.HintBytes()
	}
	return g
}

// Pool exposes the node's connection pool (transport for gossip, failure
// detection, and replication fan-out).
func (s *Server) Pool() *client.Pool {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	return s.pool
}

// Table exposes the membership table.
func (s *Server) Table() *cluster.MembershipTable {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	return s.table
}

func (s *Server) ID() string {
	return s.nodeID
}
