// Package tcp implements the Cinder v6 TCP server: framing, dispatch, and
// connection lifecycle. It mirrors src/net/tcp (connection + server):
// sequential request/response per connection, 30s idle timeout, 10k
// connection cap, oversized frames closed, decode failures answered with
// InvalidArgument, and trace IDs minted at the connection entry point.
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
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/shard"
	"github.com/kraytos17/cinder/internal/store"

	"github.com/rs/zerolog"
)

const (
	// connBufferSize bounds one connection's staged frame, like C++
	// TcpConnection::K_BUFFER_SIZE. Larger payloads close the connection;
	// bulk transfer is the client's job, not the server's.
	connBufferSize = 1 << 20
	// maxConnections caps concurrent clients; beyond it new sockets are
	// rejected instead of buffering unbounded descriptors.
	maxConnections = 10_000
	// idleTimeout closes connections with no completed request/response.
	idleTimeout = 30 * time.Second
	// maxReusePayload bounds the retained per-connection staging buffer:
	// payloads above it still work, but allocate fresh each time instead
	// of pinning a ~1 MiB slab per connection after one large value.
	maxReusePayload = 64 << 10
	// compactTickInterval is the auto-compact poll cadence. The persist
	// manager itself decides whether the snapshot interval elapsed or the
	// WAL passed its byte budget, so this only bounds check latency.
	compactTickInterval = time.Second
)

// nopLogger is the shared disabled logger for servers constructed
// without SetLogger. Disabled level: every event is nil, nothing emits.
var nopLogger = log.Nop()

// Server is a v6 TCP server. Without ConfigureCluster it owns every key
// (standalone). With a cluster view, writes route by ring ownership and
// non-owners answer NotReady with a "moved to" redirect, mirroring
// TcpConnection::checkWriteOwnership; local hits still serve reads.
// Auth is open: with no secrets configured every opcode class is allowed,
// mirroring the C++ no-secret behavior.
type Server struct {
	store  *store.Store
	nodeID string
	port   int
	clock  store.Clock
	// counters holds CRDT state beside the LWW store, always on like C++
	// (no flag, no CLI surface). Sized at a quarter of the LWW capacity,
	// mirroring counterCapacity.
	counters *counter.Store
	// met is the observability collector, always on like C++ (no
	// flag; the /metrics endpoint gates exposure, not recording).
	// Wired into the store, replication, AE, gossip, FD, and
	// rebalance managers where configured.
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

	aeMu       sync.RWMutex
	ae         *ae.Manager
	aeInterval time.Duration

	// Membership and liveness. The ring always reflects the alive set:
	// every membership change rebuilds it, so redirects, replica sets,
	// and the pool follow failures without a restart.
	table  *cluster.MembershipTable
	gossip *cluster.GossipManager
	fd     *cluster.FailureDetector
	pool   *client.Pool
	stopBg chan struct{}

	gossipInterval time.Duration
	probeInterval  time.Duration
	suspectTimeout time.Duration
	// replayInterval is the hint-replay tick period. Like C++ scheduleReplay
	// (~1s): hints queue on failed fan-out and drain on this cadence once
	// their target is reachable again.
	replayInterval time.Duration

	replMu sync.RWMutex
	repl   *repl.Manager
	rf     int
	mode   repl.Mode

	tlsMu  sync.RWMutex
	tlsCfg *tls.Config

	authMu             sync.RWMutex
	sharedSecret       string
	clientSharedSecret string

	// Rebalance on membership change. Every ring rebuild schedules one
	// debounced scan; quarantine deferrals and migration failures
	// reschedule through the same gate, mirroring CacheNodeServer's
	// scheduleRebalanceDebounced/scheduleRebalance/scheduleMigrationRetry.
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

// ConfigureReplication attaches the replication manager used when the
// replica factor exceeds 1. With RF=1 (or nil manager) every op stays
// local, mirroring CacheOperations' replica_factor_ checks.

func NewServer(nodeID string, port int, st *store.Store, clock store.Clock) *Server {
	if clock == nil {
		clock = store.SystemClock{}
	}
	// Counter budget is a quarter of the LWW capacity, mirroring C++
	// counterCapacity. Zero LWW capacity still yields a usable minimum:
	// the counter store clamps to one shard regardless.
	counterCap := st.Capacity() / 4
	srv := &Server{
		store:          st,
		counters:       counter.NewStore(counterCap),
		met:            metrics.NewCollector(),
		nodeID:         nodeID,
		port:           port,
		clock:          clock,
		conns:          make(map[net.Conn]struct{}),
		sem:            make(chan struct{}, maxConnections),
		stopBg:         make(chan struct{}),
		gossipInterval: time.Second,
		probeInterval:  time.Second,
		suspectTimeout: 3 * time.Second,
		replayInterval: time.Second,
		aeInterval:     30 * time.Second,
	}
	st.SetMetrics(srv.met)
	nop := log.Nop()
	srv.logger.Store(&nop)
	return srv
}

// ConfigureRebalance creates the shard manager over the live ring, pool,
// and membership table. Call after ConfigureCluster (and
// ConfigureReplication for the replica factor). A zero quarantine
// disables the warming deferral, mirroring --quarantine-interval 0.

func (s *Server) Serve(l net.Listener) error {
	s.tlsMu.RLock()
	tlsCfg := s.tlsCfg
	s.tlsMu.RUnlock()
	if tlsCfg != nil {
		l = tls.NewListener(l, tlsCfg)
	}
	s.mu.Lock()
	s.listener = l
	shut := s.shut.Load()
	s.mu.Unlock()
	if shut {
		// Shut down before Serve started: never serve, just release.
		l.Close()
		return nil
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
		// Disable Nagle before any I/O: see setNoDelay in the client
		// for why sequential small frames want it off. Non-TCP
		// listeners (TLS wraps the socket) skip this gracefully.
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
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
		_ = pm.Close()
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

// Peer is a reachable cluster member.

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// ConfigureAntiEntropy creates the AE manager over the live ring and
// pool transport. Call after ConfigureCluster.

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

// SetGossipInterval changes the gossip tick period (tests use short ones).

func (s *Server) ID() string {
	return s.nodeID
}

// Dispatch routes one request through the full TCP dispatch (ownership,
// replication, admin). The gRPC gateway uses it so both frontends share
// identical Get/Set/Delete semantics. No auth token is attached; the
// gateway enforces its bearer separately.
