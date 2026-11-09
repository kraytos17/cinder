// Package tcp implements the Cinder v6 TCP server: framing, dispatch, and
// connection lifecycle. It mirrors src/net/tcp (connection + server):
// sequential request/response per connection, 30s idle timeout, 10k
// connection cap, oversized frames closed, decode failures answered with
// InvalidArgument, and trace IDs minted at the connection entry point.
package tcp

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/shard"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
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
)

// Server is a v6 TCP server. Without ConfigureCluster it owns every key
// (standalone). With a cluster view, writes route by ring ownership and
// non-owners answer NotReady with a "moved to" redirect, mirroring
// TcpConnection::checkWriteOwnership; local hits still serve reads.
// Auth is open: with no secrets configured every opcode class is allowed,
// mirroring the C++ no-secret behavior.
type Server struct {
	store  *store.LRUStore
	nodeID string
	port   int
	clock  store.Clock

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
func (s *Server) ConfigureReplication(rf int, mode repl.Mode, m *repl.Manager) {
	s.replMu.Lock()
	s.repl = m
	s.rf = rf
	s.mode = mode
	s.replMu.Unlock()
}

// ConfigureTLS enables TLS with a cert chain + key, optionally verifying
// client certificates against a CA file. Mirrors the C++ --tls/--tls-cert/
// --tls-key/--tls-ca wiring (TLS 1.2+, chain-only peer verification).
// Call before Serve; it also flips the inter-node pool to TLS so gossip,
// probes, replication, AE, and rebalance all speak TLS.
func (s *Server) ConfigureTLS(certFile, keyFile, caFile string) error {
	cfg, err := tlscfg.Server(certFile, keyFile, caFile)
	if err != nil {
		return err
	}
	s.tlsMu.Lock()
	s.tlsCfg = cfg
	s.tlsMu.Unlock()
	s.ringMu.RLock()
	pool := s.pool
	s.ringMu.RUnlock()
	if pool != nil {
		// Inter-node connections present this node's own certificate,
		// exactly like the C++ client context built from the same files.
		ccfg, err := tlscfg.Client(certFile, keyFile, caFile)
		if err != nil {
			return err
		}
		pool.SetTLS(ccfg)
	}
	return nil
}

// ConfigurePersistence enables the WAL + snapshots in dir, recovering
// before serving. The store hook is wired after recovery so replay never
// logs itself. Call before Serve.
func (s *Server) ConfigurePersistence(dir string) error {
	m := persist.NewManager(dir, s.store, nil)
	if err := m.Recover(); err != nil {
		return err
	}
	s.store.SetPersistence(func(key string, e store.Entry, hardDelete bool) {
		m.OnWrite(key, e, hardDelete)
	})
	s.persistMu.Lock()
	s.persist = m
	s.persistMu.Unlock()
	return nil
}

// Compact snapshots and rotates the WAL; wired to ADMIN_COMPACT.
func (s *Server) Compact() error {
	s.persistMu.RLock()
	m := s.persist
	s.persistMu.RUnlock()
	if m == nil {
		return nil
	}
	return m.Compact()
}

// ReplayHints retries queued hints; exposed for tests and the replay tick.
func (s *Server) ReplayHints() int {
	s.replMu.RLock()
	m := s.repl
	s.replMu.RUnlock()
	if m == nil {
		return 0
	}
	return m.ReplayHints()
}

func (s *Server) replication() (*repl.Manager, int, repl.Mode) {
	s.replMu.RLock()
	defer s.replMu.RUnlock()
	return s.repl, s.rf, s.mode
}

// replicaNodes mirrors CacheOperations::replicaNodesFor: ring owners minus
// self.
func (s *Server) replicaNodes(key string) []string {
	s.ringMu.RLock()
	ring := s.ring
	s.ringMu.RUnlock()
	if ring == nil {
		return nil
	}
	nodes, err := ring.GetNodes(key, s.replicaFactor())
	if err != nil || len(nodes) == 0 {
		return nil
	}
	out := nodes[:0:0]
	for _, n := range nodes {
		if n != s.nodeID {
			out = append(out, n)
		}
	}
	return out
}

func (s *Server) replicaFactor() int {
	s.replMu.RLock()
	defer s.replMu.RUnlock()
	return s.rf
}

// NewServer returns a server over an existing store.
func NewServer(nodeID string, port int, st *store.LRUStore, clock store.Clock) *Server {
	if clock == nil {
		clock = store.SystemClock{}
	}
	return &Server{
		store:          st,
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
}

// ConfigureRebalance creates the shard manager over the live ring, pool,
// and membership table. Call after ConfigureCluster (and
// ConfigureReplication for the replica factor). A zero quarantine
// disables the warming deferral, mirroring --quarantine-interval 0.
func (s *Server) ConfigureRebalance(rf int, quarantine time.Duration) {
	s.ringMu.RLock()
	ring := s.ring
	pool := s.pool
	table := s.table
	s.ringMu.RUnlock()
	m := shard.NewManager(s.store, ring, pool, table, s.nodeID, rf, quarantine, nil)
	m.SetOnMigrationFailed(func() { s.scheduleMigrationRetry() })
	s.shardMu.Lock()
	s.shard = m
	s.quarantineInterval = quarantine
	s.shardMu.Unlock()
}

// SetQuarantineInterval changes the rejoin warming window (tests use short
// or zero ones).
func (s *Server) SetQuarantineInterval(d time.Duration) {
	s.shardMu.Lock()
	defer s.shardMu.Unlock()
	s.quarantineInterval = d
}

// RebalanceNow runs one synchronous rebalance scan (tests drive this
// directly; the timer path is best-effort). False when unconfigured.
func (s *Server) RebalanceNow() bool {
	s.shardMu.Lock()
	m := s.shard
	s.shardMu.Unlock()
	if m == nil {
		return false
	}
	return m.Rebalance()
}

// scheduleRebalanceDebounced coalesces rapid membership flaps into a single
// scan 200ms out, covering the latest ring view.
func (s *Server) scheduleRebalanceDebounced() {
	s.shardMu.Lock()
	if s.rebalancePending {
		s.shardMu.Unlock()
		return
	}
	s.rebalancePending = true
	if s.rebalanceTimer != nil {
		s.rebalanceTimer.Stop()
	}
	s.rebalanceTimer = time.AfterFunc(200*time.Millisecond, func() {
		s.shardMu.Lock()
		s.rebalancePending = false
		s.shardMu.Unlock()
		if s.shut.Load() {
			return
		}
		if s.RebalanceNow() {
			s.scheduleQuarantineRetry()
		}
	})
	s.shardMu.Unlock()
}

// scheduleQuarantineRetry re-runs the scan after the warming window, and
// keeps retrying while any owner is still quarantined (a flapping node
// keeps resetting joined_at).
func (s *Server) scheduleQuarantineRetry() {
	s.shardMu.Lock()
	if s.quarantineTimer != nil {
		s.quarantineTimer.Stop()
		s.quarantineTimer = nil
	}
	if s.quarantineInterval <= 0 {
		s.shardMu.Unlock()
		return
	}
	s.quarantineTimer = time.AfterFunc(s.quarantineInterval, func() {
		if s.shut.Load() {
			return
		}
		if s.RebalanceNow() {
			s.scheduleQuarantineRetry()
		}
	})
	s.shardMu.Unlock()
}

// scheduleMigrationRetry re-runs the scan 2s after an async migration
// failure, through the debounce gate. Cheap and non-blocking: it only arms
// a timer (invoked on the rebalance goroutine, never inline on send).
func (s *Server) scheduleMigrationRetry() {
	s.shardMu.Lock()
	if s.migrationPending || s.rebalancePending {
		s.shardMu.Unlock()
		return
	}
	s.migrationPending = true
	if s.migrationTimer != nil {
		s.migrationTimer.Stop()
	}
	s.migrationTimer = time.AfterFunc(2*time.Second, func() {
		s.shardMu.Lock()
		s.migrationPending = false
		s.shardMu.Unlock()
		if s.shut.Load() {
			return
		}
		s.scheduleRebalanceDebounced()
	})
	s.shardMu.Unlock()
}

func (s *Server) stopRebalanceTimers() {
	s.shardMu.Lock()
	defer s.shardMu.Unlock()
	for _, t := range []*time.Timer{s.rebalanceTimer, s.quarantineTimer, s.migrationTimer} {
		if t != nil {
			t.Stop()
		}
	}
	s.rebalanceTimer, s.quarantineTimer, s.migrationTimer = nil, nil, nil
	s.rebalancePending, s.migrationPending = false, false
}

// Serve accepts on l until Shutdown. It blocks; run it in a goroutine.
// The listener is published under the connection mutex so a concurrent
// Shutdown always observes it. Background gossip and failure-detector
// ticks start here when a cluster is configured.
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
	var bgWG sync.WaitGroup
	if gossip != nil && fd != nil {
		bgWG.Add(2)
		go func() {
			defer bgWG.Done()
			for {
				select {
				case <-s.stopBg:
					return
				case <-time.After(s.currentGossipInterval()):
				}
				gossip.Tick()
			}
		}()
		go func() {
			defer bgWG.Done()
			for {
				select {
				case <-s.stopBg:
					return
				case <-time.After(s.currentProbeInterval()):
				}
				fd.Tick()
			}
		}()
	}
	if mgr != nil {
		bgWG.Go(func() {
			for {
				select {
				case <-s.stopBg:
					return
				case <-time.After(s.currentAEInterval()):
				}
				mgr.RunRound(s.replicaFactor())
			}
		})
	}
	if replMgr != nil {
		bgWG.Go(func() {
			for {
				select {
				case <-s.stopBg:
					return
				case <-time.After(s.currentReplayInterval()):
				}
				// Best-effort like C++ scheduleReplay: ticks that fire
				// while the target is still down fail silently and the
				// next tick retries.
				if replMgr.HintCount() > 0 {
					replMgr.ReplayHints()
				}
			}
		})
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
	<-s.sem
	s.wg.Done()
}

// Peer is a reachable cluster member.
type Peer struct {
	ID   string
	Host string
	Port int
}

// ParsePeers parses comma-separated "id@host:port" entries, mirroring the
// C++ --peers flag (parsePeer). Empty input yields no peers.
func ParsePeers(s string) ([]Peer, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []Peer
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		at := strings.LastIndex(part, "@")
		colon := strings.LastIndex(part, ":")
		if at <= 0 || colon <= at+1 {
			return nil, fmt.Errorf("tcp: bad peer %q, want id@host:port", part)
		}
		port, err := strconv.Atoi(part[colon+1:])
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("tcp: bad peer %q, want id@host:port", part)
		}
		host := part[at+1 : colon]
		if host == "" {
			return nil, fmt.Errorf("tcp: bad peer %q, want id@host:port", part)
		}
		out = append(out, Peer{ID: part[:at], Host: host, Port: port})
	}
	return out, nil
}

// ConfigureCluster installs the ring view: self plus peers own keys by
// consistent hash, and redirects name reachable addresses. It also seeds
// the membership table, builds the gossip/failure-detector pair over a
// shared pool, and keeps the ring tracking the alive set.
func (s *Server) ConfigureCluster(peers []Peer) error {
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		return err
	}
	now := time.Now
	s.ringMu.Lock()
	s.ring = ring
	s.peers = append([]Peer(nil), peers...)
	s.ringMu.Unlock()

	table := cluster.NewMembershipTable(s.nodeID, now)
	var seeds []cluster.NodeInfo
	var addrs []client.NodeAddr
	for _, p := range peers {
		seeds = append(seeds, cluster.NodeInfo{ID: p.ID, Host: p.Host, Port: uint16(p.Port)})
		addrs = append(addrs, client.NodeAddr{ID: p.ID, Host: p.Host, Port: p.Port})
	}
	table.SetSelfAddress("127.0.0.1", uint16(s.port))
	table.Seed(seeds)
	pool := client.NewPool(addrs, 5*time.Second)
	gossip := cluster.NewGossipManager(now, pool, table, s.nodeID, s.gossipInterval)
	fd := cluster.NewFailureDetector(now, pool, table, s.nodeID, s.suspectTimeout)
	s.ringMu.Lock()
	s.table = table
	s.gossip = gossip
	s.fd = fd
	s.pool = pool
	s.ringMu.Unlock()
	gossip.Start()
	fd.Start()
	table.OnChange(func() { s.rebuildRingFromTable() })
	s.rebuildRingFromTable()
	return nil
}

// Addr returns the listen address (valid after Serve starts).
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
func (s *Server) ConfigureAntiEntropy(numBuckets uint32, fetchBytes int) {
	s.ringMu.RLock()
	ring := s.ring
	pool := s.pool
	s.ringMu.RUnlock()
	m := ae.NewManager(s.store, ring, s.nodeID, pool, numBuckets, fetchBytes)
	s.aeMu.Lock()
	s.ae = m
	s.aeMu.Unlock()
}

// SetAntiEntropyInterval changes the AE round period.
func (s *Server) SetAntiEntropyInterval(d time.Duration) {
	s.aeMu.Lock()
	defer s.aeMu.Unlock()
	s.aeInterval = d
}

func (s *Server) currentAEInterval() time.Duration {
	s.aeMu.RLock()
	defer s.aeMu.RUnlock()
	if s.aeInterval <= 0 {
		return 30 * time.Second
	}
	return s.aeInterval
}

// AntiEntropy exposes the AE manager for tests.
func (s *Server) AntiEntropy() *ae.Manager {
	s.aeMu.RLock()
	defer s.aeMu.RUnlock()
	return s.ae
}

// Store exposes the underlying store for introspection and tests.
func (s *Server) Store() *store.LRUStore {
	return s.store
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
func (s *Server) SetGossipInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.gossipInterval = d
	if s.gossip != nil {
		s.gossip.SetInterval(d)
	}
}

// SetProbeInterval changes the failure-detector tick period.
func (s *Server) SetProbeInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.probeInterval = d
}

// SetSuspectTimeout changes the suspect/dead timeout.
func (s *Server) SetSuspectTimeout(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.suspectTimeout = d
	if s.fd != nil {
		s.fd.SetSuspectTimeout(d)
	}
}

// SetReplayInterval changes the hint-replay tick period (tests use short
// ones). Zero or negative selects the 1s default.
func (s *Server) SetReplayInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.replayInterval = d
}

func (s *Server) currentReplayInterval() time.Duration {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	if s.replayInterval <= 0 {
		return time.Second
	}
	return s.replayInterval
}

func (s *Server) currentGossipInterval() time.Duration {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	if s.gossipInterval <= 0 {
		return time.Second
	}
	return s.gossipInterval
}

func (s *Server) currentProbeInterval() time.Duration {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	if s.probeInterval <= 0 {
		return time.Second
	}
	return s.probeInterval
}

// rebuildRingFromTable publishes the alive set as the ring. C++ rebuilds
// from its membership snapshot the same way on every change.
func (s *Server) rebuildRingFromTable() {
	s.ringMu.RLock()
	table := s.table
	ring := s.ring
	pool := s.pool
	s.ringMu.RUnlock()
	if table == nil || ring == nil {
		return
	}
	var ids []string
	selfSeen := false
	for _, m := range table.Snapshot() {
		if m.ID != s.nodeID && m.Host != "" && pool != nil {
			// Reachable addresses follow membership like C++ addAddr:
			// replication fan-out, AE, and rebalance can all dial a
			// joiner as soon as gossip delivers it.
			pool.AddAddr(m.ID, m.Host, int(m.Port))
		}
		if m.State != cluster.Alive {
			continue
		}
		ids = append(ids, m.ID)
		if m.ID == s.nodeID {
			selfSeen = true
		}
	}
	if !selfSeen {
		ids = append(ids, s.nodeID)
	}
	_ = ring.SetNodes(ids)
	// Push keys this node no longer owns to their new ring owners. The scan
	// + migration burst is debounced: rapid flaps collapse into one run
	// over the latest ring view.
	s.scheduleRebalanceDebounced()
}

// OwnerOf reports the ring owner for key (self when standalone).
func (s *Server) OwnerOf(key string) string {
	return s.ownerOf(key)
}

// ID returns this node's stable identifier.
func (s *Server) ID() string {
	return s.nodeID
}

// Dispatch routes one request through the full TCP dispatch (ownership,
// replication, admin). The gRPC gateway uses it so both frontends share
// identical Get/Set/Delete semantics.
func (s *Server) Dispatch(req protocol.Request) protocol.Response {
	return s.dispatch(req)
}

// StatusOf maps store/replication errors to wire statuses, shared with
// the gRPC gateway's local-write paths (MultiSet).
func StatusOf(err error) protocol.Status {
	return statusOf(err)
}

// DeleteNoRedirect deletes without the ownership redirect, mirroring the
// C++ gateway Delete, which bypasses checkWriteOwnership at the
// connection layer and deletes through the operations path directly.
func (s *Server) DeleteNoRedirect(key string) protocol.Response {
	if m, rf, mode := s.replication(); m != nil && rf > 1 {
		return writeResult(m.Delete(key, s.replicaNodes(key), mode, 0, 0))
	}
	if err := s.store.RemoveVersioned(key, s.store.MintVersion(), 0); err != nil {
		return protocol.Response{Status: statusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// ownerOf resolves the ring owner, or self when standalone.
func (s *Server) ownerOf(key string) string {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	if s.ring == nil {
		return s.nodeID
	}
	if owner := s.ring.GetNode(key); owner != "" {
		return owner
	}
	return s.nodeID
}

// redirectTo builds the "moved to" value, appending @host:port when a
// reachable address is known, mirroring redirectValue.
func (s *Server) redirectTo(owner string) string {
	var value strings.Builder
	value.WriteString("moved to " + owner)
	s.ringMu.RLock()
	table := s.table
	s.ringMu.RUnlock()
	if table != nil {
		if info, ok := table.Get(owner); ok && info.Host != "" && info.Port != 0 {
			value.WriteString("@" + info.Host + ":" + strconv.Itoa(int(info.Port)))
		}
	}
	return value.String()
}

func (s *Server) notReady(owner string) protocol.Response {
	return protocol.Response{Status: protocol.StatusNotReady, HasValue: true, Value: []byte(s.redirectTo(owner))}
}

// mintTrace assigns entry-point trace context, mirroring the C++
// connection: one atomic add mints both IDs, trace takes the first slot.
func (s *Server) mintTrace(traceID, spanID uint64) (uint64, uint64) {
	if traceID == 0 || spanID == 0 {
		base := s.trace.Add(2) - 1
		if base == 0 {
			base = s.trace.Add(2) - 1
		}
		if traceID == 0 {
			traceID = base
		}
		if spanID == 0 {
			spanID = base + 1
		}
	}
	return traceID, spanID
}

func (s *Server) handle(c net.Conn) {
	defer s.done(c)
	header := make([]byte, protocol.FrameHeaderSize)
	for {
		c.SetDeadline(time.Now().Add(idleTimeout))
		if _, err := io.ReadFull(c, header); err != nil {
			return
		}
		if header[0] != protocol.Magic || header[1] != protocol.Version {
			return // bad protocol header: close, like C++
		}
		payloadLen := binary.BigEndian.Uint32(header[3:7])
		if payloadLen > protocol.MaxPayloadSize || uint64(payloadLen)+protocol.FrameHeaderSize > connBufferSize {
			return // oversized payload: close, like C++
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(c, payload); err != nil {
			return
		}
		frame := append(header, payload...)
		dec, err := protocol.DecodeRequest(frame)
		var res protocol.Response
		if err != nil {
			// Decode failures answer InvalidArgument without trace echo.
			res = protocol.Response{Status: protocol.StatusInvalidArgument}
		} else {
			dec.Req.TraceID, dec.Req.SpanID = s.mintTrace(dec.Req.TraceID, dec.Req.SpanID)
			res = s.dispatch(dec.Req)
			res.TraceID = dec.Req.TraceID
			res.SpanID = dec.Req.SpanID
		}
		out, err := protocol.EncodeResponse(res)
		if err != nil {
			return
		}
		if err := writeAll(c, out); err != nil {
			return
		}
	}
}

func writeAll(c net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := c.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// dispatch routes one decoded request. Single node: every key is local,
// so there are no ownership redirects yet (M4).
func (s *Server) dispatch(req protocol.Request) protocol.Response {
	switch req.Opcode {
	case protocol.OpPing:
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpGet:
		if m, rf, _ := s.replication(); m != nil && rf > 1 {
			e, err := m.Read(string(req.Key), s.replicaNodes(string(req.Key)), rf, req.TraceID, req.SpanID)
			if err != nil {
				if err == repl.ErrNotFound {
					return protocol.Response{Status: protocol.StatusNotFound}
				}
				return protocol.Response{Status: protocol.StatusNotReady}
			}
			if e.Tombstone {
				return protocol.Response{Status: protocol.StatusNotFound}
			}
			return protocol.Response{Status: protocol.StatusOK, Value: e.Value, HasValue: true}
		}
		val, ok := s.store.Get(string(req.Key))
		if !ok {
			// Reads serve from the local copy when present; only a miss
			// consults ownership, redirecting to the ring owner.
			if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
				return s.notReady(owner)
			}
			return protocol.Response{Status: protocol.StatusNotFound}
		}
		return protocol.Response{Status: protocol.StatusOK, Value: val, HasValue: true}
	case protocol.OpGetVersioned:
		e, ok := s.store.GetVersioned(string(req.Key))
		if !ok {
			if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
				return s.notReady(owner)
			}
			return protocol.Response{Status: protocol.StatusNotFound}
		}
		res := protocol.Response{
			Status:         protocol.StatusOK,
			Version:        e.Version,
			WriterNodeHash: e.WriterNodeHash,
			Tombstone:      e.Tombstone,
			HasExpiresAt:   e.HasTTL,
			ExpiresAt:      e.ExpiresAt,
		}
		if !e.Tombstone {
			res.Value = e.Value
			res.HasValue = true
		}
		return res
	case protocol.OpSet:
		if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
			return s.notReady(owner)
		}
		if m, rf, mode := s.replication(); m != nil && rf > 1 {
			var err error
			if req.HasTTL {
				err = m.Write(string(req.Key), req.Value, req.TTL, true, s.replicaNodes(string(req.Key)), mode, req.TraceID, req.SpanID)
			} else {
				err = m.Write(string(req.Key), req.Value, 0, false, s.replicaNodes(string(req.Key)), mode, req.TraceID, req.SpanID)
			}
			return writeResult(err)
		}
		var err error
		if req.HasTTL {
			err = s.store.PutWithTTL(string(req.Key), req.Value, req.TTL)
		} else {
			err = s.store.Put(string(req.Key), req.Value)
		}
		if err != nil {
			return protocol.Response{Status: statusOf(err)}
		}
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpDel:
		if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
			return s.notReady(owner)
		}
		if m, rf, mode := s.replication(); m != nil && rf > 1 {
			return writeResult(m.Delete(string(req.Key), s.replicaNodes(string(req.Key)), mode, req.TraceID, req.SpanID))
		}
		// Single-node delete plants a versioned tombstone with a minted
		// version and writer 0, exactly like CacheOperations::deleteAsync
		// without replication: deletes always succeed, even on missing keys.
		if err := s.store.RemoveVersioned(string(req.Key), s.store.MintVersion(), 0); err != nil {
			return protocol.Response{Status: statusOf(err)}
		}
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpReplicate, protocol.OpHint:
		return s.applyReplicated(req)
	case protocol.OpGossip:
		// No membership configured: accept and ignore, like C++ with a
		// nil gossip manager. Otherwise fold the view into the table.
		s.ringMu.RLock()
		gossip := s.gossip
		s.ringMu.RUnlock()
		if gossip != nil {
			gossip.HandleMessage("", req.Value)
		}
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpAntiEntropyDigest, protocol.OpAntiEntropySync:
		s.aeMu.RLock()
		mgr := s.ae
		s.aeMu.RUnlock()
		if mgr == nil {
			// No anti-entropy manager: the C++ fallthrough answers default-OK.
			return protocol.Response{Status: protocol.StatusOK}
		}
		if req.Opcode == protocol.OpAntiEntropyDigest {
			if string(req.Key) == ae.RefinementKey {
				st, val := mgr.OnBucketRefinement(req.Value)
				return protocol.Response{Status: st, Value: val, HasValue: val != nil}
			}
			st, val := mgr.OnDigestRequest(req.Value)
			return protocol.Response{Status: st, Value: val, HasValue: val != nil}
		}
		st, val := mgr.OnSyncRequest(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}
	case protocol.OpCounterAdjust, protocol.OpCounterMerge,
		protocol.OpCounterGet, protocol.OpCounterAntiEntropy:
		return protocol.Response{Status: protocol.StatusNotSupported}
	default:
		return s.admin(req)
	}
}

// applyReplicated ports TcpConnection::handleReplicate: absolute wall-clock
// expiry rides the request; a tombstone without a sender deadline falls
// back to the local GC deadline via the versioned remove path.
func (s *Server) applyReplicated(req protocol.Request) protocol.Response {
	if req.Tombstone && !req.HasExpiresAt {
		if err := s.store.RemoveVersioned(string(req.Key), req.Version, req.WriterNodeHash); err != nil {
			return protocol.Response{Status: statusOf(err)}
		}
		return protocol.Response{Status: protocol.StatusOK}
	}
	e := store.Entry{
		Value:          append([]byte(nil), req.Value...),
		Version:        req.Version,
		WriterNodeHash: req.WriterNodeHash,
		Tombstone:      req.Tombstone,
	}
	if req.HasExpiresAt {
		e.HasTTL = true
		e.ExpiresAt = req.ExpiresAt
	}
	if err := s.store.PutVersioned(string(req.Key), e); err != nil {
		return protocol.Response{Status: statusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

func statusOf(err error) protocol.Status {
	switch err {
	case nil:
		return protocol.StatusOK
	case store.ErrCapacityExceeded:
		return protocol.StatusCapacityExceeded
	case repl.ErrQuorum:
		return protocol.StatusNotReady
	case repl.ErrNotFound:
		return protocol.StatusNotFound
	default:
		return protocol.StatusInternalError
	}
}

func writeResult(err error) protocol.Response {
	return protocol.Response{Status: statusOf(err)}
}

// adminJSON shapes mirror formatNodeInfoJson/formatClusterJson/formatRingJson.
type adminInfo struct {
	NodeID                  string `json:"node_id"`
	Port                    int    `json:"port"`
	CapacityBytes           uint64 `json:"capacity_bytes"`
	CurrentBytes            uint64 `json:"current_bytes"`
	CurrentEntries          uint64 `json:"current_entries"`
	EvictionPolicy          string `json:"eviction_policy"`
	ReplicaFactor           int    `json:"replica_factor"`
	Consistency             string `json:"consistency"`
	PersistenceEnabled      bool   `json:"persistence_enabled"`
	AntiEntropyIntervalMs   int64  `json:"anti_entropy_interval_ms"`
	AntiEntropyBuckets      int    `json:"anti_entropy_buckets"`
	AntiEntropyFetchBytes   int64  `json:"anti_entropy_fetch_bytes"`
	AntiEntropyFullRehashMs int64  `json:"anti_entropy_full_rehash_interval_ms"`
	TLSEnabled              bool   `json:"tls_enabled"`
	AuthEnabled             bool   `json:"auth_enabled"`
	ClientAuthEnabled       bool   `json:"client_auth_enabled"`
	LogLevel                string `json:"log_level"`
	Peers                   []any  `json:"peers"`
}

type adminClusterNode struct {
	ID          string `json:"id"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	State       string `json:"state"`
	Incarnation uint64 `json:"incarnation"`
}

type adminRing struct {
	Self          string `json:"self"`
	VNodesPerNode int    `json:"vnodes_per_node"`
	Note          string `json:"note"`
}

func (s *Server) admin(req protocol.Request) protocol.Response {
	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			return []byte("{}")
		}
		return b
	}
	switch req.Opcode {
	case protocol.OpAdminInfo:
		curBytes, curEntries := s.store.Stats()
		return protocol.Response{Status: protocol.StatusOK, HasValue: true, Value: mustJSON(adminInfo{
			NodeID: s.nodeID, Port: s.port, CapacityBytes: s.store.Capacity(),
			CurrentBytes: curBytes, CurrentEntries: curEntries,
			EvictionPolicy: "lru", ReplicaFactor: 1, Consistency: "async",
			LogLevel: "info", Peers: []any{},
		})}
	case protocol.OpAdminCluster:
		s.ringMu.RLock()
		table := s.table
		s.ringMu.RUnlock()
		var nodes []adminClusterNode
		if table != nil {
			for _, m := range table.Snapshot() {
				state := "alive"
				switch m.State {
				case cluster.Suspect:
					state = "suspect"
				case cluster.Dead:
					state = "dead"
				}
				nodes = append(nodes, adminClusterNode{ID: m.ID, Host: m.Host, Port: int(m.Port), State: state, Incarnation: m.Incarnation})
			}
		} else {
			nodes = []adminClusterNode{{ID: s.nodeID, Host: "127.0.0.1", Port: s.port, State: "alive", Incarnation: 1}}
		}
		if nodes == nil {
			nodes = []adminClusterNode{}
		}
		return protocol.Response{Status: protocol.StatusOK, HasValue: true, Value: mustJSON(nodes)}
	case protocol.OpAdminRing:
		return protocol.Response{Status: protocol.StatusOK, HasValue: true, Value: mustJSON(adminRing{
			Self: s.nodeID, VNodesPerNode: 150,
			Note: "ring is an immutable snapshot with 150 vnodes per physical node",
		})}
	case protocol.OpAdminCompact, protocol.OpAdminConfigReload:
		if req.Opcode == protocol.OpAdminCompact {
			if err := s.Compact(); err != nil {
				return protocol.Response{Status: protocol.StatusInternalError}
			}
		}
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpAdminShutdown:
		go s.Shutdown()
		return protocol.Response{Status: protocol.StatusOK}
	default:
		return protocol.Response{Status: protocol.StatusInvalidArgument}
	}
}
