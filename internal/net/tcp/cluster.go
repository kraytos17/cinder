package tcp

// Cluster membership: static peers, ring rebuilds, rebalance
// scheduling, and quarantine/migration retry timers.

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/shard"
)

func (s *Server) ConfigureRebalance(rf hashring.ReplicaCount, quarantine time.Duration) error {
	if rf < 1 || rf > hashring.MaxReplicas {
		return hashring.ErrInvalidReplicaCount
	}
	if quarantine < 0 {
		return errors.New("tcp: quarantine interval must not be negative")
	}

	s.ringMu.RLock()
	ring := s.ring
	pool := s.pool
	table := s.table
	s.ringMu.RUnlock()
	m := shard.NewManager(shard.Config{
		Store: s.store, Ring: ring, Transport: pool, Membership: table,
		Self: s.nodeID, ReplicaFactor: rf, Quarantine: quarantine,
	})

	m.SetOnMigrationFailed(func() { s.scheduleMigrationRetry() })
	m.SetMetrics(s.met)
	m.SetLogger(s.log())
	s.shardMu.Lock()
	s.shard = m
	s.quarantineInterval = quarantine
	s.shardMu.Unlock()
	return nil
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

		s.met.RebalanceMigRetries.Add(1)
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

type Peer struct {
	ID   string
	Host string
	Port netport.Port
}

// ParsePeers parses comma-separated "id@host:port" entries. Empty input
// yields no peers.
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

		port, err := netport.ParsePort(part[colon+1:])
		if err != nil {
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
		seeds = append(seeds, cluster.NodeInfo{ID: p.ID, Host: p.Host, Port: p.Port})
		addrs = append(addrs, client.NodeAddr{ID: p.ID, Host: p.Host, Port: p.Port})
	}

	table.SetSelfAddress("127.0.0.1", s.port)
	table.Seed(seeds)
	pool := client.NewPool(addrs, 5*time.Second)
	gossip := cluster.NewGossipManager(now, pool, table, s.nodeID, s.gossipInterval)
	fd := cluster.NewFailureDetector(now, pool, table, s.nodeID, s.suspectTimeout)
	table.SetMetrics(s.met)
	gossip.SetMetrics(s.met)
	fd.SetMetrics(s.met)
	table.SetLogger(s.log())
	gossip.SetLogger(s.log())
	fd.SetLogger(s.log())
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

func (s *Server) currentAEInterval() time.Duration {
	s.aeMu.RLock()
	defer s.aeMu.RUnlock()
	if s.aeInterval <= 0 {
		return 30 * time.Second
	}
	return s.aeInterval
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

// rebuildRingFromTable publishes the current alive membership as a new ring
// snapshot. It also refreshes peer addresses and schedules rebalance work
// when the ring's membership changes.
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
			// Learn addresses for newly joined peers so replication, AE, and
			// rebalance can dial them as soon as gossip delivers their address.
			pool.AddAddr(m.ID, m.Host, m.Port)
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

	ring.SetNodes(ids)
	// Push keys this node no longer owns to their new ring owners. The scan
	// + migration burst is debounced: rapid flaps collapse into one run
	// over the latest ring view.
	s.scheduleRebalanceDebounced()
}

// OwnerOf reports the ring owner for key (self when standalone).
func (s *Server) OwnerOf(key string) string {
	return s.ownerOf(key)
}
