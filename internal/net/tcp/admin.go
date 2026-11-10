package tcp

import (
	"encoding/json/v2"

	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

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
			EvictionPolicy: s.store.Policy().String(), ReplicaFactor: 1, Consistency: "async",
			LogLevel: "info", Peers: []any{},
		})}
	case protocol.OpAdminCluster:
		s.ringMu.RLock()
		table := s.table
		s.ringMu.RUnlock()
		var nodes []adminClusterNode
		if table != nil {
			for _, m := range table.Snapshot() {
				state := m.State.String()
				switch m.State {
				case cluster.Alive:
					state = "alive"
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
