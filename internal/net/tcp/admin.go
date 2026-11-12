package tcp

import (
	"encoding/json/v2"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

type adminInfo struct {
	NodeID                  string                `json:"node_id"`
	Port                    int                   `json:"port"`
	CapacityBytes           uint64                `json:"capacity_bytes"`
	CurrentBytes            uint64                `json:"current_bytes"`
	CurrentEntries          uint64                `json:"current_entries"`
	EvictionPolicy          string                `json:"eviction_policy"`
	ReplicaFactor           hashring.ReplicaCount `json:"replica_factor"`
	Consistency             string                `json:"consistency"`
	PersistenceEnabled      bool                  `json:"persistence_enabled"`
	AntiEntropyIntervalMs   int64                 `json:"anti_entropy_interval_ms"`
	AntiEntropyBuckets      int                   `json:"anti_entropy_buckets"`
	AntiEntropyFetchBytes   int64                 `json:"anti_entropy_fetch_bytes"`
	AntiEntropyFullRehashMs int64                 `json:"anti_entropy_full_rehash_interval_ms"`
	TLSEnabled              bool                  `json:"tls_enabled"`
	AuthEnabled             bool                  `json:"auth_enabled"`
	ClientAuthEnabled       bool                  `json:"client_auth_enabled"`
	LogLevel                string                `json:"log_level"`
	Peers                   []string              `json:"peers"`
}

type adminClusterNode struct {
	ID          string       `json:"id"`
	Host        string       `json:"host"`
	Port        netport.Port `json:"port"`
	State       string       `json:"state"`
	Incarnation uint64       `json:"incarnation"`
}

type adminRing struct {
	Self          string `json:"self"`
	VNodesPerNode int    `json:"vnodes_per_node"`
	Note          string `json:"note"`
}

type adminReloadResult struct {
	Applied []string `json:"applied"`
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
		_, port := s.advertisedAddress()
		return protocol.Response{Status: protocol.StatusOK, HasValue: true, Value: mustJSON(adminInfo{
			NodeID: s.nodeID, Port: int(port), CapacityBytes: s.store.Capacity(),
			CurrentBytes: curBytes, CurrentEntries: curEntries,
			EvictionPolicy: s.store.Policy().String(), ReplicaFactor: 1, Consistency: "async",
			LogLevel: "info", Peers: []string{},
		})}
	case protocol.OpAdminCluster:
		s.ringMu.RLock()
		table := s.table
		s.ringMu.RUnlock()
		var nodes []adminClusterNode
		if table != nil {
			for _, m := range table.Snapshot() {
				nodes = append(nodes, adminClusterNode{ID: m.ID, Host: m.Host, Port: m.Port, State: m.State.String(), Incarnation: m.Incarnation})
			}
		} else {
			host, port := s.advertisedAddress()
			nodes = []adminClusterNode{{ID: s.nodeID, Host: host, Port: port, State: "alive", Incarnation: 1}}
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
	case protocol.OpAdminCompact:
		if err := s.Compact(); err != nil {
			return protocol.Response{Status: protocol.StatusInternalError}
		}
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpAdminConfigReload:
		var payload map[string]any
		if len(req.Value) > 0 {
			if err := json.Unmarshal(req.Value, &payload); err != nil {
				return protocol.Response{Status: protocol.StatusInvalidArgument}
			}
		}

		var bad []string
		for k := range payload {
			if !isReloadableKey(k) {
				bad = append(bad, k)
			}
		}
		if len(bad) > 0 {
			return protocol.Response{Status: protocol.StatusInvalidArgument, HasValue: true, Value: mustJSON(map[string]any{
				"error":      "not reloadable, requires restart",
				"rejected":   bad,
				"reloadable": reloadableKeys,
			})}
		}

		var rc ReloadableConfig
		if len(payload) == 0 {
			return protocol.Response{Status: protocol.StatusOK}
		}

		raw, err := json.Marshal(payload)
		if err != nil {
			return protocol.Response{Status: protocol.StatusInternalError}
		}
		if err := json.Unmarshal(raw, &rc); err != nil {
			return protocol.Response{Status: protocol.StatusInvalidArgument}
		}
		if _, err := s.Reload(rc); err != nil {
			return protocol.Response{Status: protocol.StatusInvalidArgument, HasValue: true, Value: mustJSON(map[string]any{
				"error": err.Error(),
			})}
		}

		present := make([]string, 0, len(payload))
		for k := range payload {
			present = append(present, k)
		}
		return protocol.Response{Status: protocol.StatusOK, HasValue: true, Value: mustJSON(adminReloadResult{Applied: present})}
	case protocol.OpAdminShutdown:
		return protocol.Response{Status: protocol.StatusOK}
	default:
		return protocol.Response{Status: protocol.StatusInvalidArgument}
	}
}
