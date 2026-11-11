// Package metrics provides atomic counters and latency histograms for
// request handling, storage, replication, membership, rebalance, and
// connections.
// AppendPrometheus exposes the collector using the metric names documented
// by the service. Gauges whose source values are maintained elsewhere are
// supplied at scrape time rather than duplicated here. Metric names and
// labels are a monitoring API and must remain stable. Per-replica lag and
// replay-snapshot byte metrics are not emitted because this collector does
// not receive those measurements.
//
// Counters use atomic values and can be updated concurrently.
// Gauges for store bytes/entries/capacity are read live from the store at
// scrape time and passed in rather than maintained a second time here.
package metrics

import (
	"sync/atomic"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

// OpName returns the stable Prometheus label for an opcode, or "unknown"
// for an unrecognized value.
func OpName(op protocol.Opcode) string {
	switch op {
	case protocol.OpGet:
		return "get"
	case protocol.OpSet:
		return "set"
	case protocol.OpDel:
		return "del"
	case protocol.OpPing:
		return "ping"
	case protocol.OpGossip:
		return "gossip"
	case protocol.OpReplicate:
		return "replicate"
	case protocol.OpHint:
		return "hint"
	case protocol.OpGetVersioned:
		return "get_versioned"
	case protocol.OpAntiEntropyDigest:
		return "anti_entropy_digest"
	case protocol.OpAntiEntropySync:
		return "anti_entropy_sync"
	case protocol.OpAdminInfo:
		return "admin_info"
	case protocol.OpAdminCluster:
		return "admin_cluster"
	case protocol.OpAdminRing:
		return "admin_ring"
	case protocol.OpAdminCompact:
		return "admin_compact"
	case protocol.OpAdminConfigReload:
		return "admin_config_reload"
	case protocol.OpAdminShutdown:
		return "admin_shutdown"
	case protocol.OpCounterAdjust:
		return "counter_adjust"
	case protocol.OpCounterMerge:
		return "counter_merge"
	case protocol.OpCounterGet:
		return "counter_get"
	case protocol.OpCounterAntiEntropy:
		return "counter_anti_entropy"
	default:
		return "unknown"
	}
}

// maxOpcode is the highest assigned wire opcode; index 0 is unused.
const maxOpcode = 20

func opIndex(op protocol.Opcode) (int, bool) {
	if op >= 1 && op <= maxOpcode {
		return int(op), true
	}
	return 0, false
}

// Collector holds every counter. Use NewCollector and bump the fields
// directly (they are plain atomics); scrape with AppendPrometheus.
type Collector struct {
	ops     [maxOpcode + 1]atomic.Uint64
	latency [maxOpcode + 1]Histogram

	Hits          atomic.Uint64
	Misses        atomic.Uint64
	Writes        atomic.Uint64
	Deletes       atomic.Uint64
	EvictionsTTL  atomic.Uint64
	EvictionsCap  atomic.Uint64
	ExpiresOnRead atomic.Uint64

	AsyncWrites     atomic.Uint64
	AsyncDeletes    atomic.Uint64
	QuorumWritesOK  atomic.Uint64
	QuorumWritesErr atomic.Uint64
	QuorumDeletesOK atomic.Uint64
	QuorumDeletesEr atomic.Uint64
	QuorumReads     atomic.Uint64
	ReadsCoalesced  atomic.Uint64
	ReadRepairs     atomic.Uint64
	// QuorumLateFail counts replica sends that failed after the quorum
	// decision already fired (stragglers drained into hints).
	QuorumLateFail   atomic.Uint64
	HintsEnqueued    atomic.Uint64
	HintsReplayed    atomic.Uint64
	HintsDropped     atomic.Uint64
	HintEntries      atomic.Uint64
	HintBytes        atomic.Uint64
	HintBytesPeak    atomic.Uint64
	ReplicaUnreach   atomic.Uint64
	AERounds         atomic.Uint64
	AEKeysRepaired   atomic.Uint64
	AEMetaSent       atomic.Uint64
	AEMetaRecv       atomic.Uint64
	AEFetched        atomic.Uint64
	AEFullRehashes   atomic.Uint64
	AERehashMismatch atomic.Uint64
	AEGroupMismatch  atomic.Uint64
	AEBucketMismatch atomic.Uint64
	AEIBLTOk         atomic.Uint64
	AEIBLTFallback   atomic.Uint64
	// Counter AE repair shares the AE round/repair counters above only
	// when the LWW round runs; dedicated counter-round outcomes:
	CounterAERounds   atomic.Uint64
	CounterAERepaired atomic.Uint64

	GossipRounds  atomic.Uint64
	ProbeSent     atomic.Uint64
	ProbeReceived atomic.Uint64
	SuspectMarked atomic.Uint64
	DeadMarked    atomic.Uint64
	AliveMarked   atomic.Uint64
	RefutedRumors atomic.Uint64

	RebalanceCopies       atomic.Uint64
	RebalanceMigrations   atomic.Uint64
	RebalanceMigFailures  atomic.Uint64
	RebalanceMigRetries   atomic.Uint64
	RebalanceDroppedUnder atomic.Uint64

	ConnsOpened    atomic.Uint64
	ConnsClosed    atomic.Uint64
	DecodeFailures atomic.Uint64
	Redirects      atomic.Uint64
	WriteFailures  atomic.Uint64
	// SlowConsumerDrops counts connections closed because a response
	// write exceeded the write timeout: the peer was not reading.
	// Distinct from WriteFailures (encode errors, resets).
	SlowConsumerDrops atomic.Uint64
}

// NewCollector returns an empty collector.
func NewCollector() *Collector { return &Collector{} }

// Record increments the opcode counter and latency histogram for a known
// opcode. Unknown opcodes are ignored; the protocol decoder rejects them
// before dispatch.
func (c *Collector) Record(op protocol.Opcode, ns uint64) {
	if i, ok := opIndex(op); ok {
		c.ops[i].Add(1)
		c.latency[i].Record(ns)
	}
}

// OpCount returns the recorded count for op (0 for unknown).
func (c *Collector) OpCount(op protocol.Opcode) uint64 {
	if i, ok := opIndex(op); ok {
		return c.ops[i].Load()
	}
	return 0
}
