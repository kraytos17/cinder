package repl

import (
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// ReplicateCounter fans out full PN-Counter state as OpCounterMerge to
// every replica except self. Best-effort like C++
// replicateCounterState: duplicate/reordered delivery is safe (merge is
// idempotent), and failed sends are NOT converted to hints — counter
// merges are not LWW writes, so the hint path (which replays via
// applyReplicated LWW) cannot carry them. Anti-entropy repairs drops.
func (m *Manager) ReplicateCounter(key string, encodedState []byte, replicas []string, traceID, spanID uint64) {
	if len(encodedState) == 0 {
		return
	}
	req := protocol.Request{
		Opcode: protocol.OpCounterMerge, Key: []byte(key),
		Value:   append([]byte(nil), encodedState...),
		TraceID: traceID, SpanID: spanID,
	}
	for _, node := range replicas {
		if node == m.self {
			continue
		}
		go func(node string) {
			// Best-effort: failures are logged by dropping; the next
			// adjust re-sends full state and AE converges the rest.
			_, _ = m.transport.Send(node, req)
		}(node)
	}
}
