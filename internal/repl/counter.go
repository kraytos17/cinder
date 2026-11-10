package repl

import (
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// ReplicateCounter sends full PN-Counter state as OpCounterMerge to every
// replica except self. Duplicate or reordered delivery is safe because
// merge is idempotent. Failed sends are not queued as hints: hints replay
// through the LWW path and cannot carry counter state. Anti-entropy repairs
// missed merges.
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
			// Best-effort: the next adjustment resends full state, and
			// anti-entropy repairs any remaining divergence.
			_, _ = m.transport.Send(node, req)
		}(node)
	}
}
