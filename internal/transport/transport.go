// Package transport declares the inter-node send contract every
// subsystem shares: replication, shard migration, and anti-entropy all
// fan out protocol.Request frames and collect protocol.Responses.
// client.Pool is the production implementation; tests substitute fakes.
package transport

import (
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// Sender delivers one request frame to a named node.
type Sender interface {
	Send(node string, req protocol.Request) (protocol.Response, error)
}
