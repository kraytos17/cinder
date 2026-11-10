// Package testkit holds the fake transports shared by subsystem tests:
// a scripted result replayer, a store-backed fake cluster, and a function
// adapter. One canonical implementation replaces the four near-identical
// fakes (repl, shard, ae journal, cluster script), which differed only in
// counting-field names and tombstone edge handling; behavior here is the
// repl fake's full semantics (Replicate/Hint with the tombstone fallback
// path, GetVersioned, NotSupported otherwise), a superset the other
// suites' tests also satisfy.
package testkit

import (
	"errors"
	"sync"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

// Func adapts a function to the transport.Sender interface.
type Func func(node string, req protocol.Request) (protocol.Response, error)

// Send calls f.
func (f Func) Send(node string, req protocol.Request) (protocol.Response, error) {
	return f(node, req)
}

// Script replays canned results per node: each Send consumes the head of
// the node's queue (nil entries answer OK), so tests script exact
// failure sequences. Sends counts every call.
type Script struct {
	mu      sync.Mutex
	results map[string][]error
	Sends   map[string]int
}

// NewScript returns an empty script.
func NewScript() *Script {
	return &Script{results: map[string][]error{}, Sends: map[string]int{}}
}

// Fail queues errs for node, consumed in order by Send.
func (s *Script) Fail(node string, errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[node] = append(s.results[node], errs...)
}

// Send replays the next canned result for node, or OK when unscripted.
func (s *Script) Send(node string, req protocol.Request) (protocol.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Sends[node]++
	if len(s.results[node]) > 0 {
		err := s.results[node][0]
		s.results[node] = s.results[node][1:]
		if err != nil {
			return protocol.Response{}, err
		}
	}
	return protocol.Response{Status: protocol.StatusOK}, nil
}

// Count returns the sends to node.
func (s *Script) Count(node string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Sends[node]
}

// Stores serves requests against per-node stores, with per-node failure
// injection and send counts. Replicate/Hint apply versioned writes (the
// tombstone fallback removes with sender identity); GetVersioned serves
// voters; anything else is NotSupported.
type Stores struct {
	mu sync.Mutex
	// Stores holds the per-node state; tests seed and inspect it directly.
	Stores map[string]*store.Store
	// Fail, when true for a node, fails every send to it.
	Fail map[string]bool
	// Sends counts sends per node; prefer SendCount outside the package.
	Sends map[string]int
}

// NewStores returns a fake over fresh stores for nodes.
func NewStores(nodes ...string) *Stores {
	ft := &Stores{Stores: map[string]*store.Store{}, Fail: map[string]bool{}, Sends: map[string]int{}}
	for _, n := range nodes {
		ft.Stores[n] = store.NewLRUStore(1<<20, nil)
	}
	return ft
}

// Send routes one request to the node's store.
func (f *Stores) Send(node string, req protocol.Request) (protocol.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Sends[node]++
	if f.Fail[node] {
		return protocol.Response{}, errors.New("unreachable: " + node)
	}
	st, ok := f.Stores[node]
	if !ok {
		return protocol.Response{}, errors.New("unknown node: " + node)
	}
	switch req.Opcode {
	case protocol.OpReplicate, protocol.OpHint:
		e := store.Entry{Value: append([]byte(nil), req.Value...), Version: req.Version, WriterNodeHash: req.WriterNodeHash, Tombstone: req.Tombstone}
		if req.HasExpiresAt {
			e.HasTTL = true
			e.ExpiresAt = req.ExpiresAt
		}
		if err := st.PutVersioned(string(req.Key), e); err != nil {
			return protocol.Response{Status: protocol.StatusCapacityExceeded}, nil
		}
		if req.Tombstone && !req.HasExpiresAt {
			// Fallback path: versioned remove with sender identity.
			if err := st.RemoveVersioned(string(req.Key), req.Version, req.WriterNodeHash); err != nil {
				return protocol.Response{Status: protocol.StatusInternalError}, nil
			}
		}
		return protocol.Response{Status: protocol.StatusOK}, nil
	case protocol.OpGetVersioned:
		e, ok := st.GetVersioned(string(req.Key))
		if !ok {
			return protocol.Response{Status: protocol.StatusNotFound}, nil
		}
		res := protocol.Response{Status: protocol.StatusOK, Version: e.Version, WriterNodeHash: e.WriterNodeHash, Tombstone: e.Tombstone}
		if !e.Tombstone {
			res.Value = append([]byte(nil), e.Value...)
			res.HasValue = true
		}
		if e.HasTTL {
			res.HasExpiresAt = true
			res.ExpiresAt = e.ExpiresAt
		}
		return res, nil
	default:
		return protocol.Response{Status: protocol.StatusNotSupported}, nil
	}
}

// SendCount returns the sends to node.
func (f *Stores) SendCount(node string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Sends[node]
}
