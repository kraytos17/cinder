package e2e_test

import (
	"fmt"
	"maps"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// ackLog is the scenario verifier: every write the cluster acknowledged,
// keyed by key (keys are unique per write, so the log is the exact set of
// values the final state must contain).
type ackLog struct {
	mu   sync.Mutex
	keys map[string]string
}

func (l *ackLog) add(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.keys == nil {
		l.keys = map[string]string{}
	}
	l.keys[key] = value
}

func (l *ackLog) snapshot() map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.keys)
}

// bgWriter hammers unique keys through ring owners until stopped, recording
// every acknowledged write. One dial per op keeps connections fresh across
// kills and restarts; pacing bounds the key count so final verification
// stays fast. The ring is swappable so writers can follow topology changes
// (e.g. a join) the way a topology-refreshing client would.
type bgWriter struct {
	acked  atomic.Uint64
	failed atomic.Uint64
	ring   atomic.Pointer[hashring.Ring]
	log    *ackLog
	stop   chan struct{}
	done   chan struct{}
}

// setRing publishes a new routing view for subsequent writes.
func (w *bgWriter) setRing(ring *hashring.Ring) { w.ring.Store(ring) }

func startWriter(addrs map[string]string, ring *hashring.Ring, prefix string) *bgWriter {
	w := &bgWriter{log: &ackLog{}, stop: make(chan struct{}), done: make(chan struct{})}
	w.ring.Store(ring)
	go func() {
		defer close(w.done)
		var n uint64
		for {
			select {
			case <-w.stop:
				return
			default:
			}
			n++
			key := fmt.Sprintf("%s-%d", prefix, n)
			value := "v-" + key
			c, err := client.Dial(addrs[w.ring.Load().GetNode(key)], 500*time.Millisecond)
			if err != nil {
				w.failed.Add(1)
				continue
			}
			res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(value)})
			c.Close()
			if err != nil || res.Status != protocol.StatusOK {
				w.failed.Add(1)
				continue
			}
			w.acked.Add(1)
			w.log.add(key, value)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return w
}

func (w *bgWriter) finish() {
	close(w.stop)
	<-w.done
}

// TestE2EKillPrimaryDuringWrite runs continuous writes while SIGKILLing a
// node mid-stream, keeps writing through the outage, restarts the victim
// empty, and requires every acknowledged write to be present on all three
// nodes afterwards. It also requires the outage to have been real: at least
// one write must have failed while the node was down, and at least one must
// have been acknowledged.
func TestE2EKillPrimaryDuringWrite(t *testing.T) {
	if os.Getenv("CINDER_E2E") == "" {
		t.Skip("set CINDER_E2E=1 to run real-process e2e (builds cinderd, binds loopback ports)")
	}
	bin := buildCinderd(t)
	ids := []string{"n1", "n2", "n3"}
	ports := map[string]int{}
	for _, id := range ids {
		ports[id] = freePort(t)
	}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", ports[id])
	}
	peersOf := func(id string) []string {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, fmt.Sprintf("%s@%s", other, addrs[other]))
			}
		}
		return peers
	}

	procs := map[string]*proc{}
	start := func(id string) {
		procs[id] = startNode(t, bin, id, ports[id], peersOf(id), 3)
	}
	for _, id := range ids {
		start(id)
	}
	for _, id := range ids {
		waitReady(t, addrs[id], 20*time.Second)
	}

	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes(ids); err != nil {
		t.Fatal(err)
	}

	w := startWriter(addrs, ring, "e2e-kw")
	time.Sleep(1500 * time.Millisecond) // phase A: healthy writes
	ackedA, failedA := w.acked.Load(), w.failed.Load()

	procs["n1"].kill(t)                 // SIGKILL mid-stream, fixed victim for a stable schedule
	time.Sleep(2500 * time.Millisecond) // phase B: outage past the 1s suspect timeout
	ackedB, failedB := w.acked.Load(), w.failed.Load()

	start("n1") // restart empty on the old port
	waitReady(t, addrs["n1"], 20*time.Second)
	time.Sleep(3 * time.Second) // rejoin, rebalance, anti-entropy
	ackedC, failedC := w.acked.Load(), w.failed.Load()
	w.finish()

	if failedB-failedA == 0 {
		t.Fatal("no write failed during the outage: kill did not overlap traffic")
	}
	if ackedB-ackedA == 0 {
		t.Fatal("no write acknowledged during the outage: cluster stalled")
	}
	t.Logf("acked A/B/C-total=%d/%d/%d failed A/B/C-total=%d/%d/%d",
		ackedA, ackedB, ackedC, failedA, failedB, failedC)

	// Every acknowledged write must be present on every node. Poll: the
	// restarted victim repopulates asynchronously.
	acked := w.log.snapshot()
	deadline := time.Now().Add(60 * time.Second)
	for key, want := range acked {
		for _, id := range ids {
			for {
				if res := doGetVersioned(t, addrs[id], key); res.Status == protocol.StatusOK && string(res.Value) == want {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("acked %q missing/wrong on %s", key, id)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
