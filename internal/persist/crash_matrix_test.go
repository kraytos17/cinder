package persist

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

// TestDurabilityCrashRecoveryMatrix runs 100 deterministic randomized
// crash/restart cases spanning KV state, tombstones, CRDT counters, hints,
// and snapshot/WAL cutovers. Seeds are fixed and included in failures so a
// failing case is exactly reproducible.
func TestDurabilityCrashRecoveryMatrix(t *testing.T) {
	const cases = 100
	for seed := range cases {
		seed := int64(seed)
		t.Run(fmt.Sprintf("seed-%03d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed + 20261113))
			dir := t.TempDir()
			cache := store.NewLRUStore(1<<20, nil)
			counters := counter.NewStore(1 << 20)
			manager := NewManager(dir, cache, nil)
			manager.SetCounterStore(counters)
			if err := manager.Recover(); err != nil {
				t.Fatalf("seed=%d recover initial: %v", seed, err)
			}
			if err := manager.SetFsyncPolicy(FsyncAlways, 0); err != nil {
				t.Fatalf("seed=%d fsync policy: %v", seed, err)
			}
			cache.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
				return manager.OnWrite(key, e, hardDelete)
			})
			counters.SetPersistence(func(key string, state counter.PNCounter, remove bool) error {
				return manager.OnCounterWrite(key, state, remove)
			})

			hints := repl.NewHintQueue()
			journal, _, err := ae.OpenJournal(filepath.Join(dir, "hints.log"), hints)
			if err != nil {
				t.Fatalf("seed=%d open journal: %v", seed, err)
			}
			kvKeys := []string{"kv-a", "kv-b", "kv-c", "kv-d"}
			counterKeys := []string{"counter-a", "counter-b", "counter-c"}
			expectedKV := map[string]store.Entry{}
			expectedCounters := map[string]counter.PNCounter{}
			expectedHints := map[string]repl.Hint{}

			for op := range 24 {
				key := kvKeys[rng.Intn(len(kvKeys))]
				version := uint64(seed*100 + int64(op) + 1)
				switch rng.Intn(4) {
				case 0, 1:
					value := fmt.Sprintf("value-%d-%d", seed, op)
					e := store.Entry{Value: []byte(value), Version: version, WriterNodeHash: uint64(seed + 1)}
					if err := cache.PutVersioned(key, e); err != nil {
						t.Fatalf("seed=%d put %q: %v", seed, key, err)
					}
					expectedKV[key] = e
				case 2:
					tomb := store.Entry{
						Version: version, WriterNodeHash: uint64(seed + 1), Tombstone: true,
						Expiry: expiry.At(time.Now().Add(24 * time.Hour)),
					}
					if err := cache.PutVersioned(key, tomb); err != nil {
						t.Fatalf("seed=%d tombstone %q: %v", seed, key, err)
					}
					expectedKV[key] = tomb
				case 3:
					if err := cache.RemoveVersioned(key, version, uint64(seed+1)); err != nil {
						t.Fatalf("seed=%d delete %q: %v", seed, key, err)
					}
					got, ok := cache.GetVersioned(key)
					if !ok {
						t.Fatalf("seed=%d deleted key %q has no tombstone", seed, key)
					}
					expectedKV[key] = got
				}

				ck := counterKeys[rng.Intn(len(counterKeys))]
				actor := fmt.Sprintf("actor-%d", rng.Intn(3))
				amount := uint64(rng.Intn(20) + 1)
				var err error
				if rng.Intn(2) == 0 {
					err = counters.Increment(ck, actor, amount)
				} else {
					err = counters.Decrement(ck, actor, amount)
				}
				if err != nil {
					t.Fatalf("seed=%d counter mutation %q: %v", seed, ck, err)
				}
				if rng.Intn(4) == 0 {
					var remote counter.PNCounter
					if err := remote.Increment(fmt.Sprintf("remote-%d", rng.Intn(3)), amount); err != nil {
						t.Fatal(err)
					}
					if err := counters.Merge(ck, remote); err != nil {
						t.Fatalf("seed=%d counter merge %q: %v", seed, ck, err)
					}
				}
				state, _ := counters.Get(ck)
				expectedCounters[ck] = state

				if rng.Intn(5) == 0 {
					hintKey := fmt.Sprintf("hint-%d-%d", seed, rng.Intn(4))
					hint := repl.Hint{
						Target: "peer",
						Req: protocol.Request{
							Opcode: protocol.OpReplicate, Key: []byte(hintKey),
							Value:   fmt.Appendf(nil, "hint-value-%d", op),
							Version: version, WriterNodeHash: uint64(seed + 1),
							ExpiresAt: expiry.At(time.Now().Add(time.Hour)),
						},
						ExpiresAt: expiry.At(time.Now().Add(time.Hour)),
					}
					hints.Push(hint)
					journal.AppendAdd(hint)
					expectedHints[hintKey] = hint
				}
			}

			if seed%3 == 0 {
				if err := manager.Compact(); err != nil {
					t.Fatalf("seed=%d compact: %v", seed, err)
				}
			}
			if err := manager.Sync(); err != nil {
				t.Fatalf("seed=%d sync persistence: %v", seed, err)
			}
			if err := journal.Sync(); err != nil {
				t.Fatalf("seed=%d sync journal: %v", seed, err)
			}

			// Simulated crash: do not call manager.Close (which performs an
			// implicit sync); all expected data was explicitly synced above.
			if manager.wal != nil {
				if err := manager.wal.Close(); err != nil {
					t.Fatalf("seed=%d close WAL handle: %v", seed, err)
				}
				manager.wal = nil
			}
			if err := journal.Close(); err != nil {
				t.Fatalf("seed=%d close journal handle: %v", seed, err)
			}

			recoveredCache := store.NewLRUStore(1<<20, nil)
			recoveredCounters := counter.NewStore(1 << 20)
			restarted := NewManager(dir, recoveredCache, nil)
			restarted.SetCounterStore(recoveredCounters)
			if err := restarted.Recover(); err != nil {
				t.Fatalf("seed=%d restart recovery: %v", seed, err)
			}
			defer restarted.Close()
			for key, want := range expectedKV {
				got, ok := recoveredCache.GetVersioned(key)
				if !ok || got.Version != want.Version || got.WriterNodeHash != want.WriterNodeHash || got.Tombstone != want.Tombstone || string(got.Value) != string(want.Value) {
					t.Fatalf("seed=%d KV %q got=%+v present=%v want=%+v", seed, key, got, ok, want)
				}
			}
			for key, want := range expectedCounters {
				got, ok := recoveredCounters.Get(key)
				if !ok || !got.Equal(want) {
					t.Fatalf("seed=%d counter %q got=%+v present=%v want=%+v", seed, key, got, ok, want)
				}
			}
			recoveredHints := repl.NewHintQueue()
			recoveredJournal, _, err := ae.OpenJournal(filepath.Join(dir, "hints.log"), recoveredHints)
			if err != nil {
				t.Fatalf("seed=%d reopen journal: %v", seed, err)
			}
			defer recoveredJournal.Close()
			gotHints := recoveredHints.Snapshot(time.Now(), 4096)
			if len(gotHints) != len(expectedHints) {
				t.Fatalf("seed=%d hints=%d, want %d", seed, len(gotHints), len(expectedHints))
			}
			for _, hint := range gotHints {
				want, ok := expectedHints[string(hint.Req.Key)]
				if !ok || hint.Target != want.Target || string(hint.Req.Value) != string(want.Req.Value) || hint.Req.Version != want.Req.Version {
					t.Fatalf("seed=%d unexpected recovered hint %+v", seed, hint)
				}
			}
		})
	}
}
