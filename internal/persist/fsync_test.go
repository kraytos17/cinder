package persist

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

func TestParseFsyncPolicy(t *testing.T) {
	tests := []struct {
		input   string
		mode    FsyncMode
		want    time.Duration
		wantErr bool
	}{
		{input: "none", mode: FsyncNone},
		{input: "always", mode: FsyncAlways},
		{input: "interval:250", mode: FsyncInterval, want: 250 * time.Millisecond},
		{input: "interval:0", wantErr: true},
		{input: "interval:-1", wantErr: true},
		{input: "interval:abc", wantErr: true},
		{input: "sometimes", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			mode, interval, err := ParseFsyncPolicy(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected parse error")
				}
				return
			}
			if err != nil || mode != tc.mode || interval != tc.want {
				t.Fatalf("ParseFsyncPolicy(%q) = (%v, %v, %v)", tc.input, mode, interval, err)
			}
		})
	}
}

func TestFsyncPolicies(t *testing.T) {
	tests := []struct {
		name           string
		mode           FsyncMode
		interval       time.Duration
		wantAppendSync int
	}{
		{name: "none", mode: FsyncNone},
		{name: "always", mode: FsyncAlways, wantAppendSync: 1},
		{name: "interval", mode: FsyncInterval, interval: time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(t.TempDir(), store.NewLRUStore(1<<20, nil), nil)
			if err := m.Recover(); err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if err := m.SetFsyncPolicy(tc.mode, tc.interval); err != nil {
				t.Fatal(err)
			}
			var syncs int
			m.syncWAL = func() error {
				syncs++
				return nil
			}
			if err := m.OnWrite("k", store.Entry{Value: []byte("v"), Version: 1}, false); err != nil {
				t.Fatal(err)
			}
			if syncs != tc.wantAppendSync {
				t.Fatalf("append syncs=%d, want %d", syncs, tc.wantAppendSync)
			}
			if tc.mode == FsyncInterval {
				if err := m.SyncPeriodic(); err != nil {
					t.Fatal(err)
				}
			}
			if tc.mode == FsyncInterval && syncs != 1 {
				t.Fatalf("periodic sync count=%d, want 1", syncs)
			}
		})
	}
}

func TestAlwaysFsyncErrorPropagatesFromStoreWrite(t *testing.T) {
	st := store.NewLRUStore(1<<20, nil)
	m := NewManager(t.TempDir(), st, nil)
	if err := m.Recover(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.SetFsyncPolicy(FsyncAlways, 0); err != nil {
		t.Fatal(err)
	}
	syncErr := errors.New("injected sync failure")
	m.syncWAL = func() error { return syncErr }
	st.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
		return m.OnWrite(key, e, hardDelete)
	})
	if err := st.Put("k", []byte("v")); !errors.Is(err, syncErr) {
		t.Fatalf("Put error = %v, want injected sync failure", err)
	}
	if value, ok := st.Get("k"); !ok || string(value) != "v" {
		t.Fatalf("in-memory mutation unexpectedly rolled back: %q, %v", value, ok)
	}
}

func TestFsyncPolicyCrashRecovery(t *testing.T) {
	tests := []struct {
		name          string
		mode          FsyncMode
		interval      time.Duration
		periodicSync  bool
		wantRecovered bool
	}{
		{name: "none loses unsynced append", mode: FsyncNone},
		{name: "always recovers acknowledged append", mode: FsyncAlways, wantRecovered: true},
		{name: "interval loses before tick", mode: FsyncInterval, interval: time.Hour},
		{name: "interval recovers after tick", mode: FsyncInterval, interval: time.Hour, periodicSync: true, wantRecovered: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, walFileName)
			st := store.NewLRUStore(1<<20, nil)
			m := NewManager(dir, st, nil)
			if err := m.Recover(); err != nil {
				t.Fatal(err)
			}
			if err := m.SetFsyncPolicy(tc.mode, tc.interval); err != nil {
				t.Fatal(err)
			}

			// Model the stable-storage watermark: only bytes present when
			// Sync returns survive the simulated loss of the OS page cache.
			durableSize := int64(8) // current WAL header
			m.syncWAL = func() error {
				durableSize = m.wal.Size()
				return nil
			}
			st.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
				return m.OnWrite(key, e, hardDelete)
			})
			if err := st.Put("acknowledged", []byte("value")); err != nil {
				t.Fatalf("write: %v", err)
			}
			if tc.periodicSync {
				if err := m.SyncPeriodic(); err != nil {
					t.Fatalf("periodic sync: %v", err)
				}
			}

			// Deliberately do not call Close: it would sync and would model
			// graceful shutdown, not a crash. Truncate away unsynced bytes.
			if err := os.Truncate(path, durableSize); err != nil {
				t.Fatalf("simulate loss of unsynced bytes: %v", err)
			}
			if err := m.wal.Close(); err != nil {
				t.Fatalf("close crashed WAL handle without syncing: %v", err)
			}
			m.wal = nil

			recovered := store.NewLRUStore(1<<20, nil)
			restart := NewManager(dir, recovered, nil)
			if err := restart.Recover(); err != nil {
				t.Fatalf("restart recovery: %v", err)
			}
			defer restart.Close()
			value, ok := recovered.Get("acknowledged")
			if ok != tc.wantRecovered || (ok && string(value) != "value") {
				t.Fatalf("recovered value=%q present=%v, want present=%v", value, ok, tc.wantRecovered)
			}
		})
	}
}
