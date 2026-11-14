package ae_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

func BenchmarkReplayWithJournal256(b *testing.B) {
	dir := b.TempDir()
	ft := testkit.NewStores("n2")
	m := repl.NewManager(repl.Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Transport: ft})
	j, _, err := ae.OpenJournal(dir+"/hints.log", m.HintQueue())
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()
	m.SetHintJournal(j)
	exp := expiry.At(time.Now().Add(time.Hour))
	mk := func(i int) repl.Hint {
		return repl.Hint{Target: "n2", Req: protocol.Request{Opcode: protocol.OpReplicate, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v"), Version: uint64(i + 1)}, ExpiresAt: exp}
	}
	for i := range 256 {
		m.HintQueue().Push(mk(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if n := m.ReplayHints(); n != 256 {
			b.Fatalf("replayed = %d", n)
		}
		for j := range 256 {
			m.HintQueue().Push(mk(j))
		}
	}
}
