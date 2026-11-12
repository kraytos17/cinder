package client

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

// stubServer answers every request with StatusOK. One connection at a
// time is enough: the client under test is sequential.
func stubServer(t *testing.T, l net.Listener) {
	t.Helper()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				for {
					var header [protocol.FrameHeaderSize]byte
					if _, err := io.ReadFull(c, header[:]); err != nil {
						return
					}
					n := int(uint32(header[3])<<24 | uint32(header[4])<<16 | uint32(header[5])<<8 | uint32(header[6]))
					if n > 0 {
						if _, err := io.CopyN(io.Discard, c, int64(n)); err != nil {
							return
						}
					}
					out, err := protocol.EncodeResponse(protocol.Response{Status: protocol.StatusOK})
					if err != nil {
						return
					}
					if _, err := c.Write(out); err != nil {
						return
					}
				}
			}(c)
		}
	}()
}

// Zero timeout means no deadline: Dial(0) + Ping must succeed. Before
// the setDeadline fix this failed with an instant i/o timeout.
func TestZeroTimeoutMeansNoDeadline(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	stubServer(t, l)

	c, err := Dial(l.Addr().String(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if res, err := c.Ping(); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("ping = %+v, %v", res, err)
	}
}

// A positive timeout still bounds and permits a fast RPC.
func TestPositiveTimeoutStillWorks(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	stubServer(t, l)

	c, err := Dial(l.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if res, err := c.Ping(); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("ping = %+v, %v", res, err)
	}
}

func TestPoolSetTimeoutRedialsWithUpdatedBudget(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	stubServer(t, l)

	p := NewPool([]NodeAddr{{ID: "n1", Host: "127.0.0.1", Port: portFromAddr(t, l.Addr().String())}}, time.Second)
	defer p.Close()
	if _, err := p.Send("n1", protocol.Request{Opcode: protocol.OpPing}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	old := p.conns["n1"]
	p.mu.Unlock()
	if old == nil || old.timeout != time.Second {
		t.Fatalf("initial pooled client = %#v, want 1s timeout", old)
	}

	p.SetTimeout(25 * time.Millisecond)
	p.mu.Lock()
	timeout, open := p.timeout, len(p.conns)
	p.mu.Unlock()
	if timeout != 25*time.Millisecond || open != 0 {
		t.Fatalf("SetTimeout left pool state timeout=%v open=%d", timeout, open)
	}
	if _, err := p.Send("n1", protocol.Request{Opcode: protocol.OpPing}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	got := p.conns["n1"].timeout
	p.mu.Unlock()
	if got != 25*time.Millisecond {
		t.Fatalf("redial timeout = %v, want 25ms", got)
	}
}

func portFromAddr(t *testing.T, addr string) netport.Port {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var n uint16
	if _, err := fmt.Sscanf(port, "%d", &n); err != nil {
		t.Fatal(err)
	}
	return netport.Port(n)
}
