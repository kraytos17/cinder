package client

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

// readinessStub serves scripted responses: primary always answers bare
// NotReady (warming miss / quorum failure with no redirect), secondary
// answers OK. It lets the test prove whether Cluster retries the same
// unready owner or routes around it for reads.
func readinessStub(t *testing.T, l net.Listener, primary bool, okValue []byte) {
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
					payload := make([]byte, n)
					if n > 0 {
						if _, err := io.ReadFull(c, payload); err != nil {
							return
						}
					}
					dec, err := protocol.DecodeRequest(header[:], payload)
					if err != nil {
						return
					}
					var res protocol.Response
					switch dec.Req.Opcode {
					case protocol.OpGet, protocol.OpGetVersioned, protocol.OpCounterGet:
						if primary {
							// Warming miss: bare NotReady, no redirect target.
							res = protocol.Response{Status: protocol.StatusNotReady}
						} else {
							res = protocol.Response{Status: protocol.StatusOK, HasValue: true, Value: okValue, Version: 1}
						}
					default:
						// Writes must stay owner-routed: primary reports bare
						// NotReady (quorum failure), secondary would succeed
						// if incorrectly tried.
						if primary {
							res = protocol.Response{Status: protocol.StatusNotReady}
						} else {
							res = protocol.Response{Status: protocol.StatusOK}
						}
					}
					out, err := protocol.EncodeResponse(res)
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

func TestClusterReadAvoidsBareNotReadyOwner(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	readinessStub(t, l1, true, []byte("v"))
	readinessStub(t, l2, false, []byte("v"))

	c, err := NewCluster(ClusterConfig{
		Nodes: []NodeAddr{
			{ID: "n1", Host: "127.0.0.1", Port: portFromAddr(t, l1.Addr().String())},
			{ID: "n2", Host: "127.0.0.1", Port: portFromAddr(t, l2.Addr().String())},
		},
		MaxRetries:  3,
		BaseBackoff: time.Millisecond,
		RPCTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Find a key owned by the unready primary so the first attempt hits
	// bare NotReady and must route around it for reads.
	var key string
	for i := range 1000 {
		k := "readiness-avoid-" + string(rune('a'+i%26)) + "-" + itoa(i)
		if c.ring.GetNode(k) == "n1" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by n1")
	}

	res, err := c.GetVersioned(key)
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if res.Status != protocol.StatusOK {
		t.Fatalf("GetVersioned via warming owner = %v, want OK via alternate replica", res.Status)
	}

	// Writes must stay owner-routed: the same key's Set hits the primary
	// and surfaces its bare NotReady instead of succeeding elsewhere.
	wres, err := c.Set(key, []byte("v"))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if wres.Status != protocol.StatusNotReady {
		t.Fatalf("Set via warming owner = %v, want NotReady (no read-style fallback for writes)", wres.Status)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// Ensure netport import is used (port parsing helper lives in timeout_test).
var _ netport.Port
