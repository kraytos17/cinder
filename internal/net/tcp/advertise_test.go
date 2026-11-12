package tcp_test

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

func TestServeAdvertisesConfiguredHostAndActualPort(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{
		NodeID: "node1", Port: 0, AdvertiseHost: "192.0.2.10", Store: store.NewLRUStore(1<<20, nil),
	})
	if err := srv.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	t.Cleanup(func() {
		srv.Shutdown()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not stop")
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		self, ok := srv.Table().Self()
		if ok && self.Host == "192.0.2.10" && self.Port != 0 {
			if self.Port != netport.Port(l.Addr().(*net.TCPAddr).Port) {
				t.Fatalf("advertised port = %d, actual = %d", self.Port, l.Addr().(*net.TCPAddr).Port)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("self membership not updated with actual address: %+v", self)
		}
		time.Sleep(time.Millisecond)
	}

	res := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminCluster})
	if res.Status != protocol.StatusOK {
		t.Fatalf("admin cluster status = %v", res.Status)
	}
	var nodes []struct {
		ID   string       `json:"id"`
		Host string       `json:"host"`
		Port netport.Port `json:"port"`
	}
	if err := json.Unmarshal(res.Value, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Host != "192.0.2.10" || nodes[0].Port != netport.Port(l.Addr().(*net.TCPAddr).Port) {
		t.Fatalf("admin cluster self address = %+v", nodes)
	}

	info := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminInfo})
	if info.Status != protocol.StatusOK {
		t.Fatalf("admin info status = %v", info.Status)
	}
	var nodeInfo struct {
		Port int `json:"port"`
	}
	if err := json.Unmarshal(info.Value, &nodeInfo); err != nil {
		t.Fatal(err)
	}
	if nodeInfo.Port != l.Addr().(*net.TCPAddr).Port {
		t.Fatalf("admin info port = %d, actual = %d", nodeInfo.Port, l.Addr().(*net.TCPAddr).Port)
	}
}

func TestThreeLoopbackAddressesAppearInAdminCluster(t *testing.T) {
	hosts := []string{"127.0.0.2", "127.0.0.3", "127.0.0.4"}
	listeners := make([]net.Listener, len(hosts))
	ports := make([]netport.Port, len(hosts))
	for i, host := range hosts {
		l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			for _, opened := range listeners {
				if opened != nil {
					opened.Close()
				}
			}
			t.Skipf("cannot bind loopback address %s: %v", host, err)
		}
		listeners[i] = l
		ports[i] = netport.Port(l.Addr().(*net.TCPAddr).Port)
	}

	servers := make([]*tcp.Server, len(hosts))
	dones := make([]chan error, len(hosts))
	for i, host := range hosts {
		peers := make([]tcp.Peer, 0, len(hosts)-1)
		for j, peerHost := range hosts {
			if i != j {
				peers = append(peers, tcp.Peer{ID: fmt.Sprintf("node%d", j+1), Host: peerHost, Port: ports[j]})
			}
		}
		i := i
		host := host
		srv := tcp.NewServer(tcp.ServerConfig{
			NodeID: fmt.Sprintf("node%d", i+1), Port: ports[i], AdvertiseHost: host,
			Store: store.NewLRUStore(1<<20, nil),
		})
		if err := srv.ConfigureCluster(peers); err != nil {
			t.Fatal(err)
		}
		servers[i] = srv
		dones[i] = make(chan error, 1)
		go func() { dones[i] <- srv.Serve(listeners[i]) }()
	}
	t.Cleanup(func() {
		for _, srv := range servers {
			if srv != nil {
				srv.Shutdown()
			}
		}
		for i, done := range dones {
			if done == nil {
				continue
			}
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("node %d Serve: %v", i+1, err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("node %d Serve did not stop", i+1)
			}
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		ready := true
		for i, srv := range servers {
			self, ok := srv.Table().Self()
			if !ok || self.Host != hosts[i] || self.Port != ports[i] {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("servers did not publish their configured advertise addresses")
		}
		time.Sleep(time.Millisecond)
	}

	for i, srv := range servers {
		res := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminCluster})
		if res.Status != protocol.StatusOK {
			t.Fatalf("node %d admin cluster status = %v", i+1, res.Status)
		}
		var nodes []struct {
			ID   string       `json:"id"`
			Host string       `json:"host"`
			Port netport.Port `json:"port"`
		}
		if err := json.Unmarshal(res.Value, &nodes); err != nil {
			t.Fatal(err)
		}
		got := make(map[string]struct {
			host string
			port netport.Port
		}, len(nodes))
		for _, node := range nodes {
			got[node.ID] = struct {
				host string
				port netport.Port
			}{node.Host, node.Port}
		}
		for j, host := range hosts {
			id := fmt.Sprintf("node%d", j+1)
			if addr := got[id]; addr.host != host || addr.port != ports[j] {
				t.Errorf("node %d admin cluster %s = %+v, want host=%s port=%d", i+1, id, addr, host, ports[j])
			}
		}
	}
}

func TestRestartedNodeAddressSupersedesStaleRumor(t *testing.T) {
	peerListener, err := net.Listen("tcp", "127.0.0.3:0")
	if err != nil {
		t.Skipf("cannot bind loopback address 127.0.0.3: %v", err)
	}
	peerPort := netport.Port(peerListener.Addr().(*net.TCPAddr).Port)

	oldListener, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		peerListener.Close()
		t.Skipf("cannot bind loopback address 127.0.0.2: %v", err)
	}
	oldPort := netport.Port(oldListener.Addr().(*net.TCPAddr).Port)

	peer := tcp.NewServer(tcp.ServerConfig{
		NodeID: "peer", Port: peerPort, AdvertiseHost: "127.0.0.3", Store: store.NewLRUStore(1<<20, nil),
	})
	oldNode := tcp.NewServer(tcp.ServerConfig{
		NodeID: "node", Port: oldPort, AdvertiseHost: "127.0.0.2", Store: store.NewLRUStore(1<<20, nil),
	})
	if err := peer.ConfigureCluster([]tcp.Peer{{ID: "node", Host: "127.0.0.2", Port: oldPort}}); err != nil {
		t.Fatal(err)
	}
	if err := oldNode.ConfigureCluster([]tcp.Peer{{ID: "peer", Host: "127.0.0.3", Port: peerPort}}); err != nil {
		t.Fatal(err)
	}
	peerDone := make(chan error, 1)
	oldDone := make(chan error, 1)
	go func() { peerDone <- peer.Serve(peerListener) }()
	go func() { oldDone <- oldNode.Serve(oldListener) }()
	defer func() {
		peer.Shutdown()
		select {
		case <-peerDone:
		case <-time.After(5 * time.Second):
			t.Error("peer Serve did not stop")
		}
	}()

	oldSelf, ok := oldNode.Table().Self()
	if !ok {
		t.Fatal("old node has no self membership")
	}
	peer.Table().ApplyRumor(oldSelf)
	knownOld, ok := peer.Table().Get("node")
	if !ok || knownOld.Host != "127.0.0.2" || knownOld.Port != oldPort || knownOld.Incarnation != oldSelf.Incarnation {
		t.Fatalf("peer did not capture old address/incarnation before restart: %+v", knownOld)
	}
	if err := oldListener.Close(); err != nil {
		t.Fatal(err)
	}
	oldNode.Kill()
	select {
	case <-oldDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old node Serve did not stop")
	}

	newListener, err := net.Listen("tcp", "127.0.0.4:0")
	if err != nil {
		t.Skipf("cannot bind loopback address 127.0.0.4: %v", err)
	}
	newPort := netport.Port(newListener.Addr().(*net.TCPAddr).Port)
	newNode := tcp.NewServer(tcp.ServerConfig{
		NodeID: "node", Port: newPort, AdvertiseHost: "127.0.0.4", Store: store.NewLRUStore(1<<20, nil),
	})
	if err := newNode.ConfigureCluster([]tcp.Peer{{ID: "peer", Host: "127.0.0.3", Port: peerPort}}); err != nil {
		t.Fatal(err)
	}
	newDone := make(chan error, 1)
	go func() { newDone <- newNode.Serve(newListener) }()
	defer func() {
		newNode.Shutdown()
		select {
		case <-newDone:
		case <-time.After(5 * time.Second):
			t.Error("restarted node Serve did not stop")
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		got, ok := peer.Table().Get("node")
		if ok && got.State == cluster.Alive && got.Host == "127.0.0.4" && got.Port == newPort && got.Incarnation > oldSelf.Incarnation {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer did not adopt restarted node address: old=%+v current=%+v", oldSelf, got)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A delayed old-incarnation rumor must not flap the peer back to the
	// previous address after the restart has converged.
	peer.Table().ApplyRumor(cluster.NodeInfo{
		ID: "node", Host: "127.0.0.2", Port: oldPort, State: cluster.Alive, Incarnation: oldSelf.Incarnation,
	})
	got, _ := peer.Table().Get("node")
	if got.Host != "127.0.0.4" || got.Port != newPort {
		t.Fatalf("stale rumor flapped node address back: %+v", got)
	}
}
