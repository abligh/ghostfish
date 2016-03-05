package svcrouter_test

import (
	"fmt"
	"github.com/abligh/ghostfish/svcrouter"
	zmq "github.com/pebbe/zmq4"
	"testing"
)

func dummyPeerName(n int) string {
	return fmt.Sprintf("p%02dp", n)
}

func dummyServiceName(n int) (string, string) {
	return fmt.Sprintf("st%02d", n), fmt.Sprintf("si%02d", n)
}

func TestServiceRouter(t *testing.T) {

	numPeers := 50
	numServices := 100

	var c *zmq.Context
	var m *Mirror
	var socks []*zmq.Socket = make([]*zmq.Socket, numPeers)
	var addr []string = make([]string, numPeers)
	var err error

	defer func() {
		for _, s := range socks {
			if s != nil {
				s.Close()
			}
		}
		if m != nil {
			m.Close()
		}
		if c != nil {
			c.Term()
		}
	}()

	if c, err = zmq.NewContext(); err != nil {
		t.Fatalf("Failed to create ZMQ context: %v", err)
	}

	if m, err = NewMirror(t, c, 1); err != nil {
		t.Fatalf("Failed to create a new mirror: %v", err)
	}

	for np := 0; np < numPeers; np++ {
		if socks[np], addr[np], err = svcrouter.NewHalfPair(c, true); err != nil {
			t.Fatalf("Failed to create half pair A: %v", err)
		}

		if err = m.AddPeer(svcrouter.PeerDefinition{
			Name:     dummyPeerName(np),
			ZmqType:  zmq.PAIR,
			Address:  addr[np],
			Bind:     false,
			PeerImpl: &MirrorPeerImpl{},
		}); err != nil {
			t.Fatalf("Could not add peer %d: %v", np, err)
		}
	}

	svcrouter.Barrier()

	r := svcrouter.NewServiceRouter()

	t.Log("Adding by Peer/Service")

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		for ns := 0; ns < numServices; ns++ {
			serviceType, serviceId := dummyServiceName(ns)
			e := svcrouter.NewEndpointEnvelope(serviceType, serviceId, "")
			if err := r.AddService(m.RouterElement, peerName, e); err != nil {
				t.Fatalf("Could not add service %d to peer %d: %v", ns, np, err)
			}
		}
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("Failed validation 1: %v", err)
	}

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if l := r.LenPeerEntries(m.RouterElement, peerName); l != numServices {
			t.Fatalf("Peer %d has service element mismatch: %d != %d", l, numServices)
		}
	}

	t.Log("Removing by Service/Peer")

	for ns := 0; ns < numServices; ns++ {
		serviceType, serviceId := dummyServiceName(ns)
		for np := 0; np < numPeers; np++ {
			peerName := dummyPeerName(np)
			e := svcrouter.NewEndpointEnvelope(serviceType, serviceId, "")
			if err := r.DeleteService(m.RouterElement, peerName, e); err != nil {
				t.Fatalf("Could not delete service %d to peer %d: %v", ns, np, err)
			}
		}

		if ns%(numServices/10) == 0 {
			if err := r.Validate(); err != nil {
				t.Fatalf("Failed validation 2: %v", err)
			}
		}

	}

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if l := r.LenPeerEntries(m.RouterElement, peerName); l != 0 {
			t.Fatalf("Peer %d has service element mismatch: %d != 0", l)
		}
	}

	if err := r.Validate(); err != nil {
		t.Fatalf("Failed validation 3: %v", err)
	}

	t.Log("Adding by Service/Peer")

	for ns := 0; ns < numServices; ns++ {
		serviceType, serviceId := dummyServiceName(ns)
		for np := 0; np < numPeers; np++ {
			peerName := dummyPeerName(np)
			e := svcrouter.NewEndpointEnvelope(serviceType, serviceId, "")
			if err := r.AddService(m.RouterElement, peerName, e); err != nil {
				t.Fatalf("Could not add service %d to peer %d: %v", ns, np, err)
			}
		}
	}

	if err := r.Validate(); err != nil {
		t.Fatalf("Failed validation 4: %v", err)
	}

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if l := r.LenPeerEntries(m.RouterElement, peerName); l != numServices {
			t.Fatalf("Peer %d has service element mismatch: %d != %d", l, numServices)
		}
	}

	t.Log("Removing by Peer/Service")

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		for ns := 0; ns < numServices; ns++ {
			serviceType, serviceId := dummyServiceName(ns)
			e := svcrouter.NewEndpointEnvelope(serviceType, serviceId, "")
			if err := r.DeleteService(m.RouterElement, peerName, e); err != nil {
				t.Fatalf("Could not delete service %d to peer %d: %v", ns, np, err)
			}
		}
		if np%10 == 0 {
			if err := r.Validate(); err != nil {
				t.Fatalf("Failed validation 5: %v", err)
			}
		}
	}

	if err := r.Validate(); err != nil {
		t.Fatalf("Failed validation 6: %v", err)
	}

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if l := r.LenPeerEntries(m.RouterElement, peerName); l != 0 {
			t.Fatalf("Peer %d has service element mismatch: %d != 0", l)
		}
	}

	t.Log("Adding by Service/Peer")

	for ns := 0; ns < numServices; ns++ {
		serviceType, serviceId := dummyServiceName(ns)
		for np := 0; np < numPeers; np++ {
			peerName := dummyPeerName(np)
			e := svcrouter.NewEndpointEnvelope(serviceType, serviceId, "")
			if err := r.AddService(m.RouterElement, peerName, e); err != nil {
				t.Fatalf("Could not add service %d to peer %d: %v", ns, np, err)
			}
		}
	}

	if err := r.Validate(); err != nil {
		t.Fatalf("Failed validation 7: %v", err)
	}

	for it := 0; it < 10; it++ {
		// How exciting. Let's try some routing!
		serviceType, serviceId := dummyServiceName(0)
		d := make(map[string]int, numPeers)
		for np := 0; np < numPeers; np++ {
			se := r.GetServerEntry(serviceType, serviceId, "")
			d[se.Peer.Name] += 1
		}
		if len(d) != numPeers {
			t.Fatalf("Failed load balance test 1")
		}
		for np := 0; np < numPeers; np++ {
			se := r.GetServerEntry(serviceType, serviceId, "")
			if d[se.Peer.Name] != 1 {
				t.Fatalf("Failed load balance test 2")
			}
			d[se.Peer.Name] += 1
		}
		if len(d) != numPeers {
			t.Fatalf("Failed load balance test 3")
		}
	}

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if l := r.LenPeerEntries(m.RouterElement, peerName); l != numServices {
			t.Fatalf("Peer %d has service element mismatch: %d != %d", l, numServices)
		}
	}

	t.Log("Removing by Peer (all entries)")

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if err := r.DeleteAllPeerEntries(m.RouterElement, peerName); err != nil {
			t.Fatalf("Could not add delete all services from peer %d: %v", np, err)
		}
	}

	for np := 0; np < numPeers; np++ {
		peerName := dummyPeerName(np)
		if l := r.LenPeerEntries(m.RouterElement, peerName); l != 0 {
			t.Fatalf("Peer %d has service element mismatch: %d != 0", l)
		}
	}

	if err := r.Validate(); err != nil {
		t.Fatalf("Failed validation final: %v", err)
	}

}
