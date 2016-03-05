package svcrouter_test

import (
	"fmt"
	"github.com/abligh/ghostfish/svcrouter"
	zmq "github.com/pebbe/zmq4"
	"sync"
	"testing"
	"time"
)

func msgEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func reflector(t *testing.T, s *zmq.Socket, n int, wg *sync.WaitGroup) chan bool {
	s.SetRcvtimeo(time.Second)
	s.SetSndtimeo(time.Second)
	svcrouter.Barrier()
	wg.Add(1)
	c := make(chan bool)
	go func() {
		/* this is a nasty hack but is OK for a test program I guess */
		for {
			if msg, err := s.RecvMessageBytes(0); err == nil {
				svcrouter.DumpMsg(fmt.Sprintf("reflector %d", n), msg)
				if _, err := s.SendMessage(msg); err != nil {
					wg.Done()
					t.Fatal(err)
					return
				}
			}
			select {
			case <-c:
				wg.Done()
				return
			default:
			}
		}
	}()
	return c
}

func TestServiceDispatcher(t *testing.T) {
	numPeers := 10
	numServices := 10

	var c *zmq.Context
	var sd *svcrouter.ServiceDispatcher
	var socks []*zmq.Socket = make([]*zmq.Socket, numPeers)
	var addr []string = make([]string, numPeers)
	var killReflectors []chan bool = make([]chan bool, numPeers)
	var err error
	var wg sync.WaitGroup

	defer func() {
		for _, s := range socks {
			if s != nil {
				s.Close()
			}
		}
		if sd != nil {
			sd.Close()
		}
		if c != nil {
			c.Term()
		}
	}()

	if c, err = zmq.NewContext(); err != nil {
		t.Fatalf("Failed to create ZMQ context: %v", err)
	}

	if sd, err = svcrouter.NewServiceDispatcher(c, 1); err != nil {
		t.Fatalf("Failed to create a new service dispatcher: %v", err)
	}

	sd.NoEnvRouterPeer = dummyPeerName(0)
	sd.EnvRouterPeer = dummyPeerName(2)

	for np := 0; np < numPeers; np++ {
		if np%2 == 0 {
			name := fmt.Sprintf("C%04d st=ST%d si=x", np, (np/2)%2)
			if socks[np], addr[np], err = svcrouter.NewSocketAndAddress(c, true, zmq.REQ, name); err != nil {
				t.Fatalf("Failed to create half pair A: %v", err)
			}
			if err = sd.AddPeer(svcrouter.PeerDefinition{
				Name:    dummyPeerName(np),
				ZmqType: zmq.ROUTER,
				Address: addr[np],
				Bind:    false,
			}, svcrouter.PT_CLIENTSNOENV); err != nil {
				t.Fatalf("Could not add peer %d: %v", np, err)
			}
		} else {
			name := fmt.Sprintf("C%04d st=ST%d si=x", np, ((np-1)/2)%2)
			if socks[np], addr[np], err = svcrouter.NewSocketAndAddress(c, true, zmq.REP, name); err != nil {
				t.Fatalf("Failed to create half pair A: %v", err)
			}
			if err = sd.AddPeer(svcrouter.PeerDefinition{
				Name:    dummyPeerName(np),
				ZmqType: zmq.DEALER,
				Address: addr[np],
				Bind:    false,
			}, svcrouter.PT_DOWNSTREAMENVREP); err != nil {
				t.Fatalf("Could not add peer %d: %v", np, err)
			}
			killReflectors[np] = reflector(t, socks[np], np, &wg)
		}
	}

	svcrouter.Barrier()

	for np := 1; np < numPeers; np += 2 {
		for ns := 0; ns < numServices; ns++ {
			serviceType := fmt.Sprintf("ST%d", ns)
			serviceId := "x"
			e := svcrouter.NewEndpointEnvelope(serviceType, serviceId, "")
			if err := sd.AddService(sd.RouterElement, dummyPeerName(np), e); err != nil {
				t.Fatalf("Could not add service %d to peer %d: %v", ns, np, err)
			}
		}
	}

	msg := [][]byte{[]byte("Hello"), []byte("World")}

	for it := 0; it < 2; it++ {
		if _, err := socks[0].SendMessage(msg); err != nil {
			t.Fatalf("socks send error: %v", err)
		}
		if msg2, err := socks[0].RecvMessageBytes(0); err != nil {
			t.Fatalf("socks receive error: %v", err)
		} else {
			svcrouter.DumpMsg("FINAL", msg2)
			if !msgEqual(msg, msg2) {
				t.Fatalf("socks messages differ")
			}
		}
	}

	for _, v := range killReflectors {
		if v != nil {
			close(v)
		}
	}
	wg.Wait()

}
