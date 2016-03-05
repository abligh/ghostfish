package svcrouter_test

import (
	"github.com/abligh/ghostfish/svcrouter"
	zmq "github.com/pebbe/zmq4"
	"sync"
	"testing"
	"unicode/utf8"
)

type Mirror struct {
	*svcrouter.RouterElement
	t *testing.T
}

type MirrorPeerImpl struct {
}

func (m *MirrorPeerImpl) GetType() svcrouter.PeerType {
	return "mirror"
}

func (m *MirrorPeerImpl) PreDelete(re *svcrouter.RouterElement, peer *svcrouter.Peer) {}

func (m *MirrorPeerImpl) PostCreate(re *svcrouter.RouterElement, peer *svcrouter.Peer) {}

func reverse(s string) string {
	o := make([]rune, utf8.RuneCountInString(s))
	i := len(o)
	for _, c := range s {
		i--
		o[i] = c
	}
	return string(o)
}

func (m *Mirror) doMirrorRouting(re *svcrouter.RouterElement, from *svcrouter.Peer, inmsg [][]byte) (*svcrouter.Peer, [][]byte, error) {
	rev := reverse(from.Name)
	if dest, ok := re.GetPeer(rev); ok {
		return dest, inmsg, nil
	} else {
		return nil, nil, nil
	}
}

func NewMirror(t *testing.T, context *zmq.Context, bufferSize int) (*Mirror, error) {
	m := &Mirror{
		t: t,
	}
	if re, err := svcrouter.NewRouterElement(context, bufferSize, m.doMirrorRouting); err != nil {
		return nil, err
	} else {
		m.RouterElement = re
		return m, nil
	}
}

func TestRouterElement(t *testing.T) {
	var c *zmq.Context
	var m *Mirror
	var sa, sb *zmq.Socket
	var err error
	var addra string
	var addrb string

	defer func() {
		if sa != nil {
			sa.Close()
		}
		if sb != nil {
			sb.Close()
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

	if sa, addra, err = svcrouter.NewHalfPair(c, true); err != nil {
		t.Fatalf("Failed to create half pair A: %v", err)
	}

	if sb, addrb, err = svcrouter.NewHalfPair(c, true); err != nil {
		t.Fatalf("Failed to create half pair B: %v", err)
	}

	if err = m.AddPeer(svcrouter.PeerDefinition{
		Name:     "ab",
		ZmqType:  zmq.PAIR,
		Address:  addra,
		Bind:     false,
		PeerImpl: &MirrorPeerImpl{},
	}); err != nil {
		t.Fatalf("Could not add peer A: %v", err)
	}

	if err = m.AddPeer(svcrouter.PeerDefinition{
		Name:     "ba",
		ZmqType:  zmq.PAIR,
		Address:  addrb,
		Bind:     false,
		PeerImpl: &MirrorPeerImpl{},
	}); err != nil {
		t.Fatalf("Could not add peer B: %v", err)
	}

	svcrouter.Barrier()

	num := 100
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < num; i++ {
			if _, err := sb.RecvMessage(0); err != nil {
				t.Fatalf("sb receive error: %v", err)
			}
		}
	}()

	msg := [][]byte{[]byte("Hello"), []byte("World")}

	for i := 0; i < num; i++ {
		if _, err := sa.SendMessage(msg); err != nil {
			t.Fatalf("sa send error: %v", err)
		}
	}
	wg.Wait()
}
