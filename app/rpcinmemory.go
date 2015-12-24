package app

import (
	"errors"
	"sync"
)

type comms struct {
	transports map[string]*RpcInMemoryTransport
	sync.RWMutex
}

var c = &comms{
	transports: make(map[string]*RpcInMemoryTransport),
}

// type RpcInMemoryTransport implements RpcTransport
type RpcInMemoryTransport struct {
	addr      RpcAddress
	pkts      chan RpcPacket
	quit      chan struct{}
	dropevery uint64
}

// type RpcInMemoryAddress implements RpcAddress
type RpcInMemoryAddress struct {
	address string
}

func (t *RpcInMemoryTransport) GetAddress() *RpcAddress {
	return &t.addr
}

func (t *RpcInMemoryTransport) Send(pkt *RpcPacket) error {
	if pkt.Dest == nil {
		return errors.New("Nil destination")
	}
	if pkt.Source == nil {
		return errors.New("Nil source")
	}
	c.RLock()
	defer c.RUnlock()
	dest, ok := c.transports[pkt.Dest.GetTransportAddress()]
	if !ok {
		return errors.New("Bad destination")
	}
	dest.pkts <- *pkt
	return nil
}

func (t *RpcInMemoryTransport) Run(mux *RpcMux) {
	for {
		select {
		case pkt := <-t.pkts:
			if t.dropevery != 0 && randUint64()%t.dropevery == 0 {
				// drop the packet
			} else {
				mux.Handle(&pkt)
			}
		case <-t.quit:
			mux.Shutdown()
			c.Lock()
			delete(c.transports, mux.GetAddress().GetTransportAddress())
			c.Unlock()
			close(t.pkts)
			return
		}
	}
}

func (a *RpcInMemoryAddress) String() string {
	return a.address
}

func (a *RpcInMemoryAddress) GetTransportAddress() string {
	return a.address
}

func NewRpcInMemoryTransport(a RpcAddress) *RpcInMemoryTransport {
	t := &RpcInMemoryTransport{
		pkts:      make(chan RpcPacket),
		quit:      make(chan struct{}),
		addr:      a,
		dropevery: 0,
	}
	c.Lock()
	c.transports[a.GetTransportAddress()] = t
	c.Unlock()
	return t
}

func (t *RpcInMemoryTransport) Close() {
	close(t.quit)
}
