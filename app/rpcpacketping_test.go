package app

import (
	"log"
	"sync/atomic"
	"testing"
	"time"
)

type RpcPacketFilterDebug struct {
	log         bool
	prefix      string
	losePackets int64
	count		int64
	action      RpcPacketFilterAction
}

func NewRpcPacketFilterDebug(prefix string, action int, log bool) *RpcPacketFilter {
	var f RpcPacketFilter = &RpcPacketFilterDebug{
		prefix: prefix,
		log:    log,
		action: RpcPacketFilterAction{Action: action},
	}
	return &f
}

func (f *RpcPacketFilterDebug) Filter(pkt *RpcPacket) RpcPacketFilterAction {
	// avoids race warning
	count := atomic.AddInt64(&f.count, 1)
	if (count <= f.losePackets) {
		return RpcPacketFilterAction{Action: RpcPacketFilterActionDrop}
	}
	if f.log {
		log.Printf("[DEBUG] %s: %v", f.prefix, pkt)
	}
	return f.action
}

func TestRpcPacketPing(t *testing.T) {
	foo := &RpcInMemoryAddress{address: "foo"}
	bar := &RpcInMemoryAddress{address: "bar"}
	trfoo := NewRpcInMemoryTransport(foo)
	trbar := NewRpcInMemoryTransport(bar)

	muxfoo := NewRpcMux(trfoo)
	go trfoo.Run(muxfoo)

	muxbar := NewRpcMux(trbar)
	go trbar.Run(muxbar)

	servicepingfoo := muxfoo.NewService(foo, "_ping", RpcParameters{Timeout: 50 * time.Millisecond, Retries: 3})
	servicepingbar := muxfoo.NewService(bar, "_ping", RpcParameters{Timeout: 50 * time.Millisecond, Retries: 3})

	muxfoo.AddPacketFilter(RpcDirectionTx, NewRpcPacketFilterDebug("[FOOTX]", RpcPacketFilterActionNext, false), 0)
	muxfoo.AddPacketFilter(RpcDirectionRx, NewRpcPacketFilterDebug("[FOORX]", RpcPacketFilterActionNext, false), 0)

	muxbar.AddPacketFilter(RpcDirectionTx, NewRpcPacketFilterDebug("[BARTX]", RpcPacketFilterActionNext, false), 0)
	muxbar.AddPacketFilter(RpcDirectionRx, NewRpcPacketFilterDebug("[BARRX]", RpcPacketFilterActionNext, false), 0)

	log.Println("[TEST] Ping foo")
	for i := 1; i <= 5; i++ {
		reply := servicepingfoo.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}

	log.Println("[TEST] Ping bar")
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}

	log.Println("[TEST] Ping bar with reject")
	barrxrejectfilterid := muxbar.AddPacketFilter(RpcDirectionRx, NewRpcPacketFilterDebug("[BARRXREJECT]", RpcPacketFilterActionReject, false), 1)
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrProhibited {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}
	muxbar.RemovePacketFilterById(RpcDirectionRx, barrxrejectfilterid)

	log.Println("[TEST] Ping bar")
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}

	log.Println("[TEST] Ping bar with drop")
	barrxdropfilterid := muxbar.AddPacketFilter(RpcDirectionRx, NewRpcPacketFilterDebug("[BARRXDROP]", RpcPacketFilterActionDrop, false), 1)
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrTimeout {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}
	muxbar.RemovePacketFilterById(RpcDirectionRx, barrxdropfilterid)

	log.Println("[TEST] Ping bar")
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}

	log.Println("[TEST] Ping bar with drop on reply")
	foorxdropfilterid := muxfoo.AddPacketFilter(RpcDirectionRx, NewRpcPacketFilterDebug("[FOORXDROP]", RpcPacketFilterActionDrop, false), 1)
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrTimeout {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}
	muxfoo.RemovePacketFilterById(RpcDirectionRx, foorxdropfilterid)

	log.Println("[TEST] Ping bar")
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}
	
	log.Println("[TEST] Ping bar with lossy drop")
	lossyFilter := NewRpcPacketFilterDebug("[BARRXLOSS]", RpcPacketFilterActionAccept, false)
	var f *RpcPacketFilterDebug = (*lossyFilter).(*RpcPacketFilterDebug)
	barrxlossydropfilterid := muxbar.AddPacketFilter(RpcDirectionRx, lossyFilter, 1)
	f.losePackets = 2
	for i := 1; i <= 5; i++ {
		atomic.StoreInt64(&f.count, 0)
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
		if atomic.LoadInt64(&f.count) != 3 {
			t.Errorf("[ERROR] Did not send correct number of packets\n", reply.ErrorType.String())
		}
	}
	f.losePackets = 3
	for i := 1; i <= 5; i++ {
		atomic.StoreInt64(&f.count, 0)
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrTimeout {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
		if atomic.LoadInt64(&f.count) != 3 {
			t.Errorf("[ERROR] Did not send correct number of packets\n", reply.ErrorType.String())
		}
	}
	muxbar.RemovePacketFilterById(RpcDirectionRx, barrxlossydropfilterid)

	log.Println("[TEST] Ping bar with lossy drop on reply")
	lossyFilter2 := NewRpcPacketFilterDebug("[FOORXLOSS]", RpcPacketFilterActionAccept, false)
	f = (*lossyFilter2).(*RpcPacketFilterDebug)
	foorxlossydropfilterid := muxfoo.AddPacketFilter(RpcDirectionRx, lossyFilter2, 1)
	f.losePackets = 2
	for i := 1; i <= 5; i++ {
		atomic.StoreInt64(&f.count, 0)
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
		if atomic.LoadInt64(&f.count) != 3 {
			t.Errorf("[ERROR] Did not send correct number of packets\n", reply.ErrorType.String())
		}
	}
	f.losePackets = 3
	for i := 1; i <= 5; i++ {
		atomic.StoreInt64(&f.count, 0)
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrTimeout {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
		if atomic.LoadInt64(&f.count) != 3 {
			t.Errorf("[ERROR] Did not send correct number of packets\n", reply.ErrorType.String())
		}
	}
	muxfoo.RemovePacketFilterById(RpcDirectionRx, foorxlossydropfilterid)

	log.Println("[TEST] Ping bar")
	for i := 1; i <= 5; i++ {
		reply := servicepingbar.SendSync(&RpcRequest{})
		if reply.ErrorType.ErrorType != RpcErrNone {
			t.Errorf("[ERROR] Ping return is %s\n", reply.ErrorType.String())
		}
	}

	muxfoo.Shutdown()
	trfoo.Close()

	muxbar.Shutdown()
	trbar.Close()
}
