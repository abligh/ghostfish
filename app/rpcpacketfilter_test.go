package app

import (
	"log"
	"strconv"
	"testing"
)

type RpcPacketFilterTest struct {
	prefix  string
	run     string
	desired *RpcPacketFilterAction
	called  int
}

func NewRpcPacketFilterTest(prefix string, desired *RpcPacketFilterAction) *RpcPacketFilter {
	var f RpcPacketFilter = &RpcPacketFilterTest{prefix: prefix, desired: desired}
	return &f
}

func (f *RpcPacketFilterTest) Filter(pkt *RpcPacket) RpcPacketFilterAction {
	log.Printf("[TEST] %s(%s): %v", f.prefix, f.run, pkt)
	action := RpcPacketFilterAction{Action: RpcPacketFilterActionNext}
	if f.desired != nil {
		action = *f.desired
	}
	f.called++
	return action
}

const (
	accept = iota
	reject = iota
	next   = iota
)

func clearfilters(tf []*RpcPacketFilterTest, s string) {
	for i := range tf {
		tf[i].called = 0
		tf[i].run = s
	}
}

func testfilters(t *testing.T, tf []*RpcPacketFilterTest, expected []int, s string) {
	for i := range tf {
		if tf[i].called != expected[i] {
			t.Errorf("%s error in %s, expected %d got %d", s, tf[i].prefix, expected[i], tf[i].called)
		}
	}
}

func TestRpcPacketFilterLists(t *testing.T) {
	list := NewRpcPacketFilterList()

	f1 := NewRpcPacketFilterTest("filter1", nil)
	f2 := NewRpcPacketFilterTest("filter2", nil)

	idf1 := list.Add(f1, 100)
	if idf1 == 0 {
		t.Errorf("Expected !=0 got %d", idf1)
	}

	if !list.Remove(f1) {
		t.Error("Failed remove #1")
	}

	if list.Remove(f1) {
		t.Error("Unexpected success remove #1")
	}

	list.Add(f1, 100)
	list.Clear()

	if list.Remove(f1) {
		t.Error("Unexpected success remove #1a")
	}

	idf1 = list.Add(f1, 100)
	if idf1 == 0 {
		t.Errorf("Expected !=0 got %d", idf1)
	}

	idf2 := list.Add(f2, 100)
	if idf1 == 0 {
		t.Errorf("Expected !=0 got %d", idf1)
	}

	if !list.RemoveById(idf1) {
		t.Error("Failed remove #2")
	}

	if list.RemoveById(idf1) {
		t.Error("Unexpected success remove #2")
	}

	if !list.RemoveById(idf2) {
		t.Error("Failed remove #3")
	}

	if list.RemoveById(idf2) {
		t.Error("Unexpected success remove #3")
	}

	var ids [100]uint64
	var filters [100]*RpcPacketFilter
	for x := 0; x < 100; x++ {
		filters[x] = NewRpcPacketFilterTest("filter"+strconv.Itoa(x), nil)
		ids[x] = list.Add(filters[x], 100)
	}
	for x := 0; x < 50; x++ {
		if !list.RemoveById(ids[x]) {
			t.Errorf("Failed remove #4 %d", x)
		}
		if list.RemoveById(ids[x]) {
			t.Errorf("Unexpected success remove #4 %d", x)
		}
		if !list.Remove(filters[99-x]) {
			t.Errorf("Failed remove #5 %d", 99-x)
		}
		if list.Remove(filters[99-x]) {
			t.Errorf("Unexpected success remove #5 %d", 99-x)
		}
	}

	var f []*RpcPacketFilter = make([]*RpcPacketFilter, 3)
	f[accept] = NewRpcPacketFilterTest("filteraccept", &RpcPacketFilterAction{Action: RpcPacketFilterActionAccept})
	f[reject] = NewRpcPacketFilterTest("filterreject", &RpcPacketFilterAction{Action: RpcPacketFilterActionReject})
	f[next] = NewRpcPacketFilterTest("filternext", &RpcPacketFilterAction{Action: RpcPacketFilterActionNext})

	var tf []*RpcPacketFilterTest = make([]*RpcPacketFilterTest, 3)
	for i := 0; i < 3; i++ {
		fltr := f[i]
		tf[i] = (*fltr).(*RpcPacketFilterTest)
	}

	pkt := &RpcPacket{}
	var action RpcPacketFilterAction

	clearfilters(tf, "run 1")
	list.Clear()
	list.Add(f[next], -100)
	list.Add(f[reject], 100)
	list.Add(f[accept], 200)
	action = list.Apply(pkt)
	if action.Action != RpcPacketFilterActionReject {
		t.Errorf("Bad action #1: %s", action)
	}
	testfilters(t, tf, []int{0, 1, 1}, "run 1")

	clearfilters(tf, "run 2")
	list.Clear()
	list.Add(f[accept], 200)
	list.Add(f[reject], 100)
	list.Add(f[next], -100)
	action = list.Apply(pkt)
	if action.Action != RpcPacketFilterActionReject {
		t.Errorf("Bad action #1: %s", action)
	}
	testfilters(t, tf, []int{0, 1, 1}, "run 2")

	clearfilters(tf, "run 3")
	list.Clear()
	list.Add(f[next], 0)
	list.Add(f[reject], 0)
	list.Add(f[accept], 0)
	action = list.Apply(pkt)
	if action.Action != RpcPacketFilterActionReject {
		t.Errorf("Bad action #1: %s", action)
	}
	testfilters(t, tf, []int{0, 1, 1}, "run 3")

	clearfilters(tf, "run 4")
	list.Clear()
	list.Add(f[next], 4)
	list.Add(f[reject], 8)
	list.Add(f[next], 1)
	list.Add(f[next], 13)
	list.Add(f[accept], 12)
	list.Add(f[next], 9)
	list.Add(f[next], 11)
	list.Add(f[next], 3)
	list.Add(f[reject], 10)
	list.Add(f[accept], 6)
	list.Add(f[next], 7)
	list.Add(f[next], 5)
	list.Add(f[next], 2)
	action = list.Apply(pkt)
	if action.Action != RpcPacketFilterActionAccept {
		t.Errorf("Bad action #1: %s", action)
	}
	testfilters(t, tf, []int{1, 0, 5}, "run 4")

}
