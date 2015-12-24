package app

import (
	"fmt"
	"github.com/HuKeping/rbtree"
	"log"
	"sync"
)

// Actions for an RpcPacketFilter
const (
	RpcPacketFilterActionNext   = iota // Pass the packet to the next filter in the list (default)
	RpcPacketFilterActionAccept = iota // Accept the packet, and do not pass to the next filter in the list
	RpcPacketFilterActionReject = iota // Reject the packet, and do not pass to the next filter in the list
	RpcPacketFilterActionDrop   = iota // Reject the packet, and do not pass to the next filter in the list
)

// Type RpcPacketFilterAction encapsulates an RpcPacketFilterAction
type RpcPacketFilterAction struct {
	Action int
}

// Maps actions to strings
var rpcPacketFilterActionMap map[int]string = map[int]string{
	RpcPacketFilterActionNext:   "Next",
	RpcPacketFilterActionAccept: "Accept",
	RpcPacketFilterActionReject: "Reject",
	RpcPacketFilterActionDrop:   "Drop",
}

// Type RpcPacketFilter is an interface that represents a packet filter for RPC
type RpcPacketFilter interface {
	Filter(pkt *RpcPacket) RpcPacketFilterAction // returns a filter action for a packet (or nil)
}

// Type RpcPacketFilterList is an type representing a sequenced list of packet filters
type rpcPacketFilterList struct {
	tree             *rbtree.Rbtree // tree of filters
	insertionCounter uint64         // the sequential insertion counter
	sync.RWMutex                    // mutex protecting tree
}

// Type rpcPacketFilterListItem is a type representing an item on the filter list
type rpcPacketFilterListItem struct {
	filter         *RpcPacketFilter // the filter
	sequence       int64            // the sequence
	insertionOrder uint64           // the insertion order
}

// func String() converts an RpcFilterActionNext to a string
func (a RpcPacketFilterAction) String() string {
	s, ok := rpcPacketFilterActionMap[a.Action]
	if ok {
		return s
	}
	return fmt.Sprintf("Unknown action (%d)", a.Action)
}

// Will order based on sequence then insertion order
func (i *rpcPacketFilterListItem) Less(than rbtree.Item) bool {
	thanSequence := than.(*rpcPacketFilterListItem).sequence
	if i.sequence < thanSequence {
		return true
	} else if i.sequence > thanSequence {
		return false
	}
	thanInsertionOrder := than.(*rpcPacketFilterListItem).insertionOrder
	if i.insertionOrder < thanInsertionOrder {
		return true
	}
	return false
}

// func Add adds an entry, returns the id
func (l *rpcPacketFilterList) Add(f *RpcPacketFilter, sequence int64) uint64 {
	l.Lock()
	defer l.Unlock()
	l.insertionCounter++
	i := rpcPacketFilterListItem{
		filter:         f,
		sequence:       sequence,
		insertionOrder: l.insertionCounter,
	}
	l.tree.Insert(&i)
	return i.insertionOrder
}

func canonicaliseRpcPacketFilterListItem(item rbtree.Item) bool {
	i := item.(*rpcPacketFilterListItem)
	log.Printf("item is %v", i)
	return false
}

// apply a packet filter list
func (l *rpcPacketFilterList) Apply(pkt *RpcPacket) RpcPacketFilterAction {
	l.RLock()
	defer l.RUnlock()
	action := RpcPacketFilterAction{Action: RpcPacketFilterActionAccept}
	l.tree.Ascend(l.tree.Min(), func(item rbtree.Item) bool {
		i := item.(*rpcPacketFilterListItem)
		action = (*i.filter).Filter(pkt)
		return action.Action == RpcPacketFilterActionNext
	})
	if action.Action == RpcPacketFilterActionNext {
		action.Action = RpcPacketFilterActionAccept
	}
	return action
}

// find an exact match
//
// Note this route assumes that the lock is already taken
func (l *rpcPacketFilterList) searchRpcPacketFilterListItem(i *rpcPacketFilterListItem) *rpcPacketFilterListItem {
	var found *rpcPacketFilterListItem = nil
	l.tree.Ascend(l.tree.Min(), func(item rbtree.Item) bool {
		compare := item.(*rpcPacketFilterListItem)
		if i.filter != nil && compare.filter != i.filter {
			return true // continue iterating
		}
		if i.insertionOrder != 0 && compare.insertionOrder != i.insertionOrder {
			return true // continue iterating
		}
		found = compare
		return false // stop iterating
	})
	return found
}

// func Remove removes an entry; pointer must be the same as supplied on add
func (l *rpcPacketFilterList) Remove(f *RpcPacketFilter) bool {
	i := rpcPacketFilterListItem{
		filter: f,
	}
	l.Lock()
	defer l.Unlock()
	if found := l.searchRpcPacketFilterListItem(&i); found != nil {
		l.tree.Delete(found)
		return true
	}
	return false
}

// func RemoveById removes an entry by Id
func (l *rpcPacketFilterList) RemoveById(id uint64) bool {
	i := rpcPacketFilterListItem{
		insertionOrder: id,
	}
	l.Lock()
	defer l.Unlock()
	if found := l.searchRpcPacketFilterListItem(&i); found != nil {
		l.tree.Delete(found)
		return true
	}
	return false
}

// func Clear clears all entries
func (l *rpcPacketFilterList) Clear() {
	l.Lock()
	defer l.Unlock()
	l.tree = rbtree.New()
}

// func NewRpcPacketFilterList returns a new list
func NewRpcPacketFilterList() *rpcPacketFilterList {
	l := &rpcPacketFilterList{}
	l.Clear()
	return l
}

// func AddPacketFilter adds a packet filter to a mux for traffic in a specified direction, with a specified sequence
//
// returns the id of the packet filter
func (mux *RpcMux) AddPacketFilter(direction int, f *RpcPacketFilter, sequence int64) uint64 {
	list := mux.filters[direction]
	return list.Add(f, sequence)
}

// func AddPacketFilter removes a packet filter from a mux for traffic in a specified direction
//
// The pointer must be the same as that given on add
func (mux *RpcMux) RemovePacketFilter(direction int, f *RpcPacketFilter) bool {
	list := mux.filters[direction]
	return list.Remove(f)
}

// func AddPacketFilter removes a packet filter from a mux for traffic in a specified direction
//
// The id must be that returned from add
func (mux *RpcMux) RemovePacketFilterById(direction int, id uint64) bool {
	list := mux.filters[direction]
	return list.RemoveById(id)
}

// func ClearPacketFilters removes all packet filter from a mux for traffic in a specified direction
func (mux *RpcMux) ClearPacketFilters(direction int) {
	list := mux.filters[direction]
	list.Clear()
}
