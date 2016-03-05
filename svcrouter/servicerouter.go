package svcrouter

import (
	"errors"
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"sync/atomic"
)

/*

Routing envelope structure

  key = SERVICETYPE,SERVICEID

  [ k1 ]
  [ k2 ] -----------> [ server3 ] [ server1 ] [ server2 ]    <- sorted either by priority (rotating) or shard ID
  [ k3 ]       \
                \---> [ server1 ] [ server2 ] [ server3 ]    <- sorted by server ID
*/
var priorityIndex int64

// func getUniqueId() uint64
//
// returns a unique ID

func getNextPriority() int64 {
	return atomic.AddInt64(&priorityIndex, 1)
}

// type ServiceLookupKey struct
//
// struct to act as a lookupkep for a given serviceType, serviceId into the routing envelope routing structures

type ServiceLookupKey struct {
	ServiceType string
	ServiceId   string
}

// type ServiceRouter struct
//
// keeps track of routing of services

type ServiceRouter struct {
	servers map[ServiceLookupKey]serverEntries // serverEntries - all the server entries for a given SLK
	peers   map[string]peerEntries             // peerEntries for a particular peer - all the SLKs on all the servers run on that peer
}

// func NewServiceRouter() ServiceRouter
//
// create an empty ServiceRouter

func NewServiceRouter() ServiceRouter {
	return ServiceRouter{
		servers: make(map[ServiceLookupKey]serverEntries),
		peers:   make(map[string]peerEntries),
	}
}

// type serverEntries
//
// this is the key data structure. A tree of ServerEntry items sorted by priority / shard, and another tree sorted by
// serverId. The key thing is both trees contain items that point to the same ServerEntry

type serverEntries struct {
	byPrioShard *llrb.LLRB // this is the tree of servers by priority / shard
	byServerId  *llrb.LLRB // this is the tree of servers by serverId
}

// func NewServerEntries() serverEntries
//
// create a blank severEntries structure

func NewServerEntries() serverEntries {
	return serverEntries{
		byPrioShard: llrb.New(),
		byServerId:  llrb.New(),
	}
}

// type ServerEntry
//
// holds the details of a ServerEntry for a particular SLK

type ServerEntry struct {
	Peer     *Peer
	Slk      ServiceLookupKey
	ServerId string
	ShardId  string
	Priority int64
}

// type byPrioShardServerEntry struct
//
// wrapper to sort serverEntries in a tree by PrioShard
// NB *byPrioShardServerEntry implements the Item interface

type byPrioShardServerEntry struct {
	*ServerEntry // pointer so the item is shared across both trees
}

// type byServerServerEntry struct
//
// wrapper to sort serverEntries in a tree by Server
// NB *byServerServerEntry implements the Item interface

type byServerServerEntry struct {
	*ServerEntry // pointer so the item is shared across both trees
}

// type peerEntries struct
//
// list of SLKs and servers for a particular peer

type peerEntries struct {
	byServerId *llrb.LLRB
}

// func NewPeerEntries() peerEntries
//
// create an empty peerEntries structure

func NewPeerEntries() peerEntries {
	return peerEntries{
		byServerId: llrb.New(),
	}
}

// func (se1 *byPrioShardServerEntry) Less (than llrb.Item) bool
//
// implement the Item interface

func (se1 *byPrioShardServerEntry) Less(than llrb.Item) bool {
	se2 := than.(*byPrioShardServerEntry)
	if se1.Slk.ServiceType < se2.Slk.ServiceType {
		return true
	} else if se1.Slk.ServiceType > se2.Slk.ServiceType {
		return false
	} else if se1.Slk.ServiceId < se2.Slk.ServiceId {
		return true
	} else if se1.Slk.ServiceId > se2.Slk.ServiceId {
		return false
	} else if se1.ShardId < se2.ShardId {
		return true
	} else if se1.ShardId > se2.ShardId {
		return false
	} else if se1.Priority < se2.Priority {
		return true
	} else if se1.Priority > se2.Priority {
		return false
	} else if se1.Peer.Name < se2.Peer.Name {
		return true
	} else if se1.Peer.Name > se2.Peer.Name {
		return false
	}
	return false
}

// func (se1 *byServerServerEntry) Less (than llrb.Item) bool
//
// implement the Item interface

func (se1 *byServerServerEntry) Less(than llrb.Item) bool {
	se2 := than.(*byServerServerEntry)
	if se1.Peer.Name < se2.Peer.Name {
		return true
	} else if se1.Peer.Name > se2.Peer.Name {
		return false
	} else if se1.Slk.ServiceType < se2.Slk.ServiceType {
		return true
	} else if se1.Slk.ServiceType > se2.Slk.ServiceType {
		return false
	} else if se1.Slk.ServiceId < se2.Slk.ServiceId {
		return true
	} else if se1.Slk.ServiceId > se2.Slk.ServiceId {
		return false
	} else if se1.ServerId < se2.ServerId {
		return true
	} else {
		return false
	}
}

// func (ses *serverEntries) getServerEntry(shardId string) *ServerEntry
//
// for a given serverEntries structure (found from the SLK), find an approriate ServerEntry

func (ses *serverEntries) getServerEntry(shardId string) *ServerEntry {
	if shardId == "" {
		se := ses.byPrioShard.DeleteMin().(*byPrioShardServerEntry)
		se.Priority = getNextPriority()
		ses.byPrioShard.ReplaceOrInsert(se)
		return se.ServerEntry
	} else {
		se := ses.byPrioShard.Max().(*byPrioShardServerEntry)
		// if we've been given a shardId, then see if there is an entry with a key less than or
		// equal to this. If not, we want the maximum
		bps := &byPrioShardServerEntry{ServerEntry: &ServerEntry{ShardId: shardId}}
		ses.byPrioShard.DescendLessOrEqual(bps, func(i llrb.Item) bool { se = i.(*byPrioShardServerEntry); return false })
		return se.ServerEntry
	}
}

// func (sr *ServiceRouter) getServerEntry(serviceType, serviceId, shardId string) *ServerEntry
//
// for a given serviceType, serviceId, and shardID, find an approriate ServerEntry

func (sr *ServiceRouter) GetServerEntry(serviceType, serviceId, shardId string) *ServerEntry {
	if ses, ok := sr.servers[ServiceLookupKey{serviceType, serviceId}]; ok {
		return ses.getServerEntry(shardId)
	} else {
		return nil
	}
}

// func (sr *ServiceRouter) addService(p *Peer, e *Envelope) {
//
// add a service to peer p with given envelope e

func (sr *ServiceRouter) AddService(re *RouterElement, peerName string, e *Envelope) error {
	if p, ok := re.peers[peerName]; !ok {
		return errors.New("No such peer")
	} else {
		serviceType := e.Attributes[EA_SERVICETYPE]
		serviceId := e.Attributes[EA_SERVICEID]
		serverId := e.Attributes[EA_SERVERID]
		shardId := e.Attributes[EA_SHARDID]
		slk := ServiceLookupKey{serviceType, serviceId}
		se := &ServerEntry{
			Slk:      slk,
			Peer:     p,
			ServerId: serverId,
			ShardId:  shardId,
			Priority: getNextPriority(),
		}
		if ses, ok := sr.servers[slk]; ok {
			if item := ses.byServerId.Get(&byServerServerEntry{se}); item != nil {
				sr.DeleteService(re, peerName, e) // ignore errors
			}
		}
		if _, ok := sr.servers[slk]; !ok {
			sr.servers[slk] = NewServerEntries()
		}
		sr.servers[slk].byPrioShard.ReplaceOrInsert(&byPrioShardServerEntry{ServerEntry: se})
		sr.servers[slk].byServerId.ReplaceOrInsert(&byServerServerEntry{ServerEntry: se})
		if _, ok := sr.peers[p.Name]; !ok {
			sr.peers[p.Name] = NewPeerEntries()
		}
		sr.peers[p.Name].byServerId.ReplaceOrInsert(&byServerServerEntry{ServerEntry: se})
	}
	return nil
}

// func (sr *ServiceRouter) deleteService(e *Envelope)
//
// delete a service with peer p, envelope e

func (sr *ServiceRouter) DeleteService(re *RouterElement, peerName string, e *Envelope) error {
	serviceType := e.Attributes[EA_SERVICETYPE]
	serviceId := e.Attributes[EA_SERVICEID]
	serverId := e.Attributes[EA_SERVERID]
	shardId := e.Attributes[EA_SHARDID]
	return sr.DeleteServiceStrings(re, peerName, serviceType, serviceId, serverId, shardId)
}

// func (sr *ServiceRouter) deleteServiceStrings(p *Peer, serviceType, serviceId, serverId, shardId string) {
//
// delete a service with peer p, and parameters serviceType, serviceId, shardId

func (sr *ServiceRouter) DeleteServiceStrings(re *RouterElement, peerName string, serviceType, serviceId, serverId, shardId string) error {
	if p, ok := re.peers[peerName]; !ok {
		return errors.New("No such peer")
	} else {
		slk := ServiceLookupKey{serviceType, serviceId}
		if ses, ok := sr.servers[slk]; !ok {
			return errors.New("No such serviceType / serviceId")
		} else {
			se := &ServerEntry{
				Slk:      slk,
				Peer:     p,
				ServerId: serverId,
				ShardId:  shardId,
			}
			if item := sr.servers[slk].byServerId.Delete(&byServerServerEntry{se}); item != nil {
				ose := item.(*byServerServerEntry).ServerEntry
				ses.byPrioShard.Delete(&byPrioShardServerEntry{ServerEntry: ose})
				if ses.byPrioShard.Len() == 0 && ses.byServerId.Len() == 0 {
					delete(sr.servers, slk)
				}
				if sesp, ok := sr.peers[p.Name]; ok {
					sesp.byServerId.Delete(&byServerServerEntry{se})
					if sesp.byServerId.Len() == 0 {
						delete(sr.peers, p.Name)
					}
				}
				return nil
			} else {
				return errors.New("No such server")
			}
		}
	}
}

// func (sr *ServiceRouter) deleteAllPeerEntries(p *Peer) {
//
// delete all services attached to a peer

func (sr *ServiceRouter) DeleteAllPeerEntries(re *RouterElement, peerName string) error {
	for {
		if sesp, ok := sr.peers[peerName]; ok {
			item := sesp.byServerId.Min()
			if item == nil {
				break
			}
			se := item.(*byServerServerEntry).ServerEntry
			if err := sr.DeleteServiceStrings(re, se.Peer.Name, se.Slk.ServiceType, se.Slk.ServiceId, se.ServerId, se.ShardId); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// func (sr *ServiceRouter) LenPeerEntries(re *RouterElement, peerName string) int {
//
// returns the number of services running on a given peer

func (sr *ServiceRouter) LenPeerEntries(re *RouterElement, peerName string) int {
	if sesp, ok := sr.peers[peerName]; !ok {
		return 0
	} else {
		return sesp.byServerId.Len()
	}
}

// func (sr *ServiceRouter) Validate() error
//
// validate internal data structures of a ServiceRouter. This is quite slow.

func (sr *ServiceRouter) Validate() error {
	numServicesByPeer := 0
	numServicesByService := 0
	// First check the peers
	for peerName, peerEntry := range sr.peers {
		if peerEntry.byServerId == nil {
			return errors.New(fmt.Sprintf("peerEntry.byServerId for %s is nil", peerName))
		}
		if peerEntry.byServerId.Len() == 0 {
			return errors.New(fmt.Sprintf("peerEntry for %s has 0 items", peerName))
		}
		numServicesByPeer += peerEntry.byServerId.Len()
		var err error
		peerEntry.byServerId.AscendGreaterOrEqual(peerEntry.byServerId.Min(), func(i llrb.Item) bool {
			se := i.(*byServerServerEntry)
			if se.Peer.Name != peerName {
				err = errors.New(fmt.Sprintf("peerEntry se has mismatched peer: %v %v", se, peerName))
				return false
			}
			// Now check we can look it up by shard / prio - this should be unique
			if ses, ok := sr.servers[se.Slk]; !ok {
				err = errors.New(fmt.Sprintf("peerEntry se has no matching SLK: %v, %v", se, se.Slk))
				return false
			} else {
				i2 := ses.byPrioShard.Get(&byPrioShardServerEntry{ServerEntry: se.ServerEntry})
				if i2 == nil {
					err = errors.New(fmt.Sprintf("peerEntry se has no matching prioshard entry: %v", se))
					return false
				}
				se2 := i2.(*byPrioShardServerEntry)
				if se2.ServerEntry != se.ServerEntry {
					err = errors.New(fmt.Sprintf("peerEntry se has inconsistent prioshard entry: %v %v", se, se2))
					return false
				}
			}
			return err == nil
		})

		if err != nil {
			return err
		}
	}
	// Now check the services
	for slk, ses := range sr.servers {
		if ses.byPrioShard == nil {
			return errors.New(fmt.Sprintf("SES.byPrioShard entry for %v is nil", slk))
		}
		if ses.byServerId == nil {
			return errors.New(fmt.Sprintf("SES.byServerId entry for %v is nil", slk))
		}
		if ses.byPrioShard.Len() == 0 {
			return errors.New(fmt.Sprintf("SES.byPrioShard entry for %v is zero length", slk))
		}
		if ses.byServerId.Len() == 0 {
			return errors.New(fmt.Sprintf("SES.byServerId entry for %v is zero length", slk))
		}
		if ses.byPrioShard.Len() != ses.byServerId.Len() {
			return errors.New(fmt.Sprintf("SES.byPrioShard/byPrioShard for %v is length mismatch %d != %d", slk, ses.byPrioShard.Len(), ses.byServerId.Len()))
		}
		numServicesByService += ses.byPrioShard.Len()
		var err error

		// now ascend by server and check it's in the peer list
		ses.byServerId.AscendGreaterOrEqual(ses.byServerId.Min(), func(i llrb.Item) bool {
			se := i.(*byServerServerEntry)
			if se.Slk != slk {
				err = errors.New(fmt.Sprintf("serverEntry has has mismatched slk: %v %v", se.Slk, se))
				return false
			}
			// Now check we can look it by peer - this should be unique
			if peerEntries, ok := sr.peers[se.Peer.Name]; !ok {
				err = errors.New(fmt.Sprintf("serverEntry se has no matching peer: %v %v", se, slk))
				return false
			} else {
				i2 := peerEntries.byServerId.Get(&byServerServerEntry{ServerEntry: se.ServerEntry})
				if i2 == nil {
					err = errors.New(fmt.Sprintf("serverEntry se has no matching peer entry: %v", se))
					return false
				}
				se2 := i2.(*byServerServerEntry)
				if se2.ServerEntry != se.ServerEntry {
					err = errors.New(fmt.Sprintf("serverEntry se has inconsistent peer entry: %v %v", se, se2))
					return false
				}
			}
			return err == nil
		})

		if err != nil {
			return err
		}

		// now ascend by prioshard and check it's in the peer list
		ses.byPrioShard.AscendGreaterOrEqual(ses.byPrioShard.Min(), func(i llrb.Item) bool {
			se := i.(*byPrioShardServerEntry)
			if se.Slk != slk {
				err = errors.New(fmt.Sprintf("prioShardEntry has has mismatched slk: %v %v", se.Slk, se))
				return false
			}
			// Now check we can look it by peer - this should be unique
			if peerEntries, ok := sr.peers[se.Peer.Name]; !ok {
				err = errors.New(fmt.Sprintf("prioShardEntry se has no matching peer: %v %v", se, slk))
				return false
			} else {
				i2 := peerEntries.byServerId.Get(&byServerServerEntry{ServerEntry: se.ServerEntry})
				if i2 == nil {
					err = errors.New(fmt.Sprintf("prioShardEntry se has no matching peer entry: %v", se))
					return false
				}
				se2 := i2.(*byServerServerEntry)
				if se2.ServerEntry != se.ServerEntry {
					err = errors.New(fmt.Sprintf("prioShardEntry se has inconsistent peer entry: %v %v", se, se2))
					return false
				}
			}
			return err == nil
		})

		if err != nil {
			return err
		}

	}
	if numServicesByPeer != numServicesByService {
		return errors.New(fmt.Sprintf("Mismatched services by peer / by service: %d != %d", numServicesByPeer, numServicesByService))
	}
	return nil
}
