package svcrouter

import (
	"crypto/rand"
	"errors"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/big"
)

const (
	PT_CLIENTSNOENV     = "clientsnoenv" // a BIND peer expecting clients to connect and not provide an envelope
	PT_CLIENTSENV       = "clientsenv"   // a BIND peer expecting clients to connect and provide an envelope
	PT_DOWNSTREAMENV    = "downstreamenv"
	PT_DOWNSTREAMENVREP = "downstreamenvrep"
)

// type ServiceDispatcher struct
//
// structure holding a complete service dispatcher

type ServiceDispatcher struct {
	id              string // this server's ID
	EnvRouterPeer   string
	NoEnvRouterPeer string
	ServiceRouter
	*RouterElement
}

// type ServiceDispatcherPeerImpl struct
//
// holds the implementation details for a client peer

type ServiceDispatcherPeerImpl struct {
	PeerType PeerType
	sd       *ServiceDispatcher
}

// func (m *ServiceDispatcherPeerImpl) GetType() string
//
// get the type of a client peer

func (m *ServiceDispatcherPeerImpl) GetType() PeerType {
	return m.PeerType
}

// func (m *ServiceDispatcherPeerImpl) PreDelete(re *RouterElement, peer *Peer)
//
// called on delete of a client peer

func (m *ServiceDispatcherPeerImpl) PreDelete(re *RouterElement, peer *Peer) {
	m.sd.DeleteAllPeerEntries(re, peer.Name)
}

// func (m *ServiceDispatcherPeerImpl) PostCreate(re *RouterElement, peer *Peer)
//
// called on create of a client peer

func (m *ServiceDispatcherPeerImpl) PostCreate(re *RouterElement, peer *Peer) {}

// func randInt(n int) int
//
// return a random int r such that 0 <= r < n

func randInt(n int64) int64 {
	big, err := rand.Int(rand.Reader, big.NewInt(n))
	if err != nil {
		panic(err)
	}
	return big.Int64()
}

// func (ServiceDispatcher *sd) doServiceDispatcherRouting(re *RouterElement, from *Peer, inmsg [][]byte) (*Peer, [][]byte, error)
//
// route a message

func (sd *ServiceDispatcher) doServiceDispatcherRouting(re *RouterElement, from *Peer, inmsg [][]byte) (*Peer, [][]byte, error) {
	var err error
	var dest *Peer
	var env *Envelope
	isReply := false
	hasEnv := true

	DumpMsg("Input", inmsg)
	log.Printf("Input type = %s", from.GetType())

	// first remove the envelope
	switch from.GetType() {
	case PT_CLIENTSNOENV: // this is a message request without an envelope
		if env, err = NewEnvelopeFromMsg(inmsg, ClientAttributeFilter); err != nil {
			return nil, nil, err
		}
		env.Attributes[EA_ORIGINATORID] = from.Name
		hasEnv = false
		DumpMsg("Made envelope but not inserted", inmsg)

	case PT_DOWNSTREAMENV: // this is a message reply - route on the outermost identifier
		isReply = true

	case PT_DOWNSTREAMENVREP: // this is a message reply - route on the outermost identifier
		isReply = true
		inmsg = inmsg[1:] // strip the first frame
		DumpMsg("Input drop", inmsg)

	default:
	}

	if hasEnv {
		if inmsg, env, err = RemoveEnvelope(inmsg); err != nil {
			return nil, nil, err
		}
		DumpMsg("After env remove", inmsg)
	}

	// replies get sent to the envelope handling router peer, unless EA_ORIGINATORID is set
	if isReply {
		dest = sd.RouterElement.peers[sd.EnvRouterPeer]
		orig := env.Attributes[EA_ORIGINATORID]
		if orig != "" {
			if d, ok := sd.RouterElement.peers[orig]; ok {
				dest = d
			}
		}
	} else {
		log.Printf("env = %v", env)
		serverEntry := sd.GetServerEntry(env.Attributes[EA_SERVICETYPE], env.Attributes[EA_SERVICEID], env.Attributes[EA_SHARDID])
		if serverEntry == nil {
			return nil, nil, errors.New("Cannot route service")
		}

		dest = serverEntry.Peer
	}

	if dest == nil {
		return nil, nil, errors.New("Cannot route service")
	}

	log.Printf("Destination is %s", dest.Name)
	DumpMsg("Pre insert", inmsg)

	destIsRep := false
	destHasEnvelope := false
	// lastly insert the envelope if our destination requires it
	switch dest.GetType() {
	case PT_CLIENTSNOENV:
		destHasEnvelope = false
		destIsRep = false
	case PT_DOWNSTREAMENVREP:
		destHasEnvelope = true
		destIsRep = true
	default:
		destHasEnvelope = true
		destIsRep = false
	}

	if destHasEnvelope {
		if inmsg, err = InsertEnvelope(inmsg, env); err != nil {
			return nil, nil, err
		}
	}

	if destIsRep {
		inmsg = InsertLeadingDelimeter(inmsg)
	}

	DumpMsg("Post insert", inmsg)

	return dest, inmsg, nil
}

// func (sd *ServiceDispatcher) NewServiceDispatcherPeerImpl (peerType PeerType) ServiceDispatcherPeerImpl
//
// Create a new peer implementer

func (sd *ServiceDispatcher) NewServiceDispatcherPeerImpl(peerType PeerType) *ServiceDispatcherPeerImpl {
	m := &ServiceDispatcherPeerImpl{
		PeerType: peerType,
		sd:       sd,
	}
	return m
}

// func (sd *ServiceDispatcher) AddPeer(def PeerDefinition) error
//
// Add a peer to the routing element. This can be called asynchronously (i.e. from another thread).

func (sd *ServiceDispatcher) AddPeer(def PeerDefinition, peerType PeerType) error {
	def.PeerImpl = sd.NewServiceDispatcherPeerImpl(peerType)
	return sd.RouterElement.AddPeer(def)
}

// func NewServiceDispatcher(context *zmq.Context, bufferSize int) (*ServiceDispatcher, error)
//
// Create a new service dispatcher

func NewServiceDispatcher(context *zmq.Context, bufferSize int) (*ServiceDispatcher, error) {
	sd := &ServiceDispatcher{
		id:            "thisrouter",
		ServiceRouter: NewServiceRouter(),
	}
	if re, err := NewRouterElement(context, bufferSize, sd.doServiceDispatcherRouting); err != nil {
		return nil, err
	} else {
		sd.RouterElement = re
		return sd, nil
	}
}
