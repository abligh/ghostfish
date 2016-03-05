package app

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/HuKeping/rbtree"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RpcPktRequest = iota // This packet is a request
	RpcPktReply   = iota // This packet is a reply
)

const (
	RpcErrNone         = iota // There has been no error
	RpcErrTimeout      = iota // A timeout has occurred
	RpcErrShutdown     = iota // A shutdown occurred whilst processing the request
	RpcErrCouldNotSend = iota // Could not send the request
	RpcErrBadService   = iota // The destination does not recognise the service
	RpcErrBadAddr      = iota // The address is bad
	RpcErrProhibited   = iota // Prohibited by filter list
	RpcErrNotLeader    = iota // The RPC command cannot be executed as it was sent to the leader but the recipient was not the leader at the relevant time
	RpcErrRedirect     = iota // The RPC command is redirected elsewhere
)

var rpcErrorMap map[int]string = map[int]string{
	RpcErrNone:         "No error",
	RpcErrTimeout:      "A timeout has occurred",
	RpcErrShutdown:     "A shutdown occurred whilst processing the request",
	RpcErrCouldNotSend: "Could not send the request",
	RpcErrBadService:   "The destination does not recongnise the service",
	RpcErrBadAddr:      "The address is bad",
	RpcErrProhibited:   "Prohibited by filter list",
	RpcErrNotLeader:    "Not the leader",
	RpcErrRedirect:     "Redirected",
}

// RpcError holds an error type
type RpcError struct {
	ErrorType int // The type of the error
}

// type RpcData is an interface representing the data within a request or reply packet
type RpcData interface {
	GetPktType() int // return whether the packet is a request or a reply
}

// Type RpcRequest is a struct containing an RPC request
//
// Note all elements are exported so they are serialized
type RpcRequest struct {
	Request interface{} // The request
}

// Type RpcReply is a struct containing an RPC reply
//
// Note all elements are exported so they are serialized
type RpcReply struct {
	ErrorType RpcError    // The type of error (if any)
	Reply     interface{} // The reply
}

// Type RpcPacket contains an inflight RPC packet
//
// Note all elements are exported so they are serialized
type RpcPacket struct {
	Source       RpcAddress // The source address
	Dest         RpcAddress // The destination address
	Service      string     // The name of the service
	Id           uint64     // The sequential Id of the request packet to which this relates
	MinUnackedId uint64     // The lowest ID we have used that remains unacknowledged (tx)
	Generation   uint64     // The generation of the requestor to which this relates
	Data         RpcData    // The RPC data
}

// Type RpcAddress is an interface that represents an opaque RPC address
type RpcAddress interface {
	String() string              // get the textual representation of the address
	GetTransportAddress() string // get the transport layer representation of the address (may e.g. do a DNS lookup)
}

// Type RpcTransport is an interface that represents an opaque RPC transport
type RpcTransport interface {
	Send(pkt *RpcPacket) error // send an RPC packet asynchronously
	GetAddress() RpcAddress    // address of the transport
}

// Directions of RPC packet
const (
	RpcDirectionTx = iota
	RpcDirectionRx = iota
)

// Type RpcParameters specifies the timeout parameters for an Rpc transaction
type RpcParameters struct {
	Timeout time.Duration // Timeout
	Retries int
}

// type RpcService is a convenience struct
//
// It allows multiple requests to be sent to the same Destination, Service with the same Mux and Parameters
type RpcService struct {
	Destination RpcAddress    // Destination
	Service     string        // Service
	Mux         *RpcMux       // Mux
	Parameters  RpcParameters // Parameters
}

// Type RpcEndpoint is an interface handling packets of a particular service
type RpcEndpoint interface {
	Handle(mux *RpcMux, request *RpcPacket) *RpcPacket //  Handle an incoming packet, returning the reply
}

// Type rpcPingEndpoint is an internal struct handling performing a ping service
type rpcPingEndpoint struct {
}

// Type RpcMux is a struct representing all the registered services, and the service originator
//
// normally there would be one of these per application with a unique generation. The address should not change once created
type RpcMux struct {
	address    RpcAddress                   // The source / destination address of this Mux
	handlers   map[string]RpcEndpoint       // a map between service strings and registered endpoints
	hmutex     sync.RWMutex                 // a mutex protecting the handlers map
	nextId     uint64                       // the next sequential Id
	generation uint64                       // the generation
	transport  RpcTransport                 // the transport used
	active     *rbtree.Rbtree               // redblack tree of active communications indexed by ID
	amutex     sync.RWMutex                 // mutex protecting 'active'
	filters    map[int]*rpcPacketFilterList // List of filters for each direction
	sessions   map[string]*rpcSession       // map between source peer name and session for that peer
	smutex     sync.RWMutex                 // mutex protecting 'sessions'
}

// Type rpcSession represents a session with a single source peer
type rpcSession struct {
	generation   uint64         // generation in use for this peer
	replies      *rbtree.Rbtree // list of replies indexed in an RB tree by ID
	sync.RWMutex                // protects replies rbtree
}

// Type rpcCommunication is a single node within the active rb tree that represents an outgoing communication or
// within the session rb tree that represents a single reply
type rpcCommunication struct {
	source  string              // source of communication (as seen by initiator)
	dest    string              // dest of communication (as seen by initiatior)
	service string              // service name
	id      uint64              // id used by initiator
	handler *rpcResponseHandler // rpc response hander
	reply   *RpcPacket          // rpc response packet
}

// Type rpcResponseHandler is an internal struct used for storing the channels associated with each active communication
//
type rpcResponseHandler struct {
	ReplyChannel chan<- RpcReply // channel through which the reply packet should be set
	quit         chan bool       // channel to signal timeout goroutine should quit - write 'true' to perform a shutdown
	redirect     chan RpcAddress // channel to signal that a packet should be resent without decrementing the retry count (e.g. if the leader has changed)
}

// func String() converts an RpcError to a string
func (e RpcError) String() string {
	s, ok := rpcErrorMap[e.ErrorType]
	if ok {
		return "RPC Error: " + s
	}
	return fmt.Sprintf("RPC Error: unknown error (%d)", e.ErrorType)
}

// func getRequestKey returns an rpcCommunicationKey for a request packet
func (pkt *RpcPacket) getRequestCommunication() *rpcCommunication {
	return &rpcCommunication{
		source:  pkt.Source.String(),
		dest:    pkt.Dest.String(),
		service: pkt.Service,
		id:      pkt.Id,
	}
}

// func getRequestKey returns an rpcCommunicationKey for a response packet
func (pkt *RpcPacket) getReplyCommunication() *rpcCommunication {
	return &rpcCommunication{
		source:  pkt.Dest.String(),
		dest:    pkt.Source.String(),
		service: pkt.Service,
		id:      pkt.Id,
	}
}

// func getRequestKey returns the packet type associated with a reply
func (d RpcRequest) GetPktType() int {
	return RpcPktRequest
}

// func getRequestKey returns the packet type associated with a request
func (d RpcReply) GetPktType() int {
	return RpcPktReply
}

func (mux *RpcMux) sendWithFilter(pkt *RpcPacket) error {
	list := mux.filters[RpcDirectionTx]
	action := list.Apply(pkt)
	if action.Action == RpcPacketFilterActionReject || action.Action == RpcPacketFilterActionDrop {
		return nil
	}
	return mux.transport.Send(pkt)
}

// func Handle handles an inbound packet
func (mux *RpcMux) handleRequest(request *RpcPacket) {
	// check whether we are already processing, and if so return cached version; if null ignore
	// drop all replies of other generations
	// drop all replies older than the last unacked
	mux.smutex.Lock()
	src := request.Source.String()
	session, ok := mux.sessions[src]
	if ok && session.generation != request.Generation {
		ok = false
	}
	if !ok {
		session = &rpcSession{generation: request.Generation, replies: rbtree.New()}
		// if there was a session from a previous generation the reply will simply not be cached
		mux.sessions[src] = session
	}
	mux.smutex.Unlock()

	session.Lock()
	// drop old reply caches
	for {
		item := session.replies.Min()
		if item == nil {
			break
		}
		comm := item.(*rpcCommunication)
		// stop if we are now into unacked packets
		// stop if we are still constructing the reply (should not happen as that should be unacked) or this is us
		if comm == nil || comm.id >= request.MinUnackedId || comm.reply == nil || comm.id == request.Id {
			break
		}
		session.replies.Delete(item)
	}

	searchcomm := request.getRequestCommunication()

	var reply *RpcPacket = nil

	// TODO: optimise this into a single 'Remove' call
	if item := session.replies.Get(searchcomm); item != nil {
		if comm := item.(*rpcCommunication); comm != nil &&
			comm.source == searchcomm.source &&
			comm.dest == searchcomm.dest &&
			comm.id == searchcomm.id &&
			comm.service == searchcomm.service {
			// OK so we've found a dupe. There are two possibilities:
			// 1. we are still processing the packet, in which case we silently discard this or
			// 2. we have already processed the packet, in which case we resend the cached reply
			reply = comm.reply
			session.Unlock()
			if reply != nil {
				// already cached so no need to recache
				if err := mux.sendWithFilter(reply); err != nil {
					log.Printf("[WARN] Could not send reply: %v", err)
				}
			}
			return
		}
	}

	// cache it, with a nil packet
	session.replies.Insert(searchcomm)

	session.Unlock()

	mux.hmutex.RLock()
	endpoint, ok := mux.handlers[request.Service]
	mux.hmutex.RUnlock()
	if ok {
		reply = endpoint.Handle(mux, request)
	} else {
		reply = request.MakeError(mux.address, RpcErrBadService)
	}

	// cache the reply
	session.Lock()
	searchcomm.reply = reply
	session.Unlock()

	if err := mux.sendWithFilter(reply); err != nil {
		log.Printf("[WARN] Could not send reply: %v", err)
	}
}

// func MakeReply generates a corresponding reply packet from a request packet
func (request *RpcPacket) MakeReply(source RpcAddress) *RpcPacket {
	pkt := &RpcPacket{
		Source:     source,
		Dest:       request.Source,
		Service:    request.Service,
		Id:         request.Id,
		Generation: request.Generation,
		Data:       RpcReply{ErrorType: RpcError{RpcErrNone}},
	}
	return pkt
}

// func MakeError generates a corresponding error reply packet from a request packet
func (request *RpcPacket) MakeError(source RpcAddress, errNum int) *RpcPacket {
	rpcErr := request.MakeReply(source)
	rpcErr.Data = RpcReply{ErrorType: RpcError{errNum}}
	return rpcErr
}

// func sendWithRetry sends a packet to a given destination
//
// this is called client side only
func (mux *RpcMux) sendWithRetry(req *RpcPacket, rrh *rpcResponseHandler, parameters *RpcParameters) {
	retriesRemaining := parameters.Retries
	for {
		// ALEX: transform leader address here
		if err := mux.sendWithFilter(req); err != nil {
			go func() {
				log.Printf("sendasync returned %v", err)
				rrh.ReplyChannel <- RpcReply{
					ErrorType: RpcError{RpcErrCouldNotSend},
				}
			}()
			return
		}
		for resend := false; !resend; {
			select {
			case <-time.After(parameters.Timeout):
				retriesRemaining--
				if retriesRemaining > 0 {
					resend = true
				} else {
					timeoutPacket := req.MakeError(req.Dest, RpcErrTimeout)
					// closes handler.quit
					mux.handleResponse(timeoutPacket)
					return
				}
			case shutdown := <-rrh.quit:
				// rrh.quit has been written to or closed. closed will give false
				if shutdown {
					shutdownPacket := req.MakeError(req.Dest, RpcErrShutdown)
					// this in turn closes rrh.quit
					mux.handleResponse(shutdownPacket)
				}
				return
			case redirectTo := <-rrh.redirect:
				// we need to resend the packet without decrementing retries
				req.Dest = redirectTo
				resend = true
			}
		}
	}
}

// func Less will order based on sequence then insertion order
func (i *rpcCommunication) Less(than rbtree.Item) bool {
	return i.id < than.(*rpcCommunication).id
}

// func handleResponse handles inbound packets which are of a response type
func (mux *RpcMux) handleResponse(response *RpcPacket) {
	// ignore responses to a previous generation
	if response.Generation != mux.generation {
		return
	}
	if response.Data.GetPktType() != RpcPktReply || response.Dest != mux.address {
		return
	}
	reply, ok := response.Data.(RpcReply)
	if !ok {
		log.Println("[WARN] Response was not actually a response type")
		return
	}
	searchcomm := response.getReplyCommunication()
	var handler *rpcResponseHandler = nil
	var redirectTo RpcAddress = nil
	mux.amutex.Lock()
	// TODO: optimise this into a single 'Remove' call
	if item := mux.active.Get(searchcomm); item != nil {
		if comm := item.(*rpcCommunication); comm != nil &&
			comm.source == searchcomm.source &&
			/* comm.dest == searchcomm.dest &&    ALEX: Don't check dest (source of reply) as may have changed if e.g. to leader */
			comm.id == searchcomm.id &&
			comm.service == searchcomm.service {
			if reply.ErrorType.ErrorType == RpcErrRedirect {
				redirectTo, _ = reply.Reply.(RpcAddress)
			}
			if redirectTo != nil {
				mux.active.Delete(item)
			}
			handler = comm.handler
		}
	}
	mux.amutex.Unlock()
	if handler == nil {
		return
	}
	if redirectTo != nil {
		handler.redirect <- redirectTo
		return
	}
	// between the Unlock() and the next statement, the timer may expire, leading to
	// a second call to HandleResponse, but it will immediately be dropped as the
	// active map will now have the handler removed
	close(handler.quit)
	close(handler.redirect)
	handler.ReplyChannel <- reply
	return
}

// func SendAsync asynchronously sends a request to a given address and service, sending the reply to the specified channel
//
// Note that send errors are transformed into synthesized received packet errors
func (mux *RpcMux) SendAsync(dest *RpcAddress, service string, request *RpcRequest, parameters *RpcParameters, replyChan chan<- RpcReply) {
	req := RpcPacket{
		Id:         atomic.AddUint64(&mux.nextId, 1),
		Generation: mux.generation,
		Source:     mux.address,
		Service:    service,
		Dest:       *dest,
		Data:       request,
	}
	responseHandler := rpcResponseHandler{
		ReplyChannel: replyChan,
		quit:         make(chan bool),
		redirect:     make(chan RpcAddress),
	}
	comm := req.getRequestCommunication()
	comm.handler = &responseHandler
	mux.amutex.Lock()
	mux.active.Insert(comm)
	minitem := mux.active.Min()
	if minitem != nil {
		req.MinUnackedId = minitem.(*rpcCommunication).id
	}
	mux.amutex.Unlock()
	go mux.sendWithRetry(&req, &responseHandler, parameters)
}

// func SendSync synchronously sends a request to a given address and service, returning the reply
//
// Note that send errors are transformed into synthesized received packet errors
func (mux *RpcMux) SendSync(dest *RpcAddress, service string, request *RpcRequest, parameters *RpcParameters) *RpcReply {
	ch := make(chan RpcReply)
	defer close(ch)
	mux.SendAsync(dest, service, request, parameters, ch)
	packet := <-ch
	return &packet
}

// func SendAsync asynchronously sends a request to a service
func (rpcs *RpcService) SendAsync(request *RpcRequest, replyChan chan<- RpcReply) {
	rpcs.Mux.SendAsync(&rpcs.Destination, rpcs.Service, request, &rpcs.Parameters, replyChan)
}

// func SendAsync synchronously sends a request to a service
func (rpcs *RpcService) SendSync(request *RpcRequest) *RpcReply {
	return rpcs.Mux.SendSync(&rpcs.Destination, rpcs.Service, request, &rpcs.Parameters)
}

// func Handle handles an incoming packet
func (mux *RpcMux) Handle(pkt *RpcPacket) {
	if pkt.Data == nil {
		log.Println("[WARN] Packet has no data")
		return
	}

	list := mux.filters[RpcDirectionRx]
	pkttype := pkt.Data.GetPktType()
	action := list.Apply(pkt)
	if action.Action == RpcPacketFilterActionReject || action.Action == RpcPacketFilterActionDrop {
		if action.Action == RpcPacketFilterActionReject && pkttype == RpcPktRequest {
			reply := pkt.MakeError(mux.address, RpcErrProhibited)
			if err := mux.sendWithFilter(reply); err != nil {
				log.Printf("[WARN] Could not send reply: %v", err)
			}
		}
		return
	}

	switch pkttype {
	case RpcPktReply:
		go mux.handleResponse(pkt)
	case RpcPktRequest:
		go mux.handleRequest(pkt)
	default:
		log.Println("[WARN] Unidentified packet type")
	}
}

// func RegisterEndpoint registers an endpoint as a service with a mux
//
// pass endpoint as 'nil' to deregister. Service names beginning with '_' are reserved
func (mux *RpcMux) RegisterEndpoint(service string, endpoint RpcEndpoint) {
	mux.hmutex.Lock()
	if endpoint == nil {
		delete(mux.handlers, service)
	} else {
		mux.handlers[service] = endpoint
	}
	mux.hmutex.Unlock()
}

// func Shutdown removes all registered endpoints and sends shutdown errors to all outbound active RPC conversations
func (mux *RpcMux) Shutdown() {
	// we should probably send some sort of signal to the transport here
	mux.hmutex.Lock()
	mux.handlers = make(map[string]RpcEndpoint)
	mux.hmutex.Unlock()

	// this is pretty foul (because of the lock) but it will just make all the handlers block until we Unlock()
	mux.amutex.Lock()
	mux.active.Ascend(mux.active.Min(), func(item rbtree.Item) bool {
		i := item.(*rpcCommunication)
		// safe as v.quit is only closed once the handler is removed from the tree
		i.handler.quit <- true
		return true // continue iterating
	})
	mux.amutex.Unlock()
}

// func GetAddress returns the address of a mux
func (mux *RpcMux) GetAddress() RpcAddress {
	return mux.address
}

// func randUint64 generates a random uint64
func randUint64() uint64 {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic("Cannot generate random number: " + err.Error())
	}
	return binary.BigEndian.Uint64(b)
}

// func Handle sends back a ping reply
func (e rpcPingEndpoint) Handle(mux *RpcMux, request *RpcPacket) *RpcPacket {
	return request.MakeReply(mux.GetAddress())
}

// func NewRpcMux generates a new RPC mux
//
// The mux initially has no services registered
func NewRpcMux(transport RpcTransport) *RpcMux {
	mux := &RpcMux{
		address:    transport.GetAddress(),
		nextId:     0,
		generation: randUint64(),
		transport:  transport,
		active:     rbtree.New(),
		handlers:   make(map[string]RpcEndpoint),
		filters:    make(map[int]*rpcPacketFilterList),
		sessions:   make(map[string]*rpcSession),
	}
	mux.RegisterEndpoint("_ping", &rpcPingEndpoint{})
	mux.filters[RpcDirectionRx] = NewRpcPacketFilterList()
	mux.filters[RpcDirectionTx] = NewRpcPacketFilterList()
	return mux
}

// func NewService generates a new RpcService
func (mux *RpcMux) NewService(destination RpcAddress, service string, parameters RpcParameters) *RpcService {
	return &RpcService{
		Destination: destination,
		Service:     service,
		Mux:         mux,
		Parameters:  parameters,
	}
}
