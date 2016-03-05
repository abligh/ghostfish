package svcrouter

import (
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"sync"
	"sync/atomic"
)

type PeerType string

type PeerImpl interface {
	GetType() PeerType
	PreDelete(re *RouterElement, peer *Peer)
	PostCreate(re *RouterElement, peer *Peer)
}

type Peer struct {
	sock   *zmq.Socket
	buffer chan [][]byte
	xmit   [][]byte
	PeerDefinition
}

type RouterElement struct {
	peers       map[string]*Peer
	context     *zmq.Context
	controlChan chan *controlMessage
	bufferSize  int
	wg          sync.WaitGroup
}

const (
	CM_ADD        = iota
	CM_DELETE     = iota
	CM_REPLACEALL = iota
	CM_DELETEALL  = iota
)

type PeerDefinition struct {
	Name    string
	ZmqType zmq.Type
	Address string
	Bind    bool
	PeerImpl
}

type replaceAllParameters struct {
	peerType PeerType
	peers    []PeerDefinition
}

type controlMessage struct {
	action     int
	parameters interface{}
	reply      chan error
}

// func barrier()
//
// Magic voodoo to provide a 'complete memory barrier' as seemingly required
// to pass zmq sockets between threads

func Barrier() {
	var mutex sync.Mutex
	mutex.Lock()
	mutex.Unlock()
}

// func DumpMsg(prefix string, msg [][]byte)
//
// dump a message to the log
func DumpMsg(prefix string, msg [][]byte) {
	for i, l := range msg {
		log.Printf("%s: %d(%3d): %s", prefix, i, len(l), string(l))
	}
}

var uniqueIndex uint64

// func getUniqueId() uint64
//
// returns a unique ID

func getUniqueId() uint64 {
	return atomic.AddUint64(&uniqueIndex, 1)
}

// func (a *PeerDefinition) Equals (b *PeerDefinition) bool
//
// Determine whether two definitions are equal

func (a *PeerDefinition) Equals(b *PeerDefinition) bool {
	return a.Name == b.Name && a.ZmqType == b.ZmqType && a.Address == b.Address && a.Bind == b.Bind && a.GetType() == b.GetType()
}

// func newPair(c *zmq.Context) (a *zmq.Socket, b *zmq.Socket, err error)
//
// Create a new socket pair

func NewPair(c *zmq.Context) (a *zmq.Socket, b *zmq.Socket, err error) {
	addr := fmt.Sprintf("inproc://_routerelement_internal-%d", getUniqueId())
	if a, err = c.NewSocket(zmq.PAIR); err != nil {
		goto Error
	}
	if err = a.Bind(addr); err != nil {
		goto Error
	}
	if b, err = c.NewSocket(zmq.PAIR); err != nil {
		goto Error
	}
	if err = b.Connect(addr); err != nil {
		goto Error
	}
	return

Error:
	if a != nil {
		a.Close()
		a = nil
	}
	if b != nil {
		b.Close()
		b = nil
	}
	return
}

// func NewSocketAndAddress(c *zmq.Context, bind bool, t zmq.Type, name n) (a *zmq.Socket, addr string, err error)
//
// Create half of a socket pair

func NewSocketAndAddress(c *zmq.Context, bind bool, t zmq.Type, name string) (a *zmq.Socket, addr string, err error) {
	addr = fmt.Sprintf("inproc://_routerelement_internal-%d", getUniqueId())
	if a, err = c.NewSocket(t); err != nil {
		goto Error
	}
	if name != "" {
		a.SetIdentity(name)
	}
	if bind {
		if err = a.Bind(addr); err != nil {
			goto Error
		}
	} else {
		if err = a.Connect(addr); err != nil {
			goto Error
		}
	}
	return

Error:
	if a != nil {
		a.Close()
		a = nil
	}
	addr = ""
	return
}

// func NewHalfPair(c *zmq.Context, bind bool) (*zmq.Socket, string, error)
//
// Create half of a socket pair

func NewHalfPair(c *zmq.Context, bind bool) (*zmq.Socket, string, error) {
	return NewSocketAndAddress(c, bind, zmq.PAIR, "")
}

// func (re *RouterElement) GetPeer(name string) (*Peer, bool)
//
// return given peer

func (re *RouterElement) GetPeer(name string) (*Peer, bool) {
	p, ok := re.peers[name]
	return p, ok
}

// func (re *RouterElement) run(doRouting func(*RouterElement, *Peer, [][]byte) (*Peer, [][]byte, error), controlIn, controlOut *zmq.Socket)
//
// main goroutine for running a RouterElement. Takes a doRouting function (which in turns takes the RE, the source peer, and the message
// and returns the destination peer, the message and an error), and the control socket pair.
//
// this exits if the control channel is closed, closes the control socket pair, then clears up after itself.
//
// an internal goroutine ensures that when the control channel is written to, the poller is woken up.

func (re *RouterElement) run(doRouting func(*RouterElement, *Peer, [][]byte) (*Peer, [][]byte, error), controlIn, controlOut *zmq.Socket) {
	var wg sync.WaitGroup
	controlChanInternal := make(chan *controlMessage, 1)
	wg.Add(1)

	// Start a goroutine to copy from the control channel to the internal
	// control channel and wake up the control socket pair on each message.
	// Quit the goroutine when the control channel itself is closed
	go func() {
		defer func() {
			controlIn.SetLinger(0)
			controlIn.Close()
			wg.Done()
		}()
		msg := [][]byte{[]byte{0}}
		for {
			select {
			case m, ok := <-re.controlChan:
				if !ok {
					close(controlChanInternal)
					// send a message to the control panel to wake it up
					controlIn.SendMessage(msg)
					// ensure we get a message back before closing it
					controlIn.RecvMessageBytes(0)
					return
				}
				controlChanInternal <- m
				controlIn.SendMessageDontwait(msg)
			}
		}
	}()

	defer func() {
		wg.Wait()
		controlOut.SetLinger(0)
		controlOut.Close()
		// Close() should already remove all the peers, so this is just belt & braces
		for _, p := range re.peers {
			p.sock.SetLinger(0)
			p.sock.Close()
			close(p.buffer)
		}
		re.wg.Done()
	}()

	for {
		poller := zmq.NewPoller()
		pollSource := make([]*Peer, len(re.peers), len(re.peers))
		blockInput := false
		i := 0
		for _, p := range re.peers {
			pollSource[i] = p
			i++
			if p.xmit == nil {
				select {
				case p.xmit = <-p.buffer:
				default:
				}
			}
			buffered := len(p.buffer)
			if p.xmit != nil {
				buffered++
			}
			if buffered >= cap(p.buffer) {
				log.Printf("Blocking input as %s cannot take more data", p.Name)
				blockInput = true
			}
		}
		for _, p := range pollSource {
			flags := zmq.State(0)
			if !blockInput {
				flags |= zmq.POLLIN
			}
			if p.xmit != nil {
				flags |= zmq.POLLOUT
			}
			poller.Add(p.sock, flags)
		}
		idxControl := poller.Add(controlOut, zmq.POLLIN)

		if polled, err := poller.PollAll(-1); err != nil {
			log.Printf("Cannot poll: %v", err)
		} else {
			for i, p := range pollSource {
				if (polled[i].Events&zmq.POLLOUT != 0) && (p.xmit != nil) {
					if _, err := p.sock.SendMessageDontwait(p.xmit); err != nil {
						log.Printf("Peer %s cannot send message: %v", p.Name, err)
					}
					p.xmit = nil
				}
			}
			for i, p := range pollSource {
				if polled[i].Events&zmq.POLLIN != 0 {
					if recv, err := p.sock.RecvMessageBytes(0); err != nil {
						log.Printf("Peer %s cannot receive message: %v", p.Name, err)
					} else {
						if dest, fwd, err := doRouting(re, p, recv); err != nil {
							log.Printf("Peer %s cannot route message: %v", p.Name, err)
						} else {
							if dest != nil && fwd != nil {
								dest.buffer <- fwd
							}
						}
					}
				}
			}
			if polled[idxControl].Events&zmq.POLLIN != 0 {
				// read from the control socket and discard. We don't care
				// if this errors as it's really only to make us read from our
				// own control socket
				controlOut.RecvMessageBytes(0)
				select {
				case msg, ok := <-controlChanInternal:
					if !ok {
						// control channel is closed
						// send a dummy message to handshake
						msg := [][]byte{[]byte{0}}
						controlOut.SendMessage(msg)
						return
					}
					msg.reply <- re.processControlMessage(msg)
				default:
				}
			}
		}
	}
}

// func (re *RouterElement) addPeerInternal(params PeerDefinition) error
//
// internal routine to add a peer. Use AddPeer() externally

func (re *RouterElement) addPeerInternal(params PeerDefinition) error {
	if _, ok := re.peers[params.Name]; ok {
		return errors.New("Peer already exists")
	}
	if s, err := re.context.NewSocket(params.ZmqType); err != nil {
		return err
	} else {
		if params.Bind {
			if err := s.Bind(params.Address); err != nil {
				s.Close()
				return err
			}
		} else {
			if err := s.Connect(params.Address); err != nil {
				s.Close()
				return err
			}
		}
		p := &Peer{
			PeerDefinition: params,
			sock:           s,
			buffer:         make(chan [][]byte, re.bufferSize),
		}
		re.peers[params.Name] = p
		p.PostCreate(re, p)
		return nil
	}
}

// func (re *RouterElement) deletePeerInternal(n string) error
//
// internal routine to delete a peer. Use DeletePeer() externally

func (re *RouterElement) deletePeerInternal(n string) error {
	if p, ok := re.peers[n]; !ok {
		return errors.New("No such peer")
	} else {
		p.PeerImpl.PreDelete(re, p)
		close(p.buffer)
		p.sock.Close()
		delete(re.peers, n)
		return nil
	}
}

// func (re *RouterElement) processControlMessage(msg *controlMessage)
//
// Internal function to process a control message. Run synchronously as part of the main run() goroutine. Do not call this
// directly or ZMQ will not love you one little bit. Instead call using the control channel.

func (re *RouterElement) processControlMessage(msg *controlMessage) error {
	switch msg.action {
	case CM_ADD:
		defn := msg.parameters.(PeerDefinition)
		return re.addPeerInternal(defn)
	case CM_DELETE:
		name := msg.parameters.(string)
		return re.deletePeerInternal(name)
	case CM_REPLACEALL:
		params := msg.parameters.(replaceAllParameters)
		toDelete := make(map[string]bool)
		replacements := make(map[string]PeerDefinition, len(params.peers))
		for _, pd := range params.peers {
			replacements[pd.Name] = pd
			if pd.GetType() != params.peerType {
				return errors.New("All CM_REPLACEALL peers must be of the specified type")
			}
		}
		for n, p := range re.peers {
			if params.peerType == p.GetType() {
				// it's of the right type. if it corresponds exactly
				// to a new one, we just forget it was even passed
				if replacement, ok := replacements[n]; ok {
					// the existing peer has a replacement
					if replacement.Equals(&p.PeerDefinition) {
						delete(replacements, n)
					} else {
						toDelete[n] = true
					}
				} else {
					// the existing peer does not have a replacement
					toDelete[n] = true
				}
			}
		}
		for n, _ := range toDelete {
			re.deletePeerInternal(n)
		}
		for _, defn := range replacements {
			if err := re.addPeerInternal(defn); err != nil {
				return err
			}
		}
	case CM_DELETEALL:
		peerType := msg.parameters.(PeerType)
		for n, p := range re.peers {
			if peerType == "" || peerType == p.GetType() {
				re.deletePeerInternal(n)
			}
		}
		return nil

	default:
		return errors.New("Unknown control message")
	}
	return nil
}

// func (re *RouterElement) sendMessage(msg *controlMessage) error
//
// send a message to the control channel and return the response

func (re *RouterElement) sendMessage(msg *controlMessage) error {
	msg.reply = make(chan error)
	re.controlChan <- msg
	err := <-msg.reply
	return err
}

// func (re *RouterElement) AddPeer(def PeerDefinition) error
//
// Add a peer to the routing element. This can be called asynchronously (i.e. from another thread).

func (re *RouterElement) AddPeer(def PeerDefinition) error {
	return re.sendMessage(&controlMessage{
		action:     CM_ADD,
		parameters: def,
	})
}

// func (re *RouterElement) ReplacePeers(peerType string, defs []PeerDefinition) error
//
// Replace all peers of a given type in the routing element with the peer definitions supplied. Those that are no
// longer needed or have changed are closed. This can be called asynchronously (i.e. from another thread).

func (re *RouterElement) ReplacePeers(peerType PeerType, defs []PeerDefinition) error {
	return re.sendMessage(&controlMessage{
		action: CM_REPLACEALL,
		parameters: replaceAllParameters{
			peerType: peerType,
			peers:    defs,
		},
	})
}

// func (re *RouterElement) DeletePeer(peerImpl peerImpl, name string) error
//
// Delete a peer from the routing element. Thread safe.

func (re *RouterElement) DeletePeer(name string) error {
	return re.sendMessage(&controlMessage{
		action:     CM_DELETE,
		parameters: name,
	})
}

// func (re *RouterElement) DeleteAllType(peerType string)
//
// Delete all peers of a given type from the routing element. Thread safe.

func (re *RouterElement) DeleteAllType(peerType PeerType) error {
	return re.sendMessage(&controlMessage{
		action:     CM_DELETEALL,
		parameters: peerType,
	})
}

// func (re *RouterElement) DeleteAll()
//
// Delete all peers of a given type from the routing element. Thread safe.

func (re *RouterElement) DeleteAll() error {
	return re.sendMessage(&controlMessage{
		action:     CM_DELETEALL,
		parameters: PeerType(""),
	})
}

// func NewRouterElement(context *zmq.Context, bufferSize int, doRouting func(*RouterElement, *Peer, [][]byte) (*Peer, [][]byte, error)) (*RouterElement, error)
//
// Create a new RouterElement in a given zmqContext, and a given peer buffer size, with the specified routing function

func NewRouterElement(context *zmq.Context, bufferSize int, doRouting func(*RouterElement, *Peer, [][]byte) (*Peer, [][]byte, error)) (*RouterElement, error) {
	re := &RouterElement{
		peers:       make(map[string]*Peer),
		context:     context,
		controlChan: make(chan *controlMessage),
		bufferSize:  bufferSize + 1, // as 1 is used internally
	}
	if controlIn, controlOut, err := NewPair(re.context); err != nil {
		return nil, err
	} else {
		Barrier()
		re.wg.Add(1)
		go re.run(doRouting, controlIn, controlOut) // closes controlIn, controlOut
		return re, nil
	}
}

// func (re *RouterElement) Close()
//
// Close a RouterElement

func (re *RouterElement) Close() {
	re.DeleteAll()
	close(re.controlChan)
	re.wg.Wait()
}
