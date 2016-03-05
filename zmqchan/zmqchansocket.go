package zmqchan

// TODO: Ensure Close() properly drains the TX channel

import (
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"sync"
	"sync/atomic"
)

var uniqueIndex uint64

type ZmqChanSocket struct {
	Context     *zmq.Context
	zSock       *zmq.Socket
	zTxIn       *zmq.Socket
	zTxOut      *zmq.Socket
	zControlIn  *zmq.Socket
	zControlOut *zmq.Socket
	wg          sync.WaitGroup
	ChanSocketImpl
}

// func barrier()
//
// Magic voodoo to provide a 'complete memory barrier' as seemingly required
// to pass zmq sockets between threads

func barrier() {
	var mutex sync.Mutex
	mutex.Lock()
	mutex.Unlock()
}

// func getUniqueId() uint64
//
// returns a unique ID

func getUniqueId() uint64 {
	return atomic.AddUint64(&uniqueIndex, 1)
}

// func (s *ZmqChanSocket) runChannels()
//
// this function
// 1. Reads the TX channel, and places the output in the zTxIn pipe pair
// 2. Reads the Control channel, and places the output in the zControlIn pipe pair
//
func (s *ZmqChanSocket) runChannels() {
	defer func() {
		s.zTxIn.Close()
		s.zControlIn.Close()
		log.Println("RUNC: quitting")
		s.wg.Done()
	}()
	for {
		select {
		case msg, ok := <-s.TxChan:
			if !ok {
				// it's closed - this should never happen
				log.Println("RUNC: read from Tx Chan ERROR")
				s.errors <- errors.New("ZMQ tx channel unexpectedly closed")
				// it's closed - this should not ever happen
				return
			} else {
				log.Println("RUNC: read from Tx Chan")
				if _, err := s.zTxIn.SendMessage(msg); err != nil {
					s.errors <- err
					return
				}
				log.Println("RUNC: write to Tx Pair socket")
			}
		case control, ok := <-s.control:
			if !ok {
				log.Println("RUNC: read from Control Chan ERROR")
				s.errors <- errors.New("ZMQ control channel unexpectedly closed")
				// it's closed - this should not ever happen
				return
			} else {
				log.Printf("RUNC: read from Control Chan %v", control)
				// If it's come externally, send a control message; ignore errors
				if control {
					log.Println("RUNC: zControl sendmsg start")
					s.zControlIn.SendMessage("")
					log.Println("RUNC: zControl sendmsg done")
				}
				return
			}
		}
	}
}

// func (s *zmqChanSocket) runSockets()
//
// this function
// 1. Reads the main socket, and place the output into the rx channel
// 2. Reads the zTxOut pipe pair and ...
// 3. Puts the output into the main socket
// 4. Reads the zControlOut pipe pair

func (s *ZmqChanSocket) runSockets() {
	defer func() {
		s.zTxOut.Close()
		s.zControlOut.Close()
		s.zSock.Close()
		s.wg.Done()
	}()
	var toXmit [][]byte = nil
	poller := zmq.NewPoller()
	idxSock := poller.Add(s.zSock, 0)
	idxTxOut := poller.Add(s.zTxOut, 0)
	idxControlOut := poller.Add(s.zControlOut, zmq.POLLIN)

	for {
		zSockflags := zmq.POLLIN
		var txsockflags zmq.State = 0
		// only if we have something to transmit are we interested in polling for output availability
		// else we just poll the input socket
		if toXmit == nil {
			txsockflags |= zmq.POLLIN
		} else {
			zSockflags |= zmq.POLLOUT
		}
		poller.Update(idxSock, zSockflags)
		poller.Update(idxTxOut, txsockflags)
		if sockets, err := poller.PollAll(-1); err != nil {
			s.errors <- err
			s.control <- false
			return
		} else {
			if sockets[idxSock].Events&zmq.POLLIN != 0 {
				// we have received something on the main socket
				// we need to send it to the RX channel
				log.Println("RUNS: Read start from main socket")
				if parts, err := s.zSock.RecvMessageBytes(0); err != nil {
					log.Println("RUNS: Read ERROR from main socket")
					s.errors <- err
					s.control <- false
					return
				} else {
					s.RxChan <- parts
				}
				log.Println("RUNS: Read done from main socket")
			}
			if sockets[idxSock].Events&zmq.POLLOUT != 0 && toXmit != nil {
				// we are ready to send something on the main socket
				log.Println("RUNS: Write start to main socket")
				if _, err := s.zSock.SendMessage(toXmit); err != nil {
					log.Println("RUNS: Write ERROR to main socket")
					s.errors <- err
					s.control <- false
					return
				} else {
					toXmit = nil
				}
				log.Println("RUNS: Write done to main socket")
			}
			if sockets[idxTxOut].Events&zmq.POLLIN != 0 && toXmit == nil {
				// we have something on the input socket, put it in xmit
				var err error
				log.Println("RUNS: Read start from tx socket")
				toXmit, err = s.zTxOut.RecvMessageBytes(0)
				if err != nil {
					log.Println("RUNS: Read ERROR from tx socket")
					s.errors <- err
					s.control <- false
					return
				}
				log.Println("RUNS: Read done from tx socket")
			}
			if sockets[idxControlOut].Events&zmq.POLLIN != 0 {
				// something has arrived on the control channel
				// ignore errors
				log.Println("RUNS: Read start from control socket")
				_, _ = s.zControlOut.RecvMessageBytes(0)
				log.Println("RUNS: Read done from control socket")
				// no need to signal the other end as we know it is already exiting
				// what we need to do is ensure any transmitted stuff is sent.

				// this is more tricky than you might think. The data could be
				// in ToXmit, in the TX socket pair, or in the TX channel.

				// block in these cases for as long as the linger value
				// FIXME: Ideally we'd block in TOTAL for the linger time,
				// rather than on each send for the linger time.
				if linger, err := s.zSock.GetLinger(); err == nil {
					s.zSock.SetSndtimeo(linger)
				}
				if toXmit != nil {
					log.Println("RUNS: Write of toXmit start")
					if _, err := s.zSock.SendMessage(toXmit); err != nil {
						s.errors <- err
						return
					}
					log.Println("RUNS: Write of toXmit end")
				} else {
					toXmit = nil
				}

				poller.Update(idxControlOut, 0)
				poller.Update(idxSock, 0)
				poller.Update(idxTxOut, zmq.POLLIN)
				for {
					if sockets, err := poller.PollAll(0); err != nil {
						s.errors <- err
						return
					} else if sockets[idxTxOut].Events&zmq.POLLIN != 0 && toXmit == nil {
						// we have something on the input socket, put it in xmit
						var err error
						log.Println("RUNS: Read drain start from tx socket")
						toXmit, err = s.zTxOut.RecvMessageBytes(0)
						if err != nil {
							log.Println("RUNS: Read drain ERROR from tx socket")
							s.errors <- err
							return
						}
						log.Println("RUNS: Read drain done from tx socket")
						log.Println("RUNS: Write of drained data start")
						if _, err := s.zSock.SendMessage(toXmit); err != nil {
							log.Println("RUNS: Write of drained data error")
							s.errors <- err
							return
						}
						log.Println("RUNS: Write of drained data done")
					} else {
						break
					}
				}

				// now read the TX channel until it is empty
				log.Println("RUNS: Emptying the TX channel start")
				done := false
				for !done {
					select {
					case msg, ok := <-s.TxChan:
						if ok {
							log.Println("RUNS: Flush write start")
							if _, err := s.zSock.SendMessage(msg); err != nil {
								log.Println("RUNS: Flush write error")
								s.errors <- err
								return
							}
							log.Println("RUNS: Flush write end")
						} else {
							log.Println("RUNS: Flush write no channel")
							s.errors <- errors.New("ZMQ tx channel unexpectedly closed")
							return
						}
					default:
						log.Println("RUNS: Flush write default")
						done = true
					}
				}
				log.Println("RUNS: Emptying the TX channel end")
				return
			}
		}
	}
}

// func (s *ZmqChanSocket) Close() error
//
// close a ZmqChanSocket. This will kill the internal goroutines, and close
// the main ZMQ Socket. It will also close the error channel, so a select() on
// it will return 'ok' as false. If an error is produced either during the close
// or has been produced prior to the close, it will be returned.

func (s *ZmqChanSocket) Close() error {
	log.Println("CLOS: writing to control")
	s.control <- true
	log.Println("CLOS: waiting for WG")
	s.wg.Wait()
	var err error = nil
	select {
	case err = <-s.errors:
	default:
	}
	s.deinitImpl()
	return err
}

// func (s *ZmqChanSocket) GetTxChan() chan<- [][]byte
//
// get the TxChannel as a write only channel

func (s *ZmqChanSocket) GetTxChan() chan<- [][]byte {
	return s.TxChan
}

// func (s *ZmqChanSocket) GetRxChan() chan<- [][]byte
//
// get the RxChannel as a read only channel

func (s *ZmqChanSocket) GetRxChan() <-chan [][]byte {
	return s.RxChan
}

// func newPair(c *zmq.Context) (a *zmq.Socket, b *zmq.Socket, err error)
//
// Create a new socket pair

func newPair(c *zmq.Context) (a *zmq.Socket, b *zmq.Socket, err error) {
	addr := fmt.Sprintf("inproc://_zmqchansocket_internal-%d", getUniqueId())
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
	}
	if b != nil {
		b.Close()
	}
	return
}

// func NewZmqChanSocket(zmqContext *zmq.Context, zSock *zmq.Socket, txbuf int, rxbuf in) (*ZmqChanSocket, error)
//
// Produce a new ZmqChanSocket. Pass a contact and a zmq.Socket, plus the buffering parameters for the channels.
//
// If this call succeeds (err == nil), then a ZmqChanSocket is returned, and control of your zmq.Socket is passed
// irrevocably to this routine. You should forget you ever had the socket. Do not attempt to use it in any way,
// as its manipulation is now the responsibility of goroutines launched by this routine. Closing the ZmqChanSocket
// will also close your zmq.Socket.
//
// If this routine errors, it is the caller's responsibility to close the zmq.Socket
//
func NewZmqChanSocket(zSock *zmq.Socket, txbuf int, rxbuf int) (ChanSocket, error) {
	s := &ZmqChanSocket{
		zSock: zSock,
	}

	zmqContext, err := zSock.Context()
	if err != nil {
		return nil, err
	}

	if s.zControlIn, s.zControlOut, err = newPair(zmqContext); err != nil {
		return nil, err
	}
	if s.zTxIn, s.zTxOut, err = newPair(zmqContext); err != nil {
		s.zControlIn.Close()
		s.zControlOut.Close()
		return nil, err
	}

	// as we should never read or send to these sockets unless they are ready
	// we set the timeout to 0 so a write or read in any other circumstance
	// returns a immediate error
	s.zSock.SetRcvtimeo(0)
	s.zSock.SetSndtimeo(0)
	s.zTxIn.SetRcvtimeo(0)
	s.zTxIn.SetSndtimeo(0)
	s.zTxOut.SetRcvtimeo(0)
	s.zTxOut.SetSndtimeo(0)
	s.zControlIn.SetRcvtimeo(0)
	s.zControlIn.SetSndtimeo(0)
	s.zControlOut.SetRcvtimeo(0)
	s.zControlOut.SetSndtimeo(0)

	s.initImpl(txbuf, rxbuf)

	barrier()
	s.wg.Add(2)
	go s.runSockets()
	go s.runChannels()
	return s, nil
}
