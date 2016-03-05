package zmqchan

import ()

type ChanSocket interface {
	Close() error
	GetTxChan() chan<- [][]byte
	GetRxChan() <-chan [][]byte
}

type ChanSocketImpl struct {
	TxChan  chan [][]byte
	RxChan  chan [][]byte
	errors  chan error
	control chan bool
}

func (impl *ChanSocketImpl) Errors() <-chan error {
	return impl.errors
}

func (impl *ChanSocketImpl) initImpl(txbuf int, rxbuf int) {
	impl.TxChan = make(chan [][]byte, txbuf)
	impl.RxChan = make(chan [][]byte, rxbuf)
	impl.errors = make(chan error, 2)
	impl.control = make(chan bool, 2)
}

func (impl *ChanSocketImpl) deinitImpl() {
	close(impl.TxChan)
	close(impl.RxChan)
	close(impl.errors)
	close(impl.control)
}
