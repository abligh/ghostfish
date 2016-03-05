package zmqchan_test

import (
	"github.com/abligh/ghostfish/zmqchan"
	zmq "github.com/pebbe/zmq4"
	"log"
	"sync"
	"testing"
	"time"
)

func runEcho(t *testing.T, num int, c zmqchan.ChanSocket) {
	for {
		select {
		case msg, ok := <-c.GetRxChan():
			if !ok {
				t.Fatal("Cannot read from echo channel")
			}
			log.Println("ECHO: got message")
			c.GetTxChan() <- msg
			log.Println("ECHO: sent message")
			num--
			if num <= 0 {
				log.Println("ECHO: done")
				return
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout in runEcho")
		}
	}
}

func msgEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func runWrite(t *testing.T, num int, c zmqchan.ChanSocket) {
	tx := 0
	rx := 0
	srcMsg := [][]byte{[]byte("Hello"), []byte("World")}
	txchan := c.GetTxChan()
	for {
		select {
		case msg, ok := <-c.GetRxChan():
			if !ok {
				t.Fatal("Cannot read from main channel")
			}
			rx++
			log.Printf("MAIN: got message %d", rx)
			if !msgEqual(msg, srcMsg) {
				t.Fatal("Messages do not match")
			}
			if rx >= num {
				log.Println("MAIN: done")
				return
			}
		case txchan <- srcMsg:
			log.Println("MAIN: sent message")
			tx++
			if tx >= num {
				txchan = nil
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout in runWrite")
		}
	}
}

func TestZmqChanSocket(t *testing.T) {

	var sb, sc *zmq.Socket
	var cb, cc zmqchan.ChanSocket
	var err error
	num := 10

	defer func() {
		if sb != nil {
			sb.SetLinger(0)
			sb.Close()
		}
		if sc != nil {
			sc.SetLinger(0)
			sc.Close()
		}
		if cb != nil {
			log.Println("MAIN: Close")
			cb.Close()
		}
		if cc != nil {
			log.Println("ECHO: Close")
			cc.Close()
		}
		log.Println("BOTH: Exit")
	}()

	if sb, err = zmq.NewSocket(zmq.PAIR); err != nil {
		t.Fatal("NewSocket:", err)
	}

	if sc, err = zmq.NewSocket(zmq.PAIR); err != nil {
		t.Fatal("NewSocket:", err)
	}

	if err = sb.Bind("tcp://127.0.0.1:9737"); err != nil {
		t.Fatal("sb.Bind:", err)
	}

	if err = sc.Connect("tcp://127.0.0.1:9737"); err != nil {
		t.Fatal("sc.Connect:", err)
	}

	if cb, err = zmqchan.NewZmqChanSocket(sb, 0, 0); err != nil {
		t.Fatal("sb.NewZmqChanSocket:", err)
	}
	sb = nil // don't access this or close it on defer

	if cc, err = zmqchan.NewZmqChanSocket(sc, 0, 0); err != nil {
		t.Fatal("sb.NewZmqChanSocket:", err)
	}
	sc = nil // don't access this or close it on defer

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		runEcho(t, num, cc)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		runWrite(t, num, cb)
		wg.Done()
	}()
	wg.Wait()
	log.Println("BOTH: done")
}
