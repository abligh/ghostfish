package app

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type App struct {
	log     *log.Logger
	config  Config
	wg      sync.WaitGroup
	cluster Cluster
	fsm     *masterFSM
	quit    chan chan error
}

func NewApp(c Config) (*App, error) {
	a := &App{}
	a.quit = make(chan chan error)
	a.log = log.New(os.Stderr, "", log.LstdFlags)
	a.config = c
	var err error

	a.fsm = a.newMasterFSM()

	a.cluster, err = newRaft(a)
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (a *App) Run() {
	a.log.Println("[INFO] Starting up")

	//a.wg.Add(1)
	t := time.NewTicker(time.Duration(2000) * time.Millisecond)
	defer func() {
		t.Stop()
		//a.wg.Done()
	}()

	for {
		select {
		case <-t.C:
			log.Printf("[INFO] Hello world, leader=%v", a.cluster.IsLeader())
			params := map[string]string{
				"key":   "key" + strconv.Itoa(rand.Intn(50)),
				"value": a.config.Raft.Addr + "/" + strconv.Itoa(rand.Intn(50)),
			}
			if err := a.cluster.Command(setCmd, params, time.Duration(2000)*time.Millisecond); err != nil {
				log.Printf("[ERROR] command received %v", err)
			}
		case errc := <-a.quit:
			a.cluster.Close()
			errc <- nil
			return
		}
	}
	//a.wg.Wait()
}

func (a *App) Close() error {
	a.log.Println("[INFO] Shutting down")
	errc := make(chan error)
	a.quit <- errc
	err := <-errc
	close(errc)
	close(a.quit)
	return err
}
