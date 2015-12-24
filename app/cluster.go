package app

import (
	"sync"
	"time"
)

type Cluster interface {
	Close()
	Command(cmd string, params interface{}, timeout time.Duration) error
	Barrier(timeout time.Duration) error
	IsLeader() bool
	LeaderCh() <-chan bool
}

type masterFSM struct {
	sync.Mutex
	data map[string]interface{}
	app  *App
}

func (a *App) newMasterFSM() *masterFSM {
	fsm := new(masterFSM)
	fsm.data = make(map[string]interface{})
	fsm.app = a
	return fsm
}

func (fsm *masterFSM) SetKey(params interface{}) {
	var ok bool
	var mapparam map[string]interface{}
	if mapparam, ok = params.(map[string]interface{}); !ok {
		fsm.app.log.Println("[ERROR] Bad parameters to add")
		return
	}
	var keyname string
	if keyname, ok = mapparam["key"].(string); !ok || len(keyname) == 0 {
		fsm.app.log.Println("[ERROR] Bad key for add")
		return
	}
	fsm.Lock()
	defer fsm.Unlock()

	if _, ok := mapparam["value"]; !ok {
		delete(fsm.data, keyname)
	} else {
		fsm.data[keyname] = mapparam["value"]
	}
}

const (
	setCmd = "set"
)

type action struct {
	Cmd    string      `json:"cmd"`
	Params interface{} `json:"params"`
}

func (fsm *masterFSM) handleAction(a *action) {
	switch a.Cmd {
	case setCmd:
		fsm.SetKey(a.Params)
	default:
		fsm.app.log.Println("[ERROR] Bad command")
	}
}
