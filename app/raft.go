package app

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"net"
	"os"
	"path"
	"time"
)

func (fsm *masterFSM) Apply(l *raft.Log) interface{} {
	var a action

	if err := json.Unmarshal(l.Data, &a); err != nil {
		log.Printf("[ERROR] Cannot unmarshal raft action:  %v", err)
		return err
	}

	fsm.handleAction(&a)

	return nil
}

func (fsm *masterFSM) Snapshot() (raft.FSMSnapshot, error) {
	snap := new(masterSnapshot)
	snap.data = make(map[string]interface{})

	fsm.Lock()
	for k, v := range fsm.data {
		snap.data[k] = v
	}
	fsm.Unlock()
	return snap, nil
}

func (fsm *masterFSM) Restore(snap io.ReadCloser) error {
	defer snap.Close()

	d := json.NewDecoder(snap)
	var data map[string]interface{}

	if err := d.Decode(&data); err != nil {
		return err
	}

	fsm.Lock()
	fsm.data = make(map[string]interface{})
	for k, v := range data {
		fsm.data[k] = v
	}
	fsm.Unlock()

	return nil
}

type masterSnapshot struct {
	data map[string]interface{}
}

func (snap *masterSnapshot) Persist(sink raft.SnapshotSink) error {
	data, _ := json.Marshal(snap.data)
	_, err := sink.Write(data)
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (snap *masterSnapshot) Release() {

}

type Raft struct {
	r *raft.Raft

	log       *os.File
	dbStore   *raftboltdb.BoltStore
	trans     *raft.NetworkTransport
	peerStore *raft.JSONPeers

	raftAddr string
}

func newRaft(a *App) (Cluster, error) {
	r := new(Raft)

	if len(a.config.Raft.Addr) == 0 {
		return nil, nil
	}

	peers := make([]string, 0, len(a.config.Raft.Cluster))

	r.raftAddr = a.config.Raft.Addr

	addr, err := net.ResolveTCPAddr("tcp", r.raftAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid raft addr format %s, must host:port, err:%v", r.raftAddr, err)
	}

	peers = raft.AddUniquePeer(peers, addr.String())

	for _, cluster := range a.config.Raft.Cluster {
		addr, err = net.ResolveTCPAddr("tcp", cluster)
		if err != nil {
			return nil, fmt.Errorf("invalid cluster format %s, must host:port, err:%v", cluster, err)
		}

		peers = raft.AddUniquePeer(peers, addr.String())
	}

	os.MkdirAll(a.config.Raft.DataDir, 0755)

	cfg := raft.DefaultConfig()

	if len(a.config.Raft.LogDir) == 0 {
		r.log = os.Stdout
	} else {
		os.MkdirAll(a.config.Raft.LogDir, 0755)
		logFile := path.Join(a.config.Raft.LogDir, "raft.log")
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		r.log = f

		cfg.LogOutput = r.log
	}

	raftDBPath := path.Join(a.config.Raft.DataDir, "raft_db")
	r.dbStore, err = raftboltdb.NewBoltStore(raftDBPath)
	if err != nil {
		return nil, err
	}

	fileStore, err := raft.NewFileSnapshotStore(a.config.Raft.DataDir, 1, r.log)
	if err != nil {
		return nil, err
	}

	r.trans, err = raft.NewTCPTransport(r.raftAddr, nil, 3, 5*time.Second, r.log)
	if err != nil {
		return nil, err
	}

	r.peerStore = raft.NewJSONPeers(a.config.Raft.DataDir, r.trans)

	if a.config.Raft.ClusterState == ClusterStateNew {
		log.Printf("[INFO] cluster state is new, use new cluster config")
		r.peerStore.SetPeers(peers)
	} else {
		log.Printf("[INFO] cluster state is existing, use previous + new cluster config")
		ps, err := r.peerStore.Peers()
		if err != nil {
			log.Printf("[INFO] get store peers error %v", err)
			return nil, err
		}

		for _, peer := range peers {
			ps = raft.AddUniquePeer(ps, peer)
		}

		r.peerStore.SetPeers(ps)
	}

	if peers, _ := r.peerStore.Peers(); len(peers) <= 1 {
		cfg.EnableSingleNode = true
		log.Println("[INFO] raft will run in single node mode, may only be used in test")
	}

	r.r, err = raft.NewRaft(cfg, a.fsm, r.dbStore, r.dbStore, fileStore, r.peerStore, r.trans)

	return r, err
}

func (r *Raft) Close() {
	if r.trans != nil {
		r.trans.Close()
	}

	if r.r != nil {
		future := r.r.Shutdown()
		if err := future.Error(); err != nil {
			log.Printf("[ERROR] Error shutting down raft: %v", err)
		}
	}

	if r.dbStore != nil {
		r.dbStore.Close()
	}

	if r.log != os.Stdout {
		r.log.Close()
	}
}

func (r *Raft) Command(cmd string, params interface{}, timeout time.Duration) error {
	var a = action{
		Cmd:    cmd,
		Params: params,
	}

	return r.apply(&a, timeout)
}

func (r *Raft) AddPeer(addr string) error {
	peer, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	f := r.r.AddPeer(peer.String())
	return f.Error()
}

func (r *Raft) DelPeer(addr string) error {
	peer, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	f := r.r.RemovePeer(peer.String())
	return f.Error()

}

func (r *Raft) SetPeers(addrs []string) error {
	peers := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		peer, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return err
		}
		peers = append(peers, peer.String())
	}

	f := r.r.SetPeers(peers)
	return f.Error()

}

func (r *Raft) GetPeers() ([]string, error) {
	peers, err := r.peerStore.Peers()
	if err != nil {
		return nil, err
	}

	addrs := make([]string, 0, len(peers))

	for _, peer := range peers {
		addrs = append(addrs, peer)
	}

	return addrs, nil
}

func (r *Raft) apply(a *action, timeout time.Duration) error {
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}

	f := r.r.Apply(data, timeout)
	return f.Error()
}

func (r *Raft) LeaderCh() <-chan bool {
	return r.r.LeaderCh()
}

func (r *Raft) IsLeader() bool {
	addr := r.r.Leader()
	return addr == r.raftAddr
}

func (r *Raft) Leader() string {
	return r.r.Leader()
}

func (r *Raft) Barrier(timeout time.Duration) error {
	f := r.r.Barrier(timeout)
	return f.Error()
}
