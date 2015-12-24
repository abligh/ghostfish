package main

import (
	"flag"
	"github.com/abligh/ghostfish/app"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var raftDataDir = flag.String("raft_data_dir", "", "raft data store path")
var raftLogDir = flag.String("raft_log_dir", "", "raft log store path")
var raftAddr = flag.String("raft_addr", "", "raft listen addr, if empty, we will disable raft")
var raftCluster = flag.String("raft_cluster", "", "raft cluster, seperated by comma")
var raftClusterState = flag.String("raft_cluster_state", "", "new or existing, if new, we will deprecate old saved cluster and use new")

func main() {
	//app.RpcInMemoryPingTest()
	return

	flag.Parse()

	var c app.Config
	var err error

	c = app.NewConfig()

	if len(*raftAddr) > 0 {
		c.Raft.Addr = *raftAddr
	}

	if len(*raftDataDir) > 0 {
		c.Raft.DataDir = *raftDataDir
	}

	if len(*raftLogDir) > 0 {
		c.Raft.LogDir = *raftLogDir
	}

	members := strings.Split(*raftCluster, ",")
	if len(members) > 0 && len(members[0]) > 0 {
		c.Raft.Cluster = members
	}

	if len(*raftClusterState) > 0 {
		c.Raft.ClusterState = *raftClusterState
	}

	a, err := app.NewApp(c)
	if err != nil {
		log.Printf("[ERROR] new failover app error %v", err)
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		<-sc
		a.Close()
	}()

	a.Run()
}
