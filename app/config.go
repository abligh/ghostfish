package app

import ()

type Config struct {
	Raft RaftConfig
}

const (
	ClusterStateNew      = "new"
	ClusterStateExisting = "existing"
)

type RaftConfig struct {
	Addr         string
	DataDir      string
	LogDir       string
	Cluster      []string
	ClusterState string
}

func NewConfig() Config {
	var c Config
	return c
}
