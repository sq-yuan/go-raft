package raft

type ClusterConfig struct {
	Nodes       []string
	CurrentNode string
}
