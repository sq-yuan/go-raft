package raft

type RaftConfig struct {
	LogFilePrefix string
	LogFilePath   string
	Nodes         []string
	CurrentNode   string
}
