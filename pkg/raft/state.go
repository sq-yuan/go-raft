package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"go-raft/pkg/plugin"
)

type RaftRole int

const (
	RaftRoleLeader    RaftRole = 1
	RaftRoleFollower  RaftRole = 2
	RaftRoleCandidate RaftRole = 3
)

type LogEntry struct {
	Payload []byte
	Term    int
}

type RaftState struct {
	CurrentTerm int
	VotedFor    string
	Logs        []LogEntry

	CommitLength  int
	CurrentRole   RaftRole
	CurrentLeader string

	VotesReceived map[string]bool

	SentLength  map[string]int
	AckedLength map[string]int

	LocalBuffer [][]byte
}

func NewRaftState() *RaftState {
	return &RaftState{
		CurrentTerm:   0,
		VotedFor:      "",
		CommitLength:  0,
		CurrentRole:   RaftRoleFollower,
		CurrentLeader: "",
		VotesReceived: map[string]bool{},
		SentLength:    map[string]int{},
		AckedLength:   map[string]int{},
		Logs:          []LogEntry{},
		LocalBuffer:   [][]byte{},
	}
}

func NewRestoredRaftState(ctx context.Context, store plugin.Storage) *RaftState {
	bytestate := store.RestoreState(ctx)
	decoder := gob.NewDecoder(bytes.NewReader(bytestate))
	var state RaftState
	decoder.Decode(&state)

	state.CurrentRole = RaftRoleFollower
	state.CurrentLeader = ""
	state.VotesReceived = map[string]bool{}
	state.SentLength = map[string]int{}
	state.AckedLength = map[string]int{}

	return &state
}

func (s *RaftState) Save(ctx context.Context, store plugin.Storage) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(s)

	store.SaveState(ctx, buf.Bytes())
}
