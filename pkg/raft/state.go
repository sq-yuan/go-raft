package raft

import (
	"bytes"
	"context"
	"encoding/gob"
)

type RaftRole int

const (
	RaftRoleLeader    RaftRole = 1
	RaftRoleFollower  RaftRole = 2
	RaftRoleCandidate RaftRole = 3
)

type PersistedState struct {
	CurrentTerm   int
	VotedFor      string
	CommitedIndex int
	// local buffer
	LocalBuffer [][]byte
}

type RaftState struct {
	PersistedState

	// Volatile states
	CurrentRole   RaftRole
	CurrentLeader string

	VotesReceived map[string]bool

	SentLength  map[string]int
	AckedLength map[string]int
}

func NewRestoredRaftState(ctx context.Context, cfg RaftConfig, store StateStore) *RaftState {
	state := &RaftState{
		PersistedState: PersistedState{
			CurrentTerm:   0,
			VotedFor:      "",
			CommitedIndex: 0,
			LocalBuffer:   [][]byte{},
		},
		CurrentRole:   RaftRoleFollower,
		CurrentLeader: "",
		VotesReceived: map[string]bool{},
		SentLength:    map[string]int{},
		AckedLength:   map[string]int{},
	}
	state.Restore(ctx, store)
	for _, node := range cfg.Nodes {
		state.SentLength[node] = state.CommitedIndex
		state.VotesReceived[node] = false
	}
	return state
}

func (s *RaftState) Restore(ctx context.Context, store StateStore) error {
	data, err := store.Restore(ctx)
	if err != nil {
		return err
	}
	decoder := gob.NewDecoder(bytes.NewReader(data))
	var pstate PersistedState
	decoder.Decode(&pstate)
	s.PersistedState = pstate
	return nil
}

func (s *RaftState) Save(ctx context.Context, store StateStore) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(s.PersistedState)

	return store.Save(ctx, buf.Bytes())
}
