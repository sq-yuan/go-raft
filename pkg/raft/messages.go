package raft

import (
	"bytes"
	"encoding/gob"
)

type VoteRequest struct {
	NodeID    string
	Term      int
	LogLength int
	LastTerm  int
}

type VoteResponse struct {
	NodeID string
	Term   int
	Vote   bool
}

type AppendRequest struct {
	Payloads [][]byte
}

type LogItem struct {
	Term    int
	Payload []byte
}

type LogRequest struct {
	LeaderID   string
	Term       int
	PerfixLen  int
	PrefixTerm int
	CommitLen  int
	Surfix     []LogItem
}

type LogResponse struct {
	NodeID    string
	Term      int
	AckLength int
	OK        bool
	GapLength int
}

type RaftMessage struct {
	VoteReq   *VoteRequest
	VoteResp  *VoteResponse
	AppendReq *AppendRequest
	LogReq    *LogRequest
	LogResp   *LogResponse
}

func Encode(msg *RaftMessage) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(msg)
	return buf.Bytes()
}

func Decode(buf []byte) *RaftMessage {
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	var msg RaftMessage
	decoder.Decode(&msg)
	return &msg
}
