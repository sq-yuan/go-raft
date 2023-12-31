package raft

import (
	"context"
	"go-raft/pkg/broadcast"
	"go-raft/pkg/config"
	"go-raft/pkg/plugin"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	ElectionTimeoutVal      time.Duration = 5 * time.Second
	ReplicationTickInterval time.Duration = 2 * time.Second
)

type TimeoutType int

const (
	ElectionTimeout TimeoutType = 1
)

type timeoutevent struct {
	Type TimeoutType
}

type appendevent struct {
	payload []byte
}

type replicateTick struct {
}

type event struct {
	net      *RaftMessage
	timeout  *timeoutevent
	append   *appendevent
	replTick *replicateTick
}

type Raft struct {
	cfg    config.ClusterConfig
	net    plugin.Networker
	store  plugin.Storage
	logger *log.Logger

	state *RaftState

	msgHandler broadcast.MessageHandler

	ch chan event

	running bool

	electTimerMu sync.Mutex
	electTimer   *time.Timer

	replicateTicker *time.Ticker
}

func NewRaft(
	ctx context.Context,
	cfg config.ClusterConfig,
	net plugin.Networker,
	store plugin.Storage,
	logger *log.Logger) *Raft {
	state := NewRestoredRaftState(ctx, store)
	if state == nil {
		panic("failed to restore from state")
	}
	return &Raft{
		cfg:     cfg,
		net:     net,
		store:   store,
		logger:  logger,
		state:   state,
		ch:      make(chan event, 10),
		running: false,
	}
}

// callback for network receive
func (r *Raft) netRecv(packet []byte) {
	if !r.running {
		return
	}
	msg := Decode(packet)
	if msg == nil {
		r.log("failed to decode network packets")
	}
	r.ch <- event{
		net: msg,
	}
}

func (r *Raft) Run() {
	r.running = true
	r.net.AddRecvCallback(r.netRecv)

	if r.replicateTicker != nil {
		r.replicateTicker.Reset(ReplicationTickInterval)
	} else {
		r.replicateTicker = time.NewTicker(ReplicationTickInterval)
	}

	// start goroutine to capture ticker event
	go func() {
		for range r.replicateTicker.C {
			r.ch <- event{replTick: &replicateTick{}}
			if !r.running {
				break
			}
		}
	}()

	r.resetElectionTimer()

	// start goroutine to capture election timeout event
	go func() {
		for range r.electTimer.C {
			r.ch <- event{timeout: &timeoutevent{Type: ElectionTimeout}}
			if !r.running {
				break
			}
		}
	}()

	for evt := range r.ch {
		// network events
		if evt.net != nil {
			switch {
			case evt.net.VoteReq != nil:
				r.onVoteRequest(evt.net.VoteReq)
			case evt.net.VoteResp != nil:
				r.onVoteResponse(evt.net.VoteResp)
			case evt.net.LogReq != nil:
				r.onLogRequest(evt.net.LogReq)
			case evt.net.LogResp != nil:
				r.onLogResponse(evt.net.LogResp)
			case evt.net.AppendReq != nil:
				r.onAppendRequest(evt.net.AppendReq)
			}
		}

		// timeout events
		if evt.timeout != nil {
			if evt.timeout.Type == ElectionTimeout {
				r.runForLeader()
			}
		}

		// append event
		if evt.append != nil {
			r.appendAndFlushBuffers(evt.append.payload)
		}

		// replication tick
		if evt.replTick != nil {
			if r.state.CurrentRole == RaftRoleLeader {
				r.replicateLogs()
			}
		}

		// shutdown
		if !r.running {
			break
		}
	}
}

func (r *Raft) Shutdown() {
	r.ch <- event{}
	r.running = false
}

// Append a message to replication log
func (r *Raft) Append(msg []byte) bool {
	if !r.running {
		return false
	}
	r.ch <- event{append: &appendevent{payload: msg}}
	return true
}

// Add a callback handler for state machine replication
func (r *Raft) AddHandle(handler broadcast.MessageHandler) {
	r.msgHandler = handler
}

func (r *Raft) log(fmt string, v ...any) {
	r.logger.Printf(r.cfg.CurrentNode+" | "+fmt+"\n", v...)
}

// send raft message to everyone
func (r *Raft) broadcast(msg *RaftMessage) {
	bytes := Encode(msg)
	for _, n := range r.cfg.Nodes {
		if n != r.cfg.CurrentNode {
			r.net.Send(context.Background(), n, bytes)
		}
	}
}

// send raft message to recepient
func (r *Raft) send(to string, msg *RaftMessage) error {
	return r.net.Send(context.Background(), to, Encode(msg))
}

// restart the election timer
func (r *Raft) resetElectionTimer() {
	timeout := ElectionTimeoutVal + time.Duration(rand.Intn(2000)*int(time.Millisecond))
	// r.log("reset election timer %s", timeout)
	if r.electTimer == nil {
		r.electTimer = time.NewTimer(timeout)
	} else {
		r.electTimer.Reset(timeout)
	}
}

func (r *Raft) cancelElectionTimer() {
	r.electTimer.Stop()
}

// persist the current state
func (r *Raft) saveState() {
	r.state.Save(context.Background(), r.store)
}

// append messages
func (r *Raft) appendMessages(payloads [][]byte) {
	if r.state.CurrentRole != RaftRoleLeader {
		r.log("appendMessages called while node is not leader")
		return
	}
	for _, payload := range payloads {
		r.state.Logs = append(r.state.Logs,
			LogEntry{Term: r.state.CurrentTerm, Payload: payload})
		r.state.AckedLength[r.cfg.CurrentNode] = len(r.state.Logs)
	}
	r.replicateLogs()
}

// replicate log to the node
func (r *Raft) replicateLog(node string) {
	if r.state.CurrentRole != RaftRoleLeader {
		r.log("error: replicateLog called while node is not leader")
		return
	}
	prefixLen := r.state.SentLength[node]
	var surfix []LogItem
	for i := prefixLen; i < len(r.state.Logs); i++ {
		surfix = append(surfix, LogItem{
			Term:    r.state.Logs[i].Term,
			Payload: r.state.Logs[i].Payload,
		})
	}
	prefixTerm := 0
	if prefixLen > 0 {
		prefixTerm = r.state.Logs[prefixLen-1].Term
	}
	r.log("replicate logs")
	r.send(node, &RaftMessage{
		LogReq: &LogRequest{
			LeaderID:   r.cfg.CurrentNode,
			Term:       r.state.CurrentTerm,
			PerfixLen:  prefixLen,
			PrefixTerm: prefixTerm,
			CommitLen:  r.state.CommitLength,
			Surfix:     surfix,
		},
	})
}

// replicate logs to all followers
func (r *Raft) replicateLogs() {
	for _, node := range r.cfg.Nodes {
		if node != r.cfg.CurrentNode {
			r.replicateLog(node)
		}
	}
}

// append log entries to local log
func (r *Raft) appendLogEntries(prefixLen, leaderCommit int, suffix []LogItem) {
	changed := false
	suffixIdx := 0
	if len(suffix) > 0 {
		if len(r.state.Logs) > int(prefixLen) {
			if (prefixLen + len(suffix)) < len(r.state.Logs) {
				r.state.Logs = r.state.Logs[0 : prefixLen+len(suffix)]
			}
			for i := prefixLen; i < len(r.state.Logs); i++ {
				r.state.Logs[i].Term = suffix[suffixIdx].Term
				r.state.Logs[i].Payload = suffix[suffixIdx].Payload
				suffixIdx++
			}
		}
		for i := suffixIdx; i < int(len(suffix)); i++ {
			r.state.Logs = append(r.state.Logs, LogEntry{
				Term:    suffix[i].Term,
				Payload: suffix[i].Payload,
			})
		}
		changed = true
	}
	if leaderCommit > r.state.CommitLength {
		for i := r.state.CommitLength; i < leaderCommit; i++ {
			r.msgHandler(r.state.Logs[i].Payload)
		}
		r.state.CommitLength = leaderCommit
		changed = true
	}
	if changed {
		r.saveState()
	}
}

// tries to commit log entries
func (r *Raft) tryCommitLog() {
	var commitLens []int
	for _, cl := range r.state.AckedLength {
		commitLens = append(commitLens, cl)
	}
	sort.Ints(commitLens)
	idx := len(commitLens) - (len(commitLens)+1)/2

	newCommitLen := commitLens[idx]
	if newCommitLen > r.state.CommitLength {
		for i := r.state.CommitLength; i < newCommitLen; i++ {
			r.msgHandler(r.state.Logs[i].Payload)
		}
		r.state.CommitLength = newCommitLen
		r.saveState()
		r.replicateLogs()
	}
}

// append and flush local buffers to replication log
func (r *Raft) appendAndFlushBuffers(payload []byte) {
	prelen := len(r.state.Logs)
	if payload != nil {
		r.state.LocalBuffer = append(r.state.LocalBuffer, payload)
	}
	if len(r.state.LocalBuffer) == 0 {
		return
	}
	if r.state.CurrentRole == RaftRoleLeader {
		r.appendMessages(r.state.LocalBuffer)
		r.state.LocalBuffer = [][]byte{}
	} else if r.state.CurrentLeader != "" {
		// forward to leader
		// Note: this is not FIFO send, so we are only doing total-order-broadcast
		// Needs to make this FIFO send to acheive FIFO total-order-broadcast
		err := r.send(r.state.CurrentLeader, &RaftMessage{
			AppendReq: &AppendRequest{Payloads: r.state.LocalBuffer}})
		if err == nil {
			r.state.LocalBuffer = [][]byte{}
		}
	} else {
		// if there's no leader, messages will stay in local buffer
	}

	if prelen != len(r.state.LocalBuffer) {
		r.saveState()
	}
}

// tries to run for the new leader
func (r *Raft) runForLeader() {
	r.state.CurrentTerm++
	r.state.CurrentRole = RaftRoleCandidate
	r.state.VotedFor = r.cfg.CurrentNode
	r.state.VotesReceived[r.cfg.CurrentNode] = true
	lastTerm := 0
	if len(r.state.Logs) > 0 {
		lastTerm = r.state.Logs[len(r.state.Logs)-1].Term
	}

	r.log("try to run for leader term=%d", r.state.CurrentTerm)
	r.saveState()
	r.broadcast(&RaftMessage{
		VoteReq: &VoteRequest{
			NodeID:    r.cfg.CurrentNode,
			Term:      r.state.CurrentTerm,
			LogLength: len(r.state.Logs),
			LastTerm:  lastTerm,
		},
	})
	r.resetElectionTimer()
}

// receives AppendRequest message
func (r *Raft) onAppendRequest(msg *AppendRequest) {
	if r.state.CurrentRole == RaftRoleLeader {
		r.appendMessages(msg.Payloads)
	}
}

// receives VoteRequest message
func (r *Raft) onVoteRequest(msg *VoteRequest) {
	if msg == nil {
		return
	}
	if msg.Term > r.state.CurrentTerm {
		r.state.CurrentTerm = msg.Term
		r.state.CurrentRole = RaftRoleFollower
		r.state.VotedFor = ""
	}
	lastTerm := 0
	if len(r.state.Logs) > 0 {
		lastTerm = r.state.Logs[len(r.state.Logs)-1].Term
	}
	logOk := msg.LastTerm > lastTerm ||
		(msg.LastTerm == lastTerm && msg.LogLength >= len(r.state.Logs))
	response := &VoteResponse{
		NodeID: r.cfg.CurrentNode,
		Term:   r.state.CurrentTerm,
	}
	if r.state.CurrentTerm == msg.Term && logOk &&
		(r.state.VotedFor == "" || r.state.VotedFor == msg.NodeID) {
		r.log("vote for %s, term=%d", msg.NodeID, r.state.CurrentTerm)
		r.state.VotedFor = msg.NodeID
		response.Vote = true
	} else {
		r.log("reject %s, term=%d, already voted for %s", msg.NodeID, r.state.CurrentTerm, r.state.VotedFor)
		response.Vote = false
	}

	r.saveState()
	r.send(msg.NodeID, &RaftMessage{VoteResp: response})
}

// receives VoteResponse message
func (r *Raft) onVoteResponse(msg *VoteResponse) {
	if msg == nil {
		return
	}
	if r.state.CurrentRole == RaftRoleCandidate && msg.Term == r.state.CurrentTerm && msg.Vote {
		r.state.VotesReceived[msg.NodeID] = true
		if len(r.state.VotesReceived) >= (len(r.cfg.Nodes)+1)/2 { // receives majority vote
			r.state.CurrentRole = RaftRoleLeader
			r.state.CurrentLeader = r.cfg.CurrentNode
			r.log("majority vote received, current node will be the leader for term %d", r.state.CurrentTerm)
			r.cancelElectionTimer()
			r.appendAndFlushBuffers(nil)
			r.replicateLogs()
		}
	} else if msg.Term > r.state.CurrentTerm {
		r.state.CurrentTerm = msg.Term
		r.state.CurrentRole = RaftRoleFollower
		r.state.VotedFor = ""
		r.resetElectionTimer()
	}
}

// receives LogRequest message
func (r *Raft) onLogRequest(msg *LogRequest) {
	if msg == nil {
		return
	}
	if msg.Term > r.state.CurrentTerm {
		r.state.CurrentTerm = msg.Term
		r.state.VotedFor = ""
	}
	if msg.Term == r.state.CurrentTerm {
		if r.state.CurrentLeader != msg.LeaderID {
			r.state.CurrentRole = RaftRoleFollower
			r.state.CurrentLeader = msg.LeaderID
			r.state.VotedFor = ""
			r.log("%s is the new leader for term %d", msg.LeaderID, msg.Term)
		}

		r.resetElectionTimer()
		// A new leader has been elected, try to flush local buffer
		r.appendAndFlushBuffers(nil)
	}
	logOk := len(r.state.Logs) >= int(msg.PerfixLen) &&
		(msg.PerfixLen == 0 || r.state.Logs[msg.PerfixLen-1].Term == msg.PrefixTerm)
	if msg.Term == r.state.CurrentTerm && logOk {
		r.appendLogEntries(msg.PerfixLen, msg.CommitLen, msg.Surfix)
		r.saveState()
		r.send(msg.LeaderID, &RaftMessage{
			LogResp: &LogResponse{
				NodeID:    r.cfg.CurrentNode,
				Term:      r.state.CurrentTerm,
				AckLength: msg.PerfixLen + len(msg.Surfix),
				OK:        true,
			},
		})
	} else {
		// If we're missing some logs then ask leader to send the gap
		// Otherwise, we have some stale logs, rollback one by one
		gapLen := 1
		if msg.PerfixLen > len(r.state.Logs) {
			gapLen = msg.PerfixLen - len(r.state.Logs)
		}
		r.send(msg.LeaderID, &RaftMessage{
			LogResp: &LogResponse{
				NodeID: r.cfg.CurrentNode,
				Term:   r.state.CurrentTerm,
				OK:     false,
				// Tell the leader how far are we behind
				GapLength: gapLen,
			},
		})
	}
}

// receives LogResponse message
func (r *Raft) onLogResponse(msg *LogResponse) {
	if msg == nil {
		return
	}
	if msg.Term == r.state.CurrentTerm && r.state.CurrentRole == RaftRoleLeader {
		if msg.OK && msg.AckLength > r.state.AckedLength[msg.NodeID] {
			r.state.SentLength[msg.NodeID] = msg.AckLength
			r.state.AckedLength[msg.NodeID] = msg.AckLength
			r.tryCommitLog()
		} else if msg.GapLength > 0 && r.state.SentLength[msg.NodeID] > 0 {
			r.state.SentLength[msg.NodeID] -= msg.GapLength
			r.replicateLog(msg.NodeID)
		}
	} else if msg.Term > r.state.CurrentTerm {
		r.state.CurrentTerm = msg.Term
		r.state.CurrentRole = RaftRoleFollower
		r.state.VotedFor = ""
		r.resetElectionTimer()
	}
}
