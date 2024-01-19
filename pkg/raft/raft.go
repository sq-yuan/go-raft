package raft

import (
	"context"
	"errors"
	"fmt"
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

type raft struct {
	cfg    ClusterConfig
	net    plugin.Networker
	logger *log.Logger

	state *RaftState

	logstore   LogStore
	statestore StateStore

	msgHandler MessageHandler

	ch chan event

	running bool

	electTimerMu sync.Mutex
	electTimer   *time.Timer

	replicateTicker *time.Ticker
}

func NewRaft(
	ctx context.Context,
	cfg ClusterConfig,
	net plugin.Networker,
	logger *log.Logger) (Raft, error) {
	logstore, err := NewLogStore(cfg.CurrentNode+"-log", logger)
	if err != nil {
		return nil, err
	}
	statestore, err := NewStateStore(cfg.CurrentNode + "-state")
	if err != nil {
		return nil, err
	}
	state := NewRestoredRaftState(ctx, cfg, statestore)
	if state == nil {
		return nil, err
	}
	return &raft{
		cfg:        cfg,
		net:        net,
		logger:     logger,
		state:      state,
		logstore:   logstore,
		statestore: statestore,
		ch:         make(chan event, 10),
		running:    false,
	}, nil
}

// callback for network receive
func (r *raft) netRecv(packet []byte) {
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

func (r *raft) Run() {
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

func (r *raft) Close() {
	r.ch <- event{}
	r.running = false
}

// Append a message to replication log
func (r *raft) Append(msg []byte) bool {
	if !r.running {
		return false
	}
	r.ch <- event{append: &appendevent{payload: msg}}
	return true
}

// Add a callback handler for state machine replication
func (r *raft) AddHandle(handler MessageHandler) {
	r.msgHandler = handler
}

// Replay the replication log from the specified LSN
func (r *raft) Replay(lsn int) error {
	if r.running {
		return errors.New("cannot replay while state machine is running")
	}
	if lsn < 0 || lsn >= r.logstore.TotalCount() {
		return fmt.Errorf("lsn out of range")
	}
	for _, log := range r.logstore.Entries(lsn, r.logstore.TotalCount()) {
		r.msgHandler(log.LogIndex, log.Payload)
	}
	return nil
}

// Truncate logs till the specified LSN
func (r *raft) Truncate(lsn int) error {
	panic("not implemented")
}

func (r *raft) log(fmt string, v ...any) {
	r.logger.Printf(r.cfg.CurrentNode+" | "+fmt+"\n", v...)
}

// send raft message to everyone
func (r *raft) broadcast(msg *RaftMessage) {
	bytes := Encode(msg)
	for _, n := range r.cfg.Nodes {
		if n != r.cfg.CurrentNode {
			r.net.Send(context.Background(), n, bytes)
		}
	}
}

// send raft message to recepient
func (r *raft) send(to string, msg *RaftMessage) error {
	return r.net.Send(context.Background(), to, Encode(msg))
}

// restart the election timer
func (r *raft) resetElectionTimer() {
	timeout := ElectionTimeoutVal + time.Duration(rand.Intn(2000)*int(time.Millisecond))
	// r.log("reset election timer %s", timeout)
	if r.electTimer == nil {
		r.electTimer = time.NewTimer(timeout)
	} else {
		r.electTimer.Reset(timeout)
	}
}

func (r *raft) cancelElectionTimer() {
	r.electTimer.Stop()
}

// persist the current state
func (r *raft) saveState() {
	r.state.Save(context.Background(), r.statestore)
}

// append messages
func (r *raft) appendMessages(payloads [][]byte) {
	if r.state.CurrentRole != RaftRoleLeader {
		r.log("appendMessages called while node is not leader")
		return
	}
	var logs []LogEntry
	for _, payload := range payloads {
		logs = append(logs, LogEntry{Term: r.state.CurrentTerm, Payload: payload})
	}
	err := r.logstore.Append(logs)
	if err != nil {
		r.log("failed to persist log, err=%s", err.Error())
	}
	r.state.AckedLength[r.cfg.CurrentNode] = r.logstore.TotalCount()
	r.replicateLogs()
}

// replicate log to the node
func (r *raft) replicateLog(node string) {
	if r.state.CurrentRole != RaftRoleLeader {
		r.log("error: replicateLog called while node is not leader")
		return
	}
	prefixLen := r.state.SentLength[node]
	var surfix []LogItem
	for i := prefixLen; i < r.logstore.TotalCount(); i++ {
		log := r.logstore.Entry(i)
		surfix = append(surfix, LogItem{
			Term:    log.Term,
			Payload: log.Payload,
		})
	}
	prefixTerm := 0
	if prefixLen > 0 {
		prefixTerm = r.logstore.Entry(prefixLen - 1).Term
	}
	r.log("replicate logs")
	r.send(node, &RaftMessage{
		LogReq: &LogRequest{
			LeaderID:   r.cfg.CurrentNode,
			Term:       r.state.CurrentTerm,
			PerfixLen:  prefixLen,
			PrefixTerm: prefixTerm,
			CommitLen:  r.state.PersistedState.CommitedIndex,
			Surfix:     surfix,
		},
	})
}

// replicate logs to all followers
func (r *raft) replicateLogs() {
	for _, node := range r.cfg.Nodes {
		if node != r.cfg.CurrentNode {
			r.replicateLog(node)
		}
	}
}

// append log entries to local log
func (r *raft) appendLogEntries(prefixLen, leaderCommit int, suffix []LogItem) {
	if len(suffix) > 0 {
		var logs []LogEntry
		for i := 0; i < int(len(suffix)); i++ {
			logs = append(logs, LogEntry{
				Term:    suffix[i].Term,
				Payload: suffix[i].Payload,
			})
		}
		err := r.logstore.RewindAndAppend(prefixLen, logs)
		if err != nil {
			r.log("failed to persist, err=%s", err.Error())
		}
	}
	if leaderCommit > r.state.CommitedIndex {
		for i := r.state.CommitedIndex; i < leaderCommit; i++ {
			log := r.logstore.Entry(i)
			r.msgHandler(log.LogIndex, log.Payload)
		}
		r.state.CommitedIndex = leaderCommit
		r.saveState()
	}
}

// tries to commit log entries
func (r *raft) tryCommitLog() {
	var commitLens []int
	for _, cl := range r.state.AckedLength {
		commitLens = append(commitLens, cl)
	}
	sort.Ints(commitLens)
	idx := len(commitLens) - (len(commitLens)+1)/2

	newCommitLen := commitLens[idx]
	if newCommitLen > r.state.CommitedIndex {
		for i := r.state.CommitedIndex; i < newCommitLen; i++ {
			log := r.logstore.Entry(i)
			r.msgHandler(log.LogIndex, log.Payload)
		}
		r.state.CommitedIndex = newCommitLen
		r.saveState()
		r.replicateLogs()
	}
}

// append and flush local buffers to replication log
func (r *raft) appendAndFlushBuffers(payload []byte) {
	prelen := r.logstore.TotalCount()
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
func (r *raft) runForLeader() {
	r.state.CurrentTerm++
	r.state.CurrentRole = RaftRoleCandidate
	r.state.VotedFor = r.cfg.CurrentNode
	r.state.VotesReceived[r.cfg.CurrentNode] = true
	lastTerm := 0
	if r.logstore.TotalCount() > 0 {
		lastTerm = r.logstore.Back().Term
	}

	r.log("try to run for leader term=%d", r.state.CurrentTerm)
	r.saveState()
	r.broadcast(&RaftMessage{
		VoteReq: &VoteRequest{
			NodeID:    r.cfg.CurrentNode,
			Term:      r.state.CurrentTerm,
			LogLength: r.logstore.TotalCount(),
			LastTerm:  lastTerm,
		},
	})
	r.resetElectionTimer()
}

// receives AppendRequest message
func (r *raft) onAppendRequest(msg *AppendRequest) {
	if r.state.CurrentRole == RaftRoleLeader {
		r.appendMessages(msg.Payloads)
	}
}

// receives VoteRequest message
func (r *raft) onVoteRequest(msg *VoteRequest) {
	if msg == nil {
		return
	}
	if msg.Term > r.state.CurrentTerm {
		r.state.CurrentTerm = msg.Term
		r.state.CurrentRole = RaftRoleFollower
		r.state.VotedFor = ""
	}
	lastTerm := 0
	if r.logstore.TotalCount() > 0 {
		lastTerm = r.logstore.Back().Term
	}
	logOk := msg.LastTerm > lastTerm ||
		(msg.LastTerm == lastTerm && msg.LogLength >= r.logstore.TotalCount())
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
func (r *raft) onVoteResponse(msg *VoteResponse) {
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
func (r *raft) onLogRequest(msg *LogRequest) {
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
	logOk := r.logstore.TotalCount() >= int(msg.PerfixLen) &&
		(msg.PerfixLen == 0 || r.logstore.Entry(msg.PerfixLen-1).Term == msg.PrefixTerm)
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
		if msg.PerfixLen > r.logstore.TotalCount() {
			gapLen = msg.PerfixLen - r.logstore.TotalCount()
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
func (r *raft) onLogResponse(msg *LogResponse) {
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
