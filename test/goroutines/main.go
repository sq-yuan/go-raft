package main

import (
	"context"
	"errors"
	"fmt"
	"go-raft/pkg/broadcast"
	"go-raft/pkg/config"
	"go-raft/pkg/plugin"
	"go-raft/pkg/raft"
	"log"
	"math/rand"
	"time"
)

// using goroutines to simulate multiple processes
func main() {
	nodeA := "nodeA"
	nodeB := "nodeB"
	nodeC := "nodeC"
	nodes := []string{nodeA, nodeB, nodeC}
	cmap := map[string]chan []byte{
		nodeA: make(chan []byte),
		nodeB: make(chan []byte),
		nodeC: make(chan []byte),
	}

	netA := &Net{C: cmap[nodeA], CMap: cmap}
	netB := &Net{C: cmap[nodeB], CMap: cmap}
	netC := &Net{C: cmap[nodeC], CMap: cmap}

	ctx := context.Background()
	raftA := raft.NewRaft(ctx, config.ClusterConfig{
		CurrentNode: nodeA,
		Nodes:       nodes,
	}, netA, &Store{}, log.Default())

	raftB := raft.NewRaft(ctx, config.ClusterConfig{
		CurrentNode: nodeB,
		Nodes:       nodes,
	}, netB, &Store{}, log.Default())

	raftC := raft.NewRaft(ctx, config.ClusterConfig{
		CurrentNode: nodeC,
		Nodes:       nodes,
	}, netC, &Store{}, log.Default())

	// create three applications
	appA := NewApp(nodeA, raftA)
	appB := NewApp(nodeB, raftB)
	appC := NewApp(nodeC, raftC)

	go appA.Run()
	go appB.Run()
	go appC.Run()

	go raftA.Run()
	go raftB.Run()
	go raftC.Run()

	for {
	}
}

type Net struct {
	C    chan []byte
	CMap map[string]chan []byte

	running bool
}

func (n *Net) Send(ctx context.Context, nodeId string, packet []byte) error {
	if c, ok := n.CMap[nodeId]; ok {
		// simulate network delay
		time.Sleep(time.Duration(rand.Intn(100) * int(time.Millisecond)))
		c <- packet
		return nil
	} else {
		return errors.New("Node not found")
	}
}

func (n *Net) AddRecvCallback(callback plugin.RecvCallback) {
	n.running = true
	go func() {
		for m := range n.C {
			callback(m)
			if !n.running {
				break
			}
		}
	}()
}

func (n *Net) Stop() {
	n.running = false
}

type Store struct {
}

func (s *Store) SaveState(ctx context.Context, state []byte) {
}

func (s *Store) RestoreState(ctx context.Context) []byte {
	return nil
}

type App struct {
	n     string
	b     broadcast.Broadcaster
	msgId int

	running bool
}

func NewApp(nodeID string, b broadcast.Broadcaster) *App {
	return &App{
		n:     nodeID,
		b:     b,
		msgId: 1,
	}
}

func (a *App) onMessage(msg []byte) {
	fmt.Println(a.n, "|apply|", string(msg))
}

func (a *App) Run() {
	a.b.AddHandle(a.onMessage)
	a.running = true

	for a.running {
		// each App will try to append a message randomly
		// the messages getting applied at each App will follow the same order
		// thus acheiving total order broadcast
		time.Sleep(time.Duration(rand.Intn(10) * int(time.Second)))
		a.b.Append([]byte(fmt.Sprintf("message %s-%d", a.n, a.msgId)))
		a.msgId++
	}
}

func (a *App) Stop() {
	a.running = false
}
