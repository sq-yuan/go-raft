package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/sq-yuan/go-raft/pkg/plugin"
	"github.com/sq-yuan/go-raft/pkg/raft"
)

// using goroutines to simulate multiple processes
func main() {
	nodeA := "nodeA"
	nodeB := "nodeB"
	nodeC := "nodeC"
	nodes := []string{nodeA, nodeB, nodeC}
	cmap := map[string]chan []byte{
		nodeA: make(chan []byte, 5),
		nodeB: make(chan []byte, 5),
		nodeC: make(chan []byte, 5),
	}

	netA := &Net{C: cmap[nodeA], CMap: cmap}
	netB := &Net{C: cmap[nodeB], CMap: cmap}
	netC := &Net{C: cmap[nodeC], CMap: cmap}

	ctx := context.Background()
	raftA, err := raft.NewRaft(ctx, raft.RaftConfig{
		LogFilePrefix: nodeA,
		CurrentNode:   nodeA,
		Nodes:         nodes,
	}, netA, log.Default())
	if err != nil {
		log.Fatal(err)
	}

	raftB, err := raft.NewRaft(ctx, raft.RaftConfig{
		LogFilePrefix: nodeB,
		CurrentNode:   nodeB,
		Nodes:         nodes,
	}, netB, log.Default())
	if err != nil {
		log.Fatal(err)
	}

	raftC, err := raft.NewRaft(ctx, raft.RaftConfig{
		LogFilePrefix: nodeC,
		CurrentNode:   nodeC,
		Nodes:         nodes,
	}, netC, log.Default())
	if err != nil {
		log.Fatal(err)
	}

	// create three applications
	appA := NewApp(nodeA, raftA)
	appB := NewApp(nodeB, raftB)
	appC := NewApp(nodeC, raftC)

	go appA.Run()
	go appB.Run()
	go appC.Run()

	raftA.Run()
	raftB.Run()
	raftC.Run()

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

type App struct {
	n     string
	b     raft.Raft
	msgId int

	running bool
}

func NewApp(nodeID string, b raft.Raft) *App {
	return &App{
		n:     nodeID,
		b:     b,
		msgId: 1,
	}
}

func (a *App) onMessage(lsn int, msg []byte) {
	fmt.Println(a.n, "|apply|", lsn, " - ", string(msg))
}

func (a *App) Run() {
	a.b.SetHandler(a.onMessage)
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
