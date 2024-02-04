package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/sq-yuan/go-raft/pkg/memkv"
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

	logger := log.New(io.Discard, "", 0)
	//logger := log.Default()

	netA := &Net{C: cmap[nodeA], CMap: cmap}
	netB := &Net{C: cmap[nodeB], CMap: cmap}
	netC := &Net{C: cmap[nodeC], CMap: cmap}

	ctx := context.Background()
	raftA, err := raft.NewRaft(ctx, raft.RaftConfig{
		LogFilePrefix: nodeA,
		CurrentNode:   nodeA,
		Nodes:         nodes,
	}, netA, logger)
	if err != nil {
		log.Fatal(err)
	}

	raftB, err := raft.NewRaft(ctx, raft.RaftConfig{
		LogFilePrefix: nodeB,
		CurrentNode:   nodeB,
		Nodes:         nodes,
	}, netB, logger)
	if err != nil {
		log.Fatal(err)
	}

	raftC, err := raft.NewRaft(ctx, raft.RaftConfig{
		LogFilePrefix: nodeC,
		CurrentNode:   nodeC,
		Nodes:         nodes,
	}, netC, logger)
	if err != nil {
		log.Fatal(err)
	}

	kvA := memkv.NewMemKVStore(memkv.StoreConfig{DatafilePrefix: "replica-A"}, raftA, log.New(os.Stdout, "replica-A: ", 0))
	kvB := memkv.NewMemKVStore(memkv.StoreConfig{DatafilePrefix: "replica-B"}, raftB, log.New(os.Stdout, "replica-B: ", 0))
	kvC := memkv.NewMemKVStore(memkv.StoreConfig{DatafilePrefix: "replica-C"}, raftC, log.New(os.Stdout, "replica-C: ", 0))

	kvA.Start()
	kvB.Start()
	kvC.Start()

	replicas := []*memkv.MemKvStore{kvA, kvB, kvC}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">")
		input, _ := reader.ReadString('\n')
		if len(input) == 0 {
			continue
		}
		input = strings.TrimSpace(input)
		if input[0] == ':' { // handling commands
			if strings.HasPrefix(input, ":q") {
				break
			}
		} else {
			if strings.Index(input, "=") != -1 { // set
				vals := strings.Split(input, "=")
				replicaIdx := rand.Intn(len(replicas))
				log.Printf("Set using replica %d. Key:%s, value:%s\n", replicaIdx, vals[0], vals[1])
				startTs := time.Now()
				resp, err := replicas[replicaIdx].Set(memkv.SetReq{
					Key:    vals[0],
					Value:  []byte(vals[1]),
					Delete: len(vals[1]) == 0,
				})
				if err != nil {
					log.Println(err)
				} else {
					log.Printf("Success:%t, LSN:%d. Time:%d ms\n", resp.Success, resp.LSN, time.Since(startTs).Milliseconds())
				}
			} else { // get
				log.Printf("Reading key:%s from replicas\n", input)
				for i, replica := range replicas {
					startTs := time.Now()
					getResp, err := replica.Get(memkv.GetReq{Key: input})
					if err != nil {
						log.Printf("replica-%d error %s. Time:%d ms\n",
							i, err, time.Since(startTs).Milliseconds())
					} else {
						log.Printf("replica-%d: value:%s, exist:%t, LSN:%d. Time:%d ms\n",
							i, string(getResp.Value), getResp.Exist, getResp.LSN, time.Since(startTs).Milliseconds())
					}
				}
			}
		}
	}

	kvA.Stop()
	kvB.Stop()
	kvC.Stop()
}

type Net struct {
	C    chan []byte
	CMap map[string]chan []byte

	running bool
}

func (n *Net) Send(ctx context.Context, nodeId string, packet []byte) error {
	if c, ok := n.CMap[nodeId]; ok {
		// simulate network delay
		time.Sleep(time.Duration(rand.Intn(50) * int(time.Millisecond)))
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
