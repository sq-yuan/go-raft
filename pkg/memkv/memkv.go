package memkv

import (
	"bytes"
	"errors"
	"log"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sq-yuan/go-raft/pkg/concurrent"
	"github.com/sq-yuan/go-raft/pkg/raft"
	"google.golang.org/protobuf/proto"
)

const CheckpointInterval = time.Minute

type setret struct {
	success bool
	lsn     int64
	err     string
}

type setwait struct {
	requestId string
	ch        chan<- setret
}

type smmsg struct {
	lsn     int64
	payload []byte
}

type mapentry struct {
	LSN     int64
	Deleted bool
	Payload []byte
}

type StoreConfig struct {
	DatafilePath   string
	DatafilePrefix string
}

// A memory based key/value store using Raft
// All data will be held in memory and we use checkpointing to periodically persist data
// back to disk along with the last seen LSN.
// Upon recovery we'll reload the data into memory and replay all raft messages since the last know LSN.
// So we literally use Raft replication log as the write ahead log for the database
type MemKvStore struct {
	// A list of in-memory maps
	// Regular write operation always update the last map and read always starts from the end till the head
	// During checkpointing, the checkpointing goroutine will append a new map to the list, which will be used
	// for update, and the old maps will be acting like a snapshot and will be iterated and persisted to disk
	// once the checkpointing is done, all the old maps will be merged into a single one. So most of the time we'll
	// only have three maps in the list.
	// Doing this we avoided using locks during checkpointing.
	store atomic.Value
	raft  raft.Raft

	config StoreConfig
	logger *log.Logger

	setch chan setwait
	smch  chan smmsg

	ready atomic.Bool

	cpTicker *time.Ticker
	cpDoneCh chan bool

	lastCheckPointTs time.Time
}

//go:generate protoc --go_out=. --go_opt=paths=source_relative message.proto
func NewMemKVStore(config StoreConfig, raft raft.Raft, logger *log.Logger) *MemKvStore {
	if raft == nil {
		return nil
	}
	if config.DatafilePath == "" {
		config.DatafilePath = "."
	}
	store := &MemKvStore{
		raft:     raft,
		config:   config,
		logger:   logger,
		cpTicker: time.NewTicker(CheckpointInterval),
		cpDoneCh: make(chan bool),
	}
	store.cpTicker.Stop()
	store.ready.Store(false)
	return store
}

func (s *MemKvStore) onMessage(lsn int, msg []byte) {
	s.smch <- smmsg{lsn: int64(lsn), payload: msg}
}

func (s *MemKvStore) Start() {
	s.setch = make(chan setwait, 100)
	s.smch = make(chan smmsg, 100)
	s.raft.SetHandler(s.onMessage)

	// restore before start raft
	s.restore()

	s.raft.Run()

	s.startCPRoutine()

	// write go-routine
	go func() {
		waitmap := map[string]*setwait{}
		for {
			swclosed := false
			smclosed := false
			select {
			case sw, ok := <-s.setch:
				if ok {
					waitmap[sw.requestId] = &sw
				} else {
					swclosed = true
				}
			case sm, ok := <-s.smch:
				if ok {
					kvreq := &KVRequest{}
					if err := proto.Unmarshal(sm.payload, kvreq); err != nil {
						s.logger.Fatal("failed unmarshal message")
						continue
					}
					setreq := kvreq.GetSet()
					if setreq != nil {
						entry := s.storeget(setreq.Key)
						precondition := true
						if setreq.IfLsnIs != nil {
							precondition = precondition && entry != nil && *setreq.IfLsnIs == entry.LSN
						}
						if setreq.IfValueIs != nil {
							precondition = precondition && entry != nil &&
								bytes.Equal(entry.Payload, setreq.IfValueIs)
						}
						if setreq.IfExistsIs != nil {
							precondition = precondition &&
								((*setreq.IfExistsIs && entry != nil && !entry.Deleted) ||
									(!*setreq.IfExistsIs && (entry == nil || entry.Deleted)))
						}
						cLSN := int64(-1)
						if entry != nil {
							cLSN = entry.LSN
						}
						var ret setret
						if !precondition {
							s.logger.Printf("SetRequest: %s, not applied, pre-condition check failed", setreq.RequestId)
							ret.success = false
							ret.lsn = cLSN
						} else {
							if setreq.Delete != nil && *setreq.Delete {
								s.logger.Printf("SetRequest: %s, applied. %s deleted!", setreq.RequestId, setreq.Key)
								s.storeset(setreq.Key, mapentry{LSN: sm.lsn, Payload: nil, Deleted: true})
							} else {
								s.logger.Printf("SetRequest: %s, applied. %s updated!", setreq.RequestId, setreq.Key)
								s.storeset(setreq.Key, mapentry{LSN: sm.lsn, Payload: setreq.Value, Deleted: false})
							}
							ret.success = true
							ret.lsn = sm.lsn
						}
						if req, ok := waitmap[setreq.RequestId]; ok {
							req.ch <- ret
						}
					}
				} else {
					smclosed = true
				}
			}

			if swclosed && smclosed {
				return
			}
		}
	}()
}

func (s *MemKvStore) storeget(key string) *mapentry {
	list := s.store.Load().([]*concurrent.Map[string, mapentry])
	for i := len(list) - 1; i >= 0; i-- {
		entry := list[i].Get(key)
		if entry != nil {
			return entry
		}
	}
	return nil
}

func (s *MemKvStore) storeset(key string, entry mapentry) {
	list := s.store.Load().([]*concurrent.Map[string, mapentry])
	list[len(list)-1].Set(key, entry)
}

func (s *MemKvStore) Stop() {
	s.raft.Close()
	s.raft.SetHandler(nil)
	close(s.setch)
	close(s.smch)
	s.stopCPRoutine()
}

var keyValidator = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9\-\_\\\/]*`)

func validateKey(key string) bool {
	if len(key) == 0 {
		return false
	}
	if !keyValidator.Match([]byte(key)) {
		return false
	}
	return true
}

func (s *MemKvStore) Set(req SetReq) (*SetResp, error) {
	if !s.ready.Load() {
		return nil, errors.New("server not ready")
	}
	if !validateKey(req.Key) {
		return nil, errors.New("invalid key")
	}
	reqid := uuid.New().String()
	kvreq := &KVRequest{
		Request: &KVRequest_Set{Set: &SetRequest{
			RequestId:  reqid,
			Key:        req.Key,
			Delete:     &req.Delete,
			Value:      req.Value,
			IfValueIs:  req.Options.IfValueIs,
			IfLsnIs:    req.Options.IfLSNIs,
			IfExistsIs: req.Options.IfExistIs,
		}},
	}
	bytes, err := proto.Marshal(kvreq)
	if err != nil {
		return nil, err
	}
	receivech := make(chan setret, 1)
	setwait := setwait{
		requestId: reqid,
		ch:        receivech,
	}
	s.setch <- setwait
	if !s.raft.Append(bytes) {
		return nil, errors.New("server not ready")
	}
	ret := <-receivech
	return &SetResp{
		Success: ret.success,
		LSN:     ret.lsn,
	}, nil
}

func (s *MemKvStore) Get(req GetReq) (*GetResp, error) {
	if !s.ready.Load() {
		return nil, errors.New("server not ready")
	}
	if !validateKey(req.Key) {
		return nil, errors.New("invalid key")
	}
	entry := s.storeget(req.Key)
	if entry != nil && !entry.Deleted {
		return &GetResp{LSN: entry.LSN, Exist: true, Value: entry.Payload}, nil
	} else if entry != nil {
		return &GetResp{LSN: entry.LSN, Exist: false}, nil
	} else {
		return &GetResp{LSN: -1, Exist: false}, nil
	}
}
