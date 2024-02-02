package memkv

import (
	"bytes"
	"errors"
	"log"

	"github.com/google/uuid"
	"github.com/sq-yuan/go-raft/pkg/concurrent"
	"github.com/sq-yuan/go-raft/pkg/raft"
	"google.golang.org/protobuf/proto"
)

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

type MemKvStore struct {
	store *concurrent.Map[string, mapentry]
	raft  raft.Raft

	logger *log.Logger

	setch chan setwait
	smch  chan smmsg
}

//go:generate protoc --go_out=. --go_opt=paths=source_relative message.proto
func NewMemKVStore(raft raft.Raft, logger *log.Logger) *MemKvStore {
	if raft == nil {
		return nil
	}
	return &MemKvStore{
		store:  concurrent.NewMap[mapentry](),
		raft:   raft,
		logger: logger,
	}
}

func (s *MemKvStore) onMessage(lsn int, msg []byte) {
	s.smch <- smmsg{lsn: int64(lsn), payload: msg}
}

func (s *MemKvStore) Start() {
	s.setch = make(chan setwait, 100)
	s.smch = make(chan smmsg, 100)
	s.raft.SetHandler(s.onMessage)
	s.raft.Run()

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
						entry := s.store.Get(setreq.Key)
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
								s.store.Remove(setreq.Key)
							} else {
								s.logger.Printf("SetRequest: %s, applied. %s updated!", setreq.RequestId, setreq.Key)
								s.store.Set(setreq.Key, mapentry{LSN: sm.lsn, Payload: setreq.Value, Deleted: false})
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

func (s *MemKvStore) Stop() {
	s.raft.Close()
	s.raft.SetHandler(nil)
	close(s.setch)
	close(s.smch)
}

func validateKey(key string) bool {
	if len(key) == 0 {
		return false
	}
	return true
}

func (s *MemKvStore) Set(req SetReq) (*SetResp, error) {
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
	receivech := make(chan setret)
	setwait := setwait{
		requestId: reqid,
		ch:        receivech,
	}
	s.setch <- setwait
	s.raft.Append(bytes)
	ret := <-receivech
	return &SetResp{
		Success: ret.success,
		LSN:     ret.lsn,
	}, nil
}

func (s *MemKvStore) Get(req GetReq) (*GetResp, error) {
	if !validateKey(req.Key) {
		return nil, errors.New("invalid key")
	}
	entry := s.store.Get(req.Key)
	if entry != nil && !entry.Deleted {
		return &GetResp{LSN: entry.LSN, Exist: true, Value: entry.Payload}, nil
	} else if entry != nil {
		return &GetResp{LSN: entry.LSN, Exist: false}, nil
	} else {
		return &GetResp{LSN: -1, Exist: false}, nil
	}
}
