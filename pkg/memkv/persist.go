package memkv

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sq-yuan/go-raft/pkg/concurrent"
)

// restore data from disk
func (s *MemKvStore) restore() {
	// restore data from the last saved data file
	lsn, data := s.restoreFromDatafiles()
	convertedData := map[string]mapentry{}
	for k, v := range data {
		convertedData[k] = mapentry{Payload: v.Val, LSN: v.LSN, Deleted: false}
	}
	cmap := concurrent.NewMapFrom[mapentry](convertedData)
	s.store.Store([]*concurrent.Map[string, mapentry]{cmap})
	s.ready.Store(true)

	// replay the state machine log to re-apply changes since the last checkpoint
	s.raft.Replay(int(lsn))
}

func (s *MemKvStore) restoreFromDatafiles() (int64, map[string]diskMapEntry) {
	existingFiles, err := os.ReadDir(s.config.DatafilePath)
	if err != nil {
		s.logger.Println("failed to enumerate dir", err)
		return 0, map[string]diskMapEntry{}
	}

	// Enumerate the data directory and try to find the latest data/lsn file
	latestLSN := int64(0)
	latestLSNFile := ""
	for _, de := range existingFiles {
		if strings.HasPrefix(de.Name(), s.config.DatafilePrefix+"lsn") {
			f, err := os.Open(de.Name())
			if err != nil {
				s.logger.Printf("failed to open file %s, %s\n", de.Name(), err)
				continue
			}
			defer f.Close()

			fLSN := int64(0)
			decoder := gob.NewDecoder(f)
			err = decoder.Decode(&fLSN)
			if err != nil {
				s.logger.Println("failed to decode lsn", err, de.Name())
				continue
			}

			if fLSN > latestLSN {
				latestLSN = fLSN
				latestLSNFile = de.Name()
			}
		}
	}

	if latestLSN == 0 {
		s.logger.Println("no data file found")
		return 0, map[string]diskMapEntry{}
	}
	dataFile := strings.Replace(latestLSNFile, s.config.DatafilePrefix+"lsn", s.config.DatafilePrefix+"store", 1)
	df, err := os.Open(dataFile)
	if err != nil {
		s.logger.Printf("failed to open data file, nothing is restored %s, %s\n", dataFile, err)
		return 0, map[string]diskMapEntry{}
	}
	defer df.Close()

	data := map[string]diskMapEntry{}

	decoder := gob.NewDecoder(df)
	err = decoder.Decode(&data)
	if err != nil {
		s.logger.Printf("failed to decode data file, %s\n", err)
		return 0, map[string]diskMapEntry{}
	}

	return latestLSN, data
}

// return two file names with timestamp
// The first file name is the main file name which is used to store the key-value data
// The second file name is a marker to indicate the first file has been saved successfully
// as well as storing the last LSN in this checkpoint
func getFileNames(destPath, prefix string) (string, string) {
	ts := time.Now().UTC().Unix()
	return path.Join(destPath, fmt.Sprintf("%sstore-%d", prefix, ts)),
		path.Join(destPath, fmt.Sprintf("%slsn-%d", prefix, ts))
}

func prefixMatch(prefix, file string) bool {
	return strings.HasPrefix(file, prefix+"store") || strings.HasPrefix(file, prefix+"lsn")
}

type diskMapEntry struct {
	LSN int64
	Val []byte
}

func (s *MemKvStore) startCPRoutine() {
	s.cpTicker.Reset(CheckpointInterval)

	go func() {
		for {
			select {
			case <-s.cpDoneCh:
				break
			case <-s.cpTicker.C:
				s.checkpoint()
			}
		}
	}()
}

func (s *MemKvStore) stopCPRoutine() {

}

// persist data to a new disk file
func (s *MemKvStore) checkpoint() {
	s.logger.Printf("performing checkpoint")
	list := s.store.Load().([]*concurrent.Map[string, mapentry])
	newList := make([]*concurrent.Map[string, mapentry], 0, len(list)+1)
	copy(newList, list)
	newmap := concurrent.NewMap[mapentry]()
	newList = append(newList, newmap)
	s.store.Store(newList)

	maxLSN := int64(0)
	mergedMap := concurrent.NewMap[mapentry]() // will be used to replace the in-memory one
	fullmap := map[string]diskMapEntry{}       // will be used to dump to disk
	for i := 0; i < len(list); i++ {
		ccmap := list[i]
		for j := 0; j < ccmap.GetShardCount(); j++ {
			shard := ccmap.GetShardByIndex(j)
			shard.RLock()
			for k, v := range shard.Items {
				if v.LSN > maxLSN {
					maxLSN = v.LSN
				}
				if v.Deleted {
					delete(fullmap, k)
					mergedMap.Remove(k)
				} else {
					fullmap[k] = diskMapEntry{Val: v.Payload, LSN: v.LSN}
					mergedMap.Set(k, v)
				}
			}
			shard.RUnlock()
		}
	}

	existingFiles, err := os.ReadDir(s.config.DatafilePath)
	if err != nil {
		s.logger.Println("failed to enumerate dir", err)
		return
	}

	mainf, lsnf := getFileNames(s.config.DatafilePath, s.config.DatafilePrefix)
	mf, err := os.OpenFile(mainf, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0700)
	if err != nil {
		s.logger.Println("failed to open file", mainf, err)
		return
	}
	defer mf.Close()

	encoder := gob.NewEncoder(mf)
	err = encoder.Encode(fullmap)
	if err != nil {
		s.logger.Println("failed to encode full map", err)
		return
	}

	mf.Sync()

	lf, err := os.OpenFile(lsnf, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0700)
	if err != nil {
		s.logger.Println("failed to open file", lsnf, err)
		return
	}
	defer lf.Close()

	lsnencoder := gob.NewEncoder(lf)
	err = lsnencoder.Encode(maxLSN)
	if err != nil {
		s.logger.Println("failed to encode lsn", err)
		return
	}
	lf.Sync()

	// delete all the old files
	for _, ef := range existingFiles {
		if ef.IsDir() {
			continue
		}
		if prefixMatch(s.config.DatafilePrefix, ef.Name()) {
			err = os.Remove(ef.Name())
			if err != nil {
				s.logger.Println("failed to delete", ef.Name(), err)
			}
		}
	}

	s.lastCheckPointTs = time.Now()

	newList = append(newList, newmap)
	s.store.Store([]*concurrent.Map[string, mapentry]{mergedMap, newmap})
}
