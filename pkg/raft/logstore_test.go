package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTestFileName() string {
	return path.Join(os.TempDir(), fmt.Sprintf("logstore-%d-%d", time.Now().Unix(), rand.Int()))
}

func TestLogstore1(t *testing.T) {
	logfile := getTestFileName()

	store, err := NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log1"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log2"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log3"), Term: 1}})
	assert.Nil(t, err)

	store.Close()

	store, err = NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log4"), Term: 2}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log5"), Term: 2}})
	assert.Nil(t, err)

	store.Close()

	store, err = NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	assert.Equal(t, 5, store.TotalCount())
	var payloads []string
	for _, entry := range store.Entries(0, store.TotalCount()) {
		payloads = append(payloads, string(entry.Payload))
	}
	assert.ElementsMatch(t, []string{"log1", "log2", "log3", "log4", "log5"}, payloads)

	store.Close()
}

func TestLogstore2(t *testing.T) {
	logfile := getTestFileName()

	store, err := NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log1"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log2"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log3"), Term: 1}})
	assert.Nil(t, err)

	err = store.RewindAndAppend(1, []LogEntry{{Payload: []byte("log11"), Term: 1}, {Payload: []byte("log111"), Term: 1}})
	assert.Nil(t, err)

	store.Close()

	store, err = NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log4"), Term: 2}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log5"), Term: 2}})
	assert.Nil(t, err)

	err = store.RewindAndAppend(3, []LogEntry{{Payload: []byte("log1111"), Term: 2}, {Payload: []byte("log11111"), Term: 2}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log111111"), Term: 2}})
	assert.Nil(t, err)

	store.Close()

	store, err = NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	assert.Equal(t, 6, store.TotalCount())
	var payloads []string
	for _, entry := range store.Entries(0, store.TotalCount()) {
		payloads = append(payloads, string(entry.Payload))
	}
	assert.ElementsMatch(t, []string{"log1", "log11", "log111", "log1111", "log11111", "log111111"}, payloads)

	store.Close()
}

func TestIndexRepair(t *testing.T) {
	logfile := getTestFileName()

	store, err := NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log1"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log2"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log3"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log4"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log5"), Term: 1}})
	assert.Nil(t, err)

	err = store.Append([]LogEntry{{Payload: []byte("log6"), Term: 1}})
	assert.Nil(t, err)

	store.Close()

	indexFile := logfile + "-index"
	f, err := os.OpenFile(indexFile, os.O_RDWR, 0700)
	assert.Nil(t, err)

	finfo, err := f.Stat()
	assert.Nil(t, err)
	assert.Equal(t, int64(48), finfo.Size())

	f.Truncate(24)
	f.Sync()
	f.Close()

	store, err = NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	assert.Equal(t, 6, store.TotalCount())
	var payloads []string
	for _, entry := range store.Entries(0, store.TotalCount()) {
		payloads = append(payloads, string(entry.Payload))
	}
	assert.ElementsMatch(t, []string{"log1", "log2", "log3", "log4", "log5", "log6"}, payloads)

	store.Close()

	f, err = os.OpenFile(indexFile, os.O_RDWR, 0700)
	assert.Nil(t, err)

	finfo, err = f.Stat()
	assert.Nil(t, err)
	assert.Equal(t, int64(48), finfo.Size())

	f.Truncate(21)
	f.Sync()
	f.Close()

	store, err = NewLogStore(logfile, log.Default())
	assert.Nil(t, err)

	payloads = []string{}
	assert.Equal(t, 6, store.TotalCount())
	for _, entry := range store.Entries(2, store.TotalCount()) {
		payloads = append(payloads, string(entry.Payload))
	}
	assert.ElementsMatch(t, []string{"log3", "log4", "log5", "log6"}, payloads)

	store.Close()
}
