package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"
)

const MagicByte = 0x34

type LogEntry struct {
	LogIndex int
	Term     int
	Payload  []byte
}

type LogStore interface {
	RewindAndAppend(pos int, logs []LogEntry) error
	Append(logs []LogEntry) error
	Entries(from, to int) []LogEntry
	Entry(idx int) *LogEntry
	Back() LogEntry
	TotalCount() int
	Close()
}

type appendReq struct {
	log []byte
	wg  sync.WaitGroup
}

type logstore struct {
	file string

	wf         *os.File
	currOffset int64
	offset     []int64
	logs       []LogEntry
}

func NewLogStore(filename string) (LogStore, error) {
	wf, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}
	fstat, err := wf.Stat()
	if err != nil {
		wf.Close()
		return nil, err
	}
	offset := fstat.Size()
	_, err = wf.Seek(offset, 0)
	if err != nil {
		wf.Close()
		return nil, err
	}
	ls := &logstore{file: filename, wf: wf, currOffset: offset}
	err = ls.restore()
	return ls, err
}

func (ls *logstore) restore() error {
	f, err := os.Open(ls.file)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		header := make([]byte, 3, 3)
		n, err := f.Read(header)
		if n != 3 || err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if header[0] != MagicByte {
			return errors.New("invalid file")
		}
		len := readLength(header[1:3])
		logb := make([]byte, len)
		n, err = f.Read(logb)
		if err != nil {
			return err
		}
		log := decodeLog(logb)
		if log == nil {
			// invalid log
		} else {
			ls.offset = append(ls.offset, offset)
			ls.logs = append(ls.logs, *log)
		}
		offset += 3
		offset += int64(len)
	}
	return nil
}

func (ls *logstore) RewindAndAppend(pos int, logs []LogEntry) error {
	if ls.TotalCount() > (pos + len(logs)) {
		ls.logs = ls.logs[0:(pos + len(logs))]
		ls.offset = ls.offset[0:(pos + len(logs))]
	}
	var idx = 0
	for ; idx < len(logs) && idx < (ls.TotalCount()-pos); idx++ {
		lidx := idx + pos
		if ls.logs[lidx].Term != logs[idx].Term || !bytes.Equal(ls.logs[lidx].Payload, logs[idx].Payload) {
			break // start override here
		}
	}

	if (pos + idx) < ls.TotalCount() {
		ls.currOffset = ls.offset[pos+idx]
		_, err := ls.wf.Seek(ls.currOffset, 0)
		if err != nil {
			return err
		}
	}
	ls.logs = ls.logs[0:(pos + idx)]
	ls.offset = ls.offset[0:(pos + idx)]

	for ; idx < len(logs); idx++ {
		logs[idx].LogIndex = ls.TotalCount()
		ls.offset = append(ls.offset, ls.currOffset)
		ls.logs = append(ls.logs, logs[idx])
		err := ls.writeLog(logs[idx])
		if err != nil {
			return err
		}
	}

	ls.wf.Truncate(ls.currOffset)
	return ls.wf.Sync()
}

func (ls *logstore) Append(logs []LogEntry) error {
	for _, log := range logs {
		log.LogIndex = ls.TotalCount()
		ls.offset = append(ls.offset, ls.currOffset)
		ls.logs = append(ls.logs, log)
		err := ls.writeLog(log)
		if err != nil {
			return err
		}
	}
	return ls.wf.Sync()
}

func (ls *logstore) Entries(from, to int) []LogEntry {
	if from < 0 || from >= len(ls.logs) || to < 0 || to > len(ls.logs) || from > to {
		return nil
	}
	return ls.logs[from:to]
}

func (ls *logstore) Entry(idx int) *LogEntry {
	if idx < 0 || idx >= len(ls.logs) {
		panic("index out of range")
	}
	return &ls.logs[idx]
}

func (ls *logstore) Back() LogEntry {
	return ls.logs[len(ls.logs)-1]
}

func (ls *logstore) TotalCount() int {
	return len(ls.logs)
}

func (ls *logstore) Close() {
	ls.wf.Close()
}

func (ls *logstore) GetAllLogs(ctx context.Context) [][]byte {
	return nil
}

func (ls *logstore) writeLog(log LogEntry) error {
	serialized := encodeLog(log)
	_, err := ls.wf.Write([]byte{MagicByte})
	if err != nil {
		return err
	}
	err = writeLength(ls.wf, uint16(len(serialized)))
	if err != nil {
		return err
	}
	_, err = ls.wf.Write(serialized)
	if err != nil {
		return err
	}
	ls.currOffset += 3
	ls.currOffset += int64(len(serialized))
	return nil
}

func writeLength(w io.Writer, len uint16) error {
	ret := []byte{0, 0}
	ret[0] = byte(len & 0xff)
	ret[1] = byte(len >> 8)
	_, err := w.Write(ret)
	return err
}

func readLength(raw []byte) uint16 {
	return uint16(raw[0]) | uint16(raw[1])<<8
}

func encodeLog(log LogEntry) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(log)
	return buf.Bytes()
}

func decodeLog(buf []byte) *LogEntry {
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	var log LogEntry
	decoder.Decode(&log)
	return &log
}
