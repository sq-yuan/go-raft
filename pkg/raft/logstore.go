package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"log"
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

// logstore maintains two files
// log file, sequentially appended file for raw logs
// index file, mapping from log index to their log file offset to allow us to quickly locate a log record in the log file
//
// we do fsync on every write to log file to make sure logs are persisted
// write to index file are not fsynced to reduce IO, in cases of missing index, we rebuild them during startup
type logstore struct {
	logfile   string
	indexfile string

	logf       *os.File
	idxf       *os.File
	currOffset int64

	// logfile offset for the corresponding log entry
	offset []int64
	// the log index for the first log in the logs slice
	firstLogIndex int
	// log entries
	logs []LogEntry

	logger *log.Logger
}

// Create a new log store
//
//	filename is the filename for the logfile
func NewLogStore(file string, logger *log.Logger) (LogStore, error) {
	logfileName := file
	indexfilename := file + "-index"

	logf, err := os.OpenFile(logfileName, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}

	idxf, err := os.OpenFile(indexfilename, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}

	ls := &logstore{
		logfile:       logfileName,
		indexfile:     indexfilename,
		logf:          logf,
		idxf:          idxf,
		firstLogIndex: -1,
		logger:        logger,
	}
	err = ls.restore()
	if err != nil {
		return nil, err
	}

	err = ls.prepareForWrite()
	if err != nil {
		return nil, err
	}

	return ls, nil
}

// reset file pointers to the end of index file and log file
func (ls *logstore) prepareForWrite() error {
	finfo, err := ls.logf.Stat()
	if err != nil {
		return err
	}
	ls.currOffset = finfo.Size()
	_, err = ls.logf.Seek(0, 2)
	if err != nil {
		return err
	}
	_, err = ls.idxf.Seek(0, 2)
	return err
}

func writeIndex(w io.Writer, offset int64) error {
	return binary.Write(w, binary.LittleEndian, offset)
}

func readIndex(r io.Reader) (int64, error) {
	var offset int64
	err := binary.Read(r, binary.LittleEndian, &offset)
	return offset, err
}

const LogHeaderSize = 3

// append the last index entry to index file
func (ls *logstore) appendIndex() error {
	return binary.Write(ls.idxf, binary.LittleEndian, ls.offset[len(ls.offset)-1])
}

// populate offset slice from index file
func (ls *logstore) loadIndexFile() error {
	ls.idxf.Seek(0, 0)
	for {
		offset, err := readIndex(ls.idxf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// end of file, done
				return nil
			}
			return err
		}
		ls.offset = append(ls.offset, offset)
	}
}

func (ls *logstore) saveIndexFile(start int) error {
	_, err := ls.idxf.Seek(int64(8*start), 0)
	if err != nil {
		return err
	}
	for i := start; i < len(ls.offset); i++ {
		err = binary.Write(ls.idxf, binary.LittleEndian, ls.offset[i])
		if err != nil {
			return err
		}
	}
	finfo, err := ls.idxf.Stat()
	if finfo.Size() > int64(8*len(ls.offset)) {
		ls.idxf.Truncate(int64(8 * len(ls.offset)))
	}
	ls.idxf.Sync()
	return nil
}

// rebuild index file from log files and populate in-mem index slice
func (ls *logstore) rebuildIndex(start int64) (int, error) {
	_, err := ls.logf.Seek(start, 0)
	if err != nil {
		return 0, err
	}

	offset := start
	n := 0
	for {
		valid, len, err := readHeader(ls.logf)
		if err != nil {
			return 0, err
		}
		if !valid {
			// end of file
			break
		}
		ls.offset = append(ls.offset, offset)
		offset += LogHeaderSize
		offset += int64(len)
		n++

		// skip log content
		_, err = ls.logf.Seek(int64(len), 1)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// load index file and populate in-mem index slice
func (ls *logstore) restoreIndex() error {
	finfo, err := ls.idxf.Stat()
	if err != nil {
		return err
	}
	fsize := finfo.Size()
	if (fsize % 8) != 0 {
		// possibly corrupted index file, rebuild index file froms scratch
		ls.logger.Println("index file is corrupted, try to rebuild it")
		_, err := ls.rebuildIndex(0)
		if err != nil {
			return err
		}
		return ls.saveIndexFile(0)
	}

	err = ls.loadIndexFile()
	if err != nil {
		return err
	}
	if len(ls.offset) == 0 {
		return nil
	}

	// try to restore possibly missing index at the end
	lastoffset := ls.offset[len(ls.offset)-1]
	posToSave := len(ls.offset)
	ls.offset = ls.offset[0 : len(ls.offset)-1]
	n, err := ls.rebuildIndex(lastoffset)
	if err != nil {
		return err
	}
	if n > 1 && posToSave < len(ls.offset) {
		err = ls.saveIndexFile(posToSave)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ls *logstore) restore() error {
	return ls.restoreIndex()
}

// try to load logs starting from stIdx into logs slice
// should only be called after index has been loaded
func (ls *logstore) loadLogs(stIdx int) error {
	if ls.firstLogIndex != -1 && stIdx >= ls.firstLogIndex {
		// log has already been loaded
		return nil
	}
	if stIdx == len(ls.offset) {
		// nothing to load
		return nil
	}
	if stIdx < 0 || stIdx > len(ls.offset) {
		return errors.New("invalid stIdx")
	}
	if ls.firstLogIndex == -1 {
		ls.firstLogIndex = len(ls.offset)
	}

	var logs []LogEntry
	for i := stIdx; i < ls.firstLogIndex; i++ {
		offset := ls.offset[i]
		_, err := ls.logf.Seek(offset, 0)
		if err != nil {
			return err
		}
		log, err := readOneLogEntry(ls.logf)
		if err != nil {
			return err
		}
		logs = append(logs, *log)
	}

	logs = append(logs, ls.logs...)
	ls.logs = logs
	ls.firstLogIndex = stIdx
	return nil
}

// append logs to the specific position
func (ls *logstore) RewindAndAppend(pos int, logs []LogEntry) error {
	err := ls.loadLogs(pos)
	if err != nil {
		ls.logger.Printf("failed to load logs %s", err.Error())
		return err
	}

	// truncate local logs if we have more logs in local even then the expected append
	if ls.TotalCount() > (pos + len(logs)) {
		ls.logs = ls.logs[0:(pos + len(logs) - ls.firstLogIndex)]
		ls.offset = ls.offset[0:(pos + len(logs))]
	}

	// compare local logs with appends to find the first mismatch
	// to determine the position to start override
	var idx = 0
	for ; idx < len(logs) && idx < (ls.TotalCount()-pos); idx++ {
		lidx := idx + pos - ls.firstLogIndex
		if ls.logs[lidx].Term != logs[idx].Term || !bytes.Equal(ls.logs[lidx].Payload, logs[idx].Payload) {
			break // found gap, start override here
		}
	}

	// if overwrite position is valid, then truncate local logs again
	if (pos + idx) < ls.TotalCount() {
		// reset log file write position
		ls.currOffset = ls.offset[pos+idx]
		_, err := ls.logf.Seek(ls.currOffset, 0)
		if err != nil {
			return err
		}

		ls.logs = ls.logs[0:(pos + idx - ls.firstLogIndex)]
		ls.offset = ls.offset[0:(pos + idx)]
	}
	stIdx := pos + idx

	for ; idx < len(logs); idx++ {
		logs[idx].LogIndex = ls.TotalCount()
		ls.offset = append(ls.offset, ls.currOffset)
		ls.logs = append(ls.logs, logs[idx])
		err := ls.appendLog(logs[idx])
		if err != nil {
			return err
		}
	}

	ls.logf.Truncate(ls.currOffset)
	ls.logf.Sync()
	return ls.saveIndexFile(stIdx)
}

func (ls *logstore) Append(logs []LogEntry) error {
	for _, log := range logs {
		log.LogIndex = ls.TotalCount()
		ls.offset = append(ls.offset, ls.currOffset)
		ls.logs = append(ls.logs, log)
		err := ls.appendIndex()
		if err != nil {
			return err
		}
		err = ls.appendLog(log)
		if err != nil {
			return err
		}
	}
	return ls.logf.Sync()
}

func (ls *logstore) Entries(from, to int) []LogEntry {
	if from < 0 || from > len(ls.offset) || to < 0 || to > len(ls.offset) || from > to {
		return nil
	}
	err := ls.loadLogs(from)
	if err != nil {
		ls.logger.Panic("cannot load logs")
	}
	return ls.logs[from-ls.firstLogIndex : to-ls.firstLogIndex]
}

func (ls *logstore) Entry(idx int) *LogEntry {
	if idx < 0 || idx >= len(ls.offset) {
		ls.logger.Panicf("index out of range %d", idx)
	}
	err := ls.loadLogs(idx)
	if err != nil {
		ls.logger.Panic("cannot load logs")
	}
	return &ls.logs[idx-ls.firstLogIndex]
}

func (ls *logstore) Back() LogEntry {
	err := ls.loadLogs(len(ls.offset) - 1)
	if err != nil {
		ls.logger.Panic("cannot load logs")
	}
	return ls.logs[len(ls.logs)-1]
}

func (ls *logstore) TotalCount() int {
	return len(ls.offset)
}

func (ls *logstore) Close() {
	ls.logf.Close()
	ls.idxf.Close()
}

func (ls *logstore) appendLog(log LogEntry) error {
	serialized := encodeLog(log)
	_, err := ls.logf.Write([]byte{MagicByte})
	if err != nil {
		return err
	}
	err = writeLength(ls.logf, uint16(len(serialized)))
	if err != nil {
		return err
	}
	_, err = ls.logf.Write(serialized)
	if err != nil {
		return err
	}
	ls.currOffset += LogHeaderSize
	ls.currOffset += int64(len(serialized))
	return nil
}

func readHeader(r io.Reader) (bool, uint16, error) {
	header := make([]byte, LogHeaderSize, LogHeaderSize)
	n, err := r.Read(header)
	if n != LogHeaderSize || err != nil {
		if errors.Is(err, io.EOF) {
			// end of file
			return false, 0, nil
		}
		return false, 0, err
	}
	if header[0] != MagicByte {
		return false, 0, errors.New("invalid file")
	}
	len := readLength(header[1:3])
	return true, len, nil
}

func readOneLogEntry(r io.Reader) (*LogEntry, error) {
	valid, len, err := readHeader(r)
	if err != nil {
		return nil, err
	}
	if !valid {
		// end of file
		return nil, nil
	}
	logb := make([]byte, len)
	_, err = r.Read(logb)
	if err != nil {
		return nil, err
	}
	log := decodeLog(logb)
	return log, nil
}

func writeOneLogEntry(w io.Writer, log LogEntry) (int, error) {
	serialized := encodeLog(log)
	_, err := w.Write([]byte{MagicByte})
	if err != nil {
		return 0, err
	}
	err = writeLength(w, uint16(len(serialized)))
	if err != nil {
		return 0, err
	}
	_, err = w.Write(serialized)
	if err != nil {
		return 0, err
	}
	return LogHeaderSize + len(serialized), nil
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
