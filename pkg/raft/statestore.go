package raft

import (
	"context"
	"io"
	"os"
)

type StateStore interface {
	Save(ctx context.Context, data []byte) error
	Restore(ctx context.Context) ([]byte, error)
	Close()
}

type statestore struct {
	f *os.File
}

func NewStateStore(file string) (StateStore, error) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}
	return &statestore{f: f}, nil
}

func (ss *statestore) Save(ctx context.Context, data []byte) error {
	offset, err := ss.f.Seek(0, 0)
	if offset != 0 || err != nil {
		return err
	}
	n, err := ss.f.Write(data)
	if n != len(data) || err != nil {
		return err
	}
	ss.f.Truncate(int64(len(data)))
	ss.f.Sync()
	return nil
}

func (ss *statestore) Restore(ctx context.Context) ([]byte, error) {
	offset, err := ss.f.Seek(0, 0)
	if offset != 0 || err != nil {
		return nil, err
	}
	return io.ReadAll(ss.f)
}

func (ss *statestore) Close() {
	ss.f.Close()
}
