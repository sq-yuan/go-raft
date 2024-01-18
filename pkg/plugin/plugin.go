package plugin

import "context"

type RecvCallback func(packet []byte)

type Networker interface {
	Send(ctx context.Context, nodeId string, packet []byte) error
	AddRecvCallback(callback RecvCallback)
}

type LogStore interface {
	Append(ctx context.Context, data []byte)
	GetAllLogs(ctx context.Context) [][]byte
}
