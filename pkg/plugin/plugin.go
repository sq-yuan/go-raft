package plugin

import "context"

type RecvCallback func(packet []byte)

type Networker interface {
	Send(ctx context.Context, nodeId string, packet []byte) error
	AddRecvCallback(callback RecvCallback)
}

type Storage interface {
	SaveState(ctx context.Context, state []byte)
	RestoreState(ctx context.Context) []byte
}
