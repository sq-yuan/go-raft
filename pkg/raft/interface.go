package raft

// handler to receive state machine replicated messages
// This method should not fail and needs to have its own retry logics if necessary
// There'd be some de-dup logic in the handler if it desires exactly-once sementics
type MessageHandler func(lsn int, msg []byte)

// Raft interface
type Raft interface {
	// Starts the state machine
	Run()
	// Stops the state machine
	Close()
	// Try to append a message replication queue
	Append(msg []byte) bool
	// Register a message handler callback
	AddHandle(handler MessageHandler)
	// Replay the replication log from the specified LSN
	Replay(lsn int) error
	// Truncate logs till the specified LSN
	Truncate(lsn int) error
}
