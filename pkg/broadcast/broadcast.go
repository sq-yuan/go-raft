package broadcast

// handler to receive state machine replicated messages
// This method should not fail and needs to have its own retry logics if necessary
// There'd be some de-dup logic in the handler if it desires exactly-once sementics
type MessageHandler func(msg []byte)

// A interface for broadbaster
type Broadcaster interface {
	// Try to append a message to the sequence
	Append(msg []byte) bool
	// Register a message handler callback
	AddHandle(handler MessageHandler)
}
