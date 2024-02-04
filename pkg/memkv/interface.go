package memkv

type SetOptions struct {
	// concurrency control
	// only proceed the operation if timestamp matches
	IfValueIs []byte
	IfLSNIs   *int64
	IfExistIs *bool
}

type SetReq struct {
	Key   string
	Value []byte
	// Set true to delete the value
	Delete  bool
	Options SetOptions
}

type SetResp struct {
	Success bool

	// current LSN
	LSN int64
}

type GetOptions struct {
	// Requires strong consistent read
	// Guarentees to see all the writes that happens before this read request in physical time
	Linearizability bool
	// The last seen LSN
	// By default session consistency is provided by connecting to the same replica
	// however, in case of failover, a provided SessionLSN will provide the same session
	// consistency guarentee by blocking the request until LSN has been observed by the replica
	// Session consistency provides the following guarentees: Read-your-write, Monotic Read, Read after write
	// Will be ignored if Linearizability is true
	SessionLSN *int64
}

type GetReq struct {
	Key     string
	Options GetOptions
}

type GetResp struct {
	LSN   int64 // the most recent LSN this request had been observed
	Exist bool
	Value []byte
}

// A key-value store
type KVStore interface {
	Set(req SetReq) (*SetResp, error)
	Get(req GetReq) (*GetResp, error)
}
