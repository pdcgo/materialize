package stat_replica

type Replica interface {
	Start() error
}

type repImpl struct{}

// Start implements Replica.
func (r *repImpl) Start() error {
	panic("unimplemented")
}

func NewReplica() Replica {
	return &repImpl{}
}
