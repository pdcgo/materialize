package stat_process

type Sideload interface {
}

type sideloadImpl struct{}

func NewPostgresSideload() Sideload {
	return &sideloadImpl{}
}
