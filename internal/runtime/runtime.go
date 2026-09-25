package runtime

import "context"

type Workload struct {
	ID      string
	Payload []byte
}

type Runtime interface {
	Name() string
	Run(ctx context.Context, workload Workload) error
}
