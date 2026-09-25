package node

import "github.com/Georgecane/openhost/internal/resource"

// LogicalNode represents an allocation of distributed resources.
// Its identity is independent of any physical participant.
type LogicalNode struct {
	ID          string
	Resources   resource.ResourceFragment
	Allocations []Allocation
	Runtime     RuntimeSpec
}

type Allocation struct {
	ParticipantID string
	Resources     resource.ResourceFragment
}

type RuntimeSpec struct {
	Name    string
	Version string
}
