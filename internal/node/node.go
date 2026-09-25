package node

import "github.com/Georgecane/openhost/internal/resource"

// LogicalNode represents an allocation of distributed resources.
// It is deliberately independent of any single physical participant.
type LogicalNode struct {
	ID        string
	Resources resource.ResourceFragment
	Runtime   RuntimeSpec
}

type RuntimeSpec struct {
	Name    string
	Version string
}
