package scheduler

import (
	"github.com/Georgecane/openhost/internal/node"
	"github.com/Georgecane/openhost/internal/resource"
)

// Scheduler selects resources for a workload from the distributed fabric.
type Scheduler interface {
	Plan(requirement resource.ResourceFragment) (node.LogicalNode, error)
}
