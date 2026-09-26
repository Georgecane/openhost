package node

import "github.com/Georgecane/openhost/internal/resource"

// LogicalNode represents a logical execution resource assembled from
// independent participant allocations. Its identity is independent of the
// physical participants currently backing it.
type LogicalNode struct {
	ID string

	// Resources is the canonical resource representation exposed by the
	// logical node. Its Capacity is the aggregate resource and its
	// Allocations describe the physical backing.
	Resources resource.CompositeResource

	Runtime RuntimeSpec
}

type RuntimeSpec struct {
	Name    string
	Version string
}
