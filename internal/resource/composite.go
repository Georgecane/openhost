package resource

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyComposite       = errors.New("composite resource must contain at least one allocation")
	ErrDuplicateAllocation  = errors.New("composite resource contains duplicate participant allocation")
	ErrInvalidAllocation    = errors.New("invalid resource allocation")
)

// Allocation binds one resource fragment to the participant that contributes it.
type Allocation struct {
	ParticipantID string
	Resources     ResourceFragment
}

// CompositeResource is the logical resource presented to an execution runtime.
//
// Capacity is the aggregate resource visible to the logical node. Allocations
// retain the physical distribution that backs that capacity. This is a
// composition abstraction, not a shared-memory abstraction.
type CompositeResource struct {
	Capacity    ResourceFragment
	Allocations []Allocation
}

// Compose builds one logical resource from independent participant
// allocations. The returned resource owns its allocation slice.
func Compose(allocations []Allocation) (CompositeResource, error) {
	if len(allocations) == 0 {
		return CompositeResource{}, ErrEmptyComposite
	}

	result := CompositeResource{
		Allocations: make([]Allocation, 0, len(allocations)),
	}

	seen := make(map[string]struct{}, len(allocations))
	for _, allocation := range allocations {
		if allocation.ParticipantID == "" {
			return CompositeResource{}, fmt.Errorf("%w: participant id must not be empty", ErrInvalidAllocation)
		}
		if _, exists := seen[allocation.ParticipantID]; exists {
			return CompositeResource{}, fmt.Errorf("%w: %s", ErrDuplicateAllocation, allocation.ParticipantID)
		}
		if err := ValidateRequirement(allocation.Resources); err != nil {
			return CompositeResource{}, fmt.Errorf("%w: %w", ErrInvalidAllocation, err)
		}

		seen[allocation.ParticipantID] = struct{}{}
		result.Allocations = append(result.Allocations, allocation)
		result.Capacity = result.Capacity.Add(allocation.Resources)
	}

	return result, nil
}

// Validate checks that the aggregate capacity exactly matches its backing
// allocations.
func (r CompositeResource) Validate() error {
	if len(r.Allocations) == 0 {
		return ErrEmptyComposite
	}

	rebuilt, err := Compose(r.Allocations)
	if err != nil {
		return err
	}

	if rebuilt.Capacity != r.Capacity {
		return fmt.Errorf("%w: aggregate capacity does not match allocations", ErrInvalidAllocation)
	}

	return nil
}

// Satisfies reports whether the composed resource can satisfy a requirement.
func (r CompositeResource) Satisfies(requirement ResourceFragment) bool {
	return r.Capacity.Satisfies(requirement)
}

// AllocationFor returns the contribution belonging to participantID.
func (r CompositeResource) AllocationFor(participantID string) (Allocation, bool) {
	for _, allocation := range r.Allocations {
		if allocation.ParticipantID == participantID {
			return allocation, true
		}
	}
	return Allocation{}, false
}
