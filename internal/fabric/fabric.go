package fabric

import "github.com/Georgecane/openhost/internal/resource"

// Participant is a source of voluntary resource fragments.
type Participant interface {
	ID() string
	Resources() resource.ResourceFragment
}

// Fabric describes the resource pool visible to the control plane.
type Fabric interface {
	Participants() []Participant
}
