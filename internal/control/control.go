package control

import (
	"errors"

	"github.com/Georgecane/openhost/internal/node"
	"github.com/Georgecane/openhost/internal/participant"
	"github.com/Georgecane/openhost/internal/registry"
	"github.com/Georgecane/openhost/internal/resource"
	"github.com/Georgecane/openhost/internal/scheduler"
)

var (
	ErrNilPlane     = errors.New("control plane must not be nil")
	ErrNilRegistry  = errors.New("registry must not be nil")
	ErrNilScheduler = errors.New("scheduler must not be nil")
)

// Plane coordinates participant registration and logical-node allocation.
// Domain rules remain owned by participant, registry, and scheduler.
type Plane struct {
	registry  *registry.Registry
	scheduler scheduler.Scheduler
}

func NewPlane(r *registry.Registry, s scheduler.Scheduler) (*Plane, error) {
	if r == nil {
		return nil, ErrNilRegistry
	}
	if s == nil {
		return nil, ErrNilScheduler
	}
	return &Plane{registry: r, scheduler: s}, nil
}

func (p *Plane) RegisterParticipant(part *participant.Participant) error {
	if p == nil {
		return ErrNilPlane
	}
	return p.registry.Register(part)
}

func (p *Plane) CreateLogicalNode(requirement resource.ResourceFragment) (node.LogicalNode, error) {
	if p == nil {
		return node.LogicalNode{}, ErrNilPlane
	}
	return p.scheduler.Plan(requirement)
}
