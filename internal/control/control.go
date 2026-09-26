package control

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Georgecane/openhost/internal/discovery"
	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/lease"
	"github.com/Georgecane/openhost/internal/node"
	"github.com/Georgecane/openhost/internal/participant"
	"github.com/Georgecane/openhost/internal/registry"
	"github.com/Georgecane/openhost/internal/resource"
	"github.com/Georgecane/openhost/internal/scheduler"
)

var (
	ErrNilPlane             = errors.New("control plane must not be nil")
	ErrNilRegistry          = errors.New("registry must not be nil")
	ErrNilDiscovery         = errors.New("discovery registry must not be nil")
	ErrNilScheduler         = errors.New("scheduler must not be nil")
	ErrInvalidTime          = errors.New("control-plane timestamp must not be zero")
	ErrInvalidLeaseDuration = errors.New("lease duration must be positive")
	ErrLeaseNotFound        = errors.New("lease not found")
)

// Plane coordinates participant registration, discovery, logical-node
// allocation, and lease creation. Domain rules remain owned by their
// respective packages.
type Plane struct {
	mu        sync.RWMutex
	registry  *registry.Registry
	discovery discovery.Registry
	scheduler scheduler.Scheduler
	leases    map[string]lease.Lease
}

func NewPlane(
	r *registry.Registry,
	d discovery.Registry,
	s scheduler.Scheduler,
) (*Plane, error) {
	if r == nil {
		return nil, ErrNilRegistry
	}
	if d == nil {
		return nil, ErrNilDiscovery
	}
	if s == nil {
		return nil, ErrNilScheduler
	}
	return &Plane{
		registry:  r,
		discovery: d,
		scheduler: s,
		leases:    make(map[string]lease.Lease),
	}, nil
}

func (p *Plane) RegisterParticipant(part *participant.Participant) error {
	if p == nil {
		return ErrNilPlane
	}
	return p.registry.Register(part)
}

func (p *Plane) AnnounceParticipant(advertisement discovery.Advertisement, observedAt time.Time) error {
	if p == nil {
		return ErrNilPlane
	}
	return p.discovery.Upsert(advertisement, observedAt)
}

func (p *Plane) RemoveDiscoveredParticipant(id identity.Identity) error {
	if p == nil {
		return ErrNilPlane
	}
	return p.discovery.Remove(id)
}

func (p *Plane) DiscoveredParticipants() []discovery.Member {
	if p == nil {
		return nil
	}
	return p.discovery.Members()
}

func (p *Plane) DiscoveredParticipantsAt(now time.Time) ([]discovery.Member, error) {
	if p == nil {
		return nil, ErrNilPlane
	}
	return p.discovery.MembersAt(now)
}

func (p *Plane) CreateLogicalNode(requirement resource.ResourceFragment) (node.LogicalNode, error) {
	if p == nil {
		return node.LogicalNode{}, ErrNilPlane
	}
	return p.scheduler.Plan(requirement)
}

// CreateLeasedLogicalNode allocates a logical node and creates one lease for
// each participant allocation. The lease duration must be positive.
func (p *Plane) CreateLeasedLogicalNode(
	requirement resource.ResourceFragment,
	now time.Time,
	duration time.Duration,
) (node.LogicalNode, []lease.Lease, error) {
	if p == nil {
		return node.LogicalNode{}, nil, ErrNilPlane
	}
	if now.IsZero() {
		return node.LogicalNode{}, nil, ErrInvalidTime
	}
	if duration <= 0 {
		return node.LogicalNode{}, nil, ErrInvalidLeaseDuration
	}

	logicalNode, err := p.scheduler.Plan(requirement)
	if err != nil {
		return node.LogicalNode{}, nil, err
	}

	nodeIdentity, err := identity.Parse(logicalNode.ID, identity.LogicalNodeKind)
	if err != nil {
		return node.LogicalNode{}, nil, fmt.Errorf("parse logical-node identity: %w", err)
	}

	expiresAt := now.Add(duration)
	leases := make([]lease.Lease, 0, len(logicalNode.Allocations))

	for _, allocation := range logicalNode.Allocations {
		participantIdentity, err := identity.Parse(
			allocation.ParticipantID,
			identity.ParticipantKind,
		)
		if err != nil {
			return node.LogicalNode{}, nil, fmt.Errorf("parse participant identity: %w", err)
		}

		leaseIdentity, err := identity.New(identity.LeaseKind)
		if err != nil {
			return node.LogicalNode{}, nil, fmt.Errorf("create lease identity: %w", err)
		}

		allocationResources := allocation.Resources
		if allocationResources.Lifetime == 0 || allocationResources.Lifetime > duration {
			allocationResources.Lifetime = duration
		}

		l, err := lease.New(
			leaseIdentity,
			nodeIdentity,
			participantIdentity,
			allocationResources,
			now,
			expiresAt,
		)
		if err != nil {
			return node.LogicalNode{}, nil, fmt.Errorf("create lease: %w", err)
		}
		leases = append(leases, l)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, l := range leases {
		p.leases[l.ID.ID] = l
	}

	return logicalNode, leases, nil
}

func (p *Plane) GetLease(id identity.Identity) (lease.Lease, error) {
	if p == nil {
		return lease.Lease{}, ErrNilPlane
	}
	if err := id.Validate(); err != nil || id.Kind != identity.LeaseKind {
		return lease.Lease{}, ErrLeaseNotFound
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	l, ok := p.leases[id.ID]
	if !ok {
		return lease.Lease{}, ErrLeaseNotFound
	}
	return l, nil
}
