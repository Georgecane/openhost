package participant

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/resource"
)

var (
	ErrInvalidParticipant = errors.New("invalid participant")
	ErrInvalidTransition  = errors.New("invalid participant state transition")
	ErrNotActive           = errors.New("participant is not active")
)

type State string

const (
	StateJoining State = "joining"
	StateActive  State = "active"
	StateDraining State = "draining"
	StateLeft    State = "left"
)

type Participant struct {
	mu         sync.RWMutex
	ID         identity.Identity
	Capability capability.Capability
	Resources  resource.ResourceFragment
	State      State
	JoinedAt   time.Time
	UpdatedAt  time.Time
}

func New(id identity.Identity, cap capability.Capability, resources resource.ResourceFragment, now time.Time) (*Participant, error) {
	if err := id.Validate(); err != nil || id.Kind != identity.ParticipantKind {
		return nil, fmt.Errorf("%w: participant identity must be valid", ErrInvalidParticipant)
	}
	if err := cap.Validate(); err != nil {
		return nil, err
	}
	if err := resources.Validate(); err != nil {
		return nil, err
	}
	if resources.Empty() {
		return nil, fmt.Errorf("%w: resources must not be empty", ErrInvalidParticipant)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: timestamp must not be zero", ErrInvalidParticipant)
	}

	return &Participant{
		ID:         id,
		Capability: cap,
		Resources:  resources,
		State:      StateJoining,
		JoinedAt:   now,
		UpdatedAt:  now,
	}, nil
}

func (p *Participant) Activate(now time.Time) error {
	return p.transition(StateActive, now)
}

func (p *Participant) BeginDrain(now time.Time) error {
	return p.transition(StateDraining, now)
}

func (p *Participant) Leave(now time.Time) error {
	return p.transition(StateLeft, now)
}

type Snapshot struct {
	ID         identity.Identity
	Capability capability.Capability
	Resources  resource.ResourceFragment
	State      State
	JoinedAt   time.Time
	UpdatedAt  time.Time
}

func (p *Participant) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return Snapshot{
		ID:         p.ID,
		Capability: p.Capability,
		Resources:  p.Resources,
		State:      p.State,
		JoinedAt:   p.JoinedAt,
		UpdatedAt:  p.UpdatedAt,
	}
}

func (p *Participant) Offer() (resource.ResourceFragment, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.State != StateActive {
		return resource.ResourceFragment{}, ErrNotActive
	}
	return p.Resources, nil
}

func (p *Participant) transition(next State, now time.Time) error {
	if p == nil {
		return ErrInvalidParticipant
	}
	if now.IsZero() {
		return fmt.Errorf("%w: timestamp must not be zero", ErrInvalidParticipant)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !validTransition(p.State, next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, p.State, next)
	}

	p.State = next
	p.UpdatedAt = now
	return nil
}

func validTransition(from, to State) bool {
	switch from {
	case StateJoining:
		return to == StateActive || to == StateLeft
	case StateActive:
		return to == StateDraining || to == StateLeft
	case StateDraining:
		return to == StateLeft
	case StateLeft:
		return false
	default:
		return false
	}
}
