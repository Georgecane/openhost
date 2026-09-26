package discovery

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/identity"
)

var (
	ErrInvalidAdvertisement = errors.New("invalid discovery advertisement")
	ErrParticipantExists    = errors.New("participant already discovered")
	ErrParticipantNotFound  = errors.New("participant not discovered")
	ErrInvalidParticipant   = errors.New("discovery participant identity must be valid")
	ErrInvalidTimestamp     = errors.New("discovery timestamp must not be zero")
)

type Advertisement struct {
	Participant identity.Identity
	Capability  capability.Capability
	Sequence    uint64
	ObservedAt  time.Time
}

func (a Advertisement) Validate() error {
	if err := a.Participant.Validate(); err != nil || a.Participant.Kind != identity.ParticipantKind {
		return fmt.Errorf("%w: %w", ErrInvalidParticipant, err)
	}
	if err := a.Capability.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidAdvertisement, err)
	}
	if a.ObservedAt.IsZero() {
		return fmt.Errorf("%w: %w", ErrInvalidAdvertisement, ErrInvalidTimestamp)
	}
	return nil
}

type Member struct {
	Advertisement Advertisement
	LastSeen      time.Time
}

func (m Member) Validate() error {
	if err := m.Advertisement.Validate(); err != nil {
		return err
	}
	if m.LastSeen.IsZero() {
		return fmt.Errorf("%w: %w", ErrInvalidAdvertisement, ErrInvalidTimestamp)
	}
	if m.LastSeen.Before(m.Advertisement.ObservedAt) {
		return fmt.Errorf("%w: last-seen timestamp precedes advertisement", ErrInvalidAdvertisement)
	}
	return nil
}

type Registry interface {
	Upsert(advertisement Advertisement) error
	Remove(participantID identity.Identity) error
	Get(participantID identity.Identity) (Member, error)
	Members() []Member
}

type MemoryRegistry struct {
	mu      sync.RWMutex
	members map[string]Member
}

var _ Registry = (*MemoryRegistry)(nil)

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		members: make(map[string]Member),
	}
}

func (r *MemoryRegistry) Upsert(advertisement Advertisement) error {
	if r == nil {
		return ErrInvalidAdvertisement
	}
	if err := advertisement.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := advertisement.Participant.ID
	member, exists := r.members[key]
	if exists && advertisement.Sequence < member.Advertisement.Sequence {
		return fmt.Errorf("%w: sequence regressed", ErrInvalidAdvertisement)
	}

	r.members[key] = Member{
		Advertisement: advertisement,
		LastSeen:      advertisement.ObservedAt,
	}
	return nil
}

func (r *MemoryRegistry) Remove(participantID identity.Identity) error {
	if r == nil {
		return ErrParticipantNotFound
	}
	if err := participantID.Validate(); err != nil || participantID.Kind != identity.ParticipantKind {
		return ErrInvalidParticipant
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.members[participantID.ID]; !exists {
		return ErrParticipantNotFound
	}
	delete(r.members, participantID.ID)
	return nil
}

func (r *MemoryRegistry) Get(participantID identity.Identity) (Member, error) {
	if r == nil {
		return Member{}, ErrParticipantNotFound
	}
	if err := participantID.Validate(); err != nil || participantID.Kind != identity.ParticipantKind {
		return Member{}, ErrInvalidParticipant
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	member, exists := r.members[participantID.ID]
	if !exists {
		return Member{}, ErrParticipantNotFound
	}
	return member, nil
}

func (r *MemoryRegistry) Members() []Member {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]Member, 0, len(r.members))
	for _, member := range r.members {
		members = append(members, member)
	}
	return members
}
