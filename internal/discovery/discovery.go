package discovery

import (
	"errors"
	"fmt"
	"sort"
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
	ErrInvalidFreshness     = errors.New("discovery freshness policy must be positive")
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

type State string

const (
	StateActive  State = "active"
	StateStale   State = "stale"
	StateExpired State = "expired"
)

type FreshnessPolicy struct {
	StaleAfter   time.Duration
	ExpireAfter  time.Duration
}

func (p FreshnessPolicy) Validate() error {
	if p.StaleAfter <= 0 || p.ExpireAfter <= 0 || p.StaleAfter >= p.ExpireAfter {
		return ErrInvalidFreshness
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

func (m Member) StateAt(now time.Time, policy FreshnessPolicy) (State, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	if now.IsZero() {
		return "", ErrInvalidTimestamp
	}
	if err := policy.Validate(); err != nil {
		return "", err
	}

	age := now.Sub(m.LastSeen)
	if age < 0 {
		return "", fmt.Errorf("%w: state evaluated before last-seen timestamp", ErrInvalidTimestamp)
	}
	switch {
	case age >= policy.ExpireAfter:
		return StateExpired, nil
	case age >= policy.StaleAfter:
		return StateStale, nil
	default:
		return StateActive, nil
	}
}

type Registry interface {
	Upsert(advertisement Advertisement, observedAt time.Time) error
	Remove(participantID identity.Identity) error
	Get(participantID identity.Identity) (Member, error)
	Members() []Member
	MembersAt(now time.Time) ([]Member, error)
}

type MemoryRegistry struct {
	mu      sync.RWMutex
	members map[string]Member
	policy  FreshnessPolicy
}

var _ Registry = (*MemoryRegistry)(nil)

func NewMemoryRegistry(policy FreshnessPolicy) (*MemoryRegistry, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &MemoryRegistry{
		members: make(map[string]Member),
		policy:  policy,
	}, nil
}

func (r *MemoryRegistry) Upsert(advertisement Advertisement, observedAt time.Time) error {
	if r == nil {
		return ErrInvalidAdvertisement
	}
	if err := advertisement.Validate(); err != nil {
		return err
	}
	if observedAt.IsZero() {
		return fmt.Errorf("%w: %w", ErrInvalidAdvertisement, ErrInvalidTimestamp)
	}
	if observedAt.Before(advertisement.ObservedAt) {
		return fmt.Errorf("%w: observation precedes advertisement", ErrInvalidAdvertisement)
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
		LastSeen:      observedAt,
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
	sort.Slice(members, func(i, j int) bool {
		return members[i].Advertisement.Participant.ID < members[j].Advertisement.Participant.ID
	})
	return members
}

func (r *MemoryRegistry) MembersAt(now time.Time) ([]Member, error) {
	members := r.Members()
	for _, member := range members {
		if _, err := member.StateAt(now, r.policy); err != nil {
			return nil, err
		}
	}
	return members, nil
}

func (r *MemoryRegistry) Policy() FreshnessPolicy {
	if r == nil {
		return FreshnessPolicy{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.policy
}
