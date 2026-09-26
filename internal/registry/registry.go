package registry

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Georgecane/openhost/internal/fabric"
	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/participant"
)

var (
	ErrParticipantExists    = errors.New("participant already registered")
	ErrParticipantNotFound  = errors.New("participant not found")
	ErrParticipantNotActive = errors.New("participant is not active")
)

type Registry struct {
	mu           sync.RWMutex
	participants map[string]*participant.Participant
}

func New() *Registry {
	return &Registry{
		participants: make(map[string]*participant.Participant),
	}
}

func (r *Registry) Register(p *participant.Participant) error {
	if r == nil || p == nil {
		return ErrParticipantNotFound
	}

	snapshot := p.Snapshot()
	if err := snapshot.ID.Validate(); err != nil || snapshot.ID.Kind != identity.ParticipantKind {
		return fmt.Errorf("%w: participant identity must be valid", participant.ErrInvalidParticipant)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.participants[snapshot.ID.ID]; exists {
		return ErrParticipantExists
	}
	r.participants[snapshot.ID.ID] = p
	return nil
}

func (r *Registry) Remove(id identity.Identity) error {
	if r == nil {
		return ErrParticipantNotFound
	}
	if err := id.Validate(); err != nil || id.Kind != identity.ParticipantKind {
		return fmt.Errorf("%w: participant identity must be valid", participant.ErrInvalidParticipant)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.participants[id.ID]; !exists {
		return ErrParticipantNotFound
	}
	delete(r.participants, id.ID)
	return nil
}

func (r *Registry) Get(id identity.Identity) (*participant.Participant, error) {
	if r == nil {
		return nil, ErrParticipantNotFound
	}
	if err := id.Validate(); err != nil || id.Kind != identity.ParticipantKind {
		return nil, fmt.Errorf("%w: participant identity must be valid", participant.ErrInvalidParticipant)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	p, exists := r.participants[id.ID]
	if !exists {
		return nil, ErrParticipantNotFound
	}
	return p, nil
}

func (r *Registry) Offers() []fabric.ResourceOffer {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	offers := make([]fabric.ResourceOffer, 0, len(r.participants))
	for id, p := range r.participants {
		resources, err := p.Offer()
		if err != nil {
			continue
		}
		offers = append(offers, fabric.ResourceOffer{
			ParticipantID: id,
			Resources:     resources,
		})
	}
	return offers
}
