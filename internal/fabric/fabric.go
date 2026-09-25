package fabric

import (
	"sync"

	"github.com/Georgecane/openhost/internal/resource"
)

// Participant is a source of voluntary resource fragments.
type Participant interface {
	ID() string
	Resources() resource.ResourceFragment
}

// ResourceOffer is the scheduler-visible offer of one participant.
type ResourceOffer struct {
	ParticipantID string
	Resources     resource.ResourceFragment
}

// Fabric describes the resource pool visible to the control plane.
type Fabric interface {
	Offers() []ResourceOffer
}

// MemoryFabric is a concurrency-safe in-memory implementation of Fabric.
type MemoryFabric struct {
	mu     sync.RWMutex
	offers map[string]ResourceOffer
}

func NewMemoryFabric() *MemoryFabric {
	return &MemoryFabric{
		offers: make(map[string]ResourceOffer),
	}
}

func (f *MemoryFabric) Upsert(offer ResourceOffer) error {
	if offer.ParticipantID == "" {
		return ErrInvalidParticipant
	}
	if err := offer.Resources.Validate(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.offers[offer.ParticipantID] = offer
	return nil
}

func (f *MemoryFabric) Remove(participantID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.offers, participantID)
}

func (f *MemoryFabric) Offers() []ResourceOffer {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make([]ResourceOffer, 0, len(f.offers))
	for _, offer := range f.offers {
		result = append(result, offer)
	}
	return result
}
