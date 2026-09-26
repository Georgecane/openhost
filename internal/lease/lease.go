package lease

import (
	"errors"
	"fmt"
	"time"

	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/resource"
)

var (
	ErrEmptyLeaseID       = errors.New("lease id must not be empty")
	ErrInvalidLease       = errors.New("invalid lease")
	ErrLeaseExpired       = errors.New("lease has expired")
	ErrInvalidExpiry      = errors.New("lease expiry must be after creation")
	ErrInvalidParticipant = errors.New("lease participant identity must be valid")
)

type Lease struct {
	ID            identity.Identity
	ParticipantID identity.Identity
	Resources     resource.ResourceFragment
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

func New(id, participant identity.Identity, resources resource.ResourceFragment, createdAt time.Time, expiresAt time.Time) (Lease, error) {
	if err := id.Validate(); err != nil || id.Kind != identity.LogicalNodeKind {
		return Lease{}, fmt.Errorf("%w: lease identity must be a logical-node identity", ErrInvalidLease)
	}
	if err := participant.Validate(); err != nil || participant.Kind != identity.ParticipantKind {
		return Lease{}, fmt.Errorf("%w: %w", ErrInvalidParticipant, err)
	}
	if err := resources.Validate(); err != nil {
		return Lease{}, err
	}
	if createdAt.IsZero() || expiresAt.IsZero() || !expiresAt.After(createdAt) {
		return Lease{}, ErrInvalidExpiry
	}

	return Lease{
		ID:            id,
		ParticipantID: participant,
		Resources:     resources,
		CreatedAt:     createdAt,
		ExpiresAt:     expiresAt,
	}, nil
}

func (l Lease) Validate() error {
	if err := l.ID.Validate(); err != nil || l.ID.Kind != identity.LogicalNodeKind {
		return fmt.Errorf("%w: lease identity must be a logical-node identity", ErrInvalidLease)
	}
	if err := l.ParticipantID.Validate(); err != nil || l.ParticipantID.Kind != identity.ParticipantKind {
		return fmt.Errorf("%w: %w", ErrInvalidParticipant, err)
	}
	if err := l.Resources.Validate(); err != nil {
		return err
	}
	if l.CreatedAt.IsZero() || l.ExpiresAt.IsZero() || !l.ExpiresAt.After(l.CreatedAt) {
		return ErrInvalidExpiry
	}
	return nil
}

func (l Lease) ActiveAt(now time.Time) bool {
	return !now.Before(l.CreatedAt) && now.Before(l.ExpiresAt)
}

func (l Lease) RemainingAt(now time.Time) time.Duration {
	if !l.ExpiresAt.After(now) {
		return 0
	}
	return l.ExpiresAt.Sub(now)
}
