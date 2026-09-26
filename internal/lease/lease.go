package lease

import (
	"errors"
	"fmt"
	"time"

	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/resource"
)

var (
	ErrInvalidLease       = errors.New("invalid lease")
	ErrInvalidExpiry      = errors.New("lease expiry must be after creation")
	ErrInvalidLifetime    = errors.New("lease duration exceeds resource lifetime")
	ErrInvalidParticipant = errors.New("lease participant identity must be valid")
	ErrInvalidNode        = errors.New("lease logical-node identity must be valid")
)

type Lease struct {
	ID             identity.Identity
	LogicalNodeID  identity.Identity
	ParticipantID  identity.Identity
	Resources      resource.ResourceFragment
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

func New(
	id identity.Identity,
	logicalNodeID identity.Identity,
	participantID identity.Identity,
	resources resource.ResourceFragment,
	createdAt time.Time,
	expiresAt time.Time,
) (Lease, error) {
	if err := id.Validate(); err != nil || id.Kind != identity.LeaseKind {
		return Lease{}, fmt.Errorf("%w: lease identity must be a lease identity", ErrInvalidLease)
	}
	if err := logicalNodeID.Validate(); err != nil || logicalNodeID.Kind != identity.LogicalNodeKind {
		return Lease{}, fmt.Errorf("%w: %w", ErrInvalidNode, err)
	}
	if err := participantID.Validate(); err != nil || participantID.Kind != identity.ParticipantKind {
		return Lease{}, fmt.Errorf("%w: %w", ErrInvalidParticipant, err)
	}
	if err := resource.ValidateRequirement(resources); err != nil {
		return Lease{}, err
	}
	if createdAt.IsZero() || expiresAt.IsZero() || !expiresAt.After(createdAt) {
		return Lease{}, ErrInvalidExpiry
	}

	duration := expiresAt.Sub(createdAt)
	if resources.Lifetime > 0 && resources.Lifetime < duration {
		return Lease{}, ErrInvalidLifetime
	}

	return Lease{
		ID:            id,
		LogicalNodeID: logicalNodeID,
		ParticipantID: participantID,
		Resources:     resources,
		CreatedAt:     createdAt,
		ExpiresAt:     expiresAt,
	}, nil
}

func (l Lease) Validate() error {
	if err := l.ID.Validate(); err != nil || l.ID.Kind != identity.LeaseKind {
		return fmt.Errorf("%w: lease identity must be a lease identity", ErrInvalidLease)
	}
	if err := l.LogicalNodeID.Validate(); err != nil || l.LogicalNodeID.Kind != identity.LogicalNodeKind {
		return fmt.Errorf("%w: %w", ErrInvalidNode, err)
	}
	if err := l.ParticipantID.Validate(); err != nil || l.ParticipantID.Kind != identity.ParticipantKind {
		return fmt.Errorf("%w: %w", ErrInvalidParticipant, err)
	}
	if err := resource.ValidateRequirement(l.Resources); err != nil {
		return err
	}
	if l.CreatedAt.IsZero() || l.ExpiresAt.IsZero() || !l.ExpiresAt.After(l.CreatedAt) {
		return ErrInvalidExpiry
	}

	duration := l.ExpiresAt.Sub(l.CreatedAt)
	if l.Resources.Lifetime > 0 && l.Resources.Lifetime < duration {
		return ErrInvalidLifetime
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
