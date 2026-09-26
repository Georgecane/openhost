package participant

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/resource"
)

func testParticipant(t *testing.T) *Participant {
	t.Helper()

	id, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}

	p, err := New(
		id,
		capability.Capability{
			Compute: capability.ComputeCapability{CPUCores: 2},
		},
		resource.ResourceFragment{
			CPU:    resource.CPUCapacity{Cores: 1},
			Memory: resource.MemoryCapacity{Bytes: 1024},
		},
		time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNewParticipantStartsJoining(t *testing.T) {
	p := testParticipant(t)
	if p.State != StateJoining {
		t.Fatalf("State = %q, want %q", p.State, StateJoining)
	}
	if p.JoinedAt.IsZero() || p.UpdatedAt.IsZero() {
		t.Fatal("participant timestamps must be initialized")
	}
}

func TestParticipantLifecycle(t *testing.T) {
	p := testParticipant(t)
	active := time.Date(2026, 9, 26, 12, 1, 0, 0, time.UTC)
	draining := active.Add(time.Minute)
	left := draining.Add(time.Minute)

	if err := p.Activate(active); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Offer(); err != nil {
		t.Fatalf("Offer() error = %v", err)
	}

	if err := p.BeginDrain(draining); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Offer(); !errors.Is(err, ErrNotActive) {
		t.Fatalf("Offer() error = %v, want ErrNotActive", err)
	}

	if err := p.Leave(left); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Offer(); !errors.Is(err, ErrNotActive) {
		t.Fatalf("Offer() after leave error = %v, want ErrNotActive", err)
	}
}

func TestParticipantRejectsInvalidTransitions(t *testing.T) {
	p := testParticipant(t)
	now := time.Date(2026, 9, 26, 12, 1, 0, 0, time.UTC)

	if err := p.BeginDrain(now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("BeginDrain() error = %v, want ErrInvalidTransition", err)
	}
	if err := p.Leave(now); err != nil {
		t.Fatal(err)
	}
	if err := p.Activate(now.Add(time.Minute)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Activate() after leave error = %v, want ErrInvalidTransition", err)
	}
}

func TestParticipantRejectsInvalidConstruction(t *testing.T) {
	participantID, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}
	validCapability := capability.Capability{
		Compute: capability.ComputeCapability{CPUCores: 1},
	}
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	if _, err := New(participantID, validCapability, resource.ResourceFragment{}, now); !errors.Is(err, ErrInvalidParticipant) {
		t.Fatalf("New() empty resources error = %v, want ErrInvalidParticipant", err)
	}

	if _, err := New(participantID, validCapability, resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}}, time.Time{}); !errors.Is(err, ErrInvalidParticipant) {
		t.Fatalf("New() zero timestamp error = %v, want ErrInvalidParticipant", err)
	}
}
