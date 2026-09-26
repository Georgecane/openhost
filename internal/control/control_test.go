package control

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/participant"
	"github.com/Georgecane/openhost/internal/registry"
	"github.com/Georgecane/openhost/internal/resource"
	"github.com/Georgecane/openhost/internal/scheduler"
)

func newTestParticipant(t *testing.T, cores float64) *participant.Participant {
	t.Helper()

	id, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}

	p, err := participant.New(
		id,
		capability.Capability{
			Compute: capability.ComputeCapability{CPUCores: cores},
		},
		resource.ResourceFragment{
			CPU: resource.CPUCapacity{Cores: cores},
		},
		time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNewPlaneRejectsNilDependencies(t *testing.T) {
	r := registry.New()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := NewPlane(nil, s); !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("NewPlane(nil, scheduler) error = %v, want ErrNilRegistry", err)
	}
	if _, err := NewPlane(r, nil); !errors.Is(err, ErrNilScheduler) {
		t.Fatalf("NewPlane(registry, nil) error = %v, want ErrNilScheduler", err)
	}
}

func TestPlaneRegistersParticipantsAndCreatesLogicalNode(t *testing.T) {
	r := registry.New()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, s)
	if err != nil {
		t.Fatal(err)
	}

	p1 := newTestParticipant(t, 1)
	p2 := newTestParticipant(t, 1)

	if err := p.RegisterParticipant(p1); err != nil {
		t.Fatal(err)
	}
	if err := p.RegisterParticipant(p2); err != nil {
		t.Fatal(err)
	}

	if err := p1.Activate(time.Date(2026, 9, 26, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := p2.Activate(time.Date(2026, 9, 26, 12, 2, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	n, err := p.CreateLogicalNode(resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: 1.5},
	})
	if err != nil {
		t.Fatal(err)
	}

	if n.ID == "" {
		t.Fatal("logical node ID must not be empty")
	}
	if len(n.Allocations) != 2 {
		t.Fatalf("logical node allocations = %d, want 2", len(n.Allocations))
	}
	if n.Resources.CPU.Cores != 1.5 {
		t.Fatalf("logical node CPU = %.2f, want 1.50", n.Resources.CPU.Cores)
	}
}

func TestPlaneObservesParticipantLifecycleThroughRegistry(t *testing.T) {
	r := registry.New()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, s)
	if err != nil {
		t.Fatal(err)
	}

	participant := newTestParticipant(t, 1)
	if err := p.RegisterParticipant(participant); err != nil {
		t.Fatal(err)
	}

	if _, err := p.CreateLogicalNode(resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: 1},
	}); !errors.Is(err, scheduler.ErrInsufficientResources) {
		t.Fatalf("allocation while joining error = %v, want ErrInsufficientResources", err)
	}

	if err := participant.Activate(time.Date(2026, 9, 26, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	if _, err := p.CreateLogicalNode(resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: 1},
	}); err != nil {
		t.Fatalf("allocation after activation error = %v", err)
	}

	if err := participant.BeginDrain(time.Date(2026, 9, 26, 12, 2, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	if _, err := p.CreateLogicalNode(resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: 1},
	}); !errors.Is(err, scheduler.ErrInsufficientResources) {
		t.Fatalf("allocation while draining error = %v, want ErrInsufficientResources", err)
	}
}
