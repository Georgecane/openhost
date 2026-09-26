package control

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/discovery"
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
	d := discovery.NewMemoryRegistry()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := NewPlane(nil, d, s); !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("NewPlane(nil, scheduler) error = %v, want ErrNilRegistry", err)
	}
	if _, err := NewPlane(r, d, nil); !errors.Is(err, ErrNilScheduler) {
		t.Fatalf("NewPlane(registry, nil) error = %v, want ErrNilScheduler", err)
	}
}

func TestPlaneRegistersParticipantsAndCreatesLogicalNode(t *testing.T) {
	r := registry.New()
	d := discovery.NewMemoryRegistry()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, d, s)
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
	d := discovery.NewMemoryRegistry()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, d, s)
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

func TestPlaneCreatesAndStoresLeasesForLogicalNodeAllocations(t *testing.T) {
	r := registry.New()
	d := discovery.NewMemoryRegistry()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, d, s)
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

	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	if err := p1.Activate(now); err != nil {
		t.Fatal(err)
	}
	if err := p2.Activate(now); err != nil {
		t.Fatal(err)
	}

	n, leases, err := p.CreateLeasedLogicalNode(
		resource.ResourceFragment{
			CPU: resource.CPUCapacity{Cores: 1.5},
		},
		now,
		10*time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(leases) != len(n.Allocations) {
		t.Fatalf("leases = %d, allocations = %d", len(leases), len(n.Allocations))
	}

	nodeID, err := identity.Parse(n.ID, identity.LogicalNodeKind)
	if err != nil {
		t.Fatal(err)
	}

	for _, l := range leases {
		if l.LogicalNodeID != nodeID {
			t.Fatalf("lease logical node ID = %v, want %v", l.LogicalNodeID, nodeID)
		}
		if !l.ActiveAt(now) {
			t.Fatal("lease must be active at creation time")
		}
		if got := l.RemainingAt(now); got != 10*time.Minute {
			t.Fatalf("lease remaining = %v, want 10m", got)
		}

		stored, err := p.GetLease(l.ID)
		if err != nil {
			t.Fatalf("GetLease() error = %v", err)
		}
		if stored.ID != l.ID {
			t.Fatalf("stored lease ID = %v, want %v", stored.ID, l.ID)
		}
	}
}

func TestPlaneRejectsInvalidLeaseCreationArguments(t *testing.T) {
	r := registry.New()
	d := discovery.NewMemoryRegistry()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, d, s)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	if _, _, err := p.CreateLeasedLogicalNode(
		resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}},
		time.Time{},
		time.Minute,
	); !errors.Is(err, ErrInvalidTime) {
		t.Fatalf("zero timestamp error = %v, want ErrInvalidTime", err)
	}

	if _, _, err := p.CreateLeasedLogicalNode(
		resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}},
		now,
		0,
	); !errors.Is(err, ErrInvalidLeaseDuration) {
		t.Fatalf("zero duration error = %v, want ErrInvalidLeaseDuration", err)
	}
}

func TestPlaneIntegratesDiscoveryWithoutOwningParticipantLifecycle(t *testing.T) {
	r := registry.New()
	d := discovery.NewMemoryRegistry()
	s, err := scheduler.NewAggregatingScheduler(r)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPlane(r, d, s)
	if err != nil {
		t.Fatal(err)
	}

	participantID, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}

	observedAt := time.Date(2026, 9, 26, 13, 0, 0, 0, time.UTC)
	advertisement := discovery.Advertisement{
		Participant: participantID,
		Capability: capability.Capability{
			Compute: capability.ComputeCapability{CPUCores: 2},
		},
		Sequence:   1,
		ObservedAt: observedAt,
	}

	if err := p.AnnounceParticipant(advertisement); err != nil {
		t.Fatal(err)
	}

	members := p.DiscoveredParticipants()
	if len(members) != 1 {
		t.Fatalf("discovered participants = %d, want 1", len(members))
	}
	if members[0].Advertisement.Participant != participantID {
		t.Fatalf("discovered participant = %v, want %v", members[0].Advertisement.Participant, participantID)
	}

	// Discovery is intentionally separate from the local participant registry:
	// an advertisement alone must not make the participant scheduler-visible.
	if _, err := p.CreateLogicalNode(resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: 1},
	}); !errors.Is(err, scheduler.ErrInsufficientResources) {
		t.Fatalf("allocation from discovery-only participant error = %v, want ErrInsufficientResources", err)
	}

	updated := advertisement
	updated.Sequence = 2
	updated.ObservedAt = observedAt.Add(time.Minute)
	updated.Capability.Compute.CPUCores = 4
	if err := p.AnnounceParticipant(updated); err != nil {
		t.Fatal(err)
	}

	member, err := d.Get(participantID)
	if err != nil {
		t.Fatal(err)
	}
	if member.Advertisement.Sequence != 2 {
		t.Fatalf("discovery sequence = %d, want 2", member.Advertisement.Sequence)
	}
	if member.Advertisement.Capability.Compute.CPUCores != 4 {
		t.Fatalf("discovery CPU capability = %.2f, want 4", member.Advertisement.Capability.Compute.CPUCores)
	}

	if err := p.RemoveDiscoveredParticipant(participantID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get(participantID); !errors.Is(err, discovery.ErrParticipantNotFound) {
		t.Fatalf("Get() after discovery removal error = %v, want ErrParticipantNotFound", err)
	}
}
