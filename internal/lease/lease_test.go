package lease

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/resource"
)

func testIdentities(t *testing.T) (identity.Identity, identity.Identity, identity.Identity) {
	t.Helper()

	leaseID, err := identity.New(identity.LeaseKind)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identity.New(identity.LogicalNodeKind)
	if err != nil {
		t.Fatal(err)
	}
	participantID, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}
	return leaseID, nodeID, participantID
}

func TestNewCreatesValidLease(t *testing.T) {
	leaseID, nodeID, participantID := testIdentities(t)
	created := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	got, err := New(
		leaseID,
		nodeID,
		participantID,
		resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 2}},
		created,
		expires,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got.ID != leaseID || got.LogicalNodeID != nodeID || got.ParticipantID != participantID {
		t.Fatal("lease identities were not preserved")
	}
}

func TestNewRejectsWrongIdentityKinds(t *testing.T) {
	leaseID, nodeID, participantID := testIdentities(t)
	resources := resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}}
	created := time.Now()
	expires := created.Add(time.Hour)

	if _, err := New(nodeID, nodeID, participantID, resources, created, expires); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("New() error = %v, want ErrInvalidLease", err)
	}

	if _, err := New(leaseID, participantID, participantID, resources, created, expires); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("New() error = %v, want ErrInvalidNode", err)
	}

	if _, err := New(leaseID, nodeID, nodeID, resources, created, expires); !errors.Is(err, ErrInvalidParticipant) {
		t.Fatalf("New() error = %v, want ErrInvalidParticipant", err)
	}
}

func TestNewRejectsEmptyResources(t *testing.T) {
	leaseID, nodeID, participantID := testIdentities(t)
	created := time.Now()
	expires := created.Add(time.Hour)

	if _, err := New(leaseID, nodeID, participantID, resource.ResourceFragment{}, created, expires); !errors.Is(err, resource.ErrZeroRequirement) {
		t.Fatalf("New() error = %v, want ErrZeroRequirement", err)
	}
}

func TestNewRejectsResourceLifetimeShorterThanLease(t *testing.T) {
	leaseID, nodeID, participantID := testIdentities(t)
	created := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	resources := resource.ResourceFragment{
		CPU:      resource.CPUCapacity{Cores: 1},
		Lifetime: 30 * time.Minute,
	}

	if _, err := New(leaseID, nodeID, participantID, resources, created, created.Add(time.Hour)); !errors.Is(err, ErrInvalidLifetime) {
		t.Fatalf("New() error = %v, want ErrInvalidLifetime", err)
	}
}

func TestLeaseActivityBoundaries(t *testing.T) {
	leaseID, nodeID, participantID := testIdentities(t)
	created := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	lease, err := New(
		leaseID,
		nodeID,
		participantID,
		resource.ResourceFragment{Memory: resource.MemoryCapacity{Bytes: 1024}},
		created,
		expires,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !lease.ActiveAt(created) {
		t.Fatal("lease should be active at creation time")
	}
	if !lease.ActiveAt(created.Add(59 * time.Minute)) {
		t.Fatal("lease should be active before expiry")
	}
	if lease.ActiveAt(expires) {
		t.Fatal("lease should be inactive at expiry")
	}
	if got := lease.RemainingAt(expires); got != 0 {
		t.Fatalf("RemainingAt(expiry) = %v, want 0", got)
	}
}
