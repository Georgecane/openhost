package lease

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/resource"
)

func testIdentities(t *testing.T) (identity.Identity, identity.Identity) {
	t.Helper()

	nodeID, err := identity.New(identity.LogicalNodeKind)
	if err != nil {
		t.Fatal(err)
	}
	participantID, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}
	return nodeID, participantID
}

func TestNewCreatesValidLease(t *testing.T) {
	nodeID, participantID := testIdentities(t)
	created := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	got, err := New(
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
	if got.ID != nodeID || got.ParticipantID != participantID {
		t.Fatal("lease identities were not preserved")
	}
}

func TestNewRejectsWrongIdentityKinds(t *testing.T) {
	nodeID, participantID := testIdentities(t)

	if _, err := New(participantID, participantID, resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}}, time.Now(), time.Now().Add(time.Hour)); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("New() error = %v, want ErrInvalidLease", err)
	}

	if _, err := New(nodeID, nodeID, resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}}, time.Now(), time.Now().Add(time.Hour)); !errors.Is(err, ErrInvalidParticipant) {
		t.Fatalf("New() error = %v, want ErrInvalidParticipant", err)
	}
}

func TestLeaseActivityBoundaries(t *testing.T) {
	nodeID, participantID := testIdentities(t)
	created := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	lease, err := New(
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
