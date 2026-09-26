package registry

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/identity"
	"github.com/Georgecane/openhost/internal/participant"
	"github.com/Georgecane/openhost/internal/resource"
)

func newTestParticipant(t *testing.T) *participant.Participant {
	t.Helper()
	id, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}
	p, err := participant.New(
		id,
		capability.Capability{Compute: capability.ComputeCapability{CPUCores: 2}},
		resource.ResourceFragment{CPU: resource.CPUCapacity{Cores: 1}},
		time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRegistryPublishesOnlyActiveOffers(t *testing.T) {
	r := New()
	p := newTestParticipant(t)

	if err := r.Register(p); err != nil {
		t.Fatal(err)
	}
	if got := len(r.Offers()); got != 0 {
		t.Fatalf("Offers() before activation = %d, want 0", got)
	}

	if err := p.Activate(time.Date(2026, 9, 26, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	offers := r.Offers()
	if len(offers) != 1 {
		t.Fatalf("Offers() after activation = %d, want 1", len(offers))
	}
	if offers[0].ParticipantID != p.Snapshot().ID.ID {
		t.Fatalf("offer participant id = %q, want %q", offers[0].ParticipantID, p.Snapshot().ID.ID)
	}
}

func TestRegistryRejectsDuplicateAndUnknownRemoval(t *testing.T) {
	r := New()
	p := newTestParticipant(t)

	if err := r.Register(p); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(p); !errors.Is(err, ErrParticipantExists) {
		t.Fatalf("duplicate Register() error = %v, want ErrParticipantExists", err)
	}

	unknown, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Remove(unknown); !errors.Is(err, ErrParticipantNotFound) {
		t.Fatalf("Remove() unknown error = %v, want ErrParticipantNotFound", err)
	}
}

func TestRegistryGetAndRemove(t *testing.T) {
	r := New()
	p := newTestParticipant(t)
	id := p.Snapshot().ID

	if err := r.Register(p); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Fatal("Get() returned a different participant")
	}

	if err := r.Remove(id); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(id); !errors.Is(err, ErrParticipantNotFound) {
		t.Fatalf("Get() after Remove() error = %v, want ErrParticipantNotFound", err)
	}
}
