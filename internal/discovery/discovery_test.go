package discovery

import (
	"errors"
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/capability"
	"github.com/Georgecane/openhost/internal/identity"
)

func newTestAdvertisement(t *testing.T, sequence uint64, observedAt time.Time) Advertisement {
	t.Helper()

	participantID, err := identity.New(identity.ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}

	return Advertisement{
		Participant: participantID,
		Capability: capability.Capability{
			Compute: capability.ComputeCapability{CPUCores: 2},
			Memory:  capability.MemoryCapability{Bytes: 1 << 30},
		},
		Sequence:   sequence,
		ObservedAt: observedAt,
	}
}

func TestAdvertisementValidation(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	if err := advertisement.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	advertisement.ObservedAt = time.Time{}
	if err := advertisement.Validate(); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("zero timestamp error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestMemoryRegistryUpsertAndGet(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	r := NewMemoryRegistry()
	if err := r.Upsert(advertisement); err != nil {
		t.Fatal(err)
	}

	member, err := r.Get(advertisement.Participant)
	if err != nil {
		t.Fatal(err)
	}
	if member.Advertisement.Participant != advertisement.Participant {
		t.Fatalf("participant = %v, want %v", member.Advertisement.Participant, advertisement.Participant)
	}
	if !member.LastSeen.Equal(now) {
		t.Fatalf("last seen = %v, want %v", member.LastSeen, now)
	}
}

func TestMemoryRegistryRejectsSequenceRegression(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 2, now)

	r := NewMemoryRegistry()
	if err := r.Upsert(advertisement); err != nil {
		t.Fatal(err)
	}

	older := advertisement
	older.Sequence = 1
	older.ObservedAt = now.Add(time.Minute)

	if err := r.Upsert(older); !errors.Is(err, ErrInvalidAdvertisement) {
		t.Fatalf("regressed sequence error = %v, want ErrInvalidAdvertisement", err)
	}
}

func TestMemoryRegistryAllowsMonotonicUpdates(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	r := NewMemoryRegistry()
	if err := r.Upsert(advertisement); err != nil {
		t.Fatal(err)
	}

	updated := advertisement
	updated.Sequence = 2
	updated.ObservedAt = now.Add(time.Minute)
	if err := r.Upsert(updated); err != nil {
		t.Fatal(err)
	}

	member, err := r.Get(advertisement.Participant)
	if err != nil {
		t.Fatal(err)
	}
	if member.Advertisement.Sequence != 2 {
		t.Fatalf("sequence = %d, want 2", member.Advertisement.Sequence)
	}
}

func TestMemoryRegistryRemove(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	r := NewMemoryRegistry()
	if err := r.Upsert(advertisement); err != nil {
		t.Fatal(err)
	}
	if err := r.Remove(advertisement.Participant); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Get(advertisement.Participant); !errors.Is(err, ErrParticipantNotFound) {
		t.Fatalf("Get() error = %v, want ErrParticipantNotFound", err)
	}
}

func TestMemoryRegistryRejectsInvalidParticipantIdentity(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)
	advertisement.Participant.Kind = identity.LeaseKind

	r := NewMemoryRegistry()
	if err := r.Upsert(advertisement); !errors.Is(err, ErrInvalidParticipant) {
		t.Fatalf("Upsert() error = %v, want ErrInvalidParticipant", err)
	}
}
