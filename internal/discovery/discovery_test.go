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

func testPolicy() FreshnessPolicy {
	return FreshnessPolicy{
		StaleAfter:  10 * time.Second,
		ExpireAfter: 30 * time.Second,
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

func TestFreshnessPolicyValidation(t *testing.T) {
	if _, err := NewMemoryRegistry(FreshnessPolicy{}); !errors.Is(err, ErrInvalidFreshness) {
		t.Fatalf("NewMemoryRegistry() error = %v, want ErrInvalidFreshness", err)
	}
}

func TestMemoryRegistryUpsertAndGet(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)
	receivedAt := now.Add(2 * time.Second)

	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, receivedAt); err != nil {
		t.Fatal(err)
	}

	member, err := r.Get(advertisement.Participant)
	if err != nil {
		t.Fatal(err)
	}
	if !member.LastSeen.Equal(receivedAt) {
		t.Fatalf("last seen = %v, want %v", member.LastSeen, receivedAt)
	}
}

func TestMemoryRegistryRejectsObservationBeforeAdvertisement(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now.Add(-time.Second)); !errors.Is(err, ErrInvalidAdvertisement) {
		t.Fatalf("Upsert() error = %v, want ErrInvalidAdvertisement", err)
	}
}

func TestMemoryRegistryRejectsSequenceRegression(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 2, now)

	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now); err != nil {
		t.Fatal(err)
	}

	older := advertisement
	older.Sequence = 1
	older.ObservedAt = now.Add(time.Minute)

	if err := r.Upsert(older, now.Add(time.Minute)); !errors.Is(err, ErrInvalidAdvertisement) {
		t.Fatalf("regressed sequence error = %v, want ErrInvalidAdvertisement", err)
	}
}

func TestMemoryRegistryAllowsMonotonicUpdates(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now); err != nil {
		t.Fatal(err)
	}

	updated := advertisement
	updated.Sequence = 2
	updated.ObservedAt = now.Add(time.Minute)
	receivedAt := now.Add(61 * time.Second)
	if err := r.Upsert(updated, receivedAt); err != nil {
		t.Fatal(err)
	}

	member, err := r.Get(advertisement.Participant)
	if err != nil {
		t.Fatal(err)
	}
	if member.Advertisement.Sequence != 2 {
		t.Fatalf("sequence = %d, want 2", member.Advertisement.Sequence)
	}
	if !member.LastSeen.Equal(receivedAt) {
		t.Fatalf("last seen = %v, want %v", member.LastSeen, receivedAt)
	}
}

func TestMemberStateTransitionsByFreshness(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)
	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now); err != nil {
		t.Fatal(err)
	}
	member, err := r.Get(advertisement.Participant)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		at   time.Time
		want State
	}{
		{"active", now.Add(9 * time.Second), StateActive},
		{"stale boundary", now.Add(10 * time.Second), StateStale},
		{"stale", now.Add(20 * time.Second), StateStale},
		{"expired boundary", now.Add(30 * time.Second), StateExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := member.StateAt(tt.at, testPolicy())
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("StateAt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMemoryRegistryMembersAtValidatesTime(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)
	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now); err != nil {
		t.Fatal(err)
	}

	if _, err := r.MembersAt(time.Time{}); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("MembersAt() error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestMemoryRegistryRemove(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	advertisement := newTestAdvertisement(t, 1, now)

	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now); err != nil {
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

	r, err := NewMemoryRegistry(testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(advertisement, now); !errors.Is(err, ErrInvalidParticipant) {
		t.Fatalf("Upsert() error = %v, want ErrInvalidParticipant", err)
	}
}
