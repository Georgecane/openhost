package identity

import (
	"errors"
	"testing"
)

func TestNewCreatesValidParticipantIdentity(t *testing.T) {
	first, err := New(ParticipantKind)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	second, err := New(ParticipantKind)
	if err != nil {
		t.Fatalf("New() second error = %v", err)
	}

	if err := first.Validate(); err != nil {
		t.Fatalf("first identity invalid: %v", err)
	}
	if err := second.Validate(); err != nil {
		t.Fatalf("second identity invalid: %v", err)
	}
	if first.ID == second.ID {
		t.Fatal("New() generated duplicate identities")
	}
	if first.Kind != ParticipantKind || second.Kind != ParticipantKind {
		t.Fatal("New() returned the wrong identity kind")
	}
}

func TestParsePreservesIdentity(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"

	got, err := Parse(id, LogicalNodeKind)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.ID != id {
		t.Fatalf("Parse() ID = %q, want %q", got.ID, id)
	}
	if got.Kind != LogicalNodeKind {
		t.Fatalf("Parse() Kind = %q, want %q", got.Kind, LogicalNodeKind)
	}
}

func TestParseRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name string
		id   string
		kind Kind
		err  error
	}{
		{"empty", "", ParticipantKind, ErrEmptyID},
		{"malformed", "not-a-uuid", ParticipantKind, ErrInvalidID},
		{"wrong-version", "550e8400-e29b-11d4-a716-446655440000", ParticipantKind, ErrInvalidID},
		{"wrong-variant", "550e8400-e29b-41d4-0716-446655440000", ParticipantKind, ErrInvalidID},
		{"invalid-kind", "550e8400-e29b-41d4-a716-446655440000", "unknown", ErrInvalidID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.id, tt.kind)
			if !errors.Is(err, tt.err) {
				t.Fatalf("Parse() error = %v, want errors.Is(..., %v)", err, tt.err)
			}
		})
	}
}

func TestIdentityKindsAreDistinct(t *testing.T) {
	participant, err := New(ParticipantKind)
	if err != nil {
		t.Fatal(err)
	}
	node, err := Parse(participant.ID, LogicalNodeKind)
	if err != nil {
		t.Fatal(err)
	}

	if participant.Kind == node.Kind {
		t.Fatal("participant and logical-node identities must have distinct kinds")
	}
}
