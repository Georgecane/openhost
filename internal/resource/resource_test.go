package resource

import (
	"errors"
	"testing"
	"time"
)

func TestAddUsesShortestBoundedLifetime(t *testing.T) {
	a := ResourceFragment{
		CPU:      CPUCapacity{Cores: 1},
		Lifetime: 30 * time.Minute,
	}
	b := ResourceFragment{
		CPU:      CPUCapacity{Cores: 2},
		Lifetime: 10 * time.Minute,
	}

	got := a.Add(b)

	if got.CPU.Cores != 3 {
		t.Fatalf("got %.2f cores, want 3", got.CPU.Cores)
	}
	if got.Lifetime != 10*time.Minute {
		t.Fatalf("got lifetime %s, want 10m", got.Lifetime)
	}
}

func TestZeroLifetimeMeansUnbounded(t *testing.T) {
	got := ResourceFragment{Lifetime: 0}.Add(ResourceFragment{Lifetime: time.Hour})
	if got.Lifetime != time.Hour {
		t.Fatalf("got lifetime %s, want 1h", got.Lifetime)
	}
}

func TestComposeCreatesUnifiedLogicalResource(t *testing.T) {
	composed, err := Compose([]Allocation{
		{
			ParticipantID: "a",
			Resources: ResourceFragment{
				CPU:    CPUCapacity{Cores: 0.75},
				Memory: MemoryCapacity{Bytes: 512},
			},
		},
		{
			ParticipantID: "b",
			Resources: ResourceFragment{
				CPU:    CPUCapacity{Cores: 1.25},
				Memory: MemoryCapacity{Bytes: 1024},
			},
		},
	})
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}

	if composed.Capacity.CPU.Cores != 2 {
		t.Fatalf("capacity CPU = %.2f, want 2", composed.Capacity.CPU.Cores)
	}
	if composed.Capacity.Memory.Bytes != 1536 {
		t.Fatalf("capacity memory = %d, want 1536", composed.Capacity.Memory.Bytes)
	}
	if len(composed.Allocations) != 2 {
		t.Fatalf("allocations = %d, want 2", len(composed.Allocations))
	}
	if err := composed.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	allocation, ok := composed.AllocationFor("b")
	if !ok {
		t.Fatal("AllocationFor(b) did not find allocation")
	}
	if allocation.Resources.Memory.Bytes != 1024 {
		t.Fatalf("allocation memory = %d, want 1024", allocation.Resources.Memory.Bytes)
	}
}

func TestComposeRejectsDuplicateParticipants(t *testing.T) {
	_, err := Compose([]Allocation{
		{ParticipantID: "a", Resources: ResourceFragment{CPU: CPUCapacity{Cores: 1}}},
		{ParticipantID: "a", Resources: ResourceFragment{CPU: CPUCapacity{Cores: 1}}},
	})
	if !errors.Is(err, ErrDuplicateAllocation) {
		t.Fatalf("Compose() error = %v, want ErrDuplicateAllocation", err)
	}
}

func TestComposeRejectsEmptyAllocation(t *testing.T) {
	_, err := Compose([]Allocation{{ParticipantID: "a"}})
	if !errors.Is(err, ErrInvalidAllocation) {
		t.Fatalf("Compose() error = %v, want ErrInvalidAllocation", err)
	}
}

func TestCompositeResourceDetectsTamperedCapacity(t *testing.T) {
	composed, err := Compose([]Allocation{
		{ParticipantID: "a", Resources: ResourceFragment{CPU: CPUCapacity{Cores: 1}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	composed.Capacity.CPU.Cores = 2
	if err := composed.Validate(); !errors.Is(err, ErrInvalidAllocation) {
		t.Fatalf("Validate() error = %v, want ErrInvalidAllocation", err)
	}
}
