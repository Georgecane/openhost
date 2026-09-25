package scheduler

import (
	"testing"

	"github.com/Georgecane/openhost/internal/fabric"
	"github.com/Georgecane/openhost/internal/resource"
)

func TestAggregatingSchedulerBuildsLogicalNodeFromMultipleParticipants(t *testing.T) {
	f := fabric.NewMemoryFabric()

	offers := []fabric.ResourceOffer{
		{
			ParticipantID: "a",
			Resources: resource.ResourceFragment{
				CPU:    resource.CPUCapacity{Cores: 0.4},
				Memory: resource.MemoryCapacity{Bytes: 128},
			},
		},
		{
			ParticipantID: "b",
			Resources: resource.ResourceFragment{
				CPU:    resource.CPUCapacity{Cores: 0.6},
				Memory: resource.MemoryCapacity{Bytes: 256},
			},
		},
	}

	for _, offer := range offers {
		if err := f.Upsert(offer); err != nil {
			t.Fatal(err)
		}
	}

	s, err := NewAggregatingScheduler(f)
	if err != nil {
		t.Fatal(err)
	}

	n, err := s.Plan(resource.ResourceFragment{
		CPU:    resource.CPUCapacity{Cores: 1.0},
		Memory: resource.MemoryCapacity{Bytes: 384},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(n.Allocations) != 2 {
		t.Fatalf("got %d allocations, want 2", len(n.Allocations))
	}

	if n.Resources.CPU.Cores != 1.0 {
		t.Fatalf("got %.2f CPU cores, want 1.0", n.Resources.CPU.Cores)
	}

	if !n.Resources.Satisfies(resource.ResourceFragment{
		CPU:    resource.CPUCapacity{Cores: 1.0},
		Memory: resource.MemoryCapacity{Bytes: 384},
	}) {
		t.Fatal("logical node does not satisfy requirement")
	}
}

func TestAggregatingSchedulerRejectsInsufficientResources(t *testing.T) {
	f := fabric.NewMemoryFabric()
	_ = f.Upsert(fabric.ResourceOffer{
		ParticipantID: "a",
		Resources: resource.ResourceFragment{
			CPU: resource.CPUCapacity{Cores: 0.5},
		},
	})

	s, _ := NewAggregatingScheduler(f)
	_, err := s.Plan(resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: 1},
	})
	if err == nil {
		t.Fatal("expected insufficient resource error")
	}
}
