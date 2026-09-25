package scheduler

import (
	"testing"
	"time"

	"github.com/Georgecane/openhost/internal/fabric"
	"github.com/Georgecane/openhost/internal/resource"
)

func TestSchedulerRejectsOffersThatExpireBeforeRequirement(t *testing.T) {
	f := fabric.NewMemoryFabric()
	if err := f.Upsert(fabric.ResourceOffer{
		ParticipantID: "short-lived",
		Resources: resource.ResourceFragment{
			CPU:      resource.CPUCapacity{Cores: 4},
			Lifetime: 5 * time.Minute,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s, err := NewAggregatingScheduler(f)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Plan(resource.ResourceFragment{
		CPU:      resource.CPUCapacity{Cores: 1},
		Lifetime: 10 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected short-lived offer to be rejected")
	}
}
