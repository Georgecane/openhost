package fabric

import (
	"testing"

	"github.com/Georgecane/openhost/internal/resource"
)

func TestMemoryFabricUpsertAndRemove(t *testing.T) {
	f := NewMemoryFabric()

	err := f.Upsert(ResourceOffer{
		ParticipantID: "participant-a",
		Resources: resource.ResourceFragment{
			CPU: resource.CPUCapacity{Cores: 0.5},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := len(f.Offers()); got != 1 {
		t.Fatalf("got %d offers, want 1", got)
	}

	f.Remove("participant-a")

	if got := len(f.Offers()); got != 0 {
		t.Fatalf("got %d offers after remove, want 0", got)
	}
}
