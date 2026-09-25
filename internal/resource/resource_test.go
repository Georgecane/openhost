package resource

import (
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
