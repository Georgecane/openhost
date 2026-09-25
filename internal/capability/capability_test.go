package capability

import (
	"errors"
	"testing"
	"time"
)

func TestCapabilityValidate(t *testing.T) {
	c := Capability{
		Compute:     ComputeCapability{CPUCores: 4, GPUUnits: 1},
		Memory:      MemoryCapability{Bytes: 8 << 30},
		Network:     NetworkCapability{BitsPerSecond: 1_000_000_000},
		Reliability: ReliabilityProfile{Availability: 0.99},
		Latency:     LatencyProfile{ToParticipant: 10 * time.Millisecond},
		Lifetime:    LifetimeProfile{Duration: time.Hour},
	}

	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCapabilityValidateRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		c    Capability
		err  error
	}{
		{
			name: "negative cpu",
			c:    Capability{Compute: ComputeCapability{CPUCores: -1}},
			err:  ErrNegativeValue,
		},
		{
			name: "invalid availability",
			c: Capability{
				Compute:     ComputeCapability{CPUCores: 1},
				Reliability: ReliabilityProfile{Availability: 1.1},
			},
			err: ErrInvalidCapability,
		},
		{
			name: "negative latency",
			c: Capability{
				Compute: ComputeCapability{CPUCores: 1},
				Latency: LatencyProfile{ToParticipant: -time.Second},
			},
			err: ErrInvalidLatency,
		},
		{
			name: "negative lifetime",
			c: Capability{
				Compute:  ComputeCapability{CPUCores: 1},
				Lifetime: LifetimeProfile{Duration: -time.Second},
			},
			err: ErrNegativeValue,
		},
		{
			name: "no capacity",
			c:    Capability{Reliability: ReliabilityProfile{Availability: 1}},
			err:  ErrInvalidCapability,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.Validate()
			if !errors.Is(err, tt.err) {
				t.Fatalf("Validate() error = %v, want errors.Is(..., %v)", err, tt.err)
			}
		})
	}
}

func TestCapabilityAvailableFor(t *testing.T) {
	c := Capability{
		Compute:  ComputeCapability{CPUCores: 2},
		Lifetime: LifetimeProfile{Duration: 30 * time.Minute},
	}

	if !c.AvailableFor(30 * time.Minute) {
		t.Fatal("capability should satisfy exact lifetime")
	}
	if c.AvailableFor(31 * time.Minute) {
		t.Fatal("capability should reject longer lifetime")
	}
	if c.AvailableFor(-time.Second) {
		t.Fatal("capability should reject negative duration")
	}

	unbounded := Capability{
		Compute: ComputeCapability{CPUCores: 1},
	}
	if !unbounded.AvailableFor(24 * time.Hour) {
		t.Fatal("zero lifetime should represent an unbounded lifetime")
	}
}
