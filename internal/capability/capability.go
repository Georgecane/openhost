package capability

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidCapability = errors.New("invalid capability")
	ErrNegativeValue     = errors.New("capability value must not be negative")
	ErrInvalidLatency    = errors.New("latency must not be negative")
)

type Capability struct {
	Compute     ComputeCapability
	Memory      MemoryCapability
	Storage     StorageCapability
	Network     NetworkCapability
	Reliability ReliabilityProfile
	Latency     LatencyProfile
	Lifetime    LifetimeProfile
	Security    SecurityProfile
}

type ComputeCapability struct {
	CPUCores float64
	GPUUnits uint32
}

type MemoryCapability struct {
	Bytes uint64
}

type StorageCapability struct {
	Bytes uint64
}

type NetworkCapability struct {
	BitsPerSecond uint64
}

type ReliabilityProfile struct {
	Availability float64
}

type LatencyProfile struct {
	ToParticipant time.Duration
}

type LifetimeProfile struct {
	Duration time.Duration
}

type SecurityProfile struct {
	Trusted bool
}

func (c Capability) Validate() error {
	if c.Compute.CPUCores < 0 {
		return fmt.Errorf("%w: cpu cores: %v", ErrNegativeValue, c.Compute.CPUCores)
	}
	if c.Memory.Bytes == 0 && c.Storage.Bytes == 0 && c.Network.BitsPerSecond == 0 &&
		c.Compute.CPUCores == 0 && c.Compute.GPUUnits == 0 {
		return fmt.Errorf("%w: no resource capacity", ErrInvalidCapability)
	}
	if c.Reliability.Availability < 0 || c.Reliability.Availability > 1 {
		return fmt.Errorf("%w: availability must be between 0 and 1", ErrInvalidCapability)
	}
	if c.Latency.ToParticipant < 0 {
		return fmt.Errorf("%w: latency", ErrInvalidLatency)
	}
	if c.Lifetime.Duration < 0 {
		return fmt.Errorf("%w: lifetime", ErrNegativeValue)
	}
	return nil
}

func (c Capability) AvailableFor(duration time.Duration) bool {
	if duration < 0 {
		return false
	}
	return c.Lifetime.Duration == 0 || c.Lifetime.Duration >= duration
}
