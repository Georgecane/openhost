package resource

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrNegativeCapacity = errors.New("resource capacity cannot be negative")
	ErrZeroRequirement  = errors.New("resource requirement must not be empty")
)

// ResourceFragment describes a bounded contribution of resources.
type ResourceFragment struct {
	CPU      CPUCapacity
	Memory   MemoryCapacity
	Storage  StorageCapacity
	Network  NetworkCapacity
	GPU      GPUCapacity
	Lifetime time.Duration
}

type CPUCapacity struct {
	Cores float64
}

type MemoryCapacity struct {
	Bytes uint64
}

type StorageCapacity struct {
	Bytes uint64
}

type NetworkCapacity struct {
	BitsPerSecond uint64
}

type GPUCapacity struct {
	Units uint32
}

func (r ResourceFragment) Validate() error {
	if r.CPU.Cores < 0 {
		return fmt.Errorf("%w: cpu", ErrNegativeCapacity)
	}
	if r.Lifetime < 0 {
		return fmt.Errorf("%w: lifetime", ErrNegativeCapacity)
	}
	return nil
}

func (r ResourceFragment) Empty() bool {
	return r.CPU.Cores == 0 &&
		r.Memory.Bytes == 0 &&
		r.Storage.Bytes == 0 &&
		r.Network.BitsPerSecond == 0 &&
		r.GPU.Units == 0
}

// Add combines two resource fragments. A zero lifetime means no explicit
// lifetime bound. For bounded fragments, the resulting lifetime is the
// shortest lifetime because an allocation cannot outlive its shortest-lived
// backing resource.
func (r ResourceFragment) Add(other ResourceFragment) ResourceFragment {
	lifetime := combineLifetime(r.Lifetime, other.Lifetime)

	return ResourceFragment{
		CPU: CPUCapacity{
			Cores: r.CPU.Cores + other.CPU.Cores,
		},
		Memory: MemoryCapacity{
			Bytes: r.Memory.Bytes + other.Memory.Bytes,
		},
		Storage: StorageCapacity{
			Bytes: r.Storage.Bytes + other.Storage.Bytes,
		},
		Network: NetworkCapacity{
			BitsPerSecond: r.Network.BitsPerSecond + other.Network.BitsPerSecond,
		},
		GPU: GPUCapacity{
			Units: r.GPU.Units + other.GPU.Units,
		},
		Lifetime: lifetime,
	}
}

func combineLifetime(a, b time.Duration) time.Duration {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

// Satisfies reports whether r can provide every requested resource.
func (r ResourceFragment) Satisfies(request ResourceFragment) bool {
	return r.CPU.Cores >= request.CPU.Cores &&
		r.Memory.Bytes >= request.Memory.Bytes &&
		r.Storage.Bytes >= request.Storage.Bytes &&
		r.Network.BitsPerSecond >= request.Network.BitsPerSecond &&
		r.GPU.Units >= request.GPU.Units &&
		(r.Lifetime == 0 || request.Lifetime == 0 || r.Lifetime >= request.Lifetime)
}

func ValidateRequirement(r ResourceFragment) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Empty() {
		return ErrZeroRequirement
	}
	return nil
}
