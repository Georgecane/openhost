package resource

import "time"

// ResourceFragment describes a bounded, voluntary contribution of resources.
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
