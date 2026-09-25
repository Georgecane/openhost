package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/Georgecane/openhost/internal/fabric"
	"github.com/Georgecane/openhost/internal/node"
	"github.com/Georgecane/openhost/internal/resource"
)

var (
	ErrInsufficientResources = errors.New("insufficient resources in fabric")
	ErrNilFabric             = errors.New("fabric must not be nil")
)

type Scheduler interface {
	Plan(requirement resource.ResourceFragment) (node.LogicalNode, error)
}

// AggregatingScheduler builds logical nodes from multiple resource offers.
type AggregatingScheduler struct {
	fabric fabric.Fabric
	serial atomic.Uint64
}

func NewAggregatingScheduler(f fabric.Fabric) (*AggregatingScheduler, error) {
	if f == nil {
		return nil, ErrNilFabric
	}
	return &AggregatingScheduler{fabric: f}, nil
}

func (s *AggregatingScheduler) Plan(requirement resource.ResourceFragment) (node.LogicalNode, error) {
	if err := resource.ValidateRequirement(requirement); err != nil {
		return node.LogicalNode{}, err
	}

	offers := append([]fabric.ResourceOffer(nil), s.fabric.Offers()...)
	sort.Slice(offers, func(i, j int) bool {
		return offers[i].ParticipantID < offers[j].ParticipantID
	})

	var total resource.ResourceFragment
	allocations := make([]node.Allocation, 0, len(offers))

	for _, offer := range offers {
		if offer.ParticipantID == "" || offer.Resources.Empty() {
			continue
		}

		remaining := remainingResources(requirement, total)
		share := minFragment(offer.Resources, remaining)
		if share.Empty() {
			continue
		}

		allocations = append(allocations, node.Allocation{
			ParticipantID: offer.ParticipantID,
			Resources:     share,
		})
		total = total.Add(share)

		if total.Satisfies(requirement) {
			break
		}
	}

	if !total.Satisfies(requirement) {
		return node.LogicalNode{}, fmt.Errorf(
			"%w: requested=%+v available=%+v",
			ErrInsufficientResources,
			requirement,
			total,
		)
	}

	return node.LogicalNode{
		ID:          fmt.Sprintf("logical-%d", s.serial.Add(1)),
		Resources:   total,
		Allocations: allocations,
	}, nil
}

func remainingResources(requirement, allocated resource.ResourceFragment) resource.ResourceFragment {
	return resource.ResourceFragment{
		CPU: resource.CPUCapacity{
			Cores: maxFloat(requirement.CPU.Cores-allocated.CPU.Cores, 0),
		},
		Memory: resource.MemoryCapacity{
			Bytes: maxUint64(requirement.Memory.Bytes, allocated.Memory.Bytes) - allocated.Memory.Bytes,
		},
		Storage: resource.StorageCapacity{
			Bytes: maxUint64(requirement.Storage.Bytes, allocated.Storage.Bytes) - allocated.Storage.Bytes,
		},
		Network: resource.NetworkCapacity{
			BitsPerSecond: maxUint64(requirement.Network.BitsPerSecond, allocated.Network.BitsPerSecond) - allocated.Network.BitsPerSecond,
		},
		GPU: resource.GPUCapacity{
			Units: maxUint32(requirement.GPU.Units, allocated.GPU.Units) - allocated.GPU.Units,
		},
		Lifetime: requirement.Lifetime,
	}
}

func minFragment(available, required resource.ResourceFragment) resource.ResourceFragment {
	if required.Lifetime > 0 && available.Lifetime > 0 && available.Lifetime < required.Lifetime {
		return resource.ResourceFragment{}
	}

	lifetime := required.Lifetime
	if available.Lifetime > 0 && (lifetime == 0 || available.Lifetime < lifetime) {
		lifetime = available.Lifetime
	}

	return resource.ResourceFragment{
		CPU: resource.CPUCapacity{Cores: minFloat(available.CPU.Cores, required.CPU.Cores)},
		Memory: resource.MemoryCapacity{
			Bytes: minUint64(available.Memory.Bytes, required.Memory.Bytes),
		},
		Storage: resource.StorageCapacity{
			Bytes: minUint64(available.Storage.Bytes, required.Storage.Bytes),
		},
		Network: resource.NetworkCapacity{
			BitsPerSecond: minUint64(available.Network.BitsPerSecond, required.Network.BitsPerSecond),
		},
		GPU: resource.GPUCapacity{
			Units: minUint32(available.GPU.Units, required.GPU.Units),
		},
		Lifetime: lifetime,
	}
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
