package main

import (
	"fmt"

	"github.com/Georgecane/openhost/internal/fabric"
	"github.com/Georgecane/openhost/internal/resource"
	"github.com/Georgecane/openhost/internal/scheduler"
)

func main() {
	resourceFabric := fabric.NewMemoryFabric()

	_ = resourceFabric.Upsert(fabric.ResourceOffer{
		ParticipantID: "local-demo",
		Resources: resource.ResourceFragment{
			CPU:    resource.CPUCapacity{Cores: 1},
			Memory: resource.MemoryCapacity{Bytes: 256 << 20},
		},
	})

	s, err := scheduler.NewAggregatingScheduler(resourceFabric)
	if err != nil {
		panic(err)
	}

	node, err := s.Plan(resource.ResourceFragment{
		CPU:    resource.CPUCapacity{Cores: 0.5},
		Memory: resource.MemoryCapacity{Bytes: 128 << 20},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("OpenHost")
	fmt.Println("The infrastructure is the network, not the machine.")
	fmt.Printf(
		"logical node: %s (%d participant allocation, %.2f CPU cores, %d bytes memory)\n",
		node.ID,
		len(node.Resources.Allocations),
		node.Resources.Capacity.CPU.Cores,
		node.Resources.Capacity.Memory.Bytes,
	)
}
