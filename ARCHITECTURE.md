# OpenHost Architecture

## 1. Resource Fabric

The Resource Fabric represents available capabilities independently of physical machines.

```
Participant A ──┐
Participant B ──┼── Resource Fabric ── Scheduler ── Logical Node
Participant C ──┤
Participant D ──┘
```

A participant can contribute a small resource fragment for a bounded lifetime.

## 2. Resource Fragment

A fragment describes a bounded contribution rather than exposing an entire machine.

Example:

- 0.07 CPU cores
- 128 MiB memory
- 500 MiB storage
- 2 Mbps network capacity
- 15 minute lifetime

Fragments are additive for allocation purposes, but they do not imply shared memory or shared CPU semantics.

## 3. Composite Logical Resource

OpenHost represents the result of multi-participant allocation as a `CompositeResource`.

```
Participant A ──┐
Participant B ──┼──> CompositeResource
Participant C ──┘          │
                           ├── Capacity
                           └── Allocations
```

The composite resource has two inseparable views:

1. **Capacity** — the aggregate resource presented to the logical node.
2. **Allocations** — the participant fragments that physically back that capacity.

For example:

```
CompositeResource
├── Capacity
│   ├── CPU: 3.0 cores
│   └── Memory: 12 GiB
│
└── Allocations
    ├── Participant A: 1.0 CPU / 4 GiB
    ├── Participant B: 0.5 CPU / 2 GiB
    └── Participant C: 1.5 CPU / 6 GiB
```

The aggregate is therefore a **logical resource**, not a claim that three machines have become one physical machine.

## 4. Multi-participant allocation

A logical node may be backed by several participants:

```
Logical Node X
└── CompositeResource
    ├── Participant A: 20%
    ├── Participant B: 30%
    └── Participant C: 50%
```

The scheduler constructs the composite resource directly from the allocations it selected. This makes the allocation map part of the logical resource representation instead of keeping aggregate capacity and physical backing as unrelated pieces of state.

If a participant leaves, the scheduler can reconstruct the allocation from the remaining fabric when the workload permits it.

## 5. Scheduler

The scheduler matches workload requirements against the capabilities visible in the fabric.

It asks:

> Which set of capabilities can execute this workload?

rather than:

> Which server should run this workload?

The first scheduler implementation is deliberately simple and deterministic. More advanced policies can later consider topology, latency, reliability, trust, energy, locality, GPU capability, and cost.

The scheduler currently produces a canonical composite resource:

```
Offers
  ↓
Selected allocations
  ↓
CompositeResource
  ├── aggregate capacity
  └── backing allocations
```

## 6. Runtime

The runtime executes workloads against logical resources.

The runtime is intentionally abstract so that WebAssembly, functions, containers, distributed processes, and virtual machines can be introduced without coupling them to resource discovery.

A critical distinction is maintained:

```
Logical aggregation ≠ physical resource fusion
```

OpenHost can expose 3 CPU cores aggregated from multiple participants as one **logical capacity contract**, but an ordinary process cannot automatically execute arbitrary instructions across those machines as if they shared one CPU cache hierarchy and one RAM address space.

To make the logical resource executable, a runtime must map the composite resource onto a distributed execution model. Examples include task partitioning, actor/process placement, sharding, remote memory services, or other explicitly distributed execution mechanisms.

## 7. Control and data planes

```
OpenHost
├── Control Plane
│   ├── Discovery
│   ├── Identity
│   ├── Resource Fabric
│   └── Scheduler
│
└── Data Plane
    ├── Transport
    ├── Execution
    └── Storage
```

The control plane decides **what logical resource exists and which participants back it**. The data plane determines **how execution and data movement actually use those allocations**.

## 8. Important constraint

OpenHost does not attempt to create a conventional shared-memory computer from arbitrary remote machines.

Network latency makes that abstraction impractical for general workloads. Existing disaggregated-computing research likewise treats network characteristics as a fundamental constraint, and practical systems often rely on high-performance interconnects or specialized mechanisms when exposing remote memory or other resources.

Therefore:

```
Resource Composition
        ↓
Logical Resource Contract
        ↓
Distributed Runtime
        ↓
Execution
```

rather than:

```
Remote Machines
        ↓
pretend they are one normal computer
```

The composite resource model gives OpenHost a concrete boundary where future distributed runtimes can implement the actual execution semantics without corrupting the resource-management model.
