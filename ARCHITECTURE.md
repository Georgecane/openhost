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

## 3. Multi-participant allocation

A logical node may be backed by several participants:

```
Logical Node X
├── Participant A: 20%
├── Participant B: 30%
└── Participant C: 50%
```

If a participant leaves, the scheduler can reconstruct the allocation from the remaining fabric when the workload permits it.

## 4. Logical Nodes

A Logical Node is an abstract execution allocation.

Its identity is independent of the physical participants currently backing it.

The node records its contributing allocations so that the control plane can reason about placement, ownership, lifetime, and recovery.

## 5. Scheduler

The scheduler matches workload requirements against the capabilities visible in the fabric.

It asks:

> Which set of capabilities can execute this workload?

rather than:

> Which server should run this workload?

The first scheduler implementation is deliberately simple and deterministic. More advanced policies can later consider topology, latency, reliability, trust, energy, locality, GPU capability, and cost.

## 6. Runtime

The runtime executes workloads against logical resources.

The runtime is intentionally abstract so that WebAssembly, functions, containers, distributed processes, and virtual machines can be introduced without coupling them to resource discovery.

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

## 8. Important constraint

OpenHost does not attempt to create a conventional shared-memory computer from remote machines.

Network latency makes that abstraction impractical for general workloads.

Instead, OpenHost provides **distributed execution over dynamically assembled logical resources**. Workloads must be scheduled according to the communication patterns and capabilities they actually require.
