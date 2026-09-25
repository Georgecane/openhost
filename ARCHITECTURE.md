# OpenHost Architecture

## 1. Resource Fabric

The Resource Fabric represents available computational resources independently of physical machines.

```
Participant A ──┐
Participant B ──┼── Resource Fabric ── Logical Node
Participant C ──┤
Participant D ──┘
```

A participant can contribute a small resource fragment for a limited lifetime.

## 2. Resource Fragment

A fragment describes a bounded capability rather than exposing an entire machine.

Examples:

- 0.07 CPU cores
- 128 MiB memory
- 500 MiB storage
- 2 Mbps network capacity
- 15 minute lifetime

## 3. Logical Nodes

A Logical Node is an allocation of capabilities selected from the fabric.

Its identity is independent of the physical participants currently backing it.

If a participant disappears, the scheduler can replace the lost allocation when possible.

## 4. Scheduler

The scheduler matches workload requirements against capabilities.

It should answer:

> Which set of capabilities can execute this workload?

rather than:

> Which server should run this workload?

## 5. Runtime

The runtime executes workloads against logical resources.

The initial architecture keeps the runtime abstract so multiple execution models can be introduced without changing the resource fabric.

## 6. Control and data planes

```
OpenHost
├── Control Plane
│   ├── Discovery
│   ├── Scheduler
│   └── Identity
│
└── Data Plane
    ├── Execution
    └── Storage
```

## 7. Important constraint

OpenHost does not attempt to create a conventional shared-memory computer from remote machines.

Network latency makes that abstraction impractical for general workloads.

Instead, OpenHost provides **distributed execution over dynamically assembled logical resources**.
