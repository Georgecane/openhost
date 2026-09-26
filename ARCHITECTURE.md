# OpenHost Architecture

## 1. Core model

OpenHost treats infrastructure as a distributed resource fabric.

```
Physical Devices
      ↓
Resource Fragments
      ↓
Participation
      ↓
Resource Fabric
      ↓
Composition
      ↓
Logical Nodes
      ↓
Distributed Runtime
      ↓
Applications
```

The physical machine is a contributor, not the fundamental abstraction.

## 2. Composite resources

A `CompositeResource` is the canonical representation of a logical resource assembled from independent allocations.

```
Participant A ──┐
Participant B ──┼──> CompositeResource
Participant C ──┘          │
                           ├── Capacity
                           └── Allocations
```

Capacity is the aggregate resource visible to the logical node. Allocations preserve the physical distribution that backs it.

This distinction is fundamental:

```
Logical aggregation ≠ physical resource fusion
```

Three remote participants do not become one conventional shared-memory CPU and RAM system. The composite is a capacity contract that a distributed runtime must execute explicitly.

## 3. Resource semantics

Resource fragments contain CPU, memory, storage, network, GPU, and lifetime.

Lifetime uses an explicit Rust `Option<Duration>`:

- `Some(duration)` means bounded.
- `None` means unbounded.

When resources are composed, the shortest bounded lifetime wins.

## 4. Participation

Participants have a lifecycle:

```
joining → active → draining → left
joining → left
active → left
draining → left
```

Only active participants publish scheduler-visible offers.

The participant registry is concurrency-safe and owns the membership index, while the participant owns its lifecycle state.

## 5. Discovery

Discovery is separate from local participation.

Advertisements contain:

- participant identity
- capabilities
- monotonic sequence
- sender observation timestamp

The discovery registry tracks local `last_seen` time and classifies members as:

```
ACTIVE → STALE → EXPIRED
```

Freshness is evaluated by policy rather than silently changing participant ownership.

## 6. Scheduler

The first scheduler is deterministic and aggregation-based:

```
Offers
  ↓
Requirement matching
  ↓
Partial allocations
  ↓
CompositeResource
  ↓
LogicalNode
```

It sorts participant IDs to make planning deterministic.

Future scheduling policies can incorporate latency, topology, reliability, trust, energy, locality, GPU topology, cost, and failure domains.

## 7. Leases

A logical-node allocation can produce one lease per physical allocation.

```
LogicalNode
   │
   ├── Lease → Participant A
   ├── Lease → Participant B
   └── Lease → Participant C
```

Lease duration cannot exceed the lifetime of its backing allocation.

## 8. Control plane

The control plane coordinates:

- local participant registration
- discovery
- scheduling
- logical-node creation
- lease creation

It does not own the domain invariants of those components. Those remain in their respective modules.

## 9. Runtime and future VM

The runtime boundary is deliberately independent of resource discovery.

The intended progression is:

```
Resource Fabric
      ↓
Resource Composition
      ↓
Logical Machine Contract
      ↓
Distributed Execution Model
      ↓
Virtual Machine
```

The future VM should unify the **computational model**, not fake physical shared memory.

A distributed virtual CPU may map execution units to different participants. Virtual memory may be local, remote, replicated, or sharded. Storage may be distributed. Transport and placement become runtime mechanisms.

The VM therefore consumes a composite resource rather than creating a separate abstraction that hides the fabric.

## 10. Concurrency model

The Rust implementation uses ownership plus explicit synchronization:

- `RwLock` for shared registries and lifecycle state
- `Arc` for shared ownership
- immutable snapshots for cross-component observation
- deterministic ordering where scheduling decisions must be reproducible

The current implementation deliberately avoids unsafe Rust.

## 11. Layer boundary

The project follows:

```
Design → Implement → Debug → Test → Fix → Verify → Continue
```

A higher layer should not become authoritative until the current layer's invariants are tested and verified.
