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

The aggregation scheduler supports two deterministic strategies:

- `DeterministicId`: sorts validated participant offers by canonical participant ID.
- `MinFragmentation`: prefers a single full-fit offer with the least normalized excess capacity; otherwise it ranks candidates by balanced coverage across the requested resource dimensions.

Both strategies validate the complete offer set, reject duplicate participant identities, preserve allocation lifetime bounds, and produce a `CompositeResource` for the `LogicalNode`. `MinFragmentation` is a greedy heuristic, not a globally optimal placement solver.

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

- local participant registration and discovery
- scheduling and logical-node creation
- lease creation, expiry, and release
- virtual-machine creation, execution, start/stop, and destruction
- participant health audits and VM healing

Domain invariants remain in their respective modules. VM/node/lease registry transitions use explicit lock ordering and snapshot checks to reject stale concurrent updates. These are in-process consistency guarantees, not durable transactions.

## 9. Runtime and virtual machine

The runtime boundary is deliberately independent of resource discovery. The current implementation includes:

- `EchoRuntime` for validation and connectivity tests
- `FunctionRuntime` for in-memory handlers
- `ProcessRuntime` for explicitly allowlisted external programs with a timeout
- `VirtualMachine` as a leased control-plane object with a runtime specification and lifecycle state
- TCP transport with an mTLS mode and explicit participant-to-client-certificate authorization

The current VM is not yet a distributed CPU or a shared virtual-memory system. It represents a logical resource contract backed by participant allocations. The longer-term goal is to unify the **computational model**, not fake physical shared memory.

A future distributed virtual CPU may map execution units to different participants. Virtual memory may be local, remote, replicated, or sharded. Storage may be distributed. Transport and placement become runtime mechanisms.

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

## 12. Distributed execution engine (initial implementation)

ExecutionEngine accepts a batch of independent workloads. It expands each workload's execution plan into participant requests and sends the combined request set through a bounded worker pool. The concurrency limit applies to the whole batch, not separately to each workload. Responses are matched to request IDs and receipts are returned in the original workload order. Duplicate workload IDs are rejected within a batch.

The engine supports two execution modes. The batch API fans each complete workload out to participant units. The partitioned API accepts application-defined WorkloadPartition values, places them deterministically across participant units, sends each participant only its assigned partition, collects result bytes, and returns them ordered by partition index. A caller-supplied PartitionReducer defines how those results become an application result.

OpenHost deliberately does **not** split opaque payload bytes automatically: partition boundaries and computation semantics belong to the application. The current placement policy is round-robin, not yet resource-weighted or topology-aware. Dependency graphs, retry/idempotency, cancellation/deadlines, durable execution state, and shared virtual memory are still future work.

The worker pool is persistent for the lifetime of an ExecutionEngine instance. It bounds simultaneous outbound participant requests across concurrent batch submissions, and its bounded queue applies backpressure rather than creating one operating-system thread per allocation or batch. The limit is per engine instance, not process-wide.

## 13. Task lifecycle (in-memory coordinator)

The initial TaskManager wraps an ExecutionEngine and records one task per idempotency key. Task IDs are UUIDs. Re-submitting an identical submission with the same idempotency key returns the existing record; reusing that key with a different submission or retry limit is rejected.

The task state machine is:

```
Pending → Dispatched → Running → Completed
   │                         └──→ Failed → Pending (explicit retry)
   └──→ Cancelled
```

The first attempt is numbered 1. Retry is an explicit operation, increments the attempt number, and is bounded by max_attempts. Cancellation is currently supported only while a task is Pending; it does not interrupt work already running. A successful task record stores either its workload receipt or its partition outputs.

This registry is process-local and memory-only. A process restart loses task records and results. Retries are at-least-once: if a participant performed a side effect but its response was lost, a retry may repeat that work. Side-effecting runtimes must implement their own idempotency until task/attempt identity is propagated to durable participant-side deduplication. Exactly-once execution is not claimed.

Remaining execution work includes process-wide admission control across engine instances, deadlines and cooperative cancellation, dependency graphs, resource-weighted/topology-aware placement, durable task state, and participant-side deduplication/recovery.

