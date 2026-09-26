# OpenHost

> **The infrastructure is the network, not the machine.**

OpenHost is an open-source distributed infrastructure for hosting and cloud computing.

Instead of treating a server as a single physical machine, OpenHost treats infrastructure as a **resource fabric**. Small, voluntary resource contributions from many devices can be discovered, aggregated, scheduled, and assigned to logical nodes.

## Architecture

```
Physical Devices
      ↓
Resource Fragments
      ↓
Participation Layer
      ↓
Resource Fabric
      ↓
Resource Aggregation
      ↓
Logical Nodes
      ↓
Distributed Runtime
      ↓
Applications / Services
```

A logical node is a logical resource contract backed by independent participant allocations. It is not a claim that remote machines share CPU caches or one physical address space.

## Rust rewrite

OpenHost is implemented in **Rust 2024**. The rewrite uses Rust's ownership and synchronization primitives as the foundation for a future distributed execution engine and virtual-machine abstraction.

The current code is organized as explicit domain modules:

- resource composition
- identity
- capability
- participant lifecycle
- resource fabric
- discovery
- registry
- scheduling
- logical nodes
- leases
- control plane
- runtime

The long-term runtime boundary is:

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

The VM is intended to consume the Resource Fabric rather than pretending that arbitrary remote machines are one conventional shared-memory computer.

## Runtime targets

OpenHost is designed to support multiple execution models:

- WebAssembly
- Functions
- Containers
- Distributed processes
- Virtual machines

## Development

Requires a current stable Rust toolchain.

```bash
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo run --bin openhost
```

## License

OpenHost is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE).
