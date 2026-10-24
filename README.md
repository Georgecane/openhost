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

## Network transport and TLS

OpenHost currently provides two TCP transports:

- `TcpTransport` uses the custom framed protocol over plain TCP. Use it only on a trusted local network or for tests.
- `TlsTcpTransport` encrypts traffic and authenticates the server and client using mutual TLS (mTLS).

The mTLS server requires a server certificate chain and private key, plus a CA certificate bundle used to validate client certificates:

```rust
use std::sync::Arc;
use openhost::transport::{TcpEndpointServer, TlsTcpTransport};

// `endpoint` implements ExecutionHandler.
let server = TcpEndpointServer::bind_mtls(
    "0.0.0.0:7443",
    endpoint,
    "certs/server-chain.pem",
    "certs/server-key.pem",
    "certs/trusted-client-ca.pem",
)?;
```

Configure a client with its trusted server CA, client certificate chain, and private key:

```rust
use openhost::transport::TlsTcpTransport;

let transport = TlsTcpTransport::from_pem_files(
    "certs/trusted-server-ca.pem",
    "certs/client-chain.pem",
    "certs/client-key.pem",
)?;
```

Pass the resulting transport to the existing execution dispatcher in place of `TcpTransport`. The endpoint locator must contain a hostname or IP address and port. The hostname or IP address must match the server certificate's subject alternative name.

**Certificate management is the operator's responsibility.** Keep private keys out of source control, rotate and revoke certificates through your CA process, and scope client CA bundles narrowly. This mTLS implementation validates certificate chains, but the transport protocol does not yet bind a TLS certificate to a declared source-participant identity. Do not treat possession of a CA-issued certificate as a complete application authorization policy. For untrusted networks, use only certificates from a dedicated trust domain and enforce network-level access controls until participant-to-certificate authorization is implemented.

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
