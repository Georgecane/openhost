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

The mTLS server requires a server certificate chain and private key, a CA bundle used to validate client certificates, and explicit participant-to-certificate grants. A grant binds a caller's participant ID to the exact client leaf certificate:

```rust
use std::sync::Arc;
use openhost::transport::{AuthorizedClientCertificate, TcpEndpointServer};

// `endpoint` implements ExecutionHandler and represents the target participant.
let caller_id = Identity::parse(caller_uuid, Kind::Participant)?;
let grant = AuthorizedClientCertificate::from_pem(
    caller_id.clone(),
    "certs/controller-client-cert.pem",
)?;
let server = TcpEndpointServer::bind_mtls(
    "0.0.0.0:7443",
    endpoint,
    "certs/server-chain.pem",
    "certs/server-key.pem",
    "certs/trusted-client-ca.pem",
    vec![grant],
)?;
```

Configure a client with its trusted server CA, client certificate chain, and private key:

```rust
use openhost::transport::TlsTcpTransport;

let transport = TlsTcpTransport::from_pem_files(
    "certs/trusted-server-ca.pem",
    "certs/controller-client-chain.pem",
    "certs/controller-client-key.pem",
)?;
```

For execution dispatch, supply the same caller identity used by the server's grant:

```rust
let dispatcher = ExecutionDispatcher::with_identity(
    caller_id,
    transport,
    execution_endpoints,
);
let receipt = dispatcher.dispatch(&plan, &workload)?;
```

The endpoint locator must contain a hostname or IP address and port. That hostname or IP address must match the server certificate's subject alternative name. To rotate a certificate, configure multiple grants for the same participant ID during the transition, then remove the old grant after rollout. TCP payloads carry wire-protocol version 1; peers reject unknown versions instead of attempting to decode incompatible messages.

**Certificate management remains the operator's responsibility.** Keep private keys out of source control and rotate/revoke certificates through your CA process. mTLS validates the certificate chain; the explicit grant additionally ensures a caller cannot claim a different participant ID using a certificate assigned to another identity. This implementation does not yet provide automated certificate issuance, revocation distribution, or a cluster-wide trust-policy control plane.

## Discovery and network beacons

Participants can broadcast their capabilities and resource availability using UDP beacons:

- `DiscoveryBeacon` periodically broadcasts binary-encoded `Advertisement` frames over UDP.
- `DiscoveryListener` listens for incoming discovery datagrams, verifies wire protocol version 1, and populates the discovery `Registry` with real-time freshness tracking (`Active` → `Stale` → `Expired`).

## Execution runtimes

OpenHost provides built-in concrete runtime engines:

- `ProcessRuntime`: executes external processes specified by `ProcessCommand` with args, environment variables, optional command allowlisting, and bounded timeouts.
- `EchoRuntime`: lightweight validation runtime for connectivity and integration verification.

## Fault tolerance and node healing

When a participant experiences churn, drains, or disconnects during execution:

- `audit_virtual_machine_health`: checks active VM leases against local registry state, expiration, and discovery status.
- `heal_virtual_machine`: dynamic reallocation that locates an alternative active participant offer in the fabric, releases the failed participant's lease, issues a replacement lease, and recomposes the `LogicalNode` capacity without tearing down the VM.

## CLI commands

The `openhost` CLI supports multiple operational modes:

```bash
# Run in-memory scheduling and resource allocation demo
cargo run --bin openhost -- demo

# Compute a multi-participant allocation plan
cargo run --bin openhost -- plan --cpu 2.5 --memory-mb 1024

# Launch participant daemon listening on TCP and broadcasting discovery beacon
cargo run --bin openhost -- participant --bind 127.0.0.1:7443 --cpu 4.0 --memory-mb 2048 --beacon 127.0.0.1:7444

# Dispatch execution to a participant endpoint
cargo run --bin openhost -- exec --endpoint 127.0.0.1:7443 --runtime echo
cargo run --bin openhost -- exec --endpoint 127.0.0.1:7443 --runtime process --program true
```

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
