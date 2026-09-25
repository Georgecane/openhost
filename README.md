# OpenHost

> **The infrastructure is the network, not the machine.**

OpenHost is an open-source distributed infrastructure for hosting and cloud computing.

Instead of treating a server as a single physical machine, OpenHost treats infrastructure as a **resource fabric**. Small, voluntary resource contributions from many devices can be discovered, aggregated, scheduled, and assigned to logical nodes.

## Core idea

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
Execution Runtime
      ↓
Applications / Services
```

A logical node is an allocation of distributed resources, not a physical server.

## Design principles

- Resource ownership remains with participants.
- Participation is explicit and voluntary.
- A participant may contribute only a fraction of its resources.
- Logical node identity is independent of physical participants.
- Network latency is treated as a first-class scheduling constraint.
- Participant churn is expected.
- Interfaces separate the resource fabric from execution runtimes.
- The architecture is cross-platform.

## Runtime targets

OpenHost is designed to support multiple execution models:

- WebAssembly
- Functions
- Containers
- Distributed processes
- Virtual machines

## Current status

OpenHost is in the foundational development stage.

The current implementation establishes the resource model, in-memory resource fabric, multi-participant aggregation scheduler, and logical-node allocation model.

## Development

```bash
go test ./...
go run ./cmd/openhost
```

## Contributing

Contributions, architecture discussions, experiments, and criticism are welcome.

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

OpenHost is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE).
