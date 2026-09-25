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

## Design goals

- Distributed resource aggregation
- Logical infrastructure independent of physical machines
- Dynamic resource allocation and reallocation
- Support for persistent and ephemeral participants
- Strong workload isolation
- Fault tolerance and node churn handling
- Pluggable execution runtimes
- Cross-platform architecture
- Open protocols and implementation

## Initial runtime targets

OpenHost is designed to support multiple execution models:

- WebAssembly
- Functions
- Containers
- Distributed processes
- Virtual machines

## Status

OpenHost is in the architectural and foundational development stage.

The project is experimental. APIs and protocols are expected to evolve substantially.

## Contributing

Contributions, architecture discussions, experiments, and criticism are welcome.

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0
