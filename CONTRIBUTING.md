# Contributing to OpenHost

OpenHost is an experimental infrastructure project and welcomes contributions in architecture, networking, scheduling, runtime design, storage, security, documentation, and testing.

## Before contributing

For large architectural changes, open a GitHub Discussion or Issue first so the design can be discussed before implementation.

## Areas

- Resource model
- Fabric and discovery
- Scheduling
- Logical node management
- Runtime isolation
- Networking and transport
- Storage
- Identity and security
- Observability
- Cross-platform support
- Documentation

## Development

OpenHost is written in Go.

```bash
go test ./...
go run ./cmd/openhost
```

## Principles

1. Explicit participation only. No hidden resource usage.
2. Resource ownership remains with the participant.
3. Logical infrastructure must not depend on a single physical machine.
4. Components should remain replaceable through interfaces.
5. Network failure and participant churn are normal conditions, not exceptional cases.
