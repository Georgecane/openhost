# Contributing to OpenHost

OpenHost is an experimental distributed infrastructure project. Contributions are welcome in architecture, networking, scheduling, runtime design, storage, security, documentation, and testing.

## Development

OpenHost is written in Rust 2024.

```bash
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

## Architecture principles

1. Explicit participation only. No hidden resource usage.
2. Resource ownership remains with the participant.
3. Logical infrastructure must not depend on a single physical machine.
4. Components remain replaceable through explicit traits.
5. Network failure and participant churn are normal conditions.
6. A composite resource is a logical capacity contract, not shared physical memory.
7. The virtual machine layer must consume the distributed resource model rather than bypass it.

## Workflow

Large changes follow:

```
Design → Implement → Debug → Test → Fix → Verify → Continue
```

The repository should not move to a higher architectural layer until the current layer has passing tests and verified invariants.
