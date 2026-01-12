# DefraDB.rs

Rust implementation of [DefraDB](https://github.com/sourcenetwork/defradb).

## Status

🚧 **Early exploration** - Implementing CRDT subsystem, testing against Go implementation.

## Documentation

**The Go implementation is the source of truth.**

For DefraDB documentation, architecture, and specifications:
- [DefraDB (Go)](https://github.com/sourcenetwork/defradb)
- [DefraDB Docs](https://docs.source.network/defradb)

## Building

```bash
# Build all crates
cargo build

# Run tests
cargo test

# Run tests for specific crate
cargo test -p crdt

# Lint and format
cargo clippy --all
cargo fmt --all
```

## Structure

```
crates/
├── defra-core/      # Core types and traits
├── crdt/            # CRDT implementations
├── storage/         # Multi-store architecture
├── blockstore/      # IPLD block storage
├── schema/          # Schema validation
├── query/           # Query planner
├── p2p/             # P2P networking
├── crypto/          # Cryptographic operations
├── http/            # HTTP API server
└── cli/             # Command-line interface
```

See `CLAUDE.md` for development principles and workflow.

## License

Apache-2.0 OR MIT
