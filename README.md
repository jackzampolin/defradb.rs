# DefraDB.rs

Rust implementation of [DefraDB](https://github.com/sourcenetwork/defradb), designed for embedded, edge, and WASM deployments with **full Go DefraDB network interoperability**.

## Status

✅ **~85% feature complete** - Query engine (~80%), P2P sync, ACP (NAC), indexing, and identity systems working. Go/Rust nodes can connect and replicate data.

See [Issue #18](https://github.com/sourcenetwork/defradb.rs/issues/18) for the full roadmap.

## The North Star: Go Interoperability

**The Go implementation is the source of truth.** This Rust implementation must behave identically to Go DefraDB in all observable ways:

- Same GraphQL query results
- Same document IDs (content-addressed)
- Same CRDT merge behavior
- Same P2P protocol (libp2p + Bitswap)

The **interop test suite** (`tests/interop/`) validates this. If the interop tests pass, the implementations are compatible.

## Go/Rust Interop Tests (Critical)

The interop tests prove that Rust and Go nodes can work together. **Run these before any PR that touches P2P, query, or schema code.**

```bash
cd tests/interop

# Build both Rust and Go binaries
make build-all

# Run all interop tests
make test

# Run just connection tests (faster, Rust-only)
make test-connection

# Run cross-implementation tests
make test-cross
```

**Requirements:**
- Go DefraDB repo (set `DEFRA_GO_PATH`, e.g., `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb`)
- Go 1.21+
- Rust toolchain

## Building

```bash
# Build all crates
cargo build

# Build release binary
cargo build --release -p cli

# Run Rust unit tests
cargo test

# Lint and format
cargo clippy --all -- -D warnings
cargo fmt --all
```

## Structure

```
crates/
├── acp/             # Access Control Policy (Zanzibar model)
├── blockstore/      # IPLD block storage
├── cli/             # Command-line interface
├── crdt/            # CRDT implementations (LWW, counters)
├── crypto/          # Cryptographic operations
├── datastore/       # Data persistence abstractions
├── db/              # Database core (collections, merging)
├── defra-core/      # Core types and traits
├── document/        # Document handling
├── http/            # HTTP API server
├── identity/        # Identity and key management
├── keyring/         # Key storage
├── p2p/             # P2P networking (libp2p, Bitswap, sync)
├── query/           # Query engine (GraphQL, planner, execution)
├── schema/          # Schema validation and CID generation
└── storage/         # Storage backends (redb, memory)

tests/
└── interop/         # Go/Rust interoperability tests (critical!)
```

## Documentation

For DefraDB concepts, architecture, and specifications:
- [DefraDB (Go)](https://github.com/sourcenetwork/defradb)
- [DefraDB Docs](https://docs.source.network/defradb)

For development workflow: See `CLAUDE.md`

## License

Apache-2.0 OR MIT
