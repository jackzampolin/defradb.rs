# Development Principles

## 0. The North Star: 1.0 Feature Parity with Go

**defradb.rs ships 1.0 simultaneously with Go DefraDB (end of February 2026).**

Go's `develop` branch is the upstream reference. We track it continuously — pulling changes, identifying new features, and implementing them in Rust. The FFI test phase bootstrapped the implementation; now we validate with Rust-native integration tests that exercise the full CLI, HTTP API, and P2P stack.

### What "1.0 Parity" Means

- Same GraphQL query → Same results (field values, ordering, errors)
- Same document content → Same document ID (content-addressed CIDs)
- Same CRDT operations → Same merged state (convergence)
- Same P2P protocol → Nodes can discover, connect, and replicate
- Same CLI commands → Same behavior and output
- Same HTTP API → Same wire format and response structure

---

## 1. Information Hygiene

This codebase is designed for **AI-human pair programming**. Every structural choice optimizes for **rapid context acquisition**.

**Context clarity is oxygen for productive collaboration.**

## 2. Temporal Boundaries

| Zone | Contains | Lives in |
|------|----------|----------|
| **Past** | How we got here | Git history, closed issues/PRs |
| **Present** | What the code does now | Working tree |
| **Future** | What we might do next | GitHub issues |

**No commented-out code. No TODO comments (create issues instead). No speculative docs.**

## 3. No Documentation Files

Only allowed: `ARCHITECTURE.md`, `README.md`, `CLAUDE.md`, `Cargo.toml` files.

No `ROADMAP.md`, `DEVELOPMENT.md`, `docs/` directories, or planning documents.

## 4. File Organization

**One concept per file. Small files over large files.**

### Crate Structure

```
crates/
├── acp/             # Access Control Policy
├── blockstore/      # IPLD block storage
├── cli/             # Command-line interface
├── crdt/            # CRDT implementations
├── crypto/          # Cryptographic operations
├── datastore/       # Data persistence abstractions
├── db/              # Database core
├── defra-core/      # Core types and traits
├── document/        # Document handling
├── http/            # HTTP API server
├── identity/        # Identity and JWT management
├── keyring/         # Key storage
├── p2p/             # P2P networking
├── query/           # Query engine
├── schema/          # Schema validation
└── storage/         # Storage backends (redb, fjall, rocksdb, memory)

tools/
├── ffi-test/          # FFI compatibility testing against Go
└── integration-test/  # Rust-native integration tests (primary validation)
```

### File Size Guidelines

- Under 200 lines: Fine
- 200-400 lines: Check if doing one thing
- Over 400 lines: Consider splitting

## 5. Naming Conventions

| Thing | Convention | Example |
|-------|------------|---------|
| Crates | lowercase, hyphens | `defra-core` |
| Files/Modules | snake_case | `lww.rs` |
| Types | PascalCase | `LwwDelta` |
| Functions | snake_case | `encode_priority()` |
| Constants | SCREAMING_SNAKE_CASE | `MAX_PRIORITY` |

## 6. Comments Policy

**Minimal comments. Code should be self-documenting.**

✅ Comment: Non-obvious WHY, safety invariants, public API docs (`///`)

❌ Don't: What the code does, TODO/FIXME, commented-out code, change history

## 7. Git Worktree Workflow

```bash
cd ../defradb.rs-foo     # Work on feature foo
cd ../defradb.rs-bar     # Work on feature bar
```

Each worktree is isolated, no branch switching overhead.

## Common Commands

### Integration Tests (Primary Validation)

```bash
# Run all integration tests
cargo test -p integration-test

# Run a specific area
cargo test -p integration-test --test acp
cargo test -p integration-test --test p2p
cargo test -p integration-test --test basic

# Run a specific submodule within an area
cargo test -p integration-test --test acp -- negative::

# Run a specific test
cargo test -p integration-test --test acp -- basic::rust_acp_basic
```

Integration tests live in `tools/integration-test/tests/` and exercise the full
Rust node via CLI + HTTP API. Each area is a `[[test]]` binary with submodules:

| Area | Binary | Modules |
|------|--------|---------|
| Basic | `--test basic` | smoke, document_lifecycle, transactions, collection_management, multi_collection, truncate_parallel |
| Query | `--test query` | view, lens, lens_persistence, sdl_generate, index_management, explain_nested, subscription_docid, stubs |
| ACP | `--test acp` | basic, multi_identity, multi_role, revoke_lifecycle, node_access, p2p, negative, negative_p2p, xarchive_access_matrix, stubs |
| NAC | `--test nac` | document_acp, operations, core_operations, p2p_management, relation_admin, cross_compartment_isolation, policy_evolution |
| P2P | `--test p2p` | document, sync, management, trust_boundary, replication, replication_advanced, stubs |
| Encryption | `--test encryption` | index, acp, block_verify, stubs |
| Identity | `--test identity` | lifecycle, types, negative, node_identity, keyring_lifecycle |
| Backup | `--test backup` | restore, dump, purge |

SourceHub tests live in the [backbone](https://github.com/sourcenetwork/backbone) repo
(`crates/defra-harness/tests/sourcehub/`). Run them with `DEFRA_WORKSPACE_ROOT` pointing here.

### Rust Commands

```bash
cargo test                         # Run all unit tests
cargo test -p crdt                 # Test specific crate
cargo clippy --all -- -D warnings  # Lint
cargo fmt --all                    # Format
cargo build --release              # Build release
```

### Tracking Go Upstream

```bash
# Go repo location
cd /Users/johnzampolin/go/src/github.com/sourcenetwork/defradb

# Check what's landed on develop
git fetch origin develop
git log origin/develop --oneline -20

# Compare with our last sync point
git log origin/develop --oneline --since="1 week ago"
```

The Go repo has two remotes:
- `origin` → `sourcenetwork/defradb` (upstream)
- `fork` → `jackzampolin/defradb` (our fork, `jack/ffi-rust-compat` branch)

### Git Worktrees

```bash
git worktree list                                  # List worktrees
git worktree add ../defradb.rs-foo -b feat/foo     # Create worktree
git worktree remove ../defradb.rs-foo              # Remove worktree
```

## Before Committing

1. `cargo test` passes
2. `cargo clippy --all -- -D warnings` clean
3. `cargo fmt --all` applied
4. If touching core behavior: `cargo test -p integration-test` passes

## ACP / Searchable Encryption

- When fixing ACP (Access Control Policy) filtering, always verify BOTH User queries AND Commits queries are filtered. These are two separate code paths that both require ACP checks.
- After fixing any ACP-related code, run the full ACP test suite not just the immediately failing one.

## Storage Backends

Four backends available, selectable via `--store` flag or `STORE` env var:

| Backend | Type | Use Case |
|---------|------|----------|
| `redb` | COW B+ tree | Default, single-writer, reliable |
| `fjall` | LSM-tree | High write throughput, Shinzo indexer |
| `rocksdb` | LSM-tree | Production, configurable via `ROCKS_*` env vars |
| `memory` | In-memory | Testing only |

## Goal

**New contributor feels ready to do productive work immediately.**

Fast context acquisition → Confident changes → Productive iteration.
