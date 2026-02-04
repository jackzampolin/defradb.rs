# Development Principles

## 0. The North Star: Go Behavioral Compatibility

**This Rust implementation must behave identically to Go DefraDB.**

Every feature is validated against Go via the FFI test suite. If Rust and Go produce different results, the Rust implementation is wrong.

### What "Compatible" Means

- Same GraphQL query → Same results (field values, ordering, errors)
- Same document content → Same document ID (content-addressed CIDs)
- Same CRDT operations → Same merged state (convergence)
- Same P2P protocol → Nodes can discover, connect, and replicate

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
└── storage/         # Storage backends

tools/
└── ffi-test/        # FFI compatibility testing tool
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
cd ../defradb.rs-crdt     # Work on CRDT subsystem
cd ../defradb.rs-storage  # Work on storage subsystem
```

Each worktree is isolated, no branch switching overhead.

## Common Commands

### FFI Compatibility Testing

```bash
# Install the tool (once)
cargo install --path tools/ffi-test

# Run tests
ffi-test run query/simple          # Package tests
ffi-test run query                  # All query/* packages
ffi-test run query/simple -t Test  # Specific test pattern
ffi-test run query/simple --skip-build  # Skip rebuild

# View status
ffi-test status                    # Current worktree
ffi-test status --all              # All worktrees

# Manage worktrees
ffi-test worktree create foo       # Create paired Rust+Go worktrees
ffi-test worktree list             # List all pairs
ffi-test worktree remove foo       # Remove both
```

### Rust Commands

```bash
cargo test                         # Run all tests
cargo test -p crdt                 # Test specific crate
cargo clippy --all -- -D warnings  # Lint
cargo fmt --all                    # Format
cargo build --release              # Build release
```

### Git Worktrees

```bash
git worktree list                  # List worktrees
git worktree add ../defradb.rs-foo -b ffi/foo  # Create worktree
git worktree remove ../defradb.rs-foo          # Remove worktree
```

## Before Committing

1. `cargo test` passes
2. `cargo clippy --all -- -D warnings` clean
3. `cargo fmt --all` applied
4. If touching P2P/query/schema/CRDT: FFI tests pass

## Goal

**New contributor feels ready to do productive work immediately.**

Fast context acquisition → Confident changes → Productive iteration.
