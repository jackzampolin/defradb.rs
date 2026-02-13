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

## ACP / Searchable Encryption

- When fixing ACP (Access Control Policy) filtering, always verify BOTH User queries AND Commits queries are filtered. These are two separate code paths that both require ACP checks.
- After fixing any ACP-related code, run the full ACP test suite (all 36+ tests) not just the immediately failing one.

## Shinzo Benchmarking (this branch: `shinzo/memory-leak`)

### Tracking

- **Issue #419**: Persistent scratch pad. Post a comment after every ~1000-block run with metrics.
- **PR #418**: Push code fixes to this branch.

### Running a 1000-block benchmark

```bash
# 1. Always start clean
./scripts/shinzo-test.sh clean

# 2. Build release (required after code changes)
cargo build --release

# 3. Start defra + indexer (uses random ports, logs to /tmp/shinzo-test/)
./scripts/shinzo-test.sh

# 4. In another terminal / background, monitor RSS/CPU/disk every 5s
./scripts/shinzo-test.sh monitor
```

The script picks random free ports, so no conflicts. Everything lives under `/tmp/shinzo-test/`.

### Monitoring a run

- `./scripts/shinzo-test.sh status` — ports, PIDs, latest block height, disk
- `./scripts/shinzo-test.sh logs defra` — tail defra log
- `./scripts/shinzo-test.sh logs indexer` — tail indexer log
- `./scripts/shinzo-test.sh monitor` — live RSS/CPU/disk/block/errors every 5s
- `./scripts/shinzo-test.sh query '{ Ethereum__Mainnet__Block(limit:1, order:{number:DESC}) { number } }'`

### After a run completes (~1000 blocks)

1. Stop: `./scripts/shinzo-test.sh stop`
2. Capture final metrics from monitor output and logs
3. Post a comment on issue #419 with the run results
4. Save logs: `cp /tmp/shinzo-test/*.log /tmp/shinzo-run-N/`
5. If a bottleneck was found: fix, rebuild, run again

### Metrics to capture per run

| Metric | Source |
|--------|--------|
| RSS start/peak/end | `ps -o rss=` or monitor output |
| Blocks indexed | Indexer log height delta from start height |
| Wall time | Timestamps from monitor |
| Blocks/sec | blocks / wall_time |
| Disk usage | `du -sh /tmp/shinzo-test/` |
| Error count | `grep -c ERROR /tmp/shinzo-test/indexer.log` |

### Config knobs

- `CONCURRENCY=N` — concurrent blocks (default 4)
- `RECEIPT_WORKERS=N` — receipt workers (default 4)
- `START_HEIGHT_OVERRIDE=N` — start block (default 23700000)

## Goal

**New contributor feels ready to do productive work immediately.**

Fast context acquisition → Confident changes → Productive iteration.
