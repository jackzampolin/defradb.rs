# Development Principles

## 0. The North Star: Go Behavioral Compatibility

**This Rust implementation must behave identically to Go DefraDB.**

Every feature we build is validated against the Go implementation. The `tests/interop/` test suite is the ultimate arbiter of correctness. If a Rust node and Go node produce different results for the same operation, the Rust implementation is wrong.

### What "Compatible" Means

- Same GraphQL query → Same results (field values, ordering, errors)
- Same document content → Same document ID (content-addressed CIDs)
- Same CRDT operations → Same merged state (convergence)
- Same P2P protocol → Nodes can discover, connect, and replicate

### Running Interop Tests

```bash
cd tests/interop

# Set path to Go DefraDB (default assumes sibling repo structure)
export DEFRA_GO_PATH=/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb

make build-all    # Build both Rust and Go binaries
make test         # Run all interop tests (connection + sync)
```

**Run these before any PR that touches:** P2P, query, schema, CRDT, or document handling.

---

## 1. Information Hygiene (Context Clarity as First Principle)

This codebase is designed for **AI-human pair programming**. Every structural choice optimizes for **rapid context acquisition**.

### The Problem: Context Pollution

Large files with mixed concepts create noise:
- **For AIs:** Irrelevant patterns leak into context, suggesting wrong approaches
- **For humans:** Cognitive load increases, important details get missed
- **For both:** Hard to locate information, unclear boundaries

### Core Principle

**Context clarity is oxygen for productive collaboration.**

Clear context → Fast understanding → Confident changes → Productive iteration.

## 2. Cordon Sanitaire (Temporal Boundaries)

**Three temporal zones. No leakage into the working tree.**

| Zone | Contains | Lives in |
|------|----------|----------|
| **Past** | How we got here | Git history, closed issues/PRs |
| **Present** | What the code does now | Working tree |
| **Future** | What we might do next | GitHub issues |

### Enforcement

**Past stays in git/GitHub:**
- No commented-out code "for reference"
- No comments explaining what code used to do
- History lives in `git log` and closed issues/PRs

**Future stays in GitHub:**
- `// TODO` → Create an issue, delete the comment
- `// FIXME` → Create an issue or fix it now
- `// HACK` → Comment explaining necessity, or refactor
- No `PLAN.md`, `docs/research/`, or speculative documentation
- Plans live in GitHub issues/discussions

**Present stays in working tree:**
- Active, working code
- Comments explaining non-obvious WHY (present tense)
- Safety warnings (see exception below)

### Exception: Safety Warnings

Inline safety warnings are present-tense information about current behavior:
```rust
// SAFETY: This operation requires exclusive access to the store
// WARNING: This will overwrite existing data without backup
// CRITICAL: Must be called with priority > 0
```

## 3. No Documentation Files (Unless Explicitly Requested)

**DO NOT create markdown documentation files.**

Rationale: Documentation becomes stale quickly and creates maintenance burden.

**Allowed files only:**
- `ARCHITECTURE.md` (high-level design, already exists)
- `README.md` (minimal, already exists)
- `CLAUDE.md` (this file)
- `Cargo.toml` files (build configuration)

**Instead of creating docs:**
- Write clear inline comments in code (sparingly)
- Use doc comments (`///`) for public APIs
- Let the code be self-documenting

**No:**
- `ROADMAP.md`
- `DEVELOPMENT.md`
- `CONTRIBUTING.md`
- `docs/` directories with guides
- Test parity analyses
- Cross-implementation testing plans
- Any speculative planning documents

## 4. File Organization

**One concept per file. Small files over large files.**

### Rust Crate Structure (16 crates)

```
crates/
├── acp/             # Access Control Policy (Zanzibar model, NAC/FAC)
├── blockstore/      # IPLD block storage
├── cli/             # Command-line interface
├── crdt/            # CRDT implementations (LWW registers, counters)
├── crypto/          # Cryptographic operations
├── datastore/       # Data persistence abstractions
├── db/              # Database core (collections, merge handling)
├── defra-core/      # Core types, traits, IPLD encoding
├── document/        # Document handling
├── http/            # HTTP API server (REST + GraphQL)
├── identity/        # Identity and JWT token management
├── keyring/         # Key storage and management
├── p2p/             # P2P networking (libp2p, Bitswap, sync protocol)
├── query/           # Query engine (GraphQL parsing, planning, execution)
├── schema/          # Schema validation, SDL parsing, CID generation
└── storage/         # Storage backends (redb, memory)

tests/
└── interop/         # Go/Rust interoperability tests (CRITICAL)
    ├── framework/   # Test utilities (node management, HTTP client)
    ├── connection_test.go  # P2P connectivity tests
    └── sync_test.go        # Data replication tests
```

### File Size Guidelines

- **Under 200 lines:** Probably fine
- **200-400 lines:** Check if doing one thing
- **Over 400 lines:** Consider splitting (but tests can be longer)

### Rust Conventions

- Module name = file name
- One primary type per file (file named after the type)
- Tests inline (`#[cfg(test)] mod tests`) or separate `tests/` directory
- `lib.rs` only contains module declarations and re-exports

## 5. Naming Conventions

**Consistent naming prevents bugs and enables discovery.**

### Rust Naming

| Thing | Convention | Example |
|-------|------------|---------|
| Crates | lowercase, hyphens | `defra-core`, `crdt` |
| Files | snake_case | `lww.rs`, `priority.rs` |
| Modules | snake_case | `mod lww;` |
| Public types | PascalCase | `struct LwwDelta` |
| Private types | PascalCase | `struct MemoryStore` |
| Functions | snake_case | `fn encode_priority()` |
| Traits | PascalCase | `trait ReplicatedData` |
| Constants | SCREAMING_SNAKE_CASE | `const MAX_PRIORITY: u64` |

### Test File Naming

- Unit tests: Inline `#[cfg(test)] mod tests`
- Integration tests: `tests/integration_test.rs`
- Property tests: `tests/property_tests.rs`

### Multi-Word Names

Use snake_case for files and modules:
```
src/
├── lww.rs           # LWW Register
├── counter.rs       # Counter CRDT
├── priority.rs      # Priority encoding
└── composite.rs     # Composite DAG
```

## 6. Comments Policy

**Minimal comments. Code should be self-documenting.**

### When to Comment

✅ **Do comment:**
- Non-obvious WHY (algorithm choice, optimization)
- Safety invariants that aren't type-checked
- Critical warnings about behavior
- Public API documentation (`///`)

❌ **Don't comment:**
- What the code does (should be obvious from names)
- How to use basic Rust features
- TODO/FIXME (create issues instead)
- Commented-out code (delete it)
- Change history (use git)

### Example Good Comments

```rust
/// LWW Register using priority-based conflict resolution.
///
/// When two concurrent writes occur, the one with higher priority wins.
/// On tie, lexicographic comparison provides deterministic resolution.
pub struct Lww { ... }

// Use saturating_add to match Go DefraDB behavior on overflow
let new_value = current.saturating_add(increment);

// SAFETY: This must be called with the store lock held
unsafe fn update_without_lock(&mut self) { ... }
```

### Example Bad Comments

```rust
// TODO: Add float support later
// Counter increments the value
let new_value = current + increment;  // Add increment to current
// John changed this on 2024-01-15
```

## 7. Test Organization

**Tests are documentation. Keep them clear and focused.**

### Test Structure

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lww_higher_priority_wins() {
        // Arrange
        let store = MemoryStore::new();
        let mut lww = Lww::new(/* ... */);

        // Act
        lww.merge(&ctx, &delta1).await.unwrap();
        lww.merge(&ctx, &delta2).await.unwrap();

        // Assert
        assert_eq!(lww.value().await.unwrap(), expected);
    }
}
```

### Test Naming

- `test_<module>_<behavior>` for unit tests
- `test_<scenario>_<expected_outcome>` for integration tests
- Property tests: `test_<property>`

### Property-Based Tests

Critical for CRDTs. Use `proptest` to verify:
- Commutativity
- Associativity
- Idempotence
- Convergence

## 8. Git Worktree Workflow

**Multiple subsystems = multiple worktrees.**

```bash
# Work on different subsystems concurrently
cd ../defradb.rs-crdt     # CRDT work
cd ../defradb.rs-storage  # Storage work
cd ../defradb.rs-crypto   # Crypto work
```

Each worktree is isolated, no branch switching overhead.

## 9. Code-First Development

**Order of implementation:**

1. Write working code
2. Write comprehensive tests
3. Add doc comments for public APIs
4. Commit

**Don't:**
- Write specs before code
- Create placeholder files
- Add TODO comments
- Create issues for every idea (only actionable issues)

## Core Workflow

1. **New contributor (AI or human) arrives**
   - Read `CLAUDE.md` (this file)
   - Run `tree -L 3 crates/` (structure teaches)
   - Read relevant source files (small, focused)
   - Check git history if needed
   - Start working (productive immediately)

2. **During development**
   - Keep files small and focused
   - No TODO comments (create issues)
   - No commented code (delete it)
   - Tests inline with implementation
   - Commit frequently with clear messages

3. **Before committing**
   - Rust tests pass: `cargo test`
   - No warnings: `cargo clippy`
   - Formatted: `cargo fmt`
   - **Interop tests pass** (if touching P2P, query, schema, CRDT): `cd tests/interop && make test`
   - No extra markdown files created

## Common Commands

### Go/Rust Interop Tests (Most Important)

```bash
cd tests/interop

# Set Go DefraDB path
export DEFRA_GO_PATH=/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb

# Build both implementations
make build-all

# Run ALL interop tests (connection + sync)
make test

# Run just Rust-to-Rust connection tests (faster)
make test-connection

# Run cross-implementation tests (Go ↔ Rust)
make test-cross

# Clean test cache
make clean
```

### Rust Unit Tests

```bash
# Run all tests in workspace
cargo test

# Run tests for specific crate
cargo test -p crdt

# Run specific test
cargo test -p crdt test_lww_higher_priority_wins

# Run property tests only
cargo test -p crdt --test property_tests

# Run tests with output
cargo test -- --nocapture

# Run tests in release mode (faster)
cargo test --release
```

### Building

```bash
# Build entire workspace
cargo build

# Build specific crate
cargo build -p crdt

# Build in release mode
cargo build --release

# Check compilation without building
cargo check
```

### Code Quality

```bash
# Run clippy (linter)
cargo clippy --all -- -D warnings

# Format code
cargo fmt --all

# Check formatting without changing files
cargo fmt --all -- --check
```

### Development

```bash
# Watch for changes and run tests
cargo watch -x test

# Watch specific crate
cargo watch -x 'test -p crdt'

# Clean build artifacts
cargo clean

# View dependency tree
cargo tree
```

### Git Worktrees

```bash
# List worktrees
git worktree list

# Switch to worktree
cd ../defradb.rs-crdt

# See current branch
git branch

# Remove worktree (when done)
git worktree remove ../defradb.rs-crdt
```

### FFI Testing

The `ffi-test` tool runs Go integration tests against the Rust FFI implementation.

```bash
# Install the tool (once)
cargo install --path tools/ffi-test

# Run tests from any worktree
cd /path/to/defradb.rs-index
ffi-test run query/simple                    # All tests in package
ffi-test run query/simple -t TestFilter      # Specific test
ffi-test run query/simple --verbose          # Full output

# View status
ffi-test status                              # Current worktree
ffi-test status --all                        # All worktrees

# Manage worktrees
ffi-test worktree create foo                 # Create paired worktrees
ffi-test worktree list                       # List all pairs
ffi-test worktree remove foo                 # Remove both
```

## Goal

**New contributor feels "cozy and right inside their workshop, ready to do very productive work."**

Fast context acquisition → Confident changes → Productive iteration.
