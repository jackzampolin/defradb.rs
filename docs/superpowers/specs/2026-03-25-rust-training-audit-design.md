# Rust Training Audit & Codebase Improvement

**Date:** 2026-03-25
**Status:** Draft
**Source Material:** Microsoft RustTraining (7 mdBook volumes, `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/`)

## Goal

Use Microsoft's Rust training materials as a rubric to audit the defradb.rs codebase (~250K lines, 25 crates), then make targeted improvements in the highest-impact areas. Changes are moderate in scope: fix real issues and adopt clearly better patterns, but no speculative refactoring. File splitting is in scope when files exceed the 400-line guideline.

## Approach: Hybrid (Topic Audit + Crate Implementation)

**Phase 1** — Seven topic audit agents scan the entire codebase in parallel (read-only). Each agent reads the relevant training chapters and produces a structured findings report.

**Phase 2** — ~13 crate implementation agents each take ownership of one crate or cluster of small crates. Each runs in an isolated git worktree, applies findings in priority order, and produces a branch ready for PR.

**Integration** — Branches merge in dependency order with integration test gates between tiers.

---

## Phase 1: Topic Audit Agents

Seven read-only agents run in parallel. Each reads training chapters, then scans the full codebase.

### Agent 1: Async Patterns

**Training source:** `async-book/` chapters 8 (Tokio Deep Dive), 12 (Common Pitfalls), 13 (Production Patterns)

**Audit targets:**
- `MutexGuard` held across `.await` points
- Blocking calls (`std::thread::sleep`, synchronous I/O) inside async contexts
- Cancellation hazards (resources not cleaned up on task abort)
- Missing backpressure (unbounded channels, unbounded spawning)
- Shutdown patterns (proper use of `watch` channels, `CancellationToken`, `JoinSet`/`TaskTracker`)
- `spawn_blocking` usage for CPU-bound or blocking work
- `select!` fairness and starvation risks

**Primary crates to scan:** `p2p`, `db`, `query`, `http`, `events`, `embedded`, `defra-node`, `cli`

### Agent 2: Error Handling

**Training source:** `rust-patterns-book/` chapter 10, `async-book/` chapter 13

**Audit targets:**
- `anyhow` used in library crates (should be `thiserror`)
- Bare `unwrap()` / `expect()` in non-test code
- Missing `#[from]` auto-conversions causing boilerplate
- Inconsistent error types within a crate (multiple error enums, string errors)
- Error context lost during propagation (bare `?` without `.context()`)
- `catch_unwind` at FFI/thread boundaries where panics could cross

**Primary crates to scan:** All 25 crates

### Agent 3: Concurrency

**Training source:** `rust-patterns-book/` chapters 5-6, `async-book/` chapter 8

**Audit targets:**
- `Arc<Mutex<T>>` where a channel would be simpler and less contention-prone
- Atomic ordering correctness (`Relaxed` where `Acquire`/`Release` is needed)
- Missing `Send`/`Sync` bounds on public async APIs
- Lock contention patterns (holding locks across I/O, nested locks)
- `OnceLock`/`LazyLock` vs deprecated `lazy_static!` usage
- Scoped threads vs spawned threads for bounded work

**Primary crates to scan:** `p2p`, `db`, `crdt`, `events`, `blockstore`, `storage`

### Agent 4: Unsafe & Verification

**Training source:** `rust-patterns-book/` chapter 12, `engineering-book/` chapter 5

**Audit targets:**
- All `unsafe` blocks: soundness review
- Missing `SAFETY:` comments on `unsafe` blocks
- FFI boundary correctness (`extern "C"` functions, `CString`/`CStr` usage)
- Candidates for Miri testing
- Candidates for `loom` concurrency model checking
- Raw pointer usage that could be replaced with safe abstractions
- Panic-across-FFI prevention

**Primary crates to scan:** `ffi`, `storage`, `blockstore`, `crdt`, `crypto`

### Agent 5: Type Design

**Training source:** `type-driven-correctness-book/` chapters 2-5, 7, 9; `rust-patterns-book/` chapters 1-4

**Audit targets:**
- Raw primitives (`u64`, `String`, `Vec<u8>`) used for domain concepts that deserve newtypes
- State machines that could use the type-state pattern (protocol states, session lifecycle)
- Phantom type opportunities for resource tracking
- Missing `#[must_use]` on Result-returning functions and builder types
- Missing `#[non_exhaustive]` on public enums that may grow
- Newtype wrapping opportunities for compile-time safety (IDs, keys, hashes)

**Primary crates to scan:** `defra-core`, `document`, `schema`, `identity`, `acp`, `zanzibar`, `crdt`

### Agent 6: Serialization & Zero-Copy

**Training source:** `rust-patterns-book/` chapter 11

**Audit targets:**
- Unnecessary clones in serialization/deserialization paths
- `Vec<u8>` where `bytes::Bytes` would avoid copies
- Zero-copy deserialization opportunities (`&'de str` vs `String` in serde structs)
- Serde attribute optimization (`rename_all`, `skip`, `default`, `flatten`)
- `repr(C)` correctness for FFI structs
- `Cow<str>` opportunities where data is sometimes owned, sometimes borrowed

**Primary crates to scan:** `blockstore`, `crdt`, `document`, `defra-core`, `http`, `p2p`, `ffi`

### Agent 7: File Structure & API Design

**Training source:** `rust-patterns-book/` chapter 15, `engineering-book/` chapters 7-8

**Audit targets:**
- Files over 400 lines — propose split points
- Modules doing more than one conceptual thing
- Public API surface that could be narrower (pub items that should be pub(crate))
- Re-exports that leak implementation details
- Missing sealed trait pattern where extension should be prevented
- `impl Into<T>` / `impl AsRef<T>` parameter ergonomics on public APIs

**Primary crates to scan:** All 25 crates (focus on files identified as >400 lines)

**Known large files (>1000 lines):**
- `crates/db/src/downsample.rs` (2036)
- `crates/query/src/runner/query/nested.rs` (1821)
- `crates/query/src/sdl_parse/parser_tests.rs` (1813)
- `crates/query/src/runner/commits.rs` (1545)
- `crates/db/tests/index_manager_tests.rs` (1541)
- `crates/blockstore/tests/blockstore_tests.rs` (1450)
- `crates/cli/src/commands/start/server.rs` (1428)
- `crates/p2p/src/iroh/endpoint.rs` (1420)
- `crates/query/src/planner/joins/mod.rs` (1395)
- `crates/db/src/merge_handler/composite.rs` (1372)
- `crates/query/src/query_parse/parser.rs` (1270)
- `crates/crdt/tests/property_tests.rs` (1269)
- `crates/embedded/src/node.rs` (1267)
- `crates/cli/src/p2p_adapter.rs` (1214)
- `crates/query/src/mapper/filter/filter_tests.rs` (1126)
- `crates/db/src/merge_handler/mod.rs` (1100)
- `crates/defra-core/tests/block_tests.rs` (1080)
- `crates/query/src/sdl_parse/builder.rs` (1040)
- `crates/defra-node/src/benchmark_support.rs` (985)
- `crates/query/src/plan/mutation/create.rs` (969)

---

## Findings Schema

Each audit agent produces findings in this format:

```
severity: critical | high | medium | low
category: bug | unsound | anti-pattern | improvement | structure
crate: <crate name>
file: <relative path>
line: <line number or range>
pattern: <short name, e.g. "mutex-across-await">
description: <what's wrong>
training_ref: <book/chapter that informed this>
suggested_fix: <concrete suggestion>
```

Findings are merged into a single report at `docs/superpowers/specs/audit-findings.md`, grouped by crate, sorted by severity within each crate. Each Phase 2 agent receives only the findings for its assigned crate(s).

---

## Prioritization Rules

Implementation agents apply findings in this order:

1. **Critical** — Unsound `unsafe`, potential UB, data races. Always fix.
2. **High** — Bugs, cancellation hazards, blocking in async, panic-across-FFI. Always fix.
3. **Medium** — Anti-patterns (wrong error type, `Arc<Mutex>` where channel fits, unnecessary clones, missing newtypes). Fix if change is contained within the crate.
4. **Low** — Idiom improvements, file splits, API tightening, `#[must_use]` annotations. Fix if it doesn't create churn in other crates.

**Skip rule:** If fixing a finding requires changes outside the assigned crate, flag it for a cross-cutting follow-up pass. Don't reach into other crates.

---

## Phase 2: Crate Implementation Agents

~13 agents, each in an isolated git worktree, running in parallel.

### Crate Assignments

| Tier | Agent | Crates | Rationale |
|------|-------|--------|-----------|
| Large | Agent L1 | `db` | Largest crate, merge handler, downsample, index manager |
| Large | Agent L2 | `query` | Query engine, planner, parser, runner — many large files |
| Large | Agent L3 | `p2p` | Networking, iroh endpoint, libp2p |
| Large | Agent L4 | `cli` | CLI commands, server startup, p2p adapter |
| Medium | Agent M1 | `crdt` | CRDT implementations, property tests |
| Medium | Agent M2 | `blockstore` | Block storage, IPLD |
| Medium | Agent M3 | `defra-core` | Core types and traits |
| Medium | Agent M4 | `defra-node`, `embedded` | Node lifecycle, embedding |
| Small | Agent S1 | `acp`, `zanzibar`, `identity` | Access control + permissions cluster |
| Small | Agent S2 | `storage`, `datastore`, `keyring` | Storage abstraction cluster |
| Small | Agent S3 | `http`, `pg-compat` | Server protocol cluster |
| Small | Agent S4 | `schema`, `document`, `lens` | Data model cluster |
| Small | Agent S5 | `events`, `crypto`, `ffi`, `wasm`, `defra-version`, `orbis`, `sourcehub` | Remaining small crates |

### Per-Agent Workflow

1. Create worktree: `git worktree add ../defradb.rs-audit-<name> -b audit/<name>`
2. Receive consolidated findings for assigned crate(s)
3. Apply findings in priority order (critical > high > medium > low)
4. After each logical change group: `cargo check -p <crate>`
5. Split files over 400 lines where the audit identified natural split points
6. After all changes: `cargo test -p <crate>` + `cargo clippy -p <crate> -- -D warnings` + `cargo fmt --all`
7. Commit with descriptive message per logical change group
8. Output: change summary, skipped findings with reasoning

---

## Quality Gates

### Per-Agent (before branch is ready)

- `cargo fmt --all -- --check` passes
- `cargo clippy -p <crate> -- -D warnings` passes
- `cargo test -p <crate>` passes
- No new `unwrap()` introduced in non-test code
- All `unsafe` blocks have `SAFETY:` comments

### Integration (after merging each tier)

- `cargo test` (full unit test suite) passes
- `cargo clippy --all -- -D warnings` passes
- `cargo test -p integration-test` passes

---

## Merge Order

Branches merge in dependency order to catch cross-crate breakage early.

| Tier | Crates | Gate |
|------|--------|------|
| 1 | `defra-core`, `crypto`, `defra-version`, `events` | `cargo test` + clippy |
| 2 | `storage`, `datastore`, `keyring`, `document`, `schema` | `cargo test` + clippy |
| 3 | `blockstore`, `crdt`, `zanzibar`, `identity`, `acp`, `lens` | `cargo test` + clippy + integration tests |
| 4 | `db`, `query` | `cargo test` + clippy + integration tests |
| 5 | `http`, `pg-compat`, `p2p`, `embedded`, `ffi`, `wasm`, `orbis`, `sourcehub` | `cargo test` + clippy + integration tests |
| 6 | `cli`, `defra-node` | Full integration test suite |

### Rollback Policy

If a crate's changes break integration tests after merge:
1. Revert that branch
2. Flag its findings for manual review
3. Continue merging other crates
4. Do not block the pipeline on one crate

---

## What This Does NOT Cover

- **New features** — This is a quality pass, not a feature pass
- **Dependency upgrades** — Out of scope unless a finding specifically requires it
- **CI/CD changes** — The engineering book has a CI template, but changing our CI is separate work
- **Benchmarking infrastructure** — Already tracked under #568, not duplicated here
- **Domain-specific patterns** — CRDTs, P2P protocols, storage engine internals are not covered by the training material and are out of scope for this audit
