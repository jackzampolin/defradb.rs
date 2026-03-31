# FFI Test Revival Design

## Goal

Get the Rust FFI compatibility tests running against latest Go develop, establish a full baseline, and create a triage plan for fixing failures.

## Context

The FFI tests validate that the Rust implementation (defradb.rs) behaves identically to the Go implementation (defradb) by running Go's integration test suite against the Rust FFI library via CGO. The tests haven't been run in a long time, and the Go `develop` branch has diverged significantly from the `jack/ffi-rust-compat` FFI branch.

## Scope

- **Go side:** Only modify `tests/clients/rustffi/` — the Rust FFI wrapper
- **Rust side:** `crates/ffi/` and underlying crates as needed
- **Go integration tests are the source of truth** — Rust must pass them
- **`cbindings/` is out of scope** — that's a separate C bindings FFI client

## Branching

- **Rust:** `feat/ffi-update` branch on `defradb.rs`
- **Go:** `jack/ffi-rust-compat` branch on `defradb` (merged with latest develop)

## Phases

### Phase 1: Get Compiling (Complete)

- [x] Merge latest `develop` into Go `jack/ffi-rust-compat` (64 new commits)
- [x] Resolve merge conflicts (7 files)
- [x] Fix post-merge compilation errors (ExpectedDAGHeads type, sonic upgrade)
- [x] Update `wrapper.go` for new `client` interface signatures
- [x] Build Rust FFI library (`cargo build --release -p ffi`)
- [x] Install `ffi-test` tool
- [x] Document build dependencies in CLAUDE.md (protoc, cbindgen)

### Phase 2: Tiered Baseline

Run one representative package per category to find systemic blockers before running the full 103-package suite:

| Category | Representative Package | Tests |
|----------|----------------------|-------|
| query | `query/simple` | Core read path |
| mutation | `mutation/add` | Core write path |
| txn | `txn` | Transaction basics |
| collection | `collection` | Schema/collection ops |
| index | `index` | Index operations |
| net | `net/simple/peer` | P2P basics |
| acp | `acp/dac` | Access control |
| backup | `backup/simple` | Import/export |
| subscription | `subscription` | Event subscriptions |
| view | `view/simple` | View queries |
| node | `node` | Node lifecycle |
| encryption | `encryption` | Encryption ops |
| signature | `signature` | Signing |
| explain | `explain/simple` | Query explain |

14 packages. Expected wall time: ~15-20 minutes.

### Phase 3: Fix Systemic Blockers

Analyze tier 1 results. Expected failure patterns:
- **Missing FFI function** — wrapper calls a C function that doesn't exist in Rust FFI
- **Signature mismatch** — wrapper calling with wrong argument types/count
- **New feature not implemented** — specific category failures for new develop features
- **Behavioral divergence** — Rust returns different results than Go

Fix blockers that unlock the most packages first.

### Phase 4: Full Category Sweep

Run `ffi-test run <category>` for each major category:
- `query`, `mutation`, `net`, `acp`, `collection`, `collection_version`
- `backup`, `encryption`, `explain`, `index`, `view`
- Remaining standalone packages

Save all reports via `ffi-test`. Use `ffi-test status` and `ffi-test diff` to track progress.

### Phase 5: Triage & Plan

Categorize every failure into:

| Category | Description | Effort |
|----------|-------------|--------|
| Wrapper fix | Go-side `rustffi/wrapper.go` change only | Small |
| FFI plumbing | New function in `crates/ffi/` | Medium |
| Rust implementation | New feature in core crates | Large |
| Expected skip | Feature intentionally not supported in Rust | None |

Create GitHub issues for anything that can't be fixed in the current session series.

## Success Criteria

1. All 103 packages run without build errors
2. Full baseline report saved via `ffi-test`
3. Every failure categorized with clear next steps
4. Systemic blockers fixed (unlock maximum test coverage)
5. Go branch pushed, Rust branch pushed with PR
