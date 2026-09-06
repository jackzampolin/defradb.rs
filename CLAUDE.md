# Development Principles

## 0. The North Star: 1.0 Release

**The Go compatibility baseline lives in `crates/defra-version/src/lib.rs`, not
here.** Three constants define it: `GO_COMPAT_BRANCH`, `GO_COMPAT_COMMIT`, and
`GO_COMPAT_TAG`. An empty tag means CI builds the pinned commit from source
instead of downloading a release binary. `.github/workflows/ci.yml` greps those
constants directly, so they are the single source of truth — read them rather
than quoting a release number, which goes stale on every baseline bump.

Go parity is achieved across CLI, HTTP API, GraphQL query engine, and P2P replication. We now validate with Rust-native integration tests that exercise the full stack, and are building Rust-specific features (Iroh transport, BM25 full-text search, Postgres wire protocol, WASM client).

### What "Parity" Means

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

## 3. Authority, Ownership, and Callsite Discipline

The recurring high-cost failure is a correct rule in one path with a bypass in
another. Treat data access as an ownership problem, not as local helper
cleanup.

### Before Changing a Data Path

- **Name the invariant and its owner.** Identify the canonical durable state,
  the sole writer for each lifecycle transition, and the owner of retry,
  recovery, and terminal cleanup. Readers and adapters may observe or request
  transitions; they must not become alternate writers or clocks.
- **Map every production callsite.** Use `rg` on the changed trait, method, data
  type, and storage key, then trace the real entrypoints. Check CLI, HTTP and
  GraphQL, embedded, FFI/WASM, P2P live delivery, replay, and restart recovery
  as applicable. Include auto-commit, batch, explicit transaction, fast paths,
  and already-present/empty branches. A wrapper is not a chokepoint if any
  caller reaches a deeper method directly.
- **Classify state before reading it.** Distinguish canonical records from
  indexes/projections, durable hints, process-local caches, transport evidence,
  and metrics. Rebuild or replace derived state from an authoritative snapshot;
  do not union stale cache state back into truth. An empty index result does not
  prove that the underlying record is absent; compare a scan or direct key read
  when diagnosing.
- **Pin the code and feature surface.** Establish the exact revision, enabled
  features, backend, and transport used by the failing artifact before tracing
  current source. A passing Local-ACP path does not validate SourceHub ACP; one
  storage backend or transport does not stand in for the others. Validate Git
  dependencies and feature resolution from a clean state when reachability or
  hermeticity matters; a warm cache can make a broken pin appear healthy.

### Implementation Rules

- Put a semantic invariant in the lowest shared layer that has enough context
  to enforce it. Frontends and transport adapters translate inputs and expose
  outcomes; they do not choose different persistence, authorization, retry, or
  merge semantics.
- Centralize shared mechanism without erasing intentional surface policy.
  Before replacing parallel implementations, record each surface's observable
  timeouts, routing, status/error mapping, defaults, and metadata. Preserve real
  differences as explicit parameters or thin policy wrappers; do not make them
  accidental forks or silently standardize them during cleanup.
- A centralization change is complete only when every semantically equivalent
  callsite routes through the owner, or the remaining exceptions are named and
  tested. Remove obsolete bypasses and dead recovery paths rather than keeping
  a second implementation "for safety."
- Transfer ownership durably before returning a success acknowledgement. An
  in-memory map, queued event, spawned task, or live connection is not durable
  backing. If local commit succeeds but marker/index/broadcast registration
  fails, surface the partial outcome and retain or reconstruct a retryable
  obligation.
- Keep exactly one progression mechanism for a lifecycle: one fetch claim, one
  retry schedule, one merge writer (which may batch), and one terminal cleanup
  writer. Reconnect, replay, cache resync, and restart may wake that owner; they
  must not create parallel clocks or writers.
- Preserve provenance end to end. Carry authenticated caller, creator, sender,
  and provider identity through the shared path. Do not replace it with the
  current adapter caller, a relay, ambient process state, or an unsigned payload
  claim. Re-announcement must not silently rebind a durable owner.
- Describe atomicity at the real transaction boundary. Do not claim
  commit-plus-marker atomicity when a channel, coalescer, callback, or second
  store sits between them.
- Unsupported feature or backend combinations must fail explicitly at the
  construction or public boundary. Do not substitute identity transforms,
  no-op stores, permissive authorization, or another silent behavior for an
  unavailable capability.

### Verification Bar

- Keep the original failing workload or symptom as a regression fence. A unit
  test for a local property does not replace an end-to-end test for the reported
  failure, and green unrelated CI is not evidence for the changed path.
- Test the owner seam directly, then exercise at least one real public boundary.
  Add the relevant crash/restart, duplicate/replay, partial-failure, and
  authorization cases. Verify every affected path in the callsite matrix, not
  only the first path that failed.
- Build ordinary fixtures through the production owner so they satisfy current
  invariants. Direct insertion of impossible or partially owned state belongs
  only in an explicitly named corruption, migration, or recovery test.
- For negative dependency, feature, and topology assertions, inspect and assert
  the evidence-bearing output; an exit code alone may not distinguish "absent"
  from "unused," unsupported, or not inspected. Preserve the raw result when a
  harness normalizes known environmental noise.
- Tests that mutate process-global environment or singleton state belong in an
  isolated test binary unless synchronization and restoration are explicit.
- Treat a PR stack as change organization, not as a CI optimization. Verify the
  workflow's actual branch and path triggers, keep every layer reviewable and
  mergeable, and validate both the next mergeable layer and the integrated tip.
- For a nontrivial change, record the invariant, semantic owner, canonical and
  derived stores, enumerated callsites, failure boundary, and validation matrix
  in the PR or commit message. Do not create a planning document for it.

## 4. No Documentation Files

Only allowed: `README.md`, `CLAUDE.md`, `Cargo.toml` files.

`AGENTS.md` may exist only as a symlink to the nearest `CLAUDE.md`; never
maintain a second copy of the guidance.

No `ROADMAP.md`, `DEVELOPMENT.md`, `docs/` directories, or planning documents.

## 5. File Organization

**One concept per file. Small files over large files.**

### Crate Structure

```
crates/
├── acp/                # Access Control Policy
├── blockstore/         # IPLD block storage
├── cli/                # Command-line interface
├── crdt/               # CRDT implementations
├── crypto/             # Cryptographic operations
├── cursor/             # Opaque cursor token codec for GraphQL pagination
├── datastore/          # Data persistence abstractions
├── db/                 # Database core, one module per execution role:
│                       #   txn/ (the transaction seam), read/, write/,
│                       #   collection/, block/, definition/, access/,
│                       #   database/, docid/, downsample/, view/,
│                       #   event/, error/, plus the folded former crates:
│                       #   backup/, block/builder/, index/, merge/, nac/,
│                       #   search/. src/ holds only lib.rs; every test
│                       #   lives in db/tests/.
├── defra-core/         # Core types and traits
├── defra-node/         # Reusable embedded node builder
├── defra-version/      # Version metadata and Go compat tracking
├── document/           # Document handling
├── embedded/           # Embedded/mobile node assembly
├── events/             # Pub/sub event bus (subscriptions)
├── ffi/                # C-compatible FFI bindings
├── http/               # HTTP API server
├── identity/           # Identity and JWT management
├── keyring/            # Key storage
├── kms/                # DEK generation, wrapping, and distribution under NAC/DAC
├── lens/               # Schema migration via WASM transforms
├── orbis/              # Threshold BLS signing (Orbis ring client)
├── p2p/                # P2P networking (libp2p + optional Iroh)
├── p2p-adapter/        # P2P adapters for HTTP-facing operations
├── pg-compat/          # Postgres wire protocol compatibility
├── query/              # Query engine (GraphQL, BM25)
├── replication-filter/ # Query-filter-backed replication matcher
├── schema/             # Schema validation
├── sourcehub/          # On-chain ACP client (Cosmos/EVM)
├── storage/            # Storage layer over regolith, the one engine
├── telemetry/          # OpenTelemetry exporter setup
├── wasm/               # Browser client (WebAssembly)
└── zanzibar/           # Google Zanzibar permission engine

tools/
├── apple/                  # Apple embedding: .xcframework build + Swift import smoke test
├── ffi-test/               # FFI compatibility testing against Go
├── hf_embedding_server.py  # Local HuggingFace embedding server for embedding benchmarks
├── integration-test/       # Rust-native integration tests (primary validation)
├── lens-host/              # Standalone WASM Lens transform runner (JSON stdin → stdout)
├── otel-smoke/             # OTLP exporter smoke tests against a Compose-run collector
└── pg-compat-harness/      # Drizzle ORM harness for the Postgres wire protocol
```

Only `ffi-test`, `integration-test`, and `lens-host` are workspace members; the
rest are scripts and non-Rust harnesses.

### File Size Guidelines

- Under 200 lines: Fine
- 200-400 lines: Check if doing one thing
- Over 400 lines: Consider splitting

## 6. Naming Conventions

| Thing | Convention | Example |
|-------|------------|---------|
| Crates | lowercase, hyphens | `defra-core` |
| Files/Modules | snake_case | `lww.rs` |
| Types | PascalCase | `LwwDelta` |
| Functions | snake_case | `encode_priority()` |
| Constants | SCREAMING_SNAKE_CASE | `STATUS_ACTIVE` |

## 7. Comments Policy

**Minimal comments. Code should be self-documenting.**

✅ Comment: Non-obvious WHY, safety invariants, public API docs (`///`)

❌ Don't: What the code does, TODO/FIXME, commented-out code, change history

## 8. Git Worktree Workflow

```bash
cd ../defradb.rs-foo     # Work on feature foo
cd ../defradb.rs-bar     # Work on feature bar
```

Each worktree is isolated, no branch switching overhead.

## Build Dependencies

- **Rust** (1.91+): `rustup`
- **protoc**: `brew install protobuf` — required by `crates/orbis`, whose `build.rs` compiles `proto/orbis.proto` via `tonic-prost-build`
- **cbindgen**: `cargo install cbindgen` — generates C headers for FFI
- **Go** (1.26+): Required for FFI compatibility tests, and to build the pinned `GO_COMPAT_COMMIT`, which declares `go 1.26.7`

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
| Basic | `--test basic` | batch_mutations, collection_delete_4657, collection_management, document_lifecycle, regolith_crash_reopen, multi_collection, patch_secondary_relation_4709, self_ref_relations_4712, smoke, transactions, truncate_parallel |
| Query | `--test query` | commits_aggregate, commits_collection_id, commits_height_filter, continuous_rollup, datetime_index_range, default_values_v1, downsample, downsample_gc, exhaustive_orphans_4454, explain_nested, gql_list_args, index_fallback_4633, index_management, lens, lens_persistence, lens_reindex_secondary_index_979, limits, multi_cid_vectors, planner_4656, planner_4684, sdl_generate, subscription_docid, subscription_root_shape, view |
| ACP | `--test acp` | audit, basic, cross_object, custom_policy, events_sse, index, link_collection, multi_identity, multi_role, negative, negative_p2p, node_access, p2p, p2p_lifecycle, policy_validation, register_ops, relation_queries, relationship, revoke_lifecycle, secp256k1_round_trip, transaction_rollback, xarchive_access_matrix |
| P2P Iroh | `--test p2p_iroh` | acp, connection, peer, replication, schema, sync |
| NAC | `--test nac` | core_operations, cross_compartment_isolation, dac_access_matrix, document_acp, multi_doc_create, operations, p2p_management, policy_evolution, relation_admin |
| P2P | `--test p2p` | connection_manager, document, feature_binaries, filtered_replication, idempotent_replay, manage_relay, management, quarantine, receiver_pull, replication, replication_advanced, resilience, sync, transports, trust_boundary, write_contention |
| FTS | `--test fts` | basic, edge_cases, lifecycle, relation_paths, scoring |
| Encryption | `--test encryption` | acp, block_verify, cross_runtime_p2p, index, key_management, se_cross_runtime |
| Identity | `--test identity` | keyring_dev_mode, keyring_lifecycle, lifecycle, negative, node_identity, types |
| Backup | `--test backup` | dev_mode, dump, purge, restore |
| Cursor | `--test cursor` | composite_index, error_paths, reindex_datetime_visibility, smoke |
| SourceHub | `--test sourcehub` | acp_tuning, compartments, encryption_acp, p2p_acp, policy_lifecycle, resilience, smoke |
| Hub.rs | `--test hubrs` | compartments, p2p_acp, policy_lifecycle, smoke |

Single-purpose binaries, each its own `[[test]]` with no submodules:

| Binary | Covers |
|--------|--------|
| `--test p2p_admission` | replicator fan-in against a tiny pending-DAG cap (injects `DEFRA_P2P_MAX_PENDING_DAGS`) |
| `--test p2p_admission_restart` | hub restart must not lose success-acked pending-DAG registrations |
| `--test p2p_admission_fairness` | per-source-peer pending-DAG quotas under a noisy pusher |
| `--test p2p_backlog` | outbound push-backlog bounds and overload observability |
| `--test p2p_push_storm` | outbound push-storm reproduction and profiling harness |
| `--test p2p_deep_catchup` | deep full-DAG push under intake rate limiting |
| `--test p2p_interop_bench` | cross-runtime P2P replication cost |
| `--test issue1154_repro` | at-scale merge of every success-acked document after hub restart |
| `--test issue1194_repro` | concurrent updates to distinct documents must not conflict |
| `--test issue1211_repro` | same, through a branchable collection's head set |
| `--test issue1294_bytes_json` | Blob-as-Bytes create+query must return lowercase hex |
| `--test client_authored` | client-signed commit fragments pushed to `/sync`, and verified reads via `/block/signed` |

### Rust Commands

```bash
cargo test                         # Run all unit tests
cargo test -p crdt                 # Test specific crate
cargo clippy --all -- -D warnings  # Lint
just check-node-graph              # Feature-graph contracts for defra-node
cargo fmt --all                    # Format
cargo build --release              # Build release
```

### Build Profile in Worktrees

Every worktree keeps its own `target/`, and a stock dev build retains one debug
object per codegen unit forever, so concurrent worktrees add up fast. The
justfile's `profile` variable (default `dev`) selects the profile for the build
and test recipes; `super-dev` keeps `file:line` backtrace frames for workspace
crates while dropping dependency debug info, for ~7.5x less object volume.

```bash
just profile=super-dev test              # unit tests, into target/super-dev/
just profile=super-dev integration-suite acp
```

The override must precede the recipe name. Prefer it in agent worktrees; stay
on `dev` when stepping through code in lldb.

`just sweep` (keeps 7 days) deletes this worktree's stale artifacts. The
profile shrinks each build generation; sweeping bounds how many pile up.

### Tracking Go Upstream

```bash
# Go repo location — point DEFRADB_GO_REPO at your Go DefraDB checkout
cd "$DEFRADB_GO_REPO"

# Check what's landed on develop
git fetch origin develop
git log origin/develop --oneline -20

# Compare with our last sync point
git log origin/develop --oneline --since="1 week ago"
```

The Go repo has two remotes:
- `origin` → `sourcenetwork/defradb` (upstream)
- `fork` → `edjroz/defradb` (our fork; the rustffi client branches
  `edjroz/ffi-rust-compat` and `jack/ffi-rust-compat` both live on `origin`)

`DEFRADB_GO_REPO` covers the shell commands above and `ffi-test`, which also
accepts `--go-path <PATH>` to override it. Integration tests that consume Go
build artifacts resolve them at compile time under
`$HOME/go/src/github.com/sourcenetwork/defradb` — see `COPY_WASM_PATH` in
`tools/integration-test/tests/p2p_iroh/sync/version.rs`. A checkout elsewhere
needs a symlink at that path, or the `p2p_iroh` lens tests fail on a missing
WASM binary.

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

## Storage

[regolith](https://github.com/sourcenetwork/regolith) is the storage engine, on
every target: server, embedded, and WASM. There is no other backend and no
feature flag selecting one.

`--store` chooses whether anything is persisted:

| Value | Meaning |
|-------|---------|
| `regolith` | A regolith database on disk. The default. |
| `memory` | A regolith database kept in memory, which nothing outlives. |

The names of the backends regolith replaced (`lark`, `redb`, `fjall`, `rocksdb`,
`badger`) are still accepted and resolve to `regolith`, so an existing config
file keeps loading.

Profiles come from regolith itself: `Options::default()` for a server,
`Options::embedded()` for a 1-4 MiB working set, and `Options::wasm()` for a
browser or wasi module. Transactions run at `Serializable` with
`DurabilityMode::Immediate`.

## Goal

**New contributor feels ready to do productive work immediately.**

Fast context acquisition → Confident changes → Productive iteration.
