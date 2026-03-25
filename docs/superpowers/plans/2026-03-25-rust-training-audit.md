# Rust Training Audit & Codebase Improvement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Audit the defradb.rs codebase against Microsoft's Rust training materials and apply targeted improvements across all 25 crates.

**Architecture:** Two-phase hybrid approach. Phase 1 dispatches 7 read-only topic audit agents in parallel to produce structured findings. Phase 2 dispatches ~13 crate implementation agents in isolated worktrees to apply fixes. Integration merges branches in dependency order with test gates.

**Tech Stack:** Rust, Cargo, git worktrees, Claude Code agents

**Spec:** `docs/superpowers/specs/2026-03-25-rust-training-audit-design.md`

**Training repo:** `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/`

---

## File Map

| Output | Purpose |
|--------|---------|
| `docs/superpowers/specs/audit-findings/async.md` | Phase 1: Async audit findings |
| `docs/superpowers/specs/audit-findings/error-handling.md` | Phase 1: Error handling findings |
| `docs/superpowers/specs/audit-findings/concurrency.md` | Phase 1: Concurrency findings |
| `docs/superpowers/specs/audit-findings/unsafe.md` | Phase 1: Unsafe & verification findings |
| `docs/superpowers/specs/audit-findings/type-design.md` | Phase 1: Type design findings |
| `docs/superpowers/specs/audit-findings/serialization.md` | Phase 1: Serialization & zero-copy findings |
| `docs/superpowers/specs/audit-findings/file-structure.md` | Phase 1: File structure & API design findings |
| `docs/superpowers/specs/audit-findings.md` | Consolidated findings grouped by crate |
| Branches `audit/db`, `audit/query`, etc. | Phase 2: One branch per crate agent |

---

## Task 1: Setup — Create Findings Directory

**Files:**
- Create: `docs/superpowers/specs/audit-findings/` (directory)

- [ ] **Step 1: Create the output directory for audit findings**

```bash
mkdir -p docs/superpowers/specs/audit-findings
```

- [ ] **Step 2: Verify the training repo is accessible**

```bash
ls /Users/johnzampolin/go/src/github.com/microsoft/RustTraining/async-book/src/SUMMARY.md
```

Expected: file exists

---

## Task 2: Phase 1 — Async Patterns Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 3, 4, 5, 6, 7, 8**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/async-book/src/ch08-tokio-deep-dive.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/async-book/src/ch12-common-pitfalls.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/async-book/src/ch13-production-patterns.md`
- Create: `docs/superpowers/specs/audit-findings/async.md`

- [ ] **Step 1: Read training chapters**

Read these three files from the async-book:
- `ch08-tokio-deep-dive.md` — Tokio task model, scheduler, channel types, Semaphore
- `ch12-common-pitfalls.md` — blocking executor, MutexGuard across .await, cancellation, select! fairness
- `ch13-production-patterns.md` — graceful shutdown, backpressure, JoinSet/TaskTracker, timeouts, Tower

Extract a checklist of anti-patterns and best practices from these chapters.

- [ ] **Step 2: Scan crates for async anti-patterns**

Search these crates for violations: `p2p`, `db`, `query`, `http`, `events`, `embedded`, `defra-node`, `cli`

Specific searches to run:

```bash
# MutexGuard held across .await
grep -rn "\.lock()" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/ | head -50
# Then for each hit, check if .await appears before the guard is dropped

# Blocking calls in async context
grep -rn "std::thread::sleep\|std::fs::\|std::net::" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/

# Unbounded channels (missing backpressure)
grep -rn "unbounded_channel\|mpsc::channel(" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/

# spawn without JoinHandle tracking
grep -rn "tokio::spawn\|task::spawn" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/

# select! usage (check for fairness)
grep -rn "select!" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/

# Shutdown patterns
grep -rn "CancellationToken\|watch::channel\|shutdown\|graceful" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/

# spawn_blocking usage
grep -rn "spawn_blocking" crates/{p2p,db,query,http,events,embedded,defra-node,cli}/src/
```

For each hit, read the surrounding context (20-30 lines) to determine if it's actually a problem.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/async.md` using this format:

```markdown
# Async Patterns Audit Findings

## Summary
- Total findings: N
- Critical: N | High: N | Medium: N | Low: N

## Findings

### Finding 1
- **severity:** high
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/iroh/endpoint.rs
- **line:** 123-145
- **pattern:** mutex-across-await
- **description:** MutexGuard from `self.state.lock()` is held across `.await` on line 130. If the future is cancelled, the lock is not released, causing deadlock.
- **training_ref:** async-book ch12 "Holding MutexGuard Across .await"
- **suggested_fix:** Clone the data out of the lock before awaiting, or use tokio::sync::Mutex if the lock must span an await.

### Finding 2
...
```

Only report genuine issues. Do not report things that are correct usage. If something looks intentional and sound, skip it.

---

## Task 3: Phase 1 — Error Handling Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 2, 4, 5, 6, 7, 8**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch10-error-handling-patterns.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/async-book/src/ch13-production-patterns.md`
- Create: `docs/superpowers/specs/audit-findings/error-handling.md`

- [ ] **Step 1: Read training chapters**

Read these files:
- `rust-patterns-book/src/ch10-error-handling-patterns.md` — thiserror vs anyhow, ? operator, panic vs Result, catch_unwind
- `async-book/src/ch13-production-patterns.md` — async error handling, double-? pattern, error propagation through try_join!

Extract the rules: thiserror for library crates, anyhow for binaries, #[from] for auto-conversion, .context() for propagation.

- [ ] **Step 2: Scan all crates for error handling issues**

```bash
# anyhow in library crates (should only be in cli/defra-node binaries)
grep -rn "anyhow" crates/*/Cargo.toml | grep -v "cli\|defra-node"
grep -rn "anyhow::" crates/*/src/ | grep -v "cli\|defra-node" | head -50

# Bare unwrap() in non-test code
grep -rn "\.unwrap()" crates/*/src/**/*.rs | grep -v "test\|tests\|mock\|fixture" | head -50

# Bare expect() in non-test code
grep -rn "\.expect(" crates/*/src/**/*.rs | grep -v "test\|tests\|mock\|fixture" | head -50

# String errors (Box<dyn Error>, String as error)
grep -rn "Box<dyn.*Error>" crates/*/src/ | head -30
grep -rn "Result<.*String>" crates/*/src/ | head -30

# Missing catch_unwind at FFI boundaries
grep -rn "extern \"C\"" crates/ffi/src/ | head -20
# Then check if each extern "C" fn body is wrapped in catch_unwind

# Error types per crate
for crate in crates/*/; do
  echo "=== $crate ==="
  grep -rn "enum.*Error" "$crate/src/" 2>/dev/null | head -5
done
```

For each finding, read surrounding context to confirm it's a real issue.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/error-handling.md` using the same format as Task 2 Step 3.

---

## Task 4: Phase 1 — Concurrency Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 2, 3, 5, 6, 7, 8**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch05-channels-and-message-passing.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch06-concurrency-vs-parallelism-vs-threads.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/async-book/src/ch08-tokio-deep-dive.md`
- Create: `docs/superpowers/specs/audit-findings/concurrency.md`

- [ ] **Step 1: Read training chapters**

Read these files:
- `rust-patterns-book/src/ch05-channels-and-message-passing.md` — mpsc, crossbeam, select!, actor pattern, bounded vs unbounded
- `rust-patterns-book/src/ch06-concurrency-vs-parallelism-vs-threads.md` — Arc<Mutex<T>>, RwLock, atomics, Condvar, OnceLock/LazyLock, SeqLock
- `async-book/src/ch08-tokio-deep-dive.md` — tokio::sync primitives

Extract the decision flowchart: when to use Mutex vs channel vs atomic vs RwLock.

- [ ] **Step 2: Scan crates for concurrency issues**

```bash
# Arc<Mutex<T>> usage — check if channel would be simpler
grep -rn "Arc<Mutex" crates/{p2p,db,crdt,events,blockstore,storage}/src/ | head -50
grep -rn "Arc<RwLock" crates/{p2p,db,crdt,events,blockstore,storage}/src/ | head -50

# Atomic ordering — check for Relaxed where Acquire/Release needed
grep -rn "Ordering::Relaxed\|Ordering::SeqCst\|Ordering::Acquire\|Ordering::Release" crates/*/src/ | head -50

# lazy_static (should be OnceLock/LazyLock)
grep -rn "lazy_static" crates/*/src/ | head -20
grep -rn "lazy_static" crates/*/Cargo.toml

# Nested locks (potential deadlock)
# Look for functions that acquire multiple locks
grep -rn "\.lock()\|\.read()\|\.write()" crates/{p2p,db,crdt,events,blockstore,storage}/src/ | head -50

# Send/Sync bounds on public async traits
grep -rn "async fn" crates/*/src/lib.rs crates/*/src/traits.rs crates/*/src/mod.rs 2>/dev/null | head -30
```

For each Arc<Mutex> hit, read the usage pattern (how many writers? readers? is it a shared-state or message-passing pattern?) to determine if a channel would be better.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/concurrency.md` using the same format as Task 2 Step 3.

---

## Task 5: Phase 1 — Unsafe & Verification Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 2, 3, 4, 6, 7, 8**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch12-unsafe-rust-controlled-danger.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/engineering-book/src/ch05-miri-valgrind-and-sanitizers-verifying-u.md`
- Create: `docs/superpowers/specs/audit-findings/unsafe.md`

- [ ] **Step 1: Read training chapters**

Read these files:
- `rust-patterns-book/src/ch12-unsafe-rust-controlled-danger.md` — five unsafe superpowers, soundness, safe wrappers, arena allocators, UB pitfalls
- `engineering-book/src/ch05-miri-valgrind-and-sanitizers-verifying-u.md` — Miri, Valgrind, ASan/MSan/TSan, cargo-fuzz, loom

Extract the checklist: every unsafe block needs SAFETY comment, every FFI fn needs catch_unwind, every raw pointer use needs justification.

- [ ] **Step 2: Find and audit all unsafe blocks**

```bash
# All unsafe blocks in non-test code
grep -rn "unsafe " crates/*/src/ | grep -v "//\|test\|#\[" | head -100

# All extern "C" functions
grep -rn "extern \"C\"" crates/*/src/ | head -50

# Raw pointer usage
grep -rn "as \*const\|as \*mut\|\*const \|\*mut " crates/*/src/ | head -50

# SAFETY comments (to see what's already documented)
grep -rn "// SAFETY\|// Safety\|// SAFETY:" crates/*/src/ | head -50

# FFI functions without catch_unwind
grep -rn "pub extern" crates/ffi/src/ | head -20

# transmute usage
grep -rn "transmute" crates/*/src/ | head -20

# MaybeUninit usage
grep -rn "MaybeUninit" crates/*/src/ | head -20
```

For EVERY `unsafe` block found, read the full block and surrounding context. Verify:
1. Is there a `SAFETY:` comment? If not, finding.
2. Is the safety justification correct? If wrong or incomplete, finding.
3. Could this be replaced with a safe abstraction? If yes, finding.
4. Is this a Miri candidate? If yes, note it.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/unsafe.md` using the same format as Task 2 Step 3. Include a separate section at the end listing Miri and loom testing candidates.

---

## Task 6: Phase 1 — Type Design Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 2, 3, 4, 5, 7, 8**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/type-driven-correctness-book/src/ch02-typed-command-interfaces-request-determi.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/type-driven-correctness-book/src/ch03-single-use-types-cryptographic-guarantees.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/type-driven-correctness-book/src/ch04-capability-tokens-zero-cost-proof-of-aut.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/type-driven-correctness-book/src/ch05-protocol-state-machines-type-state-for-r.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/type-driven-correctness-book/src/ch07-validated-boundaries-parse-dont-validate.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/type-driven-correctness-book/src/ch09-phantom-types-for-resource-tracking.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch03-the-newtype-and-typestate-patterns.md`
- Create: `docs/superpowers/specs/audit-findings/type-design.md`

- [ ] **Step 1: Read training chapters**

Read the seven files listed above. Extract patterns:
- Newtype pattern: wrap primitives for domain concepts
- Type-state pattern: encode state machine transitions in the type system
- Capability tokens: zero-cost proof of authority
- Validated boundaries: parse, don't validate
- PhantomData for resource tracking
- `#[must_use]`, `#[non_exhaustive]`

- [ ] **Step 2: Scan crates for type design opportunities**

```bash
# Public structs with raw primitive fields that might deserve newtypes
# Look at core types
grep -rn "pub struct" crates/defra-core/src/ | head -30
grep -rn "pub struct" crates/document/src/ | head -30
grep -rn "pub struct" crates/schema/src/ | head -30
grep -rn "pub struct" crates/identity/src/ | head -30

# Fields using raw u64/u32/String where a newtype might help
grep -rn "pub.*: u64\|pub.*: u32\|pub.*: String\|pub.*: Vec<u8>" crates/{defra-core,document,schema,identity,acp,zanzibar,crdt}/src/ | head -50

# Enums without #[non_exhaustive]
grep -rn "pub enum" crates/*/src/ | head -50
# Then check if any have #[non_exhaustive]
grep -rn "non_exhaustive" crates/*/src/ | head -20

# Functions returning Result without #[must_use]
grep -rn "#\[must_use\]" crates/*/src/ | head -20

# State machine patterns (look for state enums, transition functions)
grep -rn "State\|Phase\|Stage\|Step\|Status" crates/*/src/ | grep "enum\|struct" | head -30

# Existing newtypes (to understand current patterns)
grep -rn "pub struct.*(" crates/{defra-core,document,identity}/src/ | head -30
```

For newtype candidates, only flag cases where a raw primitive is used as an ID, key, hash, or domain-specific quantity across module boundaries. Internal use of `u64` as a local counter is fine.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/type-design.md` using the same format as Task 2 Step 3.

---

## Task 7: Phase 1 — Serialization & Zero-Copy Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 2, 3, 4, 5, 6, 8**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch11-serialization-zero-copy-and-binary-data.md`
- Create: `docs/superpowers/specs/audit-findings/serialization.md`

- [ ] **Step 1: Read training chapter**

Read `rust-patterns-book/src/ch11-serialization-zero-copy-and-binary-data.md`. Extract patterns:
- serde derive attributes (rename_all, skip, default, flatten, with)
- enum representations (external/internal/adjacent/untagged)
- zero-copy deserialization (`&'de str` vs `String`)
- `Cow<str>` for sometimes-owned data
- `bytes::Bytes` vs `Vec<u8>`
- `repr(C)` for FFI structs
- `zerocopy`/`bytemuck`

- [ ] **Step 2: Scan crates for serialization issues**

```bash
# Serde derives — find all structs with Serialize/Deserialize
grep -rn "derive.*Serialize\|derive.*Deserialize" crates/{blockstore,crdt,document,defra-core,http,p2p,ffi}/src/ | head -50

# Check for String fields that could be Cow<str> or &str
# Read the actual struct definitions for serde-derived types
grep -rn -A5 "derive.*Deserialize" crates/{blockstore,crdt,document,defra-core}/src/ | head -100

# Vec<u8> usage where bytes::Bytes might help
grep -rn "Vec<u8>" crates/{blockstore,crdt,document,defra-core,http,p2p}/src/ | head -50

# .clone() on serialization/deserialization paths
grep -rn "\.clone()" crates/{blockstore,crdt,document,defra-core}/src/ | head -50

# repr(C) usage
grep -rn "repr(C)" crates/*/src/ | head -20

# bytes::Bytes current usage
grep -rn "bytes::Bytes\|use bytes" crates/*/src/ | head -20
grep -rn "bytes" crates/*/Cargo.toml | head -20

# Check for .to_vec() and .to_string() in hot paths
grep -rn "\.to_vec()\|\.to_string()\|\.to_owned()" crates/{blockstore,crdt,document}/src/ | head -50
```

For clone/to_vec/to_string hits, read the context to see if it's in a hot path (called per-document or per-query) vs. one-time setup.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/serialization.md` using the same format as Task 2 Step 3.

---

## Task 8: Phase 1 — File Structure & API Design Audit

> **Dispatch as: background agent (read-only, no code changes)**
> **Can run in parallel with: Tasks 2, 3, 4, 5, 6, 7**

**Files:**
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/rust-patterns-book/src/ch15-crate-architecture-and-api-design.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/engineering-book/src/ch07-release-profiles-and-binary-size.md`
- Read: `/Users/johnzampolin/go/src/github.com/microsoft/RustTraining/engineering-book/src/ch08-compile-time-and-developer-tools.md`
- Create: `docs/superpowers/specs/audit-findings/file-structure.md`

- [ ] **Step 1: Read training chapters**

Read the three files. Extract patterns:
- Module layout conventions
- Public API checklist (sealed traits, `#[non_exhaustive]`, `#[must_use]`)
- `impl Into<T>` / `impl AsRef<T>` / `Cow` parameter ergonomics
- Feature flags with `dep:` syntax
- Workspace organization

- [ ] **Step 2: Audit file sizes and structure**

```bash
# All files over 400 lines
find crates -name "*.rs" -exec wc -l {} + | sort -rn | awk '$1 > 400 {print}'

# Public items that might should be pub(crate)
# Check lib.rs files for pub re-exports
for crate in crates/*/; do
  echo "=== $crate ==="
  cat "$crate/src/lib.rs" 2>/dev/null | grep "^pub " | head -10
done

# Sealed trait candidates (traits with only internal implementations)
grep -rn "pub trait" crates/*/src/ | head -50

# impl Into / impl AsRef on public functions
grep -rn "pub fn\|pub async fn" crates/*/src/lib.rs | head -50
```

For each file over 400 lines, read it and identify natural split points:
- Are there multiple structs/enums that each deserve their own file?
- Are there test blocks that could move to a `tests/` file?
- Are there helper functions that form a cohesive submodule?

Propose specific split points with new file names.

- [ ] **Step 3: Write findings report**

Write findings to `docs/superpowers/specs/audit-findings/file-structure.md` using the same format as Task 2 Step 3. For file-split findings, use this extended format:

```markdown
### Finding N
- **severity:** low
- **category:** structure
- **crate:** db
- **file:** crates/db/src/downsample.rs (2036 lines)
- **pattern:** oversized-file
- **description:** File contains 3 distinct concerns: [X], [Y], [Z]
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/downsample/mod.rs` — public API re-exports
  - `crates/db/src/downsample/x.rs` — [X] (est. ~400 lines)
  - `crates/db/src/downsample/y.rs` — [Y] (est. ~600 lines)
  - `crates/db/src/downsample/z.rs` — [Z] (est. ~500 lines)
```

---

## Task 9: Phase 1 — Consolidate Findings

> **Dispatch as: foreground agent (depends on Tasks 2-8 completing)**
> **Blocked by: Tasks 2, 3, 4, 5, 6, 7, 8**

**Files:**
- Read: `docs/superpowers/specs/audit-findings/async.md`
- Read: `docs/superpowers/specs/audit-findings/error-handling.md`
- Read: `docs/superpowers/specs/audit-findings/concurrency.md`
- Read: `docs/superpowers/specs/audit-findings/unsafe.md`
- Read: `docs/superpowers/specs/audit-findings/type-design.md`
- Read: `docs/superpowers/specs/audit-findings/serialization.md`
- Read: `docs/superpowers/specs/audit-findings/file-structure.md`
- Create: `docs/superpowers/specs/audit-findings.md`

- [ ] **Step 1: Read all seven findings reports**

Read each of the seven audit findings files.

- [ ] **Step 2: Deduplicate findings**

If multiple audits flagged the same file/line for different reasons, merge into a single finding with multiple patterns noted. For example, if the async audit and concurrency audit both flag the same `Arc<Mutex>` usage, combine them.

- [ ] **Step 3: Group by crate and write consolidated report**

Write `docs/superpowers/specs/audit-findings.md` with this structure:

```markdown
# Consolidated Audit Findings

## Summary
- Total findings: N (after dedup)
- By severity: Critical: N | High: N | Medium: N | Low: N
- By category: Bug: N | Unsound: N | Anti-pattern: N | Improvement: N | Structure: N

## Findings by Crate

### db (N findings)

[All findings for the db crate, sorted by severity]

### query (N findings)

[All findings for the query crate, sorted by severity]

### p2p (N findings)
...

[Continue for all crates that have findings]

## Cross-Cutting Findings

[Findings that affect multiple crates or require coordinated changes]

## Crate Priority Ranking

[Rank crates by total finding weight: critical=4, high=3, medium=2, low=1]
[This ranking informs which crate agents to dispatch first]
```

- [ ] **Step 4: Commit all Phase 1 outputs**

```bash
git add docs/superpowers/specs/audit-findings/
git add docs/superpowers/specs/audit-findings.md
git commit -m "audit: Phase 1 findings from Rust training review

Seven topic audits (async, error handling, concurrency, unsafe,
type design, serialization, file structure) scanned the full
codebase against Microsoft RustTraining materials.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Phase 2 — Implement `defra-core` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 11, 12, 13 (other Tier 1 crates)**
> **Merge tier: 1 (leaf crate, no internal deps)**

**Files:**
- Modify: files in `crates/defra-core/` as identified by findings
- Input: `docs/superpowers/specs/audit-findings.md` (defra-core section only)

- [ ] **Step 1: Create worktree**

```bash
git worktree add ../defradb.rs-audit-defra-core -b audit/defra-core
cd ../defradb.rs-audit-defra-core
```

- [ ] **Step 2: Read findings for this crate**

Read `docs/superpowers/specs/audit-findings.md` and extract only findings where `crate: defra-core`. Sort by severity (critical first).

- [ ] **Step 3: Apply critical and high severity findings**

For each critical/high finding:
1. Read the file at the reported line
2. Apply the suggested fix
3. Run `cargo check -p defra-core`
4. If check fails, adjust the fix

- [ ] **Step 4: Apply medium severity findings**

For each medium finding:
1. Read the file, apply the fix
2. Run `cargo check -p defra-core`
3. Skip if the fix would require changes in other crates

- [ ] **Step 5: Apply low severity findings (file splits, API tightening)**

For each file over 400 lines:
1. Create the new module directory
2. Move code into split files
3. Update `mod.rs` or `lib.rs` re-exports
4. Run `cargo check -p defra-core`

- [ ] **Step 6: Run quality gates**

```bash
cargo fmt --all
cargo clippy -p defra-core -- -D warnings
cargo test -p defra-core
```

All three must pass. Fix any issues.

- [ ] **Step 7: Commit changes**

```bash
git add -A
git commit -m "audit(defra-core): apply Rust training improvements

Applied N findings from codebase audit:
- [list key changes]

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 8: Write change summary**

Output a summary: what was changed, what was skipped and why, any cross-crate findings flagged for later.

---

## Task 11: Phase 2 — Implement `crypto` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 10, 12, 13**
> **Merge tier: 1**

Same workflow as Task 10 but for the `crypto` crate. Steps:

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-crypto -b audit/crypto`
- [ ] **Step 2: Read findings** — extract `crate: crypto` from consolidated findings
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p crypto -- -D warnings`, `cargo test -p crypto`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 12: Phase 2 — Implement `defra-version` + `events` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 10, 11, 13**
> **Merge tier: 1**

Same workflow as Task 10 but for `defra-version` and `events` crates together. Steps:

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-events -b audit/events`
- [ ] **Step 2: Read findings** — extract `crate: defra-version` and `crate: events` from consolidated findings
- [ ] **Step 3: Apply critical/high findings** for both crates
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p defra-version -p events -- -D warnings`, `cargo test -p defra-version -p events`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 13: Phase 2 — Merge Tier 1 and Gate

> **Dispatch as: foreground agent**
> **Blocked by: Tasks 10, 11, 12**

- [ ] **Step 1: Merge Tier 1 branches into main**

```bash
git merge audit/defra-core --no-ff -m "audit: merge defra-core improvements"
git merge audit/crypto --no-ff -m "audit: merge crypto improvements"
git merge audit/events --no-ff -m "audit: merge events + defra-version improvements"
```

If conflicts arise, resolve them (these are leaf crates so conflicts should be minimal).

**Rollback policy:** If any merge breaks the gate, revert that branch (`git revert -m 1 HEAD`), flag its findings for manual review, and continue with remaining branches. Don't block the pipeline.

- [ ] **Step 2: Run Tier 1 gate**

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo test
```

All must pass. If any fail, identify which merge caused the issue and fix.

- [ ] **Step 3: Clean up worktrees**

```bash
git worktree remove ../defradb.rs-audit-defra-core
git worktree remove ../defradb.rs-audit-crypto
git worktree remove ../defradb.rs-audit-events
```

---

## Task 14: Phase 2 — Implement `storage` + `datastore` + `keyring` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 15, 16**
> **Blocked by: Task 13 (Tier 1 merged)**
> **Merge tier: 2**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-storage -b audit/storage`
- [ ] **Step 2: Read findings** — extract `crate: storage`, `crate: datastore`, `crate: keyring`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p storage -p datastore -p keyring -- -D warnings`, `cargo test -p storage -p datastore -p keyring`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 15: Phase 2 — Implement `document` + `schema` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 14, 16**
> **Blocked by: Task 13 (Tier 1 merged)**
> **Merge tier: 2**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-schema -b audit/schema`
- [ ] **Step 2: Read findings** — extract `crate: document`, `crate: schema`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p document -p schema -- -D warnings`, `cargo test -p document -p schema`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 16: Phase 2 — Merge Tier 2 and Gate

> **Dispatch as: foreground agent**
> **Blocked by: Tasks 14, 15**

- [ ] **Step 1: Merge Tier 2 branches**

```bash
git merge audit/storage --no-ff -m "audit: merge storage + datastore + keyring improvements"
git merge audit/schema --no-ff -m "audit: merge document + schema improvements"
```

**Rollback policy:** If any merge breaks the gate, revert that branch, flag for manual review, continue with others.

- [ ] **Step 2: Run Tier 2 gate**

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo test
```

- [ ] **Step 3: Clean up worktrees**

```bash
git worktree remove ../defradb.rs-audit-storage
git worktree remove ../defradb.rs-audit-schema
```

---

## Task 17: Phase 2 — Implement `blockstore` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 18, 19, 20**
> **Blocked by: Task 16 (Tier 2 merged)**
> **Merge tier: 3**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-blockstore -b audit/blockstore`
- [ ] **Step 2: Read findings** — extract `crate: blockstore`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — `blockstore_tests.rs` is 1450 lines, likely needs splitting
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p blockstore -- -D warnings`, `cargo test -p blockstore`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 18: Phase 2 — Implement `crdt` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 17, 19, 20**
> **Blocked by: Task 16 (Tier 2 merged)**
> **Merge tier: 3**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-crdt -b audit/crdt`
- [ ] **Step 2: Read findings** — extract `crate: crdt`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — `property_tests.rs` is 1269 lines
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p crdt -- -D warnings`, `cargo test -p crdt`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 19: Phase 2 — Implement `acp` + `zanzibar` + `identity` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 17, 18, 20**
> **Blocked by: Task 16 (Tier 2 merged)**
> **Merge tier: 3**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-acp -b audit/acp`
- [ ] **Step 2: Read findings** — extract `crate: acp`, `crate: zanzibar`, `crate: identity`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p acp -p zanzibar -p identity -- -D warnings`, `cargo test -p acp -p zanzibar -p identity`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 20: Phase 2 — Implement `lens` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 17, 18, 19**
> **Blocked by: Task 16 (Tier 2 merged)**
> **Merge tier: 3**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-lens -b audit/lens`
- [ ] **Step 2: Read findings** — extract `crate: lens`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p lens -- -D warnings`, `cargo test -p lens`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 21: Phase 2 — Merge Tier 3 and Gate

> **Dispatch as: foreground agent**
> **Blocked by: Tasks 17, 18, 19, 20**

- [ ] **Step 1: Merge Tier 3 branches**

```bash
git merge audit/blockstore --no-ff -m "audit: merge blockstore improvements"
git merge audit/crdt --no-ff -m "audit: merge crdt improvements"
git merge audit/acp --no-ff -m "audit: merge acp + zanzibar + identity improvements"
git merge audit/lens --no-ff -m "audit: merge lens improvements"
```

**Rollback policy:** If any merge breaks the gate, revert that branch, flag for manual review, continue with others.

- [ ] **Step 2: Run Tier 3 gate (includes integration tests)**

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo test
cargo test -p integration-test
```

- [ ] **Step 3: Clean up worktrees**

```bash
git worktree remove ../defradb.rs-audit-blockstore
git worktree remove ../defradb.rs-audit-crdt
git worktree remove ../defradb.rs-audit-acp
git worktree remove ../defradb.rs-audit-lens
```

---

## Task 22: Phase 2 — Implement `db` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Task 23**
> **Blocked by: Task 21 (Tier 3 merged)**
> **Merge tier: 4**

This is the largest crate. Known large files: `downsample.rs` (2036), `merge_handler/composite.rs` (1372), `merge_handler/mod.rs` (1100), `index_manager_tests.rs` (1541), `txn_registry_tests.rs` (949).

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-db -b audit/db`
- [ ] **Step 2: Read findings** — extract `crate: db` — expect this to be the largest findings set
- [ ] **Step 3: Apply critical/high findings** — especially async and concurrency patterns in merge handler
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — split `downsample.rs`, `merge_handler/composite.rs`, `merge_handler/mod.rs`, large test files
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p db -- -D warnings`, `cargo test -p db`
- [ ] **Step 7: Commit** — may need multiple commits for logical change groups
- [ ] **Step 8: Write change summary**

---

## Task 23: Phase 2 — Implement `query` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Task 22**
> **Blocked by: Task 21 (Tier 3 merged)**
> **Merge tier: 4**

Known large files: `runner/query/nested.rs` (1821), `sdl_parse/parser_tests.rs` (1813), `runner/commits.rs` (1545), `planner/joins/mod.rs` (1395), `query_parse/parser.rs` (1270), `mapper/filter/filter_tests.rs` (1126), `sdl_parse/builder.rs` (1040), `plan/mutation/create.rs` (969), `plan/type_join/type_join_one.rs` (938), `plan/type_join/type_join_many/plan_node.rs` (932).

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-query -b audit/query`
- [ ] **Step 2: Read findings** — extract `crate: query` — expect many file structure findings
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — this crate has 10+ files over 900 lines; split methodically, one file at a time, checking compilation between each
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p query -- -D warnings`, `cargo test -p query`
- [ ] **Step 7: Commit** — multiple commits recommended for this crate
- [ ] **Step 8: Write change summary**

---

## Task 24: Phase 2 — Merge Tier 4 and Gate

> **Dispatch as: foreground agent**
> **Blocked by: Tasks 22, 23**

- [ ] **Step 1: Merge Tier 4 branches**

```bash
git merge audit/db --no-ff -m "audit: merge db improvements"
git merge audit/query --no-ff -m "audit: merge query improvements"
```

**Rollback policy:** If any merge breaks the gate, revert that branch, flag for manual review, continue with others.

- [ ] **Step 2: Run Tier 4 gate**

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo test
cargo test -p integration-test
```

- [ ] **Step 3: Clean up worktrees**

```bash
git worktree remove ../defradb.rs-audit-db
git worktree remove ../defradb.rs-audit-query
```

---

## Task 25: Phase 2 — Implement `http` + `pg-compat` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 26, 27, 28**
> **Blocked by: Task 24 (Tier 4 merged)**
> **Merge tier: 5**

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-http -b audit/http`
- [ ] **Step 2: Read findings** — extract `crate: http`, `crate: pg-compat`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)**
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p http -p pg-compat -- -D warnings`, `cargo test -p http -p pg-compat`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 26: Phase 2 — Implement `p2p` Improvements

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 25, 27, 28**
> **Blocked by: Task 24 (Tier 4 merged)**
> **Merge tier: 5**

Known large file: `iroh/endpoint.rs` (1420 lines).

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-p2p -b audit/p2p`
- [ ] **Step 2: Read findings** — extract `crate: p2p` — expect async and concurrency findings
- [ ] **Step 3: Apply critical/high findings** — especially async patterns in iroh endpoint
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — split `iroh/endpoint.rs`
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p p2p -- -D warnings`, `cargo test -p p2p`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 27: Phase 2 — Implement `embedded` + `ffi` + `wasm` + remaining small crates

> **Dispatch as: agent in isolated worktree**
> **Can run in parallel with: Tasks 25, 26, 28**
> **Blocked by: Task 24 (Tier 4 merged)**
> **Merge tier: 5**

Covers: `embedded`, `ffi`, `wasm`, `orbis`, `sourcehub`. Known large file: `embedded/src/node.rs` (1267 lines).

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-misc -b audit/misc`
- [ ] **Step 2: Read findings** — extract findings for all five crates
- [ ] **Step 3: Apply critical/high findings** — especially FFI safety in `ffi` crate
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — split `embedded/src/node.rs`
- [ ] **Step 6: Run quality gates** — `cargo fmt`, clippy and test for each crate
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 28: Phase 2 — Merge Tier 5 and Gate

> **Dispatch as: foreground agent**
> **Blocked by: Tasks 25, 26, 27**

- [ ] **Step 1: Merge Tier 5 branches**

```bash
git merge audit/http --no-ff -m "audit: merge http + pg-compat improvements"
git merge audit/p2p --no-ff -m "audit: merge p2p improvements"
git merge audit/misc --no-ff -m "audit: merge embedded + ffi + wasm + remaining crate improvements"
```

**Rollback policy:** If any merge breaks the gate, revert that branch, flag for manual review, continue with others.

- [ ] **Step 2: Run Tier 5 gate**

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo test
cargo test -p integration-test
```

- [ ] **Step 3: Clean up worktrees**

```bash
git worktree remove ../defradb.rs-audit-http
git worktree remove ../defradb.rs-audit-p2p
git worktree remove ../defradb.rs-audit-misc
```

---

## Task 29: Phase 2 — Implement `cli` + `defra-node` Improvements

> **Dispatch as: agent in isolated worktree**
> **Blocked by: Task 28 (Tier 5 merged)**
> **Merge tier: 6 (final)**

Known large files: `cli/src/commands/start/server.rs` (1428), `cli/src/p2p_adapter.rs` (1214), `defra-node/src/benchmark_support.rs` (985), `defra-node/src/lib.rs` (939).

- [ ] **Step 1: Create worktree** — `git worktree add ../defradb.rs-audit-cli -b audit/cli`
- [ ] **Step 2: Read findings** — extract `crate: cli`, `crate: defra-node`
- [ ] **Step 3: Apply critical/high findings**
- [ ] **Step 4: Apply medium findings**
- [ ] **Step 5: Apply low findings (file splits)** — split the four large files
- [ ] **Step 6: Run quality gates** — `cargo fmt`, `cargo clippy -p cli -p defra-node -- -D warnings`, `cargo test -p cli -p defra-node`
- [ ] **Step 7: Commit**
- [ ] **Step 8: Write change summary**

---

## Task 30: Final Integration — Merge Tier 6 and Full Gate

> **Dispatch as: foreground agent**
> **Blocked by: Task 29**

- [ ] **Step 1: Merge final branch**

```bash
git merge audit/cli --no-ff -m "audit: merge cli + defra-node improvements"
```

**Rollback policy:** If merge breaks the gate, revert, flag for manual review.

- [ ] **Step 2: Run full integration suite**

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo test
cargo test -p integration-test
```

- [ ] **Step 3: Clean up final worktree**

```bash
git worktree remove ../defradb.rs-audit-cli
```

- [ ] **Step 4: Verify no stale worktrees remain**

```bash
git worktree list
```

Should only show the main worktree.

---

## Task 31: Cross-Cutting Follow-Up

> **Dispatch as: foreground agent**
> **Blocked by: Task 30**

- [ ] **Step 1: Collect skipped findings**

Gather all findings that implementation agents skipped because they required cross-crate changes. These were flagged in each agent's change summary (Step 8 of each implementation task).

- [ ] **Step 2: Evaluate cross-cutting changes**

For each skipped finding:
1. Is it still relevant after all per-crate changes?
2. What crates does it touch?
3. Is it safe to apply now that all tiers are merged?

- [ ] **Step 3: Apply cross-cutting fixes if any**

If there are actionable cross-cutting findings:
1. Create a single branch: `git checkout -b audit/cross-cutting`
2. Apply fixes
3. Run full quality gates: `cargo fmt`, clippy, `cargo test`, `cargo test -p integration-test`
4. Commit and merge

- [ ] **Step 4: Write final summary**

Write a summary of the full audit:
- Total findings found vs applied vs skipped
- Key improvements made per crate
- Remaining items deferred (if any)
- Recommendations for ongoing enforcement (e.g., clippy lints to enable)

---

## Dependency Graph

```
Tasks 2-8 (Phase 1 audits, all parallel)
    │
    ▼
Task 9 (Consolidate findings)
    │
    ▼
Tasks 10, 11, 12 (Tier 1: defra-core, crypto, events+defra-version — parallel)
    │
    ▼
Task 13 (Merge Tier 1 + gate)
    │
    ▼
Tasks 14, 15 (Tier 2: storage cluster, schema cluster — parallel)
    │
    ▼
Task 16 (Merge Tier 2 + gate)
    │
    ▼
Tasks 17, 18, 19, 20 (Tier 3: blockstore, crdt, acp cluster, lens — parallel)
    │
    ▼
Task 21 (Merge Tier 3 + gate with integration tests)
    │
    ▼
Tasks 22, 23 (Tier 4: db, query — parallel)
    │
    ▼
Task 24 (Merge Tier 4 + gate with integration tests)
    │
    ▼
Tasks 25, 26, 27 (Tier 5: http cluster, p2p, misc — parallel)
    │
    ▼
Task 28 (Merge Tier 5 + gate with integration tests)
    │
    ▼
Task 29 (Tier 6: cli + defra-node)
    │
    ▼
Task 30 (Final merge + full integration suite)
    │
    ▼
Task 31 (Cross-cutting follow-up)
```

**Total agents dispatched:** 7 (Phase 1) + 13 (Phase 2) + 1 (cross-cutting) = 21
**Maximum parallel agents at any point:** 7 (during Phase 1)
**Sequential gates:** 6 merge tiers
