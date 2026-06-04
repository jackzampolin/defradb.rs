# Survey: `crates/defra-version/`

## Purpose
Compile-time version/build metadata. Exposes `VersionInfo` (semver, git commit/date
from `build.rs`, HTTP API version, DocID version, P2P multicodec, Rust toolchain) plus
`GoCompat` constants tracking the upstream Go commit/branch/tag last synced. Provides
formatting helpers: `short()`, `descriptive()` (OTLP `service.version`), `full()`, and
serde camelCase JSON serialization.

## State machines
None. No lifecycle enums, no transitions, no concurrency. Values are baked at build
time (`env!`, `build.rs` git shell-out) and read immutably thereafter.

## Modelable candidates
None. This crate is constants + string formatting + JSON serialization. Correctness is
"does the output string/JSON have the expected shape", which is fully and adequately
covered by the existing unit tests in `src/lib.rs` (format prefixes, camelCase keys,
populated fields). There are no algebraic laws (Lean), no concurrent/distributed
protocols, no security state machines, and no eventual-consistency concerns (TLA+).

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | — | — | — |

## Verdict
**Plumbing.** `model_worthy: false`. Pure build-metadata glue with deterministic string
formatting; unit tests are the right and sufficient validation. No formal model warranted.
