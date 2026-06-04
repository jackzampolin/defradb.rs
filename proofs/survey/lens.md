# Survey: `crates/lens/`

## Purpose
Non-destructive schema migration via WASM transforms (mirrors Go `internal/lens`).
Documents are migrated **at query time** from their stored schema version to a target
version by walking a collection version-history graph and applying registered
forward/inverse WASM transforms along the path.

Modules: `config` (LensConfig/LensModule), `doc` (LensDoc alias), `history` (version
DAG → targeted path), `pipeline` (the migration walk `transform_to_target`), `store`
(TransformStore trait + in-memory + WASM-backed store), `wasm`/`wasm_runtime`
(wasmtime sandbox host).

## State machines
- **Targeted-history construction** (`history.rs`): collapses a `previous`/`next`
  version DAG into a per-version path-to-target (`build_targeted_history`,
  `link_forwards`/`link_backwards`, visited-guarded recursion). Output is effectively
  a doubly-linked path.
- **Migration walk** (`pipeline.rs::transform_to_target`): from a doc's source
  version, step `next` (apply forward transform) or `previous` (apply inverse
  transform) toward the target, with a `visited` set; errors if no path. This is an
  implicit traversal/termination protocol.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| migration-walk termination/reachability | Lean/either | walk over a targeted history always terminates and reaches target along a valid path, or errors; never loops | no | low |
| transform/inverse round-trip | Lean | forward∘inverse = id over a doc | n/a — transform body is opaque user WASM; not provable in-crate | low |
| transform-id content-addressing | Lean | identical lens content → identical id (dedup) | partially (content-addressing covered by integrity/CRDT slices) | low |

## Verdict
**Not model-worthy (plumbing).** The only non-trivial logic is a visited-set-guarded
walk over a path-collapsed DAG; its termination is evident from the guard and is fully
exercised by `history.rs`/`pipeline.rs` unit tests plus `integration-test --test query`
(lens, lens_persistence). The interesting algebraic law (inverse undoes transform)
lives in opaque user-supplied WASM, so it is unprovable from this crate. Content-
addressing of transform IDs is a trivial sha256 already conceptually covered by
existing content-addressing slices. No concurrency, replication, consistency, or
security state machine here. `model_worthy: false`.
