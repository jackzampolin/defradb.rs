# Query Crate Decomposition Design

## Goal

Turn issue 670 into an implementable staged refactor that reduces `crates/query` compile surface without forcing a single high-risk crate split.

## Current Reality

`crates/query` is still one workspace crate that publicly exposes parser, mapper, planner, plan nodes, runner, REST, SDL parsing, schema generation, subscriptions, transactions, and compatibility re-exports from `crates/query/src/lib.rs`.

Approximate current module size on `origin/main`:

- `plan/`: 12.6k LOC
- `runner/`: 12.2k LOC
- `planner/`: 8.8k LOC
- `sdl_parse/`: 4.8k LOC
- `query_parse/`: 4.0k LOC
- `mapper/`: 4.0k LOC
- `rest/`: 0.8k LOC
- `schema_gen/`: 0.5k LOC

## Constraints

- The top-level `query` crate is consumed broadly by `db`, `http`, `embedded`, `ffi`, `wasm`, `defra-node`, benches, and crate-local tests.
- `runner` is not parser-independent today. `runner/executor.rs` parses and validates requests directly through `query_parse`.
- `planner` and `plan` currently form a cycle at the module level:
  - `planner` builds concrete nodes from `plan`
  - `plan` nodes depend on `planner::{PlanNode, Doc, ExecInfo}`
- `rest` is not an independent subsystem. It is a thin adapter around `QueryRunner`.
- Compile-time win must be measured. A split that preserves broad facade recompilation may add complexity without real payoff.

## Design Principles

- Keep `query` as the public facade crate during the transition.
- Extract leaf-like crates first.
- Break type cycles before trying to separate planner from plan nodes.
- Prefer compatibility re-exports over workspace-wide import rewrites in the first phase.
- Each stage must compile and provide a measurable simplification even if compile-time gains arrive later.

## Proposed Target Shape

### Stable Facade

Keep `crates/query` as the compatibility surface for:

- `QueryRunner`
- `QueryExecutor`
- `RestOperations`
- transaction APIs
- compatibility re-exports used by downstream crates

### Candidate New Crates

`query-model`

- Owns mapper-style AST and shared query request types.
- Initial contents:
  - `mapper/`
  - parser-facing enums and structs that do not require execution traits

`query-parse`

- Owns GraphQL request parsing only.
- Initial contents:
  - `query_parse/`
- Depends on `query-model`.
- Does not own execution-time collection validation.

`query-sdl`

- Owns SDL parsing and optional schema generation helpers.
- Initial contents:
  - `sdl_parse/`
  - possibly `schema_gen/`
- Depends on `schema` and shared query error types or a reduced parse error surface.

`query-plan-core`

- Owns the execution traits and shared plan data structures that currently create the `planner` <-> `plan` cycle.
- Initial contents:
  - `PlanNode`
  - `Doc`
  - `DocFields`
  - `DocStatus`
  - `ExecInfo`
  - index scan parameter types if they are needed by both planner and runtime/fetcher code

`query-plan`

- Owns concrete plan nodes and planner construction once `query-plan-core` exists.
- Initial contents:
  - `plan/`
  - `planner/`

This is the highest-risk extraction and should not be attempted first.

## Recommended Staging

### Stage 0: Baseline And Compatibility Map

- Record crate-internal and cross-workspace imports.
- Record `cargo check -p query` timing and at least one whole-workspace timing baseline.
- Decide which `query::*` imports must remain stable through re-exports.

### Stage 1: Extract `query-model`

Why first:

- `mapper` is heavily shared by parsing, planning, explaining, and runtime code.
- It is large enough to matter, but conceptually cleaner than planner/runtime code.
- It creates a natural dependency direction for later crates.

Move:

- `crates/query/src/mapper/**`

Keep in `query` temporarily:

- `pub use query_model::*` style re-exports so downstream imports remain stable.

Expected effect:

- Makes parser extraction possible without copying AST types.
- Shrinks the conceptual surface of the facade crate even if build wins are initially small.

### Stage 2: Extract `query-parse`

Why second:

- `query_parse` is a substantial module and already clusters around parser concerns.
- It mainly depends on GraphQL parser crates and mapper/model types.

Move:

- `crates/query/src/query_parse/**`

Do not move yet:

- `validate_parsed_operation` in its current form, unless it is refactored to a provider-agnostic validation layer.

Refactor prerequisite:

- Split parse result construction from collection-provider validation.
- The parser crate should return parsed operations.
- The facade or executor layer should own validation against live collections.

### Stage 3: Extract `query-sdl`

Why third:

- `sdl_parse` is already separated and mostly schema-oriented.
- It is less entangled with runtime execution than `runner`, `planner`, or `plan`.

Move:

- `crates/query/src/sdl_parse/**`
- `crates/query/src/schema_gen/**` if the coupling remains small and intentional

### Stage 4: Introduce `query-plan-core`

Why before touching `planner` or `plan`:

- The current cycle blocks a clean crate split.
- `PlanNode`, `Doc`, and `ExecInfo` are shared execution concepts, not planner-only concepts.

Move or redefine:

- `crates/query/src/planner/traits.rs`
- selected shared index-scan types used by fetchers or runtime code

Risk:

- This is the stage most likely to trigger import churn and subtle trait-bound breakage.

### Stage 5: Split `query-plan`

Only after `query-plan-core` exists and downstream imports are insulated by facade re-exports.

Move:

- `crates/query/src/plan/**`
- `crates/query/src/planner/**`

Non-goal for the first design/implementation slice:

- Splitting `runner` into its own crate. It still pulls in parsing, model types, fetcher traits, planner traits, transactions, NAC/ACP behavior, explain logic, and compatibility APIs.

## Dependency Direction

Preferred final direction:

`query-model`
-> `query-parse`
-> `query-plan-core`
-> `query-plan`
-> `query` facade/runtime

And separately:

`schema`
-> `query-sdl`
-> `query` facade/runtime

This keeps the top-level runtime/facade crate at the edge rather than as the center of all shared types.

## First Implementation Slice

The first code-bearing follow-up should target `query-model`, not the planner/runtime split.

Success conditions:

- New workspace crate compiles.
- `query` re-exports preserve existing downstream imports.
- `cargo check` remains green for `query` and at least the direct consumers that import mapper types.
- No behavior changes.

Files most likely involved:

- `Cargo.toml`
- `crates/query/Cargo.toml`
- new `crates/query-model/Cargo.toml`
- new `crates/query-model/src/lib.rs`
- `crates/query/src/lib.rs`
- `crates/query/src/query_parse/**`
- `crates/query/src/planner/**`
- `crates/query/src/plan/**`
- `crates/query/src/runner/**`
- cross-workspace import sites that directly use `query::mapper::*`

## Risks

- Public API churn if re-exports are not preserved carefully.
- Hidden cyclic dependencies beyond the obvious `planner`/`plan` knot.
- Minimal compile-time improvement if the facade crate still recompiles most downstream code.
- Test and bench breakage from direct internal-path imports such as `query::planner::index_selection::*`.

## Non-Goals

- No single-step split of parse, plan, and execute into three final crates.
- No immediate `runner` extraction.
- No broad renaming of downstream imports unless required for a specific extraction.

## Success Criteria For Issue 670

- The issue becomes a parent design/execution ticket with staged follow-ups.
- The first follow-up extracts `query-model` or `query-parse` with compatibility re-exports.
- Compile-time measurement is captured before and after each stage.
- `query-plan-core` is introduced before any attempt to separate `planner` from `plan`.
