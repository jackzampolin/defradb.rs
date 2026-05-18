# Go upstream survey — 2026-04-18 → 2026-05-18

Survey of Go DefraDB upstream commits on `origin/develop` since 2026-04-18, classified for the Rust port (defradb.rs v1.0-rc1 tracking Go v1.0.0-rc1).

Sources:
- Go repo: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb` @ `origin/develop`
- Window: `git log origin/develop --since="2026-04-18"` (23 commits)

## Summary

| Category | Count |
|---|---|
| already-ported | 2 |
| already-tracked in another worktree | 1 |
| not-applicable (no equivalent stack in Rust) | 3 |
| not-applicable (Go-test-infra / dependabot / JS / build flag) | 13 |
| **port-needed** | **2** |
| **low-priority doc-only (track but don't port)** | **2** |

## Per-PR classification

| Go commit | Go PR | Title | Status | Rationale / Pointer |
|---|---|---|---|---|
| `1fab9fb3` | #4778 | fix: KMS lookup under NAC for encrypted DAG sync | **not-applicable** | Rust has no KMS service. Encryption uses a shared hardcoded key today; integration tests are `#[ignore]`'d pending KMS work. Port only relevant once KMS lands. |
| `6c874754` | #4794 | feat: Support querying with multiple cids | **already-tracked** | Tracked as Rust issue #972; separate worktree (`../defradb.rs-972-multi-cid-query`). |
| `cc6eb7a3` | #4810 | bot: Update dependencies (bulk dependabot) | not-applicable | Rust crate updates run on their own cadence. |
| `046e6caa` | #4809 | feat: Gate commit queries with DAC | **already-ported** | Rust commit `3ffc3e88` (Feb 23) — added per-doc ACP filtering at `crates/query/src/runner/commits.rs:710-792`. Test coverage in `tools/integration-test/tests/acp/audit.rs::cid_time_travel_acp_bypass_test`. Caveat: Go organizes commit-ACP tests in `tests/integration/acp/dac/commits/`; Rust consolidates them in `audit.rs`. Optional follow-up: split into `tests/acp/commits/` subdir for parity. |
| `ee87a529` | #4796 | test(i): Exclude LevelDB from multi-txn test | not-applicable | Documents a Go-side LevelDB limitation (#4795). Rust storage backends (redb, fjall, rocksdb, memory) — none are LevelDB. |
| `04a2d1eb` | #4688 | feat: Add syntax sugar `DeleteCollection` cmd | **port-needed** | Underlying `delete_collection` already exists in Rust at `crates/db/src/collection_ops/delete.rs:68`; HTTP DELETE route wired at `crates/http/src/handlers/collections.rs` + `routes.rs:81`. CLI wrapper missing. Add `Delete(CollectionDeleteArgs)` to `CollectionCommand` in `crates/cli/src/commands/client/collection/mod.rs:37`, plus HTTP client helper. |
| `f550ce2b` | #4767 | bot: Update dependencies | not-applicable | Dependabot bulk update. |
| `83b141c4` | #4681 | fix: Fix cli commands markdown output | not-applicable | Go-specific cobra markdown generation; Rust CLI (clap) doesn't use this tooling. |
| `702aee24` | #4737 | test: Document incremental lens migration bug under indexes | **low-priority doc-only** | Documents Go bug #4736 with skipped tests. Track as future Rust issue if/when lens+index combo is implemented; no immediate port. |
| `630f4f30` | #4763 | fix: Prevent txn-actions from executing concurrently | **already-ported** | Rust commit `b6255b00` in PR #951 ("fix: serialize actions per transaction"). Per-txn `action_lock: Arc<async_lock::Mutex<()>>` at `crates/db/src/txn_context.rs:29`, acquired in `crates/query/src/runner/executor.rs:302-306` and `crates/db/src/txn_registry.rs`. Test: `execute_in_txn_serializes_concurrent_actions_on_same_handle`. |
| `5d03d037` | #4775 | refactor: Use JS txn directly instead of context | not-applicable | JS-client-specific refactor; Rust has no JS client. |
| `4ac67d81` | #4761 | fix: Make silent build tag work on Android and Linux | not-applicable | Go build-tag specific. |
| `84b2398f` | #4765 | test: Introduce C binding smoke test | not-applicable | Out of FFI scope (Rust FFI is client-only per [feedback_ffi_scope](../../.claude/projects/-Users-johnzampolin-go-src-github-com-sourcenetwork-defradb-rs/memory/feedback_ffi_scope.md)). |
| `bf43a593` | #4747 | bot: Update dependencies | not-applicable | Dependabot bulk update. |
| `0374a801` | #4755 | fix: Persist test state for change detector | not-applicable | Go-specific test-harness "change detector" feature; Rust test harness is structurally different. |
| `30cf74a9` | #4746 | test: Add signed-docs test multiplier | **file as sub-issue** | Rust already has focused signing coverage (`tools/integration-test/tests/encryption/block_verify.rs`, `tests/p2p_iroh/connection/signature.rs`). Go's multiplier value is regression-detection across the whole suite via `DEFRA_MULTIPLIERS=signed-docs`. Porting requires extending `defra-harness` (external repo `sourcenetwork/backbone`), so track separately. |
| `ac820098` | #4754 | test: Migrate commit txn action to testo | not-applicable | Go test-framework migration to "testo"; Rust uses a different structure (`tools/integration-test/tests/*`). |
| `7ca3b280` | #4749 | test: Migrate UpdateDoc action to new system | not-applicable | Same as above. |
| `dc575858` | #4750 | fix: Seed CollectionVersions from DB | not-applicable | Go fix is for a test-harness reconstruction issue (change detector across process boundaries). Rust auto-loads collection versions during `DB::open_with_options()` via `load_collections()` in `crates/db/src/collection_ops/mod.rs:56-243`. Underlying durability is already correct. |
| `43250ed7` | #4723 | chore: Collection version templates in tests | not-applicable | Go test-template feature; doesn't map to Rust test layout. |
| `83af37a9` | #4639 | fix: Reduce telemetry logs | not-applicable | Go fix attaches an `otel.SetErrorHandler` to dedupe OTLP collector "connection refused" spam. Rust has **no OTEL exporter setup** today — there are no `opentelemetry`/`otlp` crate uses in `crates/*/Cargo.toml`. Nothing in the Rust codebase emits these errors. Port becomes relevant only if/when an OTEL exporter is added in Rust. |
| `a81825d4` | #4701 | chore: Fix the C binding Linux build command | not-applicable | Out of FFI scope. |
| `ce014761` | #4714 | test(i): Fix npx tests | not-applicable | JS-client-specific. |

## Port plan

This worktree (`sync/go-upstream-survey`):
- **#4688 DeleteCollection CLI** — ported. Adds `defradb client collection delete <names> [--active-only]`, Go-compatible `DELETE /api/v0/collections?name=...&active-only=...` HTTP route, and a new `DB::delete_collections(names, active_only)` orchestrator over the existing `delete_collection_versions_batch`.

Filed as tracking issues on `sourcenetwork/defradb.rs`:
- **[#976](https://github.com/sourcenetwork/defradb.rs/issues/976)** — Port KMS-under-NAC (Go #4778). Blocked on KMS infrastructure.
- **[#977](https://github.com/sourcenetwork/defradb.rs/issues/977)** — Port OTEL telemetry log dedup (Go #4639). Blocked on OTEL exporter.
- **[#978](https://github.com/sourcenetwork/defradb.rs/issues/978)** — Port signed-docs test multiplier (Go #4746). Needs `defra-harness` changes in `sourcenetwork/backbone`.
- **[#979](https://github.com/sourcenetwork/defradb.rs/issues/979)** — Track Go incremental-lens-under-indexes bug (Go #4737). Re-evaluate when Rust grows the same feature combination.

Optional follow-up (not filed):
- **#4809 commit-ACP test reorg** — split consolidated tests into `tests/acp/commits/` subdir for parity with Go layout. Pure test reorg; the functional port already landed in Rust commit `3ffc3e88`.

Out of scope here:
- Anything Go-test-infra (#4754, #4749, #4723, #4755, #4767-style dependabot, #4796 LevelDB, #4737 lens-index doc-only).
- JS / FFI-C / build-flag fixes that have no Rust equivalent.

## Notes for next survey window

- Run `git -C /Users/johnzampolin/go/src/github.com/sourcenetwork/defradb fetch origin develop && git log origin/develop --since="2026-05-18" --oneline` to start the next sync.
- The 30-day window 2026-04-18 → 2026-05-18 produced 23 upstream commits; of those, only 2 needed Rust ports. The bulk of upstream churn was Go-internal (test framework, dependabot, build tooling). Future surveys can confidently skip those categories at first glance.
