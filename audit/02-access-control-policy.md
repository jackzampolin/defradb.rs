# Audit Stream 2: Access Control Policy (ACP)

## Scope

The ACP subsystem enforces document-level access control. Audit covers:
- Policy enforcement completeness (all query paths, all mutation paths)
- Bypass vectors (direct storage access, replication paths, backup/restore)
- Policy language expressiveness vs actual enforcement
- Multi-identity edge cases
- Multi-role permission interactions
- NAC (Node Access Control) layer
- Cross-compartment isolation
- Policy evolution and migration

## Key Questions

- Can any code path reach document data without ACP checks?
- Are Commits queries filtered the same as User queries?
- Can a malicious peer inject documents that bypass ACP?
- Does backup/restore preserve or bypass ACP state?
- Are there TOCTOU races in policy checks?
- Is the SourceHub ACP provider equivalent to the local provider?

## Crates of Interest

- `acp/`
- `db/` (query execution paths)
- `query/` (query planning and filtering)
- `p2p/` (replication ACP enforcement)
- `http/` (API-level identity extraction)

## Recon Findings

### Surface Area
- **ACP crate**: 5,186 LOC (src) + 6,760 LOC (tests) across 25 source files, 14 test files
- **Three subsystems**: DAC (Document Access Control), NAC (Node Access Control), Zanzibar engine
- **DB integration**: ~700 LOC across collection_acp.rs, acp_merge_handler.rs, block_verify.rs, permission_filter.rs

### Architecture
- `LocalDocumentACP` (564 LOC) - in-memory implementation
- `PersistentAcpStore` (463 LOC) - database-backed
- `ZanzibarDocumentACP` (322 LOC) - Zanzibar-backed DAC
- **NAC**: 728 LOC, 48 NodePermission variants, 3-state machine (NotConfigured/Enabled/Disabled)
- **Policy**: YAML-based, SHA256-hashed IDs, union-only expressions

### Enforcement Points
- **Write path**: `check_doc_permission()` in collection_acp.rs
- **Read path**: `PermissionFilterNode` in query plan (fail-closed on error)
- **P2P merge**: `AcpMergeHandler` wraps merge with permission checks
- **Block verify**: `verify_block_signature()` checks Read permission

### Red Flags
- **HIGH: Recovery mode bypass** - AcpMergeHandler skips ACP during DB recovery (acp_merge_handler.rs:193-198)
- **MEDIUM: Backup/export ACP gap** - `print_dump()` iterates ALL keys including AcpStore namespace; relies on query layer for filtering
- **MEDIUM: Peer-to-DID mapping optional** - Unconfigured mapping logs warning but allows merge
- **LOW: SourceHub provider incomplete** - Interface defined but not fully integrated
- **LOW: No backup/export ACP tests** visible in integration suite

### Test Coverage: GOOD
- 697 LOC of ACP integration tests (6 files)
- 6,760 LOC unit tests including Zanzibar stress tests
- Gap: No backup/export with ACP test coverage

## Estimated Scope

**MEDIUM: 3-5 sessions**

### Session 1: DAC Implementation Review (CRITICAL) — COMPLETED

| File | Lines | Focus |
|------|-------|-------|
| `crates/acp/src/local.rs` | all (564 LOC) | LocalDocumentACP - primary in-memory DAC |
| `crates/acp/src/persistent.rs` | all (463 LOC) | PersistentAcpStore - database-backed |
| `crates/acp/src/dac.rs` | all | DAC logic abstraction |
| `crates/db/src/collection_acp.rs` | 40-61, 71-113 | `check_doc_permission()`, register/unregister doc |
| `crates/query/src/plan/permission_filter.rs` | all (176 LOC) | PermissionFilterNode - fail-closed, DAC bypass check |
| `crates/query/src/planner/builder/mod.rs` | 129-146, 200-220 | `maybe_wrap_with_acp_filter()` insertion point |

**Checklist**: Verify fail-closed on error, DAC bypass flag, policy-less collections, write mutations

**Findings (Session 1)**:
- **03 (MEDIUM)**: CID time-travel queries bypass ACP — `_caller_identity` deliberately unused
- **04 (MEDIUM)**: Encrypted search queries bypass ACP — no identity parameter in path
- **05 (LOW)**: DAC bypass thread-local not cleared after query — fragile but mitigated
- **06 (LOW)**: View plans don't apply view-collection's own ACP policy
- **07 (INFO)**: Full DAC checklist verification — core implementation is sound

Also verified with deeper evidence:
- **02 (CRITICAL)**: _commits bypass confirmed with full execution trace
- **00 (MEDIUM)**: Recovery mode bypass — no change to assessment
- **01 (MEDIUM)**: Dump bypass — no change to assessment

### Session 2: NAC and Zanzibar Evaluation (HIGH) — COMPLETED

| File | Lines | Focus |
|------|-------|-------|
| `crates/acp/src/nac/` | all (~728 LOC across 6 files) | NAC state machine, 48 NodePermission variants |
| `crates/acp/src/zanzibar/` | all (~700 LOC across 5 files) | ZanzibarDocumentACP, PersistentZanzibarStore |
| `crates/acp/src/policy_yaml/` | all (~380 LOC across 3 files) | Policy YAML parsing/validation |
| `crates/db/src/collection_acp.rs` | 172-273, 334-369 | Policy transitions, `block_unsafe_policy_transition()` |
| `crates/http/src/handlers/graphql/query.rs` | all | GraphQL NAC enforcement gap |
| `crates/http/src/nac_guard.rs` | all | NAC permission guard |

**Findings (Session 2)**:
- **08 (HIGH)**: GraphQL endpoint bypasses NAC permission checks entirely — no `require_permission()` calls
- **09 (MEDIUM)**: NAC enable endpoint has no authentication — race condition on bootstrap
- **10 (MEDIUM)**: Policy transition safety guards (`block_unsafe_policy_transition`) are dead code — never called
- **11 (LOW)**: Policy expressions support intersection (`&`) and difference (`-`), not just unions
- **12 (LOW)**: Zanzibar storage key lacks `/` delimiter sanitization — potential prefix collisions
- **13 (INFO)**: NAC DisabledTemporarily state correctly blocks relationship writes — verified sound
- **14 (LOW)**: Policy YAML parsing has no size limits — resource exhaustion risk
- **15 (MEDIUM)**: Zanzibar read permission check silently suppresses engine errors — fail-open
- **16 (MEDIUM)**: Debug dump endpoint has no NAC check — unauthenticated access to all data
- **17 (INFO)**: Policy ID is counter-dependent double SHA256, not content hash — correct for Go compat

Also verified:
- NAC 48 NodePermission variants: all properly mapped, `as_str()` and `parse()` roundtrip-tested
- NAC state machine: all 3 states correct, transitions well-guarded
- NAC query-level enforcement: REST handlers comprehensive (80+ `require_permission` calls)
- Zanzibar tuple format: `(resource, object_id, relation, subject_hash)` — exact match, no wildcards in normal lookup
- Zanzibar tuple lookup: exact key match + wildcard fallback (correct Zanzibar semantics)
- Owner auto-injection: `build_policy()` correctly prepends `owner` to all permission expressions

### Session 3: Bypass Surface & Recovery Mode (CRITICAL) — COMPLETED

| File | Lines | Focus |
|------|-------|-------|
| `crates/db/src/acp_merge_handler.rs` | 193-205 | **Recovery mode bypass** - skips all ACP |
| `crates/p2p/src/sync/replication/recovery.rs` | 37-151 | `recover_unmerged()`, `BlockMetadata::recovery()` |
| `crates/p2p/src/sync/merge.rs` | 61-89 | BlockMetadata struct, `recovery()` constructor |
| `crates/db/src/dump.rs` | 11-59 | **print_dump() bypasses ACP** - iterates Acpstore |
| `crates/cli/src/commands/server_dump.rs` | 14-70 | CLI dump wrapper (no identity) |
| `crates/cli/src/version_syncer.rs` | 308 | **Version sync uses recovery metadata** |
| `crates/db/src/block_verify.rs` | 15-112 | **verify_block_signature() not in merge path** |
| `crates/http/src/handlers/utility.rs` | 47-54 | **Dump endpoint: no auth, no NAC** |

**Findings (Session 3)**:
- **00 (HIGH↑)**: Recovery mode bypass — version sync exploitable mid-operation (upgraded from MEDIUM)
- **01 (HIGH↑)**: Dump bypasses ACP and NAC — HTTP-exposed, fully unauthenticated (upgraded from MEDIUM)
- **18 (HIGH)**: P2P merge path does not verify block signatures — untrusted blocks accepted
- **19 (HIGH)**: P2P block creator identity from metadata, not signature — identity spoofing
- **20 (MEDIUM)**: Block verification function structurally disconnected from merge path

Also verified:
- `recover_unmerged()` is startup-only (p2p crate internal) — correctly scoped
- `export_database()` uses query executor with ACP — correctly gated
- Acpstore contains policy YAML + Zanzibar relation tuples — highly sensitive
- Systemic P2P merge authentication gap: findings 00, 18, 19 form a combined attack chain

### Session 4: Integration Test Validation (MEDIUM) — COMPLETED

| File | Focus |
|------|-------|
| `tools/integration-test/tests/acp_basic.rs` | Basic read filtering (79 LOC) |
| `tools/integration-test/tests/acp_multi_identity.rs` | Multi-user scenarios |
| `tools/integration-test/tests/acp_multi_role.rs` | Role-based access |
| `tools/integration-test/tests/acp_revoke_lifecycle.rs` | Permission revocation |
| `tools/integration-test/tests/acp_node_access.rs` | NAC relationship lifecycle |
| `tools/integration-test/tests/acp_p2p.rs` | P2P merge filtering |
| `tools/integration-test/tests/encrypted_acp.rs` | SE + ACP interaction |
| `tools/integration-test/tests/nac_document_acp.rs` | Two-layer NAC + ACP |
| `tools/integration-test/tests/cross_compartment_isolation.rs` | Multi-policy compartments |
| `tools/integration-test/tests/backup_restore.rs` | Backup with ACP (CONFIRMED MISSING) |
| `tools/integration-test/tests/dump.rs` | Dump with ACP (CONFIRMED MISSING) |
| `tools/integration-test/tests/block_verify.rs` | Block signature (no ACP) |

**Findings (Session 4)**:
- **22 (HIGH)**: No integration test for `_commits` ACP bypass — CRITICAL vulnerability with zero test coverage
- **23 (MEDIUM)**: No dump or backup/restore test with ACP — HIGH bypass vulnerabilities untested
- **24 (HIGH)**: P2P ACP tests never verify merge denial — systemic P2P auth gap untested
- **25 (MEDIUM)**: No GraphQL NAC integration test — HIGH NAC bypass untested
- **26 (LOW)**: Weak mutation denial assertions use `if let Ok` silent-skip pattern
- **27 (MEDIUM)**: No unauthorized document creation test in ACP-protected collections
- **28 (LOW)**: No policy transition or DAC bypass flag tests

**Key result**: Of 17 security findings (CRITICAL through LOW, excluding INFO), zero have regression tests.

### Session 5: SourceHub Provider Integration (MEDIUM) — COMPLETED

| File | Lines | Focus |
|------|-------|-------|
| `crates/cli/src/sourcehub_acp_adapter.rs` | 55-121 | add_policy (local validate + on-chain), list/get (local cache only) |
| `crates/sourcehub/src/dac.rs` | all (254 LOC) | SourceHubDocumentACP — on-chain permission checks |
| `crates/sourcehub/src/provider.rs` | all (89 LOC) | SourceHubProvider trait definition |
| `crates/sourcehub/src/cosmos.rs` | all (189 LOC) | CosmosProvider — Cosmos SDK implementation |
| `crates/sourcehub/src/client.rs` | all (370 LOC) | HTTP/ABCI client for SourceHub queries |
| `crates/cli/src/doc_acp_adapter.rs` | all (163 LOC) | DocumentAcpAdapter — relationship validation |
| `crates/cli/src/commands/start/server.rs` | 486-527 | Provider selection at startup |

**Findings (Session 5)**:
- **30 (HIGH)**: verify_access fails open on ABCI error codes — errors masqueraded as denial, brittle protobuf
- **31 (MEDIUM)**: Policy add is non-atomic — on-chain success + local cache failure → orphaned policy
- **32 (MEDIUM)**: Policy cache has no refresh mechanism — stale reads permanent
- **33 (MEDIUM)**: Network partition — no explicit fail-closed policy, no circuit breaker
- **34 (MEDIUM)**: Bearer token requires global signing config — fails for unknown DIDs
- **35 (LOW)**: Managing relations parameter ignored by SourceHub (architecturally correct)
- **36 (HIGH)**: Recovery mode bypasses on-chain SourceHub permissions (amplified finding 00)
- **37 (INFO)**: All Session 1-4 findings apply equally to SourceHub mode
- **38 (MEDIUM)**: SourceHub integration tests cover happy path only — missing security scenarios

Also verified:
- Provider selection at startup: clean `if/else` on `AcpDocumentType::SourceHub` vs `Local`
- DocumentACP trait: both providers implement all 6 methods
- PermissionFilterNode: provider-agnostic, fail-closed on all errors
- AcpMergeHandler: provider-agnostic, recovery bypass affects all providers
- SourceHub permission checks go on-chain (correct) — cache staleness only affects policy CRUD
- Go divergence: Go queries SourceHub on-demand; Rust caches locally (design asymmetry)

## Completion Status

**Stream 2: Access Control Policy — COMPLETE (5/5 sessions)**

### Session Summary

| Session | Focus | Findings | Severity |
|---------|-------|----------|----------|
| 1 | DAC Implementation Review | 03-07 | 2 MEDIUM, 2 LOW, 1 INFO |
| 2 | NAC and Zanzibar Evaluation | 08-17 | 1 HIGH, 3 MEDIUM, 3 LOW, 3 INFO |
| 3 | Bypass Surface & Recovery | 00↑, 01↑, 18-20 | 3 HIGH (incl. 2 upgrades), 1 MEDIUM |
| 4 | Integration Test Validation | 22-28 | 2 HIGH, 3 MEDIUM, 2 LOW |
| 5 | SourceHub Provider Integration | 30-38 | 2 HIGH, 5 MEDIUM, 1 LOW, 1 INFO |

### Total Finding Count

| Severity | Count | Findings |
|----------|-------|----------|
| CRITICAL | 1 | 02 (_commits bypass) |
| HIGH | 8 | 00↑, 01↑, 08, 18, 19, 22, 24, 30, 36 |
| MEDIUM | 13 | 03, 04, 09, 10, 15, 16, 20, 23, 25, 27, 31, 32, 33, 34, 38 |
| LOW | 8 | 05, 06, 11, 12, 14, 26, 28, 35 |
| INFO | 5 | 07, 13, 17, 37, 39 (session summaries: 21, 29) |

### Cross-Stream Themes

**1. Bypass Surface is Wide**
Multiple code paths reach document data without ACP checks: `_commits` queries, dump/backup, encrypted search, CID time-travel, recovery mode. These affect both local and SourceHub providers equally via the trait abstraction.

**2. P2P Trust Boundary is Weak**
The P2P merge path has no signature verification (18), uses self-reported creator identity (19), and skips ACP entirely during recovery (00/36). These three findings form a combined attack chain that is more severe in SourceHub mode where on-chain authorization is the intended security model.

**3. Zero Regression Tests for Security Findings**
Of 22 actionable findings (CRITICAL through LOW), none have corresponding regression tests. The integration test suites cover happy-path scenarios but not the bypass vectors or edge cases identified in the audit.

**4. SourceHub Introduces Cache Consistency as a New Risk Class**
The Rust SourceHub implementation caches policies locally (diverging from Go which queries on-demand), creating a class of issues — cache staleness, non-atomic writes, stale reads — that don't exist in the local ACP path or in Go.
