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

### Session 1: DAC Implementation Review (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/acp/src/local.rs` | all (564 LOC) | LocalDocumentACP - primary in-memory DAC |
| `crates/acp/src/persistent.rs` | all (463 LOC) | PersistentAcpStore - database-backed |
| `crates/acp/src/dac.rs` | all | DAC logic abstraction |
| `crates/db/src/collection_acp.rs` | 40-61, 71-113 | `check_doc_permission()`, register/unregister doc |
| `crates/query/src/plan/permission_filter.rs` | all (176 LOC) | PermissionFilterNode - fail-closed, DAC bypass check |
| `crates/query/src/planner/builder/mod.rs` | 129-146, 200-220 | `maybe_wrap_with_acp_filter()` insertion point |

**Checklist**: Verify fail-closed on error, DAC bypass flag, policy-less collections, write mutations

### Session 2: NAC and Zanzibar Evaluation (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/acp/src/nac.rs` | all (728 LOC) | NAC state machine, 48 NodePermission variants |
| `crates/acp/src/zanzibar.rs` | all (322 LOC) | ZanzibarDocumentACP |
| `crates/acp/src/zanzibar_store.rs` | all | Relation tuple storage |
| `crates/acp/src/policy_yaml.rs` | all | Policy YAML parsing/validation |
| `crates/db/src/collection_acp.rs` | 172-273, 334-369 | Policy transitions, `block_unsafe_policy_transition()` |

**Checklist**: Policy transition safety, tuple format verification, expression evaluation, NAC state alignment

### Session 3: Bypass Surface & Recovery Mode (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/db/src/acp_merge_handler.rs` | 193-205 | **Recovery mode bypass** - skips all ACP |
| `crates/p2p/src/sync/replication/recovery.rs` | 37-151 | `recover_unmerged()`, `BlockMetadata::recovery()` |
| `crates/p2p/src/sync/merge.rs` | 61-89 | BlockMetadata struct, `recovery()` constructor |
| `crates/db/src/dump.rs` | 11-59 | **print_dump() bypasses ACP** - iterates Acpstore |
| `crates/cli/src/commands/server_dump.rs` | 14-70 | CLI dump wrapper (no identity) |
| `crates/query/src/runner/commits.rs` | ~388 | **execute_commits_query()** - may lack PermissionFilterNode |

**Checklist**: Verify recovery only at startup, dump endpoint exposure, _commits query ACP filtering

### Session 4: Integration Test Validation (MEDIUM)

| File | Focus |
|------|-------|
| `tools/integration-test/tests/acp_basic.rs` | Basic read filtering (68 LOC) |
| `tools/integration-test/tests/acp_multi_identity.rs` | Multi-user scenarios |
| `tools/integration-test/tests/acp_multi_role.rs` | Role-based access |
| `tools/integration-test/tests/acp_revoke_lifecycle.rs` | Permission revocation |
| `tools/integration-test/tests/acp_p2p.rs` | P2P merge filtering |
| `tools/integration-test/tests/encrypted_acp.rs` | SE + ACP interaction |
| `tools/integration-test/tests/backup_restore.rs` | Backup with ACP (likely MISSING) |

**Test Gaps**: No _commits query ACP test, no dump/export ACP test, no recovery mode ACP test

### Session 5: SourceHub Provider Integration (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/cli/src/sourcehub_acp_adapter.rs` | 55-121 | add_policy (local validate + on-chain), list/get (local cache only) |

**Checklist**: Policy ID sync (local vs on-chain), cache staleness during network partition, refresh strategy
