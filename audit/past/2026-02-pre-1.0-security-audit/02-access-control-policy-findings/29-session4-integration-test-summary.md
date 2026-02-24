# Session 4 Summary: Integration Test Validation

**Stream**: 02 - Access Control Policy
**Session**: 4 of 5 (MEDIUM)

## Test Suite Overview

### ACP Integration Tests Reviewed

| File | Lines | Tests | ACP Scope |
|------|-------|-------|-----------|
| `acp_basic.rs` | 79 | Read filtering, grant, denial | User query only |
| `acp_multi_identity.rs` | 133 | 5-identity visibility, grant/revoke | User query only |
| `acp_multi_role.rs` | 159 | Admin/writer/reader roles, mutation denial | User query + mutations |
| `acp_revoke_lifecycle.rs` | 114 | Grant/revoke/re-grant cycles, truncation | User query only |
| `acp_node_access.rs` | 71 | NAC relationship add/delete, disable/enable | NAC operations only |
| `acp_p2p.rs` | 146 | Public + protected doc replication | P2P replication (success only) |
| `nac_document_acp.rs` | 188 | Two-layer NAC + document ACP | NAC + DAC combined |
| `cross_compartment_isolation.rs` | 298 | Multi-policy compartment isolation | Cross-policy queries + mutations |
| `encrypted_acp.rs` | 151 | Encryption + ACP combined | Encrypted doc lifecycle |
| `backup_restore.rs` | 160 | Backup export/import | **No ACP at all** |
| `dump.rs` | 34 | Database dump | **No ACP at all, #[ignore]** |
| `block_verify.rs` | 108 | Block signature verification | **No ACP, uses _commits** |

### Total: 1,641 lines of ACP-related integration tests

## What's Well Tested

| Capability | Test | Quality |
|-----------|------|---------|
| Owner reads own document | acp_basic, acp_multi_identity | Strong |
| Unauthorized user sees empty results | acp_basic (Bob=0), acp_multi_identity (Bob/Carol/Dave/Eve=0) | Strong |
| Reader grant → visibility | acp_basic, acp_multi_identity | Strong |
| Writer grant → mutation | acp_multi_identity (Carol updates), acp_multi_role | Strong |
| Reader cannot update | acp_multi_role (Dave attempt) | Weak (if let Ok pattern) |
| Writer cannot delete | acp_multi_role (Carol attempt) | Weak (if let Ok pattern) |
| Grant/revoke cycle | acp_revoke_lifecycle (6-step cycle) | Strong |
| Immediate revocation (no cache) | acp_revoke_lifecycle (immediate check after revoke) | Strong |
| Writer+reader additive permissions | acp_revoke_lifecycle (step 4-5) | Strong |
| Cross-compartment isolation | cross_compartment_isolation | Strong |
| NAC + document ACP layering | nac_document_acp | Strong |
| Encryption + ACP combined | encrypted_acp | Strong |
| NAC REST API enforcement | nac_core_operations (8 operations) | Strong |
| P2P replication success | acp_p2p | Strong for success case |

## What's Missing — Test Gaps Identified

| # | Finding | Severity | Related Vulnerability |
|---|---------|----------|----------------------|
| 22 | No `_commits` ACP test | HIGH | Finding 02 (CRITICAL) |
| 23 | No dump/backup ACP test | MEDIUM | Findings 01 (HIGH), 16 (MEDIUM) |
| 24 | P2P tests never verify merge denial | HIGH | Findings 00, 18, 19, 20 (HIGH chain) |
| 25 | No GraphQL NAC test | MEDIUM | Finding 08 (HIGH) |
| 26 | Weak mutation denial assertions | LOW | Test quality |
| 27 | No unauthorized document creation test | MEDIUM | Security model gap |
| 28 | No policy transition or DAC bypass test | LOW | Findings 10 (MEDIUM), 05 (LOW) |

## Coverage Matrix: Findings vs Tests

| Finding | Severity | Has Test? | Test Gap Finding |
|---------|----------|-----------|-----------------|
| **02** _commits bypass | **CRITICAL** | **NO** | 22 |
| **00** Recovery mode bypass | **HIGH** | **NO** | 24 |
| **01** Dump bypass | **HIGH** | **NO** | 23 |
| **08** GraphQL NAC bypass | **HIGH** | **NO** | 25 |
| **18** No merge signature check | **HIGH** | **NO** | 24 |
| **19** Creator identity spoofing | **HIGH** | **NO** | 24 |
| **03** CID time-travel bypass | **MEDIUM** | **NO** | — |
| **04** Encrypted search bypass | **MEDIUM** | **NO** | — |
| **09** NAC enable no auth | **MEDIUM** | **NO** | — |
| **10** Policy transition dead code | **MEDIUM** | **NO** | 28 |
| **15** Zanzibar error suppression | **MEDIUM** | **NO** | — |
| **16** Debug dump no NAC | **MEDIUM** | **NO** | 23 |
| **20** Block verify disconnected | **MEDIUM** | **NO** | 24 |
| **05** DAC bypass thread-local | **LOW** | **NO** | 28 |
| **06** View plan skips ACP | **LOW** | **NO** | — |
| **12** Zanzibar key delimiter | **LOW** | **NO** | — |
| **14** Policy YAML no size limits | **LOW** | **NO** | — |

**Of 17 security findings (excluding INFO), zero have regression tests.**

## Key Observations

### 1. Tests validate the happy path, not security boundaries

The ACP tests prove that the system works correctly when used as intended (owner creates, grants, revokes). They do not test attack vectors or bypass paths. This is a common pattern in application test suites but insufficient for a security-critical subsystem.

### 2. The `_commits` gap is the most severe

Finding 02 (CRITICAL) — the single most severe vulnerability — has no test. The `block_verify.rs` test at line 40 actually exercises the bypass (queries `_commits` without identity) but doesn't recognize it as a security issue.

### 3. P2P tests create false confidence

`acp_p2p.rs` appears to test P2P ACP but only tests replication success. It asserts that protected documents ARE visible on the receiving node (because ACP relations don't replicate), which is the expected behavior. But no test verifies that ACP can be enforced after replication, or that unauthorized merge is blocked.

### 4. REST NAC tests are thorough but GraphQL is the primary bypass

`nac_core_operations.rs` and `nac_operations.rs` test 15+ REST operations with the anonymous/outsider/admin pattern. This creates a strong impression of NAC coverage. But GraphQL — the primary query interface — is completely unprotected (Finding 08) and untested (Finding 25).

### 5. Mutation denial assertions are unreliable

The `if let Ok(result)` pattern (Finding 26) means that mutation denial may not actually be asserted in tests. If the server returns an error for any reason, the test passes without verifying the denial.

## Priority Recommendations

1. **CRITICAL**: Add `_commits` ACP test (Finding 22) — covers the most severe vulnerability
2. **HIGH**: Add P2P merge denial test (Finding 24) — covers the systemic P2P authentication gap
3. **HIGH**: Add GraphQL NAC test (Finding 25) — covers the primary NAC bypass
4. **MEDIUM**: Add dump/backup ACP tests (Finding 23) — covers data exposure bypasses
5. **MEDIUM**: Fix mutation denial assertion pattern (Finding 26) — improves existing test reliability
6. **MEDIUM**: Add unauthorized creation test (Finding 27) — documents the security model
7. **LOW**: Add policy transition + DAC bypass tests (Finding 28) — defense in depth
