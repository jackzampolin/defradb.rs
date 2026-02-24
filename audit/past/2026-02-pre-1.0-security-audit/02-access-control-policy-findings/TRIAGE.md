# ACP Findings Triage Report

**Stream**: 02 - Access Control Policy
**Date**: 2026-02-21
**Total Findings**: 36 (excluding 3 session summaries)
**Breakdown**: 1 CRITICAL, 7 HIGH, 12 MEDIUM, 7 LOW, 5 INFO/GREEN, 4 INFO (meta/test)

---

## 1. Findings Table

Sorted by severity (CRITICAL first, GREEN last).

| # | File | Severity | Title | Status | One-Line Summary |
|---|------|----------|-------|--------|------------------|
| 02 | `02-commits-query-bypasses-acp.md` | CRITICAL | _commits queries bypass ACP entirely | CONFIRMED | `_commits` GraphQL queries early-return before identity check, exposing full commit history of any ACP-protected document. |
| 00 | `00-recovery-mode-acp-bypass.md` | HIGH | Recovery mode bypasses ACP on P2P merge | CONFIRMED | `BlockMetadata::recovery()` skips all ACP checks and is triggered mid-operation by HTTP-exposed version sync, not just at startup. |
| 01 | `01-dump-bypasses-acp.md` | HIGH | Database dump bypasses ACP and NAC | CONFIRMED | `GET /api/v0/debug/dump` iterates all storage namespaces with no authentication, no identity check, and no NAC gate. |
| 08 | `08-graphql-bypasses-nac-permission-checks.md` | HIGH | GraphQL endpoint bypasses NAC permission checks | CONFIRMED | GraphQL handlers never call `require_permission()`, allowing NAC-denied identities to query/mutate via `/api/v0/graphql`. |
| 18 | `18-p2p-merge-no-signature-verification.md` | HIGH | P2P merge path does not verify block signatures | CONFIRMED | Blocks from P2P peers are merged without cryptographic signature verification; `verify_block_signature()` exists but is never called during merge. |
| 19 | `19-p2p-creator-identity-from-metadata-not-signature.md` | HIGH | P2P block creator identity from peer-reported metadata | CONFIRMED | ACP permission checks during merge use self-reported `creator` from PushLog metadata, not the cryptographically-verified block signature identity. |
| 22 | `22-no-commits-acp-integration-test.md` | HIGH | No integration test for _commits ACP bypass | CONFIRMED | Zero test coverage for the CRITICAL _commits bypass (finding 02); existing tests silently rely on the bypass. |
| 24 | `24-acp-p2p-never-tests-merge-denial.md` | HIGH | P2P ACP tests never verify merge denial | CONFIRMED | `acp_p2p.rs` tests replication success but never verifies unauthorized merge rejection, signature verification, or recovery bypass. |
| 30 | `30-sourcehub-verify-access-fail-open-on-abci-error.md` | HIGH | SourceHub verify_access fails open on ABCI error | CONFIRMED | Non-zero ABCI error codes return `Ok(false)` instead of `Err(...)`, masking errors as denials; brittle hand-rolled protobuf decoding silently breaks on protocol evolution. |
| 36 | `36-sourcehub-recovery-bypass-on-chain-permissions.md` | HIGH | Recovery mode bypasses on-chain SourceHub permissions | CONFIRMED | Recovery bypass from finding 00 applies to SourceHub, creating unauditable divergence between on-chain permissions and local state. |
| 03 | `03-cid-time-travel-bypasses-acp.md` | MEDIUM | CID time-travel queries bypass ACP | CONFIRMED | CID-based queries deliberately ignore `_caller_identity`, allowing full document reconstruction at any historical state without permission checks. |
| 04 | `04-encrypted-search-bypasses-acp.md` | MEDIUM | Encrypted search queries bypass ACP | CONFIRMED | `execute_encrypted_select()` has no identity parameter; returns matching document IDs for ACP-protected documents to any caller. |
| 09 | `09-nac-enable-no-authentication.md` | MEDIUM | NAC enable endpoint has no authentication | CONFIRMED | `POST /api/v0/acp/node/enable` accepts any request; first caller becomes permanent NAC owner with no identity verification. |
| 10 | `10-policy-transition-guards-dead-code.md` | MEDIUM | Policy transition safety guards are dead code | CONFIRMED | `block_unsafe_policy_transition()` is defined, exported, and unit-tested but never called from any production code path. |
| 15 | `15-zanzibar-read-check-error-suppression.md` | MEDIUM | Zanzibar read check silently suppresses errors | CONFIRMED | Read permission check catches `Err(_)` and treats it as `Ok(false)`, masking store corruption; update/delete correctly propagate errors. |
| 16 | `16-debug-dump-no-nac-check.md` | MEDIUM | Debug dump endpoint has no NAC check | CONFIRMED | `GET /api/v0/debug/dump` handler has no `ExtractIdentity` or `require_permission()`, exposing all database contents including ACP store. |
| 20 | `20-block-verify-not-in-merge-path.md` | MEDIUM | Block verification function disconnected from merge path | CONFIRMED | `verify_block_signature()` is well-implemented but architecturally disconnected from P2P merge; structural gap supporting findings 18+19. |
| 23 | `23-no-dump-backup-acp-test.md` | MEDIUM | No integration test for dump or backup with ACP | CONFIRMED | `dump.rs` and `backup_restore.rs` tests have zero ACP awareness; dump test is `#[ignore]`d. |
| 25 | `25-no-graphql-nac-integration-test.md` | MEDIUM | No integration test for GraphQL NAC bypass | CONFIRMED | NAC tests cover all REST endpoints but never verify NAC enforcement on GraphQL queries or mutations. |
| 27 | `27-no-unauthorized-create-test.md` | MEDIUM | No test for unauthorized document creation | CONFIRMED | Every ACP test creates documents as the owner; no test verifies whether unauthorized identities are blocked from creating documents. |
| 31 | `31-sourcehub-policy-add-non-atomic.md` | MEDIUM | SourceHub policy add is non-atomic | CONFIRMED | Three-step add_policy can leave orphaned local cache or on-chain policy if step 3 fails after irreversible on-chain submission. |
| 32 | `32-sourcehub-cache-staleness-no-refresh.md` | MEDIUM | SourceHub policy cache has no refresh mechanism | CONFIRMED | `list_policies()` and `get_policy()` read only from local cache; policies added by other nodes or on-chain are permanently invisible. |
| 33 | `33-sourcehub-network-partition-no-fail-closed.md` | MEDIUM | SourceHub network partition: no explicit fail-closed policy | CONFIRMED | Fail-closed is emergent (not designed); no circuit breaker means SourceHub outage causes N HTTP requests per query to a dead endpoint. |
| 34 | `34-sourcehub-bearer-token-signing-config-dependency.md` | MEDIUM | SourceHub bearer token requires global signing config | CONFIRMED | `unregister_doc_object()` needs the document owner's private key, which the local node may not possess for remote identities. |
| 38 | `38-sourcehub-integration-test-coverage-gaps.md` | MEDIUM | SourceHub integration tests cover happy path only | CONFIRMED | Four SourceHub tests validate basic functionality but omit all security scenarios; SourceHub-specific failure modes (partition, ABCI errors) untested. |
| 05 | `05-dac-bypass-thread-local-safety.md` | LOW | DAC bypass thread-local flag safety concerns | CONFIRMED | Thread-local `RefCell<bool>` bypass flag is never explicitly cleared; a panic during execution could leave bypass set for subsequent requests. |
| 06 | `06-view-plan-skips-own-acp.md` | LOW | View plans skip view-collection ACP policy | CONFIRMED | `build_view_plan()` never applies `maybe_wrap_with_acp_filter()` for the view's own policy; source collection ACP is correctly enforced. |
| 11 | `11-policy-expressions-support-intersection-difference.md` | LOW | Policy expressions support intersection and difference | CONFIRMED | Parser and evaluator support `&` and `-` operators despite documentation suggesting union-only; owner access is preserved. |
| 12 | `12-zanzibar-storage-key-delimiter-injection.md` | LOW | Zanzibar storage key lacks delimiter sanitization | CONFIRMED | `/`-delimited storage keys accept unsanitized input; schema validation prevents exploitation in practice. |
| 14 | `14-policy-yaml-no-size-limits.md` | LOW | Policy YAML parsing has no size limits | CONFIRMED | `parse_policy_yaml()` accepts arbitrarily large input; requires `DacPolicyAdd` permission to exploit. |
| 26 | `26-weak-mutation-denial-assertions.md` | LOW | Weak mutation denial assertions in tests | CONFIRMED | `if let Ok(result)` pattern silently skips assertions when mutations return errors, masking test failures. |
| 28 | `28-no-policy-transition-test.md` | LOW | No integration test for policy transitions or DAC bypass | CONFIRMED | Zero test coverage for dead policy transition guards (finding 10) and DAC bypass flag behavior (finding 05). |
| 35 | `35-sourcehub-managing-relations-not-validated-locally.md` | LOW | SourceHub ignores managing relations parameter | CONFIRMED | `add_actor_relationship` accepts but ignores `_managing_relations`; local validation is redundant since SourceHub validates on-chain. |
| 07 | `07-dac-checklist-verification.md` | INFO | DAC implementation checklist verification | VERIFIED | Core DAC implementation is sound: fail-closed on errors, correct permission hierarchy, atomic registration, proper error masking. |
| 13 | `13-nac-disabled-state-behavior-analysis.md` | INFO | NAC disabled state behavior analysis | VERIFIED CORRECT | Three-state NAC machine correctly blocks relationship writes during disabled state, preventing privilege escalation. |
| 17 | `17-policy-id-not-content-hash-of-yaml.md` | INFO | Policy ID is not a simple content hash of YAML | VERIFIED CORRECT | Double SHA-256 with monotonic counter is correct for Go compatibility; policy IDs are node-specific. |
| 37 | `37-sourcehub-all-session1-4-findings-apply.md` | INFO | All Session 1-4 findings apply to SourceHub mode | CONFIRMED | Every finding applies to SourceHub via `DocumentACP` trait abstraction; some with amplified impact. |

---

## 2. Themes

### Theme A: Query Bypass Vectors (Findings 02, 03, 04)

Three separate code paths early-return before reaching the ACP enforcement layer:
- `_commits` queries (CRITICAL) -- expose full commit history
- CID time-travel queries (MEDIUM) -- reconstruct documents at any historical CID
- Encrypted search queries (MEDIUM) -- leak matching document IDs

These share a common root cause: the query dispatcher routes special query types to dedicated code paths that were written without ACP awareness. The `caller_identity` parameter is available but never passed through. Finding 02 chains with finding 03 (commits reveal CIDs, CID queries reveal content) for full data disclosure.

### Theme B: P2P Authentication Gap (Findings 18, 19, 20, 00, 36)

The P2P merge path has no cryptographic authentication:
- Block signatures are never verified during merge (18)
- Creator identity is self-reported, not signature-derived (19)
- `verify_block_signature()` exists but is architecturally disconnected from merge (20)
- Recovery mode bypasses all ACP during version sync, not just at startup (00)
- Recovery bypass is more severe for SourceHub where on-chain permissions are bypassed (36)

A malicious peer can inject arbitrary blocks claiming any creator identity, and the merge handler will accept them if the spoofed creator has UPDATE permission.

### Theme C: NAC Enforcement Gaps (Findings 08, 09, 16, 01)

NAC (Node Access Control) has several holes:
- GraphQL endpoint (the primary query interface) completely bypasses NAC (08)
- NAC enable endpoint requires no authentication, allowing bootstrap hijacking (09)
- Debug dump endpoint has no NAC check, exposing all database contents (16/01)

The GraphQL bypass is particularly severe because GraphQL is the main query interface, making NAC enforcement on REST endpoints irrelevant for determined attackers.

### Theme D: Integration Test Coverage Gaps (Findings 22, 23, 24, 25, 26, 27, 28, 38)

Eight findings document missing negative test coverage:
- No test for _commits bypass (22) -- the single most severe vulnerability
- No test for dump/backup with ACP (23)
- No P2P merge denial test (24) -- P2P tests only verify success
- No GraphQL NAC test (25)
- Weak mutation denial assertions (26)
- No unauthorized create test (27)
- No policy transition or DAC bypass test (28)
- SourceHub tests are happy-path only (38)

The test suite validates that authorized operations succeed but rarely validates that unauthorized operations fail. This provides no regression safety for security fixes.

### Theme E: SourceHub Provider Risks (Findings 30, 31, 32, 33, 34, 35, 36, 38)

The SourceHub ACP provider has unique operational risks:
- ABCI errors silently mask permission check failures (30)
- Policy add is non-atomic, risking local/on-chain divergence (31)
- Policy cache has no refresh, causing permanent staleness (32)
- No circuit breaker for SourceHub outages (33)
- Bearer token requires local private key, breaking for remote identities (34)
- Managing relations parameter is ignored (35)
- Recovery mode bypasses on-chain permissions (36)

These risks are specific to the distributed nature of SourceHub integration and do not exist in local ACP mode.

### Theme F: Defense-in-Depth Weaknesses (Findings 05, 06, 10, 11, 12, 14, 15)

Several findings identify missing guardrails that don't represent direct exploits but weaken the security posture:
- Thread-local bypass flag not cleared on panic (05)
- View plans skip own ACP policy (06)
- Policy transition guards are dead code (10)
- Undocumented expression operators (11)
- Storage key delimiter not sanitized (12)
- No YAML size limits (14)
- Error suppression in read check (15)

---

## 3. Actionable vs Informational

### Must Fix (1.0 Blockers)

These are CRITICAL or confirmed HIGH findings with demonstrated exploit paths:

| # | Finding | Why It Blocks 1.0 |
|---|---------|-------------------|
| 02 | _commits queries bypass ACP | Any user can read full commit history of any protected document. Trivial to exploit. |
| 08 | GraphQL bypasses NAC | Primary query interface ignores all NAC permissions. NAC is useless while this exists. |
| 18 | P2P merge no signature verification | Any connected peer can inject arbitrary document mutations. |
| 19 | P2P creator identity spoofing | Combined with 18, allows impersonating any identity during merge. |
| 00 | Recovery mode ACP bypass via version sync | HTTP-triggered version sync processes blocks from untrusted peers without ACP. |
| 01 | Dump bypasses ACP and NAC | Unauthenticated HTTP endpoint exposes all database contents including ACP policies. |
| 36 | Recovery bypass on-chain SourceHub permissions | Recovery mode creates unauditable divergence from on-chain authorization state. |
| 30 | SourceHub verify_access fails open on ABCI error | Masks real errors as access denials; brittle protobuf parsing breaks on protocol changes. |

### Should Fix (Pre-1.0)

MEDIUM findings with real exploit potential or operational risk:

| # | Finding | Risk |
|---|---------|------|
| 03 | CID time-travel bypass | Full document content disclosure when combined with finding 02. |
| 04 | Encrypted search bypass | Document ID leakage for ACP-protected documents. |
| 09 | NAC enable no authentication | Bootstrap hijacking during node startup window. |
| 10 | Policy transition guards dead code | Schema changes silently strip ACP protection from all documents in a collection. |
| 15 | Zanzibar read check error suppression | Masks store corruption; violates fail-closed principle. |
| 16 | Debug dump no NAC check | Redundant with finding 01 but represents an independent fix point (NAC gate). |
| 20 | Block verify disconnected from merge | Structural prerequisite for fixing findings 18+19 properly. |
| 33 | SourceHub network partition | SourceHub outage degrades into N blocking HTTP requests per query. |
| 32 | SourceHub cache staleness | Multi-node SourceHub deployments cannot see each other's policies. |
| 34 | SourceHub bearer token dependency | Prevents unregistering documents owned by remote identities. |
| 31 | SourceHub policy add non-atomic | On-chain/local divergence on partial failure. |
| 22 | No _commits ACP test | Regression safety for the CRITICAL fix. |
| 24 | No P2P merge denial test | Regression safety for the P2P authentication chain. |
| 25 | No GraphQL NAC test | Regression safety for the NAC fix. |
| 23 | No dump/backup ACP test | Regression safety for the dump/NAC fix. |
| 27 | No unauthorized create test | Documents the security model for creation. |
| 38 | SourceHub test coverage gaps | Security scenarios untested for SourceHub provider. |

### Accept Risk / Backlog

LOW/INFO findings that represent design trade-offs, defense-in-depth gaps, or minor quality issues:

| # | Finding | Rationale |
|---|---------|-----------|
| 05 | DAC bypass thread-local safety | Requires panic + thread reuse; each request resets flag. |
| 06 | View plan skips own ACP | Source collection ACP is enforced; views with own policies are unusual. |
| 11 | Policy expression operators | Implementation is correct; documentation gap only. |
| 12 | Zanzibar storage key injection | Schema validation prevents exploitation; defense-in-depth fix. |
| 14 | Policy YAML no size limits | Requires admin permission; standard web server limits mitigate. |
| 26 | Weak mutation denial assertions | Test quality issue; secondary assertions provide some coverage. |
| 28 | No policy transition test | Related dead-code finding (10) is in "Should Fix" tier. |
| 35 | SourceHub ignores managing relations | Architecturally correct; SourceHub validates on-chain. |

### No Action (GREEN / Informational)

Confirmed safe or informational findings requiring no changes:

| # | Finding | Conclusion |
|---|---------|------------|
| 07 | DAC checklist verification | Core DAC implementation is sound. Pass on 9/11 checks. |
| 13 | NAC disabled state behavior | Three-state machine is correctly designed. No vulnerability. |
| 17 | Policy ID not content hash | Correct for Go compatibility. No security issue. |
| 37 | All Session 1-4 findings apply to SourceHub | Meta-finding confirming provider equivalence. Fix vectors once via trait. |

---

## 4. Recommended Fix Order

### Phase 1: Seal the Critical Bypass Vectors (Week 1)

**Why first**: These are the most exploitable findings with the simplest fixes.

1. **Fix 02 (_commits bypass)** -- Add `caller_identity` to `execute_commits_query()` and check `DocumentPermission::Read` before returning results. Estimated: small change in `select.rs` and `commits.rs`. Write regression test (finding 22).

2. **Fix 08 (GraphQL NAC bypass)** -- Add `require_permission()` calls to `graphql()`, `graphql_get()`, and `graphql_transactional()` handlers, mapping operation type to NAC permission. Write regression test (finding 25).

3. **Fix 01/16 (dump ACP+NAC bypass)** -- Either gate `GET /api/v0/debug/dump` behind NAC + dev mode, or remove it from the production router entirely. This is a single-handler change. Write regression test (finding 23 partial).

### Phase 2: P2P Authentication Chain (Week 2)

**Why second**: This is the largest security gap (no authentication on the merge path) but requires more architectural work.

4. **Fix 20 (refactor verify_block_signature)** -- Extract verification logic into reusable components that can be called from both the on-demand API and the merge handler. This is the structural prerequisite.

5. **Fix 18+19 (P2P merge signature verification + identity derivation)** -- Integrate signature verification into `AcpMergeHandler::handle_block()`. Derive creator identity from the verified block signature, not from peer-reported metadata. Write regression tests (finding 24).

6. **Fix 00 (recovery mode bypass)** -- Change version sync to construct proper `BlockMetadata` from decoded block content instead of using `BlockMetadata::recovery()`. Restrict `recovery()` to the startup recovery path only. This also addresses finding 36 for SourceHub.

### Phase 3: Secondary Bypass Vectors (Week 3)

**Why third**: These require attacker foreknowledge (valid CIDs, encryption tokens) but complete the ACP coverage.

7. **Fix 03 (CID time-travel bypass)** -- Route CID queries through the planner for ACP filtering, or add a `DocumentPermission::Read` check before rendering results.

8. **Fix 04 (encrypted search bypass)** -- Pass `caller_identity` to `execute_encrypted_select()` and filter results through ACP.

9. **Fix 09 (NAC enable no authentication)** -- Require NAC enable only via CLI (local access) or require a pre-shared secret.

10. **Fix 10 (policy transition dead code)** -- Wire `block_unsafe_policy_transition()` into the schema update path.

### Phase 4: SourceHub Provider Hardening (Week 4)

**Why fourth**: SourceHub-specific issues affect a subset of deployments and are less exploitable.

11. **Fix 30 (ABCI error masking)** -- Return `Err(...)` for non-zero ABCI codes; use prost for protobuf decoding; elevate logging to warn.

12. **Fix 32 (cache staleness)** -- Add on-chain fallback for `get_policy()` cache misses; match Go's on-demand query behavior.

13. **Fix 33 (no circuit breaker)** -- Add SourceHub health check and circuit breaker with aggressive timeouts.

14. **Fix 15 (error suppression)** -- Log suppressed errors in the read permission check loop; consider returning `Err(...)` for fail-closed.

### Phase 5: Test Coverage and Defense-in-Depth (Ongoing)

15. **Write missing integration tests**: Findings 22, 23, 24, 25, 27, 38 -- each finding describes the exact test needed.

16. **Fix remaining LOW findings** (05, 06, 12, 14) as time permits. These are defense-in-depth improvements, not active vulnerabilities.

---

### Summary Statistics

| Priority | Count | Severity Range |
|----------|-------|----------------|
| Must Fix (1.0 blockers) | 8 | 1 CRITICAL + 7 HIGH |
| Should Fix (pre-1.0) | 17 | 12 MEDIUM + 5 HIGH (test gaps) |
| Accept Risk / Backlog | 8 | 7 LOW + 1 MEDIUM (test) |
| No Action (GREEN) | 4 | 4 INFO |
