# Finding: SourceHub Integration Tests Cover Happy Path Only — Missing Security Scenarios

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Test Coverage Gap
**Status**: CONFIRMED

## Summary

The four SourceHub integration tests (`sourcehub_smoke.rs`, `sourcehub_compartments.rs`, `sourcehub_p2p_acp.rs`, `sourcehub_policy_lifecycle.rs`) total ~536 lines and validate basic functionality: policy creation, document creation, grant/revoke, and P2P replication. However, they are exclusively happy-path tests. None of the security scenarios tested in local ACP (multi-role edge cases, revocation lifecycle, partial revocation) are replicated, and SourceHub-specific security scenarios (network partition, cache divergence, ABCI errors) are entirely untested.

## Affected Files

| File | Lines | Coverage |
|------|-------|----------|
| `tools/integration-test/tests/sourcehub_smoke.rs` | ~84 | Basic read filtering ✓, No denial testing |
| `tools/integration-test/tests/sourcehub_compartments.rs` | ~233 | Cross-compartment ✓ (UNIQUE to SourceHub), No edge cases |
| `tools/integration-test/tests/sourcehub_p2p_acp.rs` | ~108 | Basic P2P replication ✓, No merge denial |
| `tools/integration-test/tests/sourcehub_policy_lifecycle.rs` | ~111 | Grant/revoke ✓, No partial revocation |

## Details

### What IS Tested

1. **Smoke**: Policy add → schema → doc create → owner reads → anon denied
2. **Compartments**: 5 identities, 3 policies, cross-compartment isolation (reader in policy A can't see policy B docs)
3. **P2P**: 2 nodes, shared on-chain policy, document replicates, ACP enforced on both nodes
4. **Lifecycle**: Grant reader → Bob sees doc → Revoke reader → Bob denied

### What Is NOT Tested (Local ACP Coverage Gaps)

These scenarios are tested in local ACP but NOT in SourceHub:

| Scenario | Local Test | SourceHub Test |
|----------|-----------|----------------|
| Additive role grants (reader + writer) | `acp_revoke_lifecycle.rs` | MISSING |
| Partial revocation (revoke reader, keep writer) | `acp_revoke_lifecycle.rs` | MISSING |
| Collection truncation with ACP | `acp_revoke_lifecycle.rs` | MISSING |
| Old grants don't apply to new docs | `acp_revoke_lifecycle.rs` | MISSING |
| Multi-role permission scoping (admin > writer > reader) | `acp_multi_role.rs` | MISSING |
| Writer can't delete | `acp_multi_role.rs` | Tested in compartments |
| P2P merge denial for unauthorized peer | MISSING (finding 24) | MISSING |
| `_commits` ACP bypass | MISSING (finding 22) | MISSING |
| Dump with ACP-protected data | MISSING (finding 23) | MISSING |

### What Is NOT Tested (SourceHub-Specific Scenarios)

| Scenario | Risk | Finding |
|----------|------|---------|
| SourceHub unreachable during permission check | All reads fail | 33 |
| Policy add: on-chain succeeds, local cache fails | Orphaned policy | 31 |
| Node queries for uncached policy (added by other node) | Policy not found | 32 |
| ABCI error during verify_access | Silent false | 30 |
| Bearer token for unknown DID | PermissionDenied | 34 |
| Recovery mode with SourceHub ACP | On-chain bypass | 36 |
| Concurrent policy submissions | Account sequence errors | N/A |

### Infrastructure Note

All SourceHub tests are `#[ignore]` because they require a running Cosmos devnet. They run against **real on-chain infrastructure**, not mocks. This means:
- Tests are slow (~30s+ for chain startup)
- Not part of CI
- Must be run manually
- Infrastructure failures can cause false test failures

### Positive: Unique SourceHub Coverage

The `sourcehub_compartments.rs` test covers cross-compartment isolation — a scenario NOT tested in local ACP tests. This is valuable coverage that should be ported to local ACP tests.

## Remediation

### Priority 1: Port Local ACP Edge Cases to SourceHub
- Additive grants and partial revocation
- Multi-role permission boundaries

### Priority 2: Add SourceHub-Specific Failure Mode Tests
- Mock the SourceHub provider (implement `SourceHubProvider` trait with failure injection)
- Test network partition, ABCI errors, and cache divergence without needing a real chain

### Priority 3: Port Compartments Test to Local ACP
- The cross-compartment isolation test should run in local ACP mode too

## Test Coverage

This finding IS about test coverage. The SourceHub integration test suite is ~536 lines vs ~606 lines for the 5 core local ACP tests — similar in size but significantly narrower in security scenario coverage.
