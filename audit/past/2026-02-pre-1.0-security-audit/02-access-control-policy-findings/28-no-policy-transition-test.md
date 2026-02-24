# Finding: No Integration Test for Policy Transitions or DAC Bypass Flag

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Test Gap
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation
**Related Findings**: 10 (policy transition guards dead code — MEDIUM), 05 (DAC bypass thread-local safety — LOW)

## Summary

Two confirmed ACP findings — dead policy transition guards (Finding 10) and the DAC bypass thread-local flag (Finding 05) — have zero integration test coverage. Additionally, no integration test exists for policy transitions on collections with existing documents and active relations.

## Evidence

### Policy transition tests: zero

Grep for `policy_transition`, `block_unsafe`, `warn_on_unsafe` in integration tests: **zero matches**.

No test exists that:
- Deploys a schema with ACP policy, creates documents with relations
- Patches the schema to remove the ACP policy
- Verifies that previously protected documents become accessible (or are blocked)

This would be the test that exercises Finding 10's dead code — `block_unsafe_policy_transition()` should block the dangerous transition, but it's never called from production code and never tested.

### DAC bypass flag tests: zero

Grep for `dac_bypass`, `DAC_BYPASS`, `bypass_dac` in integration tests: **zero matches**.

The DAC bypass flag is a `thread_local! { RefCell<bool> }` that grants unrestricted read access when set to `true`. It is activated when NAC is enabled and the caller has `NodePermission::DacBypass`. No integration test verifies:
- That a DacBypass-authorized identity can read all documents regardless of ACP policy
- That a non-DacBypass identity is still subject to ACP filtering
- That the flag is correctly cleared between requests (Finding 05's concern)

### Recovery mode tests: zero

Grep for `recovery` in integration tests: **zero matches**.

No test verifies ACP behavior during crash recovery or after restart with unmerged blocks. This maps to Finding 00 (recovery mode bypass).

## Missing Tests

### Policy transition

```
1. Deploy ACP policy + schema as Alice
2. Create document and grant Bob "reader"
3. Verify Bob sees 1 document
4. Patch schema to REMOVE @policy directive
5. Assert: should this be blocked? (Finding 10: guard exists but is dead code)
6. If allowed: verify Bob can now see the document without ACP relation
```

### DAC bypass

```
1. Start node with .with_acp_local().with_nac()
2. Deploy ACP policy + schema, create protected document
3. Grant identity DacBypass NAC permission
4. Query with DacBypass identity → should see all documents regardless of ACP
5. Query with non-DacBypass identity → should see only granted documents
```

## Severity Rationale

LOW because:
- Finding 10 (dead code) is MEDIUM — the guards exist and are unit-tested, just not wired in
- Finding 05 (thread-local) is LOW — mitigated by normal request flow
- Recovery mode tests would be valuable but the vulnerability (Finding 00) is already documented
- These are defense-in-depth test gaps, not tests for active bypass vectors
