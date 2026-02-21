# Finding 54: Anonymous Access to ACP-Protected Resources Tested in Some but Not All Test Suites

**Severity**: LOW
**Category**: Test Coverage / Access Control
**Status**: Partially covered

## Summary

Anonymous (no-identity) access to ACP-protected resources IS tested in several test files (`encrypted_acp.rs`, `acp_p2p.rs`, `nac_document_acp.rs`, `sourcehub_smoke.rs`), but the primary `acp_basic.rs` test does NOT include an anonymous query attempt. This means the most fundamental ACP test file doesn't verify the most basic negative case.

## Affected Files

- `tools/integration-test/tests/acp_basic.rs` — Tests Alice (owner) and Bob (outsider before/after grant), no anonymous test
- `tools/integration-test/tests/encrypted_acp.rs:61-64` — ✅ Tests anonymous reads encrypted doc → DENY
- `tools/integration-test/tests/acp_p2p.rs:83-93` — ✅ Tests anonymous sees only public doc
- `tools/integration-test/tests/nac_document_acp.rs:49-57` — ✅ Tests anonymous query → DENY under NAC

## Details

### What `acp_basic.rs` tests

1. Alice creates ACP-protected document ✅
2. Alice queries → sees document ✅
3. Bob queries → sees 0 documents ✅ (wrong identity, pre-grant)
4. Alice grants Bob reader → Bob sees document ✅
5. **Anonymous query → not tested** ❌

### What should be added

```rust
// Anonymous queries -> sees 0 documents (no identity = no access)
let anon_result = node.query("query { User { _docID name age } }").expect("anon query");
let anon_users = anon_result["User"].as_array().expect("anon result not array");
assert_eq!(anon_users.len(), 0, "anonymous should see 0 documents");
```

### Impact

The gap is partially mitigated by the other test files covering anonymous access, but `acp_basic.rs` is the canonical ACP test and the first place developers look. It should cover all three identity states: owner, outsider, anonymous.

## Remediation

Add an anonymous query assertion to `acp_basic.rs` before the Bob grant step.

## Test Gap

- `acp_basic.rs`: no anonymous access test
- `acp_multi_identity.rs`: no anonymous access test (Eve acts as "outsider" but with an identity, not anonymous)
