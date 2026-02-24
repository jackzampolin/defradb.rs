# Finding 58: No Identity Confusion (Substitution) Integration Test

**Severity**: MEDIUM
**Category**: Test Coverage / Identity Integrity
**Status**: Confirmed

## Summary

No integration test verifies that Alice's token cannot be used to gain Bob's permissions. The `acp_multi_identity.rs` test correctly verifies that each identity can only see documents they've been granted access to, but it never tests the case where Alice's token is sent with a request that should require Bob's identity. The token is self-authenticating (the identity IS the token), so substitution attacks shouldn't work — but this property is never verified end-to-end.

## Affected Files

- `tools/integration-test/tests/acp_multi_identity.rs` — Tests visibility per identity, no token substitution test
- `tools/integration-test/tests/acp_basic.rs` — Tests Alice/Bob, no cross-identity test
- `tools/integration-test/tests/identity_types.rs` — Tests cross-key-type, no token swap test

## Details

### What the tests verify

The existing tests verify that:
- Alice (owner) sees her documents ✅
- Bob (outsider, pre-grant) sees nothing ✅
- Bob (after grant) sees granted documents ✅
- Carol (writer, post-grant) can update ✅

### What's not tested

**Scenario: Token substitution should be impossible**

Given the self-authenticating JWT design:
1. Alice creates a token → token contains Alice's public key and DID
2. If Alice's token is sent in a request, the server derives Alice's DID from the token
3. The DID is used for ACP checks → Alice's permissions apply

There is no mechanism for Alice's token to yield Bob's identity (the DID is bound to the public key in the `sub` claim, which is verified against the signature). However, this critical property is never explicitly tested:

```rust
// Missing test: Alice's token cannot grant Bob's permissions
let alice_result = node.query_with_identity(query, &alice.private_key_hex);
// Verify result is from Alice's perspective, not Bob's
```

### Why this matters

If a bug in the identity extraction pipeline caused the DID to be derived incorrectly (e.g., using a cached identity, thread-local state, or header confusion), the existing tests would not catch it because they never verify that using identity A *specifically cannot* yield identity B's access.

### Concurrent request concern

The `IdentityContext` is constructed per-request in the identity extractor and passed through the request handling pipeline. There is no shared mutable state or thread-local that could cause identity confusion between concurrent requests. This is correct by construction (Axum's extractor model guarantees this), but concurrent identity confusion is never tested.

## Remediation

Add a test to `acp_multi_identity.rs`:

```rust
// After granting Bob reader on Alpha:
// Verify that Alice querying still sees all 3 (her own)
// and that Bob querying sees exactly 1 (Alpha only)
// This confirms the identity in the token maps to the correct ACP identity
assert_eq!(count(&alice.private_key_hex), 3, "Alice should see 3");
assert_eq!(count(&bob.private_key_hex), 1, "Bob should see 1");

// The existing test already does this — but it would be valuable to also verify
// that swapping the identity mid-session doesn't carry over permissions:
let alice_seeing_bobs_docs = node.query_with_identity(query, &alice.private_key_hex);
// Should see Alice's perspective (3 docs), not Bob's (1 doc)
```

## Test Gap

- No test for identity substitution (Alice's token yields Bob's access)
- No concurrent multi-identity test
- No test for identity carry-over between sequential requests
