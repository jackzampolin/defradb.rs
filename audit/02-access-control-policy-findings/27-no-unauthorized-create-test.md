# Finding: No Test for Unauthorized Document Creation in ACP-Protected Collections

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Test Gap
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation

## Summary

Every ACP integration test creates documents as the document owner (the identity that deployed the schema with the ACP policy). **No test verifies that an unauthorized identity is prevented from creating new documents in an ACP-protected collection.** This is a distinct gap from update/delete denial testing — creation is the initial access vector that establishes ownership.

## Evidence

### All document creation uses owner identity

| Test File | Creator Identity | Tested? |
|-----------|-----------------|---------|
| `acp_basic.rs:29-34` | Alice (owner) | ✅ creates as owner |
| `acp_multi_identity.rs:28-50` | Alice (owner) | ✅ creates 3 docs as owner |
| `acp_multi_role.rs:29-43` | Alice (owner) | ✅ creates 2 docs as owner |
| `acp_revoke_lifecycle.rs:33-43` | Alice (owner) | ✅ creates as owner |
| `acp_p2p.rs:69-80` | Anonymous + Alice | ✅ public + protected create |
| `encrypted_acp.rs:26-35` | Jack (owner) | ✅ creates as owner |
| `cross_compartment_isolation.rs:63-101` | Jack (owner) | ✅ creates in both compartments |

### What's never tested

No test attempts:

```rust
// Bob (no relation to the policy) tries to create a document
let bob_create = node.query_with_identity(
    r#"mutation { create_User(input: {name: "Injected", age: 0}) { _docID } }"#,
    &bob.private_key_hex,
);
// What happens? Does Bob become owner of the new doc? Is creation denied?
```

### Why this matters

In DefraDB's ACP model, the identity that creates a document becomes its `owner`. The owner relationship is automatically registered in the Zanzibar store. This means:

1. If Bob can create a document in an ACP-protected collection, Bob becomes the owner of that document
2. As owner, Bob has full read/write/delete access to that document
3. The ACP policy's `read` and `write` expressions always include `owner`

The security question is: **Can any identity create documents in any ACP-protected collection, or is creation itself gated?**

If creation is ungated (any identity can create), the security model is "anyone can create, but only authorized users can read/modify existing documents." If creation is gated, there should be a test proving it.

### Contrast with NAC tests

`nac_core_operations.rs:138-159` does test unauthorized document creation, but via the REST API and with NAC (node-level access control), not document-level ACP:

```rust
assert!(
    node.collection_create("Product", r#"{"name":"Anon","sku":"A001","price":1}"#)
        .is_err(),
    "anonymous should be rejected from collection create"
);
```

This is a NAC test (node-level), not a DAC test (document-level). No equivalent exists for ACP-protected collections.

## Missing Test

```
1. Deploy ACP policy + schema as Alice
2. Bob (no relation) attempts to create a document via GraphQL mutation
3. Assert either:
   a. Creation is denied (Bob cannot create in ACP-protected collection), OR
   b. Creation succeeds and Bob becomes owner of the new document (document isolation)
4. If (b): verify Alice cannot see Bob's document and Bob cannot see Alice's documents
```

Both outcomes (a) and (b) are valid security models, but the test should document which one DefraDB implements.

## Severity Rationale

MEDIUM because:
- Document creation is the initial access vector — without a test, we don't know whether ACP gates it
- If any identity can create documents and become owner, the attack surface is broader than expected
- The test gap affects our understanding of the security model, not just regression coverage
