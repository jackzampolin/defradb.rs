# Finding 62: No Key Rotation or Identity Lifecycle Test

**Severity**: INFO
**Category**: Test Coverage / Identity Lifecycle
**Status**: Confirmed (by design)

## Summary

There is no test for key rotation: what happens when a node's identity key is changed and existing ACP policies reference the old DID. This is currently "by design" — DefraDB does not support key rotation (DIDs are permanently bound to the key that created them), but this assumption is never explicitly tested.

## Affected Files

- `tools/integration-test/tests/identity_lifecycle.rs` — Tests key CRUD, not rotation
- `tools/integration-test/tests/keyring_lifecycle.rs` — Tests keyring operations, not identity continuity

## Details

### Current behavior

1. Identity is derived from a private key → DID is deterministic
2. ACP policies reference DIDs for permission checks
3. If a key is deleted and a new key is generated, the new DID is different
4. Old ACP relationships referencing the old DID become orphaned
5. No mechanism exists to migrate ACP relationships from old DID to new DID

### What's not tested

- Generate identity A with DID_A
- Create ACP policy with DID_A as owner
- Delete identity A
- Generate identity B (new key, different DID)
- Verify that DID_B cannot access DID_A's documents
- Verify that DID_A's documents are still "owned" but inaccessible

### Why this is INFO

- Key rotation is not a supported feature
- DIDs are immutable (changing key = changing DID)
- This is consistent with the DID specification
- Adding rotation would require a DID method that supports key rotation (did:key does not)

## Remediation

None required for 1.0. If key rotation is ever supported:
1. Add a DID method that supports rotation (e.g., did:web, did:ion)
2. Add migration tooling for ACP relationships
3. Add integration tests covering the rotation + migration flow

## Test Gap

- No test that verifies orphaned ACP relationships after key deletion
- No test that verifies a new key cannot inherit old key's permissions
