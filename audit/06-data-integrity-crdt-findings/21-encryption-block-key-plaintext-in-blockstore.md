# Encryption Key Stored in Plaintext in Blockstore

**Severity:** Informational
**Category:** Encryption / Key Management
**Status:** Confirmed — Matches Go Architecture

## Summary

The `Encryption` block stores the AES-256-GCM encryption key in plaintext within the blockstore. Any node that receives the encryption block via Bitswap or replication has the raw key and can decrypt all data encrypted with that key. The `should_skip_encrypted_merge` ACP check only controls whether the merge handler USES the key, but the key itself is stored in plain bytes at rest and synced across the network.

## Affected Files

- `crates/defra-core/src/block.rs:578-626` (Encryption struct with plaintext `key` field)
- `crates/db/src/merge_handler/mod.rs:149-192` (loads and uses the key directly)
- `crates/db/src/merge_handler/composite.rs:16-43` (`should_skip_encrypted_merge` — ACP check only)

## Details

### Key Storage

```rust
// block.rs:578-590
pub struct Encryption {
    #[serde(rename = "docID", with = "serde_bytes")]
    pub doc_id: Vec<u8>,
    #[serde(rename = "fieldName", default, skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,  // <-- Raw AES key, stored and synced in plaintext
}
```

### Key Usage in Merge

```rust
// mod.rs:174
crypto::encryption::aes::decrypt_aes(None, data, &enc_block.key, &[])
```

### ACP Gate Is Policy, Not Access Control on Key

```rust
// composite.rs:36-43
match acp.is_doc_registered(&policy.id, &policy.resource_name, doc_id).await {
    Ok(true) => false,  // Allow decryption
    Ok(false) => true,   // Skip decryption
    Err(_) => true,      // Fail-closed, skip
}
```

This check only determines whether the merge handler proceeds with decryption. The encryption block (with the raw key) is already in the local blockstore. A determined user or compromised node can read the key directly from storage.

### Go Architecture Comparison

Go uses a separate Key Management Service (KMS) and Encstore. The encryption key is NOT synced via the main blockstore. Instead, the KMS distributes keys to authorized nodes only. Rust doesn't have KMS — it syncs encryption blocks through the main blockstore alongside data blocks. This is a fundamental architectural difference that makes the ACP skip in Rust purely advisory.

## Impact

- **At rest**: Encryption keys are stored in the same blockstore as encrypted data. An attacker who gains read access to the blockstore can decrypt all data.
- **In transit**: Encryption blocks are synced via Bitswap like any other block. Any node in the P2P network can request and receive encryption blocks.
- **The ACP check prevents accidental decryption** but does not prevent intentional key extraction.

## Remediation

This is a known architectural gap. Full remediation requires implementing KMS-style key distribution where encryption keys are stored in a separate, access-controlled store and are only distributed to authorized nodes. This is a significant architectural change beyond the scope of this audit.

Short-term mitigations:
1. Document that field-level encryption provides confidentiality against casual observers, not against nodes that store the data
2. Consider encrypting the encryption blocks themselves with a node-specific key derived from the KMS identity

## Test Gap

No test verifies that encryption keys are not accessible to unauthorized nodes at the blockstore level.
