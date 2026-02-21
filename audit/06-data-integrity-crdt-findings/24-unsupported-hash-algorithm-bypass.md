# Unsupported Hash Algorithm Bypasses All Integrity Verification

**Severity:** Medium
**Category:** Data Integrity / Verification Bypass
**Status:** Confirmed

## Summary

The blockstore's `verify_hash()` method only supports SHA2-256 (code 0x12). For any other hash algorithm code, verification is silently skipped with a log warning and the block is returned as valid. An attacker can construct a CID using an unsupported hash algorithm (e.g., SHA3-256 code 0x16, or a fabricated code like 0xFF) and pair it with arbitrary content. Even with `hash_on_read` enabled, the block passes verification without its content being checked.

## Affected Files

- `crates/blockstore/src/lib.rs:143-175` (`verify_hash()` — returns `Ok(())` for non-0x12)
- `crates/crypto/src/merkle_proof/proof_node.rs:31-51` (`verify_cid()` — correctly rejects unsupported algorithms)

## Details

### Blockstore: Permissive Read

```rust
// lib.rs:150-166
fn verify_hash(&self, cid: &Cid, data: &[u8]) -> Result<()> {
    let mh = cid.hash();
    let code = mh.code();
    let computed_digest: Vec<u8> = match code {
        0x12 => {
            // SHA2-256 — actually verified
            let mut hasher = Sha256::new();
            hasher.update(data);
            hasher.finalize().to_vec()
        }
        _ => {
            // VERIFICATION SKIPPED — block accepted as valid
            tracing::warn!(hash_code = code, cid = %cid,
                "Hash verification skipped: unsupported hash algorithm");
            return Ok(());  // <-- Returns success without checking anything
        }
    };
    // ... compare digests ...
}
```

### Attack Scenario

1. Attacker creates a malicious block with desired CRDT delta content
2. Attacker constructs a CID using SHA3-256 (code 0x16) with a random digest
3. The CID is syntactically valid and can be transmitted via PushLog/CAR
4. Target stores the block via `put()` (no verification — Finding 23)
5. Target reads the block via `get()` — even with `hash_on_read` enabled:
   - `verify_hash()` is called
   - Code 0x16 does not match 0x12
   - Warning logged, `Ok(())` returned
   - Block served as if verified
6. Merge handler processes the block with attacker-controlled delta content

### Contrast: Merkle Proof Verification

The crypto crate's `ProofNode::verify_cid()` correctly rejects unsupported algorithms:

```rust
// proof_node.rs:31-47
pub fn verify_cid(&self) -> Result<bool> {
    if self.cid.hash().code() != SHA2_256_CODE {
        return Err(Error::BlockError(format!(
            "Unsupported hash algorithm: 0x{:x} (only SHA2-256 0x12 is supported)",
            self.cid.hash().code()
        )));
    }
    // ... actually verify ...
}
```

This is the correct approach — reject rather than skip.

### Supported Hash Algorithms in Practice

DefraDB exclusively uses SHA2-256 (0x12) for all block CID generation. The `generate_cid_from_bytes()` function hardcodes `SHA2_256_CODE = 0x12`. There is no legitimate code path that creates blocks with other hash algorithms. Therefore, any block with a non-0x12 hash code is either:
- From a foreign IPFS system (not a DefraDB concern)
- Crafted by an attacker

### Comment in Code

The code comment states:
> "Unsupported algorithms are logged and skipped (verification passes) rather than erroring, matching the principle of being permissive on read."

This permissiveness is dangerous for a content-addressed database. The "be permissive on read" principle from Postel's Law applies to protocols, not to cryptographic verification. Skipping verification is fundamentally different from accepting a slightly malformed header.

## Remediation

1. **Reject unsupported hash algorithms** — match ProofNode behavior:
   ```rust
   _ => {
       return Err(Error::HashMismatch {
           cid: format!("{} (unsupported hash algorithm 0x{:x})", cid, code),
       });
   }
   ```

2. **Alternatively, validate hash algorithm on put()** — reject blocks with non-SHA2-256 CIDs at ingestion time, before they enter storage.

## Test Gap

- `test_hash_on_read_unsupported_algorithm_skipped` (line 695) **tests the wrong behavior** — it verifies that unsupported algorithms are skipped (treated as valid), which is the vulnerability. This test should be updated to expect an error.
- No test creates a block with an unsupported hash algorithm via P2P and verifies rejection
- No test verifies that `put()` rejects blocks with non-SHA2-256 CIDs
