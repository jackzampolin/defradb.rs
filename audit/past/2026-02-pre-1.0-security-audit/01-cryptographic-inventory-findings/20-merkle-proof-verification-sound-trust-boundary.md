# Finding: Merkle Proof Verification is Sound; Embedded Key Trust Boundary Needs Caller Awareness

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof
**Severity**: LOW (sound implementation; trust boundary is a usage concern, not a bug)
**Category**: Merkle Proof / Signed Proofs
**Status**: NEW

## Summary

The Merkle proof implementation is cryptographically sound. CID verification, path chain validation, and signed proof verification all correctly implement their security properties. However, the `verify_with_embedded_key()` method on `SignedMerkleProof` verifies only that the proof is self-consistent — it does NOT verify that the signer is trusted. Callers must independently validate signer identity.

## Evidence

### Proof Verification — Sound

`crates/crypto/src/merkle_proof/proof.rs:53-102`:

The `verify()` method correctly checks:
1. Path length within DoS limits (MAX_PROOF_PATH_LENGTH = 1000)
2. Non-empty path
3. First node CID matches declared leaf_cid
4. Last node CID matches declared root_cid
5. Each node's CID matches its content hash (via `verify_cid()`)
6. Each node's `heads` field contains the next node's CID (chain integrity)

### CID Verification — Sound

`crates/crypto/src/merkle_proof/proof_node.rs:31-51`:

The `verify_cid()` method:
1. Explicitly validates hash algorithm is SHA2-256 (0x12)
2. Explicitly validates codec is DAG-CBOR (0x71)
3. Recomputes SHA-256 hash of the data
4. Compares computed CID with declared CID
5. Rejects unsupported algorithms with descriptive errors

This prevents algorithm confusion attacks where a CID using a weaker hash might be substituted.

### CID Computation — Deterministic

`crates/crypto/src/merkle_proof/proof_node.rs:60-74`:

```rust
pub(crate) fn compute_cid(bytes: &[u8]) -> Result<Cid> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &digest)?;
    Ok(Cid::new_v1(DAG_CBOR_CODEC, mh))
}
```

SHA-256 → multihash → CID v1 with DAG-CBOR codec. This is deterministic and matches Go's CID computation.

### DoS Protection — Present

- `extraction.rs:12`: `MAX_TRAVERSAL_NODES = 10_000` limits BFS traversal
- `proof.rs:10`: `MAX_PROOF_PATH_LENGTH = 1000` limits proof verification
- Both are checked before doing expensive operations

### Embedded Key Trust — Caller Responsibility

`crates/crypto/src/merkle_proof/signed_proof.rs:91-102`:

```rust
pub fn verify_with_embedded_key(&self) -> Result<bool> {
    let public_key = extract_public_key_from_signature(&self.signature)?;
    let proof_bytes = self.proof.to_dag_cbor()?;
    if !public_key.verify(&proof_bytes, &self.signature.value)? {
        return Ok(false);
    }
    self.proof.verify()
}
```

This verifies that:
1. The signature is valid for the embedded key
2. The proof itself is valid

But it does NOT verify that the embedded key belongs to a trusted entity. An attacker can:
1. Create any proof (valid or fabricated from real blocks)
2. Sign it with their own key
3. Embed their key in the signature header
4. `verify_with_embedded_key()` will return `true`

The explicit-key `verify()` method at line 55-86 does check identity match, which is safer when the caller has an expected key.

### Standalone Verify Delegates to Embedded Key

`crates/crypto/src/merkle_proof/signed_proof.rs:139-141`:

```rust
pub fn verify_signed_proof(proof: &SignedMerkleProof) -> Result<bool> {
    proof.verify_with_embedded_key()
}
```

The convenience function `verify_signed_proof` uses the embedded key path, which has the trust boundary concern.

## Test Coverage — Comprehensive

The test file (`crates/crypto/tests/merkle_proof_tests.rs`) covers:
- Single block proofs, two/three block chains, branching DAGs
- CID corruption detection
- Wrong root/leaf CID rejection
- Missing link detection (unrelated blocks)
- Empty proof rejection
- Signed proof with Ed25519 and secp256k1
- Wrong key verification failure
- Tampered proof detection
- Corrupted signature detection
- Key type mismatch error handling
- Invalid UTF-8/hex identity rejection
- Invalid CBOR rejection
- Unsupported hash algorithm/codec rejection
- DAG-CBOR roundtrip serialization
- BFS extraction from blockstore
- Missing parent block error handling

This is thorough test coverage. No gaps identified in the verification logic itself.

## Signed Proof vs Batch Signing

The signed proof system (`signed_proof.rs`) supports all 4 key types:
- Ed25519 → EdDSA
- secp256k1 → ES256K
- secp256r1 → ES256
- BLS12-381 → BLS

The batch signing system (`batch.rs`, from Session 2 / Finding 06) only supports Ed25519 and secp256k1. This asymmetry means secp256r1 and BLS proofs can be signed but not batch-signed.

## Impact

The trust boundary concern is not a vulnerability in the proof system itself — it's a correct separation of concerns. However, if callers use `verify_with_embedded_key()` or `verify_signed_proof()` without additional identity checks, they may accept proofs from untrusted sources.

## Affected Code

- `crates/crypto/src/merkle_proof/signed_proof.rs:91-102` — `verify_with_embedded_key()`
- `crates/crypto/src/merkle_proof/signed_proof.rs:139-141` — `verify_signed_proof()` convenience function

## Remediation

No code fix needed — the implementation is sound. Consider:

1. Add a doc comment to `verify_with_embedded_key()` explicitly warning that callers must independently verify the signer's identity
2. Consider renaming to `verify_signature_and_proof()` to avoid implying full trust verification
3. Add a `verify_trusted()` method that takes a set of trusted public keys
