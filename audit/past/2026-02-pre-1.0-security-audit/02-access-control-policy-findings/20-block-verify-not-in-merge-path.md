# Finding: Block Verification Function Disconnected from Merge Path

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM (structural — findings 18+19 cover the security impact)
**Category**: Defense Architecture
**Status**: CONFIRMED

## Summary

`verify_block_signature()` is a well-implemented verification function that checks block signatures, validates ACP Read permission, and verifies identity consistency. However, it is architecturally disconnected from the P2P merge path — it exists as a standalone on-demand API. The merge path (which is where untrusted P2P blocks enter the system) has no equivalent verification.

This finding documents the structural gap. The direct security impacts are covered in findings 18 (no signature verification during merge) and 19 (creator identity spoofing).

## Affected Files

| File | Function | Role |
|------|----------|------|
| `crates/db/src/block_verify.rs:15-112` | `verify_block_signature()` | On-demand verification (correct implementation) |
| `crates/db/src/acp_merge_handler.rs:187-236` | `handle_block()` | P2P merge entry — no verification |
| `crates/db/src/merge_handler/mod.rs:330-435` | `handle_block()` | Inner merge — no verification |

## Details

### What `verify_block_signature()` Gets Right

The function performs a complete verification chain:

1. **Block decoding** (line 44-45): Parses DAG-CBOR block
2. **Signature existence check** (line 47): Validates block has a signature
3. **Signature loading** (line 49-53): Loads the signature block from blockstore
4. **ACP permission check** (lines 62-86): Checks caller has Read permission on the document
5. **Identity consistency** (lines 89-93): Verifies `signature.header.identity` matches provided public key
6. **Cryptographic verification** (lines 96-109): Re-serializes block without signature, verifies against signature value

### What the Merge Path Misses

| Check | `verify_block_signature()` | P2P Merge Path |
|-------|---------------------------|----------------|
| Block decoding | ✓ | ✓ |
| Signature exists | ✓ | ✗ |
| Signature loaded | ✓ | ✗ |
| ACP permission | ✓ (Read) | ✓ (Update, via AcpMergeHandler) |
| Identity matches signature | ✓ | ✗ |
| Cryptographic signature valid | ✓ | ✗ |

### Architectural Observation

The verification function and the merge handler live in the same crate (`db`) but are completely independent code paths. The verification function was likely designed for the `/api/v0/block/verify` API endpoint — a user-facing operation where a caller explicitly requests verification. The P2P merge handler was designed separately with the assumption that blocks from peers are trustworthy.

### Order of Operations in verify_block_signature()

Notably, the ACP check happens AFTER loading the block and signature but BEFORE cryptographic verification:

```
1. Load block → 2. Load signature → 3. Check ACP Read → 4. Verify identity → 5. Verify signature
```

This means ACP is checked before proving the block is authentic. If this function were integrated into the merge path, the order should be:

```
1. Load block → 2. Load signature → 3. Verify signature → 4. Extract identity from signature → 5. Check ACP
```

This ensures the identity used for ACP checks is cryptographically proven, not caller-supplied.

## Remediation

Extract the verification logic from `verify_block_signature()` into reusable components that can be called from both the on-demand API and the merge path. The merge path version should:

1. Verify signature first (cryptographic proof)
2. Extract creator identity from verified signature
3. Use that identity for ACP permission check

## Test Coverage

Integration tests exist for the `/api/v0/block/verify` endpoint but not for signature verification during merge.
