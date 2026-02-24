# Finding: Core sign_message/verify_message Logic Is Sound

**Stream**: 03 - P2P Network Security
**Severity**: GREEN
**Category**: Cryptographic Verification
**Status**: CONFIRMED

## Summary

The `sign_message()` and `verify_message()` functions in `signing.rs` are correctly implemented. The 4-point verification is complete, error handling is strict, and the signature clearing technique prevents self-referential signatures. All 13 signing tests cover the expected failure modes. The functions themselves are sound — the vulnerability is that they are not called in the two-stream production path (Finding 12).

## Verified Checklist

### sign_message() (lines 53-97)

| Check | Status | Detail |
|-------|--------|--------|
| UUID v4 for message_id | CORRECT | Only set if empty — preserves existing ID for responses |
| Version set | CORRECT | `MESSAGE_VERSION` constant |
| sender_id from keypair | CORRECT | `keypair.public().to_peer_id().to_string()` — derived, not user-supplied |
| pubkey protobuf-encoded | CORRECT | `keypair.public().encode_protobuf()` — matches Go's `crypto.MarshalPublicKey` |
| Signature cleared before serializing | CORRECT | `msg.set_signature(None)` at line 73 — prevents including old/partial signature in signed bytes |
| CBOR serialization | CORRECT | `serde_cbor::to_vec(&msg)` — deterministic for the same struct field order |
| Sign the bytes | CORRECT | `keypair.sign(&bytes)` — Ed25519 signature via libp2p |
| Error propagation | CORRECT | Both CBOR serialization and signing errors are returned as `Result::Err` |

### verify_message() (lines 118-157)

| Check | Status | Detail |
|-------|--------|--------|
| 1. Signature exists | CORRECT | `msg.signature().ok_or(Error::MissingSignature)?` — fail-fast |
| 2. Pubkey decodes | CORRECT | `PublicKey::try_decode_protobuf(msg.pubkey())?` — error on malformed protobuf |
| 3. Peer ID matches pubkey | CORRECT | `pubkey.to_peer_id()` compared to parsed `sender_id` — direction is correct (derive from pubkey, compare to claimed sender) |
| 4. Signature valid | CORRECT | `pubkey.verify(&bytes, signature)` — returns `false` on invalid |
| All 4 checks AND'd | CORRECT | Sequential `?` operators — any failure short-circuits to error |
| sender_id parsing | CORRECT | `msg.sender_id().parse::<PeerId>()` — returns `Error::InvalidPeerId` on malformed input, does not panic |
| Clone for verification | CORRECT | `msg.clone()` + `set_signature(None)` — does not mutate original message |

### Signature Verification Internals

- **Constant-time comparison**: Handled by libp2p's `PublicKey::verify()`, which delegates to `ed25519-dalek`. Ed25519 signature verification uses `ed25519::Signature::from_bytes()` + `VerifyingKey::verify_strict()`, which is not vulnerable to timing attacks (verification involves modular arithmetic, not byte comparison).
- **Error types map to rejection**: `MissingSignature`, `InvalidSignature`, `PubkeyPeerIdMismatch`, `InvalidPeerId`, `PublicKeyDecode` — all are `Error` variants that propagate as failures. None are warnings or silently ignored.

### Test Coverage (13 tests in signing_tests.rs)

| Test | Covers |
|------|--------|
| `test_sign_message_sets_all_fields` | All metadata populated correctly |
| `test_sign_verify_roundtrip` | Happy path end-to-end |
| `test_verify_tampered_message_fails` | Modified doc_id → InvalidSignature |
| `test_verify_wrong_signature_fails` | Garbage signature → InvalidSignature |
| `test_verify_pubkey_mismatch_fails` | Different keypair's peer ID → PubkeyPeerIdMismatch |
| `test_sign_preserves_existing_message_id` | Response messages keep request's message_id |
| `test_verify_missing_signature_fails` | No signature → MissingSignature |
| `test_sign_message_cloned` | Cloned signing doesn't modify original |
| `test_different_keypairs_produce_different_signatures` | Distinct keys → distinct signatures |
| `test_uuid_format` | message_id is valid UUID v4 |

All expected rejection cases are covered with specific error variant assertions.

## Notes

- The Go comparison shows Rust's `sign_message()` explicitly clears the signature (`set_signature(None)`) before serialization, while Go relies on the signature field being unset at that point in the flow. Both are correct — Rust's approach is more defensive.
- Go's `verifyMessage()` mutates the original message's signature (sets to nil). Rust clones the message first, which is safer.

## Remediation

None needed for the signing/verification functions themselves. The critical gap is that these functions are not called in the production two-stream path (Finding 12).
