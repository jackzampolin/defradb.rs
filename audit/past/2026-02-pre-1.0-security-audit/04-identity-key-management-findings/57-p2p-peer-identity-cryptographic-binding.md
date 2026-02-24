# Finding 57: P2P Peer Identity Has Cryptographic Binding (Green)

**Severity**: GREEN
**Category**: P2P Identity / Peer Authentication
**Status**: Verified sound

## Summary

The P2P message signing and verification correctly binds a peer's identity (PeerId) to their cryptographic key. The `verify_message()` function verifies three properties: (1) the message has a signature, (2) the PeerId in the message matches the public key in the message, (3) the signature is valid over the message content.

## Affected Files

- `crates/p2p/src/signing.rs:118-157` — `verify_message()`
- `crates/p2p/src/signing.rs:53-97` — `sign_message()`

## Details

### Verification steps

```rust
pub fn verify_message<M>(msg: &M) -> Result<()> {
    // 1. Signature must exist
    let signature = msg.signature().ok_or(Error::MissingSignature)?;

    // 2. Decode public key from message
    let pubkey = PublicKey::try_decode_protobuf(msg.pubkey())?;

    // 3. Derive PeerId from public key
    let id_from_key = pubkey.to_peer_id();

    // 4. Parse sender's claimed PeerId
    let sender_peer_id: PeerId = msg.sender_id().parse()?;

    // 5. Verify PeerId matches public key (prevents DID/PeerId confusion)
    if id_from_key != sender_peer_id {
        return Err(Error::PubkeyPeerIdMismatch);
    }

    // 6. Verify signature
    let mut msg_for_verify = msg.clone();
    msg_for_verify.set_signature(None);
    let bytes = serde_cbor::to_vec(&msg_for_verify)?;
    if !pubkey.verify(&bytes, signature) {
        return Err(Error::InvalidSignature);
    }
    Ok(())
}
```

### Why this is sound

- A peer cannot claim a PeerId that doesn't match their key (step 5 catches this)
- A peer cannot forge a signature without the private key (step 6 catches this)
- The message content is signed, preventing tampering
- This matches Go's `verifyMessage()` behavior

### Limitation: PeerId ≠ DID

The P2P layer uses libp2p PeerIds (derived from Ed25519 keys via multihash), while the identity/ACP layer uses DIDs (did:key format). These are different identity representations. The mapping between a peer's PeerId and their DID for ACP purposes is not directly verified in the P2P signing code — the ACP layer handles DID resolution separately.

## Remediation

None required for the P2P signing layer. The PeerId-to-DID mapping is a separate concern handled by the ACP integration.
