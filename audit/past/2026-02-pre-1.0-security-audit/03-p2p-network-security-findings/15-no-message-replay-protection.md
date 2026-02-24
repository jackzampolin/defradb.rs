# Finding: No Message Replay Protection

**Stream**: 03 - P2P Network Security
**Severity**: LOW
**Category**: Protocol Weakness
**Status**: CONFIRMED

## Summary

P2P messages use UUID v4 for `message_id` but include no timestamp, nonce, or expiration. A validly signed message can be captured and replayed indefinitely. The UUID prevents accidental duplicate processing but provides no cryptographic replay protection.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/signing.rs` | 57-59 | UUID v4 generated for message_id — random, not time-bound |
| `crates/p2p/src/message/metadata.rs` | 12-44 | MetaData struct — no timestamp or expiration field |

## Details

### What Exists

- `message_id`: UUID v4 (random) — unique per message but not time-bound
- `version`: Protocol version string — static, not per-message
- `signature`: Covers message content — but a replayed message has the same content and same valid signature

### What's Missing

- No timestamp field (Go doesn't have one either)
- No monotonic counter or sequence number
- No message expiration / TTL
- No seen-message cache for deduplication

### Go Parity

Go's implementation also lacks replay protection — the same `signAndSetMetaData` pattern generates a UUID v4 with no timestamp. This is a design-level gap shared by both implementations.

### Attack Scenario

1. Attacker observes a signed PushLogRequest from node A to node B
2. Attacker replays the exact message to node B (or node C)
3. The signature is valid, the pubkey matches, peer ID matches
4. The message is accepted and processed

### Mitigating Factors

- **Idempotent processing**: PushLogRequest contains a CID and block data. Replaying it results in storing the same block at the same CID, which is idempotent. The receiver won't corrupt data — it just re-stores what it already has.
- **Transport encryption**: Noise provides forward secrecy, making message capture difficult for passive eavesdroppers. An active MITM is prevented by Noise's authentication.
- **No state-changing commands**: PushLog is data replication, not commands. Replaying "store this block" is harmless if the block is already stored.

### Why This Is Still LOW

The idempotent nature of block storage means replay has no meaningful impact on data integrity. The primary concern would be amplification (forcing a node to repeatedly process the same block), but this is a DoS vector covered by the lack of rate limiting (Finding 01 / Session 1).

## Remediation

Matches Go — no immediate action needed. If a future protocol version adds non-idempotent operations (e.g., delete, access revocation), replay protection becomes critical.

## Test Gap

No test verifies that a replayed message is detected or handled. No seen-message deduplication exists.
