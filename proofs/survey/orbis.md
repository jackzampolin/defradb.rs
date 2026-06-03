# Survey: crates/orbis

## Purpose
gRPC client that delegates document signing to an external Orbis ring's threshold-BLS
`UtilityService`. Two files: `lib.rs` (proto include + re-exports) and `client.rs`
(`OrbisClient`). Responsibilities:
- Connect to the ring, call `DerivePublicKey` once at startup, derive the signer DID
  from the returned BLS public key.
- Build `SignRequest`s and mint short-lived JWT bearer tokens (via `identity`) carrying
  the `SigningAuthorization` context (Policy {policy_id, resource, object_id, permission}
  or Decision {decision_id}).
- Bridge sync→async: a dedicated current-thread tokio runtime lets `sign_sync()` (called
  from the block builder under `spawn_blocking`) drive the async RPC, hopping to a plain
  OS thread when already on a runtime to avoid nested `block_on`.

## State machines
None. The only lifecycle is connect → derive-pubkey → (sign)*; no status enum, no
multi-component protocol, no concurrent shared state beyond the runtime bridge. The
runtime-hop logic is a correctness concern but it's a local Tokio-idiom guard, not a
modelable protocol.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | — | — | — |

All proof-worthy behavior lives elsewhere:
- BLS threshold signing / DID-from-pubkey correctness is the *ring's* job and lives in
  `crates/crypto`, not here.
- JWT auth-token issuance on the management/RPC channel is conceptually the Auth slice.
- "signature verified before a block merges" is the Integrity slice.
- ACP authorization semantics embedded in the claims are the Acp slice.

This crate only *forwards* authorization context into a signed JWT; it makes no access
decisions and enforces no invariant of its own. Unit tests already pin the JWT-claim
and request-field plumbing and the channel-reuse behavior.

## Verdict
Plumbing. `model_worthy: false`, no candidates. Cryptographic and authorization
correctness is delegated to the external Orbis ring and to crypto/identity; integration
+ existing unit tests cover the wire plumbing.
