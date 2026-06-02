# KMS Key Distribution - TLA+ Model Design

Date: 2026-06-02. Branch target: `feat/p2p-tla-kms`, landing through PR #1014.

The requested `brainstorming` skill is not installed in this Codex session, so
this file records the equivalent up-front scoping before the spec.

## Source-Grounded Facts

| Fact | Source | Model consequence |
|---|---|---|
| `DefraKms::get_keys` asks remote peers for missing encryption CIDs and includes the requesting DID plus a per-request X25519 ephemeral public key. | `crates/kms/src/defra_kms.rs`, `crates/kms/src/wire.rs` | `IssueRequest(n, K)` publishes a request whose requester identity is the authorization principal. |
| `DefraKms::serve_request` only wraps blocks after `policy.check_release(actor, scope)` returns `Allow`. | `crates/kms/src/defra_kms.rs`, `crates/kms/src/policy.rs`, `crates/kms/src/nac_dac_policy.rs` | Green configs set `PolicyMode = "CurrentAuth"`; red configs show what breaks without it or with stale request-time auth. |
| Replies carry ECIES-wrapped encryption-block bytes; unwrap requires the requester's private ephemeral and responder AAD. | `crates/kms/src/ecies_envelope.rs` | Crypto is abstracted as `CryptoMode = "RecipientOnly"`: an envelope is usable only by its intended recipient. |
| The pubsub transport publishes bare CBOR requests on `encryption`; replies go to `encryption/<requester>/_response`. | `crates/p2p/src/kms/pubsub_transport.rs`, `crates/p2p/src/topics.rs`, `crates/p2p/src/sync/coordinator/event_handler/pubsub_raw.rs` | The network is modeled as fair pubsub delivery. Any topic peer can receive ciphertext, but receipt is not possession. |
| Encryption metadata is deliberately excluded from Bitswap DAG fetch and served through KMS only. | `crates/p2p/src/sync/manager/links.rs`, `crates/p2p/src/sync/car.rs` | This is a separate KMS request/response model, not a DAG-block replication model. `DagReplication.tla` is reused as the fairness/delivery pattern. |
| Stores persist both the plaintext DEK and the encoded `Encryption` block bytes. | `crates/kms/src/store.rs`, `crates/kms/src/memory_store.rs`, `crates/kms/src/blockstore_store.rs` | `usable[n]` represents the plaintext DEK being present in a node's KMS store. |

## Abstraction

| Spec symbol | Real thing |
|---|---|
| `Nodes`, `DIDs`, `DidOf` | P2P nodes and their authorization DIDs. |
| `Keys` | Document/collection DEKs, represented by encryption CIDs in implementation. |
| `auth[K]` | DAC/NAC authorization relation for the key scope. |
| `usable[n]` | Plaintext key material held by node `n`; this is stronger than seeing ciphertext. |
| `activeRequests` | Gossiped `FetchEncryptionKeyRequest` messages. |
| `requestLog` | Requests that existed and can be replayed by an adversary. |
| `envelopes` | ECIES ciphertext replies encrypted to one requester. |
| `received[n]` | Ciphertext envelopes seen by a node on the pubsub topic. |
| `decryptions` | Successful abstract decrypt events. |

The model does not implement ECIES math. The crypto boundary is the assumption
that `CryptoMode = "RecipientOnly"` matches ECIES: only the holder of the
requester's private ephemeral can unwrap the reply. `CryptoMode =
"AnyoneCanDecrypt"` is a red misconfiguration that proves the assumption is
load-bearing.

Revocation after a node already holds the DEK is out of scope for v1 because
there is no key rotation in the current KMS design. The `Revoke` action encodes
that scope: it may revoke only before the node has the key and before a response
envelope has been released to that node.

## State Machine

`IssueRequest(n, K)` creates a request with a snapshot bit recording whether the
requester was authorized when the request was made. This bit is only used by the
red stale-auth configuration.

`Respond(authorizer, request)` requires the responder to hold `K`. In green mode
it also checks current `auth[K]`, matching `AccessPolicy::check_release` in
`serve_request`. The response creates one envelope encrypted to the requester.

`ReceiveEnvelope(n, envelope)` lets any topic peer receive ciphertext under fair
network delivery.

`Decrypt(n, envelope)` moves `K` into `usable[n]` only when `CanDecrypt` permits
it. In the green crypto model this means `n = envelope.recipient`.

`ReplayOldRequest(request)` reintroduces a request made while authorized after
the requester has been revoked. Green current-policy response denies it; the red
stale-auth mode serves it.

## Properties

| Property | Plain English | TLC verdict | Source note |
|---|---|---|---|
| `INV_AuthorizedEventuallyHasKey` | Under fair delivery and eventual connectivity, every currently authorized node eventually holds every key it is authorized for. | GREEN: `MC_Kms_Green.cfg`, `MC_Kms_RevokeReplay_Green.cfg` | `get_keys` fan-out plus `PubsubKeyTransport` request/reply delivery. |
| `INV_OnlyAuthorizedHasKey` | No node has usable plaintext key material unless its DID is currently authorized. | GREEN in current-policy configs. RED: `MC_Kms_NoPolicy_Red.cfg` | `serve_request` policy gate; ciphertext is not plaintext. |
| `INV_OnlyIntendedRecipientDecrypts` | A successful decrypt event must be by the envelope recipient. | GREEN with `RecipientOnly`. RED: `MC_Kms_BroadcastCiphertext_Red.cfg` | `ecies_envelope::{wrap_for_requester, unwrap_with_private}`. |
| `INV_RevokedCannotObtain` | A node revoked before any response envelope is released to it never obtains the key. | GREEN with current auth. RED: `MC_Kms_Revoke_Red.cfg` | `AccessPolicy::check_release` is evaluated at response time. |
| `INV_NoReplayGrant` | Replaying an old request does not yield a key after the requester is revoked. | GREEN with current auth. RED: `MC_Kms_Replay_Red.cfg` | Request payloads are not authorization grants; serving peers must re-check policy. |

## TLC Runs

Run from `specs/`:

```bash
# GREEN: policy-gated response, recipient-only decryptability, liveness.
./tools/tlc -config MC_Kms_Green.cfg MC_Kms_Gossip.tla

# RED: missing response policy lets unauthorized Eve request and decrypt K.
./tools/tlc -config MC_Kms_NoPolicy_Red.cfg MC_Kms_Gossip.tla

# RED: treating ciphertext as broadcast plaintext lets Eve use Bob's envelope.
./tools/tlc -config MC_Kms_BroadcastCiphertext_Red.cfg MC_Kms_Gossip.tla

# GREEN: revoke before response and replay after revoke remain safe.
./tools/tlc -config MC_Kms_RevokeReplay_Green.cfg MC_Kms_Replay.tla

# RED: stale request-time authorization lets a requester revoked before
# response obtain K.
./tools/tlc -config MC_Kms_Revoke_Red.cfg MC_Kms_Replay.tla

# RED: replaying an old authorized request is treated as a grant.
./tools/tlc -config MC_Kms_Replay_Red.cfg MC_Kms_Replay.tla
```

## Result

The intended green result is:

> every authorized node eventually gets the key under eventual connectivity; no
> unauthorized, revoked, or replaying node can obtain a usable key, modulo the
> stated crypto assumption that an ECIES envelope is usable only by its intended
> recipient.

The red configs demonstrate the two load-bearing implementation obligations:
responders must check current DAC/NAC policy before wrapping, and ciphertext
delivery must not be treated as key possession.

## Verification Log

Verified with TLC 2.19 on 2026-06-02:

| Run | Verdict |
|---|---|
| `./tools/tlc -config MC_Kms_Green.cfg MC_Kms_Gossip.tla` | GREEN, no error; 78 distinct states. |
| `./tools/tlc -config MC_Kms_RevokeReplay_Green.cfg MC_Kms_Replay.tla` | GREEN, no error; 29 distinct states. |
| `./tools/tlc -config MC_Kms_NoPolicy_Red.cfg MC_Kms_Gossip.tla` | RED as intended: `INV_OnlyAuthorizedHasKey` violated by Eve requesting while unauthorized. |
| `./tools/tlc -config MC_Kms_BroadcastCiphertext_Red.cfg MC_Kms_Gossip.tla` | RED as intended: `INV_OnlyIntendedRecipientDecrypts` violated when Eve decrypts Bob's envelope under `AnyoneCanDecrypt`. |
| `./tools/tlc -config MC_Kms_Revoke_Red.cfg MC_Kms_Replay.tla` | RED as intended: `INV_RevokedCannotObtain` violated under stale request-time auth. |
| `./tools/tlc -config MC_Kms_Replay_Red.cfg MC_Kms_Replay.tla` | RED as intended: `INV_NoReplayGrant` violated after `ReplayOldRequest`. |
