# Capability — explicit-replay capability gate (design)

Models the authorizer-signed, peer+collection-bound, TTL-capped, revocable token
that gates **live encrypted-replay** between P2P peers. Family: `Capability.tla`
+ wrapper `MC_Capability_Common.tla` + five `MC_Capability_*.cfg` scenarios.

## What is being proven

> Capability tokens authorizing live encrypted-replay are unforgeable and bound
> to (peer, collection); a token presented for the wrong target peer or wrong
> collection is rejected; TTL is capped at `MAX_CAPABILITY_TTL` even under
> issuer-key compromise; a revoked token is never re-accepted.

## Source anchors (the model abstracts these exact paths)

All in `crates/p2p/src/explicit_replay.rs`:

| Spec symbol | Real code | Anchor |
|---|---|---|
| token claims `[src,tgt,coll,issuer,expiry]` | `ExplicitReplayCapabilityClaims` | l.34–43 |
| `MaxTtl` | `MAX_CAPABILITY_TTL` (= `DEFAULT_CAPABILITY_TTL`, 365d) | l.28–29 |
| `IssueToken` cap (`WithinCapAt`) | `generate_capability` lifetime check + `validate_expiration_cap` | l.201–206, 107–117 |
| `BindOk` (src/tgt/coll) | `validate_claims` source/target/collection equality | l.148–167 |
| `TtlOk` not-expired | `validate_claims` `expires_at < now` reject | l.175–181 |
| `TtlOk` cap-at-verify | `validate_claims` `expires_at - now > MAX_CAPABILITY_TTL` reject | l.183–189 |
| `SignatureVerifies` | `verify_capability_with_revocations` sig verify vs `authorizer_did` key | l.283–294 |
| `RevokeOk` / `Revoke` | `ExplicitReplayRevocationRegistry` deny-list + `is_envelope_revoked` | l.61–86, 296–300 |
| `Verify` → `accepted` | returns `ExplicitReplayAuthorization` (= replay authorized) | l.302–308 |

Enforcement wiring (the gate guards encrypted replay before merge):
`crates/p2p/src/sync/merge.rs:253–269` (`explicit_replay_authorizer_for`,
`allows_explicit_replay_for`), invoked via `validate_authorization`
(`merge.rs:333–339`, `merge.rs:360–378`). Call sites that mint/verify on the
replicator path: `crates/cli/src/p2p_adapter.rs:391–449`,
`crates/p2p-adapter/src/libp2p.rs:262–283`, re-exported in
`crates/p2p/src/lib.rs:93–99`.

## Grounding each spec symbol

- **`clock`** — verifier wall clock (`now_unix()`, l.88–93). Bounded by `MaxClock`
  to keep the model finite; `Tick` advances it. TTL checks compare to `clock`.
- **`issued`** — tokens an *honest* authorizer actually minted, respecting the
  generation-time cap. The only legitimate provenance.
- **`presented`** — attempts placed on the wire (adversary-controlled; presenting
  is free, like KMS envelope receipt). `via` is the transport-authenticated
  sender peer id that `validate_claims` pins to `claims.source_peer_id`.
- **`revoked`** — verifier-local deny-list.
- **`accepted`** — attempts the gate authorized → `ExplicitReplayAuthorization`.
- **`acceptedWhileRevoked`** — bookkeeping: attempts accepted whose token was
  *already* on the deny-list. The honest "revoked ⇒ later verifies deny"
  property is `acceptedWhileRevoked = {}` (immune to the benign accept-then-revoke
  race, where the replay legitimately happened before revocation).

## Crypto boundary (assumed, not proven)

Exactly the Kms ECIES abstraction style. `SignatureVerifies(tok)` is the model of
"ed25519 verify against `authorizer_did`'s key succeeds": true iff the token was
honestly issued, **or** the claimed issuer's key is compromised (`ForgeMode`),
**or** crypto is fully broken. Real ed25519 unforgeability is the assumed
boundary. `ClaimIssuer ∉ HonestIssuers` models a stolen signing key.

## Red/green dials (the four mode constants)

Production = `(Unforgeable, CapAtVerify, DenyRevoked, BindTargetCollection)`.
Each weakening is a concrete bug a nearby implementation could ship.

| Mode | Production | Bug variant |
|---|---|---|
| `ForgeMode` | `Unforgeable` | `IssuerKeyCompromised` (stolen key) / `FullyForgeable` |
| `TtlMode` | `CapAtVerify` (re-check at verify, l.183–189) | `CapAtIssueOnly` (trust signed expiry) |
| `RevokeMode` | `DenyRevoked` (l.296–300) | `IgnoreRevoked` (skip deny-list) |
| `BindMode` | `BindTargetCollection` (l.155–167) | `BindTargetOnly` / `NoBind` |

## Invariants

- `INV_OnlyLegitAccepted` — every accepted attempt is backed by an honestly-issued
  token bound to exactly this (via, target, collection), live, within the cap.
  This ground truth is *independent* of the gate's own checks, so any weakened
  gate that accepts something else is caught.
- `INV_TargetBound` — accepted attempt's target/collection equal the token's.
- `INV_TtlCapped` — no accepted attempt's remaining lifetime exceeds `MaxTtl`.
- `INV_RevokedNeverAccepted` — `acceptedWhileRevoked = {}`.

## Scenarios, run commands, expected verdicts

Run from `proofs/tla` (integrator wires these into `run-all.sh`; not edited here):

```
./tools/tlc -metadir states/<x> -config <cfg> MC_Capability_Common.tla
```

| Cfg | Modes (non-prod) | Invariant exercised | Expect |
|---|---|---|---|
| `MC_Capability_Green.cfg` | none (production) | all four | **green** (no error) |
| `MC_Capability_Red_Forge.cfg` | `ForgeMode=IssuerKeyCompromised` | `INV_OnlyLegitAccepted` | **red** (forged `tForgeX` accepted) |
| `MC_Capability_Red_Ttl.cfg` | `IssuerKeyCompromised`+`CapAtIssueOnly` | `INV_TtlCapped` | **red** (over-cap `tEternalX` accepted) |
| `MC_Capability_Red_WrongTarget.cfg` | `BindMode=NoBind` | `INV_TargetBound` | **red** (`tGood` accepted for wrong tgt/coll) |
| `MC_Capability_Red_Revoked.cfg` | `RevokeMode=IgnoreRevoked` | `INV_RevokedNeverAccepted` | **red** (revoked `tGood` re-accepted) |

`Red_Ttl` vs `Red_Forge` is the key contrast: both compromise issX's key, but
`Red_Forge` keeps `CapAtVerify` and so *still* rejects the over-cap `tEternalX` —
demonstrating that the verify-time TTL re-check (l.183–189) is what bounds TTL
under key compromise, exactly as the property demands.

## Observed verdicts (TLC 2.19)

- Green: `Model checking completed. No error has been found.` (245,760 distinct states, 4s)
- Red_Forge: `Invariant INV_OnlyLegitAccepted is violated.` (accepts `tForgeX`)
- Red_Ttl: `Invariant INV_TtlCapped is violated.`
- Red_WrongTarget: `Invariant INV_TargetBound is violated.`
- Red_Revoked: `Invariant INV_RevokedNeverAccepted is violated.`

## Vacuity self-check

A sanity invariant `accepted = {}` was checked under the green config and **was
violated** — i.e. green reaches a reachable state where `accepted = {[tok ↦
tGood, via ↦ p1, vtgt ↦ p2, vcoll ↦ c1]}`. So the green invariants constrain a
non-empty, reachable `accepted` set (the legit token *is* accepted), while the
forged/over-cap/cross-target/revoked attempts are also presented yet never
accepted. The green is non-trivial: each red is a one-dial neighbor that fails.

## Go parity / divergence

`origin/develop` of Go DefraDB has **no** equivalent. Verified: no `replay` /
`capability` / `authorization` / TTL token in `internal/core/net/` (only
`broadcaster.go`, `protocol.go`) or in the searchable-encryption coordinator
`internal/se/` (`coordinator.go` has no capability/TTL/revoke path; its README
describes producer-consumer SE artifacts with no per-(peer,collection) signed
replay gate). This is a **Rust-only hardening feature** — a deliberate divergence
adding a signed, peer/collection-bound, TTL-capped, revocable gate on the
encrypted-replay path that Go lacks. Consistent with the README's "Rust-specific
features" framing and the Auth/Integrity slices' Rust-hardening findings.

## Load-bearing abstractions (honest reach)

- **`effective_creator == authorizer_did` binding.** The real
  `explicit_replay_authorizer_for` (merge.rs:253–261) requires, beyond
  collection match, that the authorization's `authorizer_did` equals the block's
  effective creator — an *extra* binding tying the authorizer to the data's
  signer. The model abstracts this to (peer, collection, issuer) binding and does
  not model the block-creator linkage; that path is covered by Integrity/Commits.
- **Digest-keyed revocation.** Revocation keys on `sha256(claims ++ signature)`
  (l.101–105). The model treats a token's identity as atomic, so it does not
  witness a hash-collision or claims/signature-malleability attack on the
  revocation key — that reduces to crypto-collision-resistance, an assumed
  boundary.
- **Signature is over the canonical CBOR claims** (l.231–234, 284); CBOR
  determinism is assumed (cf. the content-addressing-determinism backlog item).
- **Bounded instance.** 2 peers, 2 collections, 2 issuers, 3 tokens, clock ≤ 2,
  cap = 5. Minimal witnessing shapes; structural, not quantity-sensitive.
- **`via` pinned to `ClaimSrc`** in the scenario `Attempts` set: a sender-peer
  mismatch is a separate, strictly-earlier rejection in `validate_claims`
  (l.148–153) and is not the property under test, so it is omitted to keep the
  state space small without weakening the binding/TTL/revocation claims.
```
