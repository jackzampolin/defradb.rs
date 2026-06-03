# Jwt slice — JWT issuer-binding / algorithm-confusion resistance

## What this proves

`identity::token::from_token` yields an authenticated actor DID `d` for a
presented JWT **only when** the token genuinely carries a signature produced by
the key whose `did:key` is `d`, the header `alg` matches the key type, and the
issuer claim binds to the DID derived from that verified public key. An adversary
who may forge every *claim* field of a token (header alg, `key_type`, `sub`,
`iss`) but cannot forge a *signature* (the crypto unforgeability boundary) can
never get a token verified under the wrong algorithm/key, and can never be minted
as a DID they cannot sign for.

This **discharges the assumption the Auth slice takes for granted** — that a
verified bearer token soundly binds the actor DID (`Auth.tla`'s `Verify` /
`ActorGate` treat "fresh valid credential ⇒ trustworthy `PresentedActor`"). The
binding/alg-confusion logic itself was previously unmodeled (see
`survey/identity.md`).

## Property (exact)

For every processed token `t`, if `from_token` returns a DID (not `rejected`),
then there exists a key `k` such that `did(k) = result(t)`, the token's true
signer is `k`, and the true signing algorithm is `k`'s curve. Stated three ways
as **independent oracle invariants** (defined from ground truth `SigBy`,
`SigAlg`, `KeyCurve`, `DidOf` — never from the mechanism's own accept decision):

- `INV_TokenBindsGenuineDid` — accepted DID is backed by a genuine signature by
  the key resolving to it.
- `INV_ResolvedDidIsReal` — the returned DID is some real key's `did:key`, never
  a cross-curve garbage/sentinel DID.
- `INV_ReturnedDidIsSignerDid` — the returned DID equals `did(true signer)`.

## Source anchors

### Rust (this tree) — the hardened mechanism modeled as GREEN
- `crates/identity/src/token/mod.rs:263-333` — `from_token`: the full pipeline.
- `crates/identity/src/token/mod.rs:284-294` — `expected_alg` is derived from the
  `key_type` **claim**; `header_alg != expected_alg ⇒ reject` (alg<->key binding).
- `crates/identity/src/token/mod.rs:296-312` — reconstruct the public key from
  `sub` under the decode-path curve, then derive the actor DID **from that
  verified public key** (`public_key.did()`), not from `iss`.
- `crates/identity/src/token/mod.rs:315-323` — `iss == did(pubkey)` issuer binding.
- `crates/identity/src/token/decoding.rs:61-109` — `decode_ed25519/_secp256k1/`
  `_secp256r1`: the curve used to reconstruct the key AND verify the signature is
  selected by the **header alg** (dispatch in `from_token:270-280`).
- `crates/identity/src/key_type.rs:19-26` — the three curves: Ed25519, Secp256k1,
  Secp256r1; `did()` is per-curve (distinct multicodec prefix) so the same `sub`
  bytes read under a different curve yield a different DID.

### Go (origin/develop) — divergence captured by the RED configs
- `acp/identity/factory.go:57-90` — `FromToken` does `jwt.Parse(WithVerify(false))`:
  it **does not verify the signature**, **does not check alg<->key_type**, and
  **does not check `iss == did(pubkey)`** at construction. The DID is taken from
  `sub`+`key_type` claims. This is the shape of `MC_Jwt_Red_NoSig` (signature gate
  off) and motivates `MC_Jwt_Red_NoIssBinding` (no iss<->did binding).
- `acp/identity/identity_impl.go:188-210` — `VerifyAuthToken` (a *separate* call)
  does `jws.Verify` keyed by `PublicKey().Type()`; lestrrat-go/jwx enforces the
  header alg matches the supplied alg, so Go guards alg-confusion **at verify
  time** — but `VerifyAuthToken` still never re-checks `iss == did(pubkey)`.

> Audit note (consistent with `proofs/README.md` Boundaries): Rust hardens this
> path (mandatory inline verify + alg-binding + iss-binding in `from_token`);
> Go's `FromToken` defers/omits these. The RED configs are not hypothetical — the
> NoSig and NoIssBinding shapes correspond to real gaps in the Go construction
> path if a caller trusts `FromToken` output without `VerifyAuthToken`.

## Model abstraction

Crypto is the assumed boundary. A signature is ground truth `(SigBy[t],
SigAlg[t])`: the key that actually signed and the algorithm actually used.
`SigVerifies(t)` holds iff the verifier reconstructs exactly that key on the
header-alg curve and the header alg equals `SigAlg[t]`. The adversary controls
`HeaderAlg`, `KtClaim`, `SubKey` (which key's bytes are in `sub`), and `IssClaim`
freely; it cannot make `SigVerifies` hold for a key it does not own.

Mechanism toggles let each RED config drop exactly one defense:
- `CheckAlgBinding` — enforce `header_alg == expected_alg(key_type claim)`.
- `CheckSig` — enforce signature verification.
- `CheckIssBinding` — enforce `iss == did(reconstructed pubkey)`.
- `IssAuthoritative` — buggy build that returns the DID from the `iss` claim
  instead of the verified pubkey (Rust returns the pubkey-derived DID).
- `IdentityCurveFromClaim` — buggy alg-confusion build that derives the actor DID
  under the `key_type`-claim curve while verifying under the header alg.

The GREEN config sets all checks `TRUE` and both bug toggles `FALSE`, matching
the Rust code.

## Scenarios (RED counterexamples have teeth)

| Config | Mechanism | Token that breaks | Oracle violated | Observed |
|---|---|---|---|---|
| `MC_Jwt_Green` | all checks on, no bugs | — (honest token accepts → `did:ed`) | none | PASS (no error) |
| `MC_Jwt_Red_NoAlgBinding` | `CheckAlgBinding=FALSE`, `IdentityCurveFromClaim=TRUE` | `t_algconf` → `did:crosscurve` | `INV_TokenBindsGenuineDid` | VIOLATED |
| `MC_Jwt_Red_NoSig` | `CheckSig=FALSE` | `t_subswap` → `did:k1` (forged by `kAdv`) | `INV_TokenBindsGenuineDid` | VIOLATED |
| `MC_Jwt_Red_NoIssBinding` | `CheckIssBinding=FALSE`, `IssAuthoritative=TRUE` | `t_issforge` → `did:k1` (signed by `kAdv`) | `INV_TokenBindsGenuineDid` | VIOLATED |

Each RED isolates one missing defense and is caught by an invariant defined from
ground truth, not from the mechanism — so the GREEN pass is not vacuous and each
check is shown load-bearing.

## Vacuity self-check

A probe invariant `result["t_honest"] # "did:ed"` run against the GREEN constants
is **refuted** by TLC (counterexample: `t_honest |-> "did:ed"`), proving the
honest token actually traverses an accept path under GREEN rather than the whole
state space being rejections. Without this, GREEN's invariants could hold
vacuously.

## Run / verify

```bash
cd proofs/tla
./tools/tlc -metadir states/jwt_Green            -config MC_Jwt_Green.cfg            MC_Jwt_Green.tla
./tools/tlc -metadir states/jwt_Red_NoAlgBinding -config MC_Jwt_Red_NoAlgBinding.cfg MC_Jwt_Red_NoAlgBinding.tla
./tools/tlc -metadir states/jwt_Red_NoSig        -config MC_Jwt_Red_NoSig.cfg        MC_Jwt_Red_NoSig.tla
./tools/tlc -metadir states/jwt_Red_NoIssBinding -config MC_Jwt_Red_NoIssBinding.cfg MC_Jwt_Red_NoIssBinding.tla
```

Expected: GREEN reports "No error has been found"; each RED reports
"Invariant INV_TokenBindsGenuineDid is violated" with the counterexample token in
the table.

## Boundaries

- **Crypto unforgeability assumed** (the modeled boundary): a signature record's
  `(SigBy, SigAlg)` is ground truth; the adversary cannot forge it. Real
  EdDSA/ECDSA security is assumed, not modeled.
- **Cross-curve byte reuse abstracted:** `sub` bytes read under a foreign curve
  are modeled as resolving to a disjoint sentinel DID (`did:crosscurve`) under
  which no signature can verify. This captures "distinct multicodec prefix ⇒
  distinct DID" and "bytes of key A on curve B are not a key the adversary signs
  for"; it does not model the (cryptographically negligible) chance of a literal
  valid cross-curve point collision.
- **Bounded:** 4 keys (one per curve + an adversary Ed25519 key), 5 tokens — the
  minimal witnessing shapes for alg-confusion, signature forgery, and issuer
  forgery. Conclusions are structural (which claim each check trusts), not
  quantity-sensitive.
- **Scope:** temporal validity (nbf/exp/skew) and audience matching are a separate
  lower-priority slice (`survey/identity.md`); this slice covers only the
  identity-binding half of `from_token`.
```
