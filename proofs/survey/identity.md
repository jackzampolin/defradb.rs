# Survey: `crates/identity/`

## Purpose

Identity management for DefraDB nodes: the `Identity`/`FullIdentity` traits,
`RawIdentity` (Ed25519 / secp256k1 / secp256r1 backed), the validated `Did`
newtype (did:key), `IdentityContext` propagation, and JWT bearer-token
generation + verification (`new_token`, `from_token`, `verify_auth_token`).
It is the layer that turns a signed bearer token into an authenticated actor
DID — the input the HTTP/NAC auth gate (Auth slice) consumes.

## State machines

- **Token verification (implicit security state machine).** `from_token`
  reconstructs the pubkey from `sub`, derives a DID, and rejects unless
  (a) the JWT header alg matches the `key_type` claim, (b) the signature
  verifies, and (c) `iss` equals the DID derived from the pubkey (issuer
  binding). `verify_auth_token_with_skew` then gates on nbf/exp with a 60s
  clock-skew window and an audience match. States:
  unverified → {rejected | identity} → {expired/not-yet-valid/aud-mismatch | ok}.
- No explicit lifecycle enums; `RawIdentity`, `Did`, `IdentityContext` are
  immutable value types / glue.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| JWT issuer binding | Lean/TLA+ | A token only validates as DID `d` if `iss == did(sub-pubkey) == d` and header-alg == key_type; a token signed by key A can never authenticate as DID B (no key/DID confusion across the 3 curves) | partial — Auth slice assumes "invalid/expired tokens do not become identities"; the binding/alg-confusion logic itself is unmodeled | medium |
| Token temporal validity | TLA+ | With clock skew S, accept iff `nbf <= now+S` and `exp+S >= now`; under bounded clock divergence between issuer and verifier no expired token is accepted and no fresh token is spuriously rejected | partial — Auth slice abstracts expiry as a boolean; the skew-boundary arithmetic is unmodeled | low |
| DID derivation determinism | Lean | `did()` is a pure deterministic function of the public key; equal pubkeys ⇒ equal DIDs (content-addressing of identity) | no | low |

## Verdict

**Mostly plumbing, one genuinely security-relevant gap.** `RawIdentity`, `Did`,
`IdentityContext`, key-type dispatch, and DER/JWT encoding are glue covered by
the crate's own go-compat + property tests. The one property worth a small
formal slice is **JWT issuer binding / algorithm-confusion resistance**: the
Auth TLA+ slice *assumes* token verification is sound but does not model it.
A focused Lean lemma (issuer == did(sub) ∧ alg == key_type ⇒ no cross-key/DID
impersonation) would discharge that assumption. Temporal/skew and DID
determinism are lower-value. Verdict: model_worthy = true, but narrowly.
