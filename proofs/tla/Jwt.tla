---- MODULE Jwt ----
\* JWT issuer-binding / algorithm-confusion resistance for crates/identity from_token.
\*
\* Models the security state machine of identity::token::from_token: a presented
\* JWT is turned into an authenticated actor DID ONLY IF
\*   (1) the header alg matches the key_type claim (alg<->key binding), AND
\*   (2) the signature verifies under the public key reconstructed from `sub`
\*       interpreted under the curve selected by the decode path, AND
\*   (3) iss == did(reconstructed pubkey)  (issuer binding).
\*
\* The crypto layer is the assumed boundary (unforgeability): a signature record
\* carries the TRUE key that produced it (sig.signedBy) and the TRUE algorithm
\* (sig.sigAlg). Verification succeeds iff the verifier's (pubkey, alg) exactly
\* match those ground-truth fields. The adversary may forge every CLAIM field of
\* a token (header alg, key_type claim, sub, iss) but cannot forge a signature.
\*
\* Anchors (Rust, this tree):
\*   crates/identity/src/token/mod.rs:263-333  from_token (alg-match + iss-bind)
\*   crates/identity/src/token/mod.rs:284-294  expected_alg from key_type; header!=expected => reject
\*   crates/identity/src/token/mod.rs:296-323  reconstruct pubkey from sub; iss == did(pubkey)
\*   crates/identity/src/token/decoding.rs:61-109  verify_signature under curve chosen by header alg
\*   crates/identity/src/key_type.rs:19-26  three curves: Ed25519 / Secp256k1 / Secp256r1
\* Anchors (Go, origin/develop):
\*   acp/identity/factory.go:57-90  FromToken: jwt.Parse(WithVerify(false)); NO sig verify,
\*                                  NO alg<->key check, NO iss<->did check at construction
\*   acp/identity/identity_impl.go:188-210  VerifyAuthToken: jws.Verify keyed by PublicKey().Type()
\*
\* This module is mechanism-parameterized so the buggy variants Go exhibits (and
\* hypothetical alg-confusion bugs) can be exercised as RED configs against an
\* INDEPENDENT ground-truth oracle invariant.

EXTENDS FiniteSets, TLC

CONSTANTS
  Keys,            \* abstract key handles, e.g. {"kEd", "kK1", "kR1", "kEd2"}
  Algs,            \* {"EdDSA", "ES256K", "ES256"}
  KeyCurve,        \* [Keys -> Algs]: the real curve each key belongs to
  DidOf,           \* [Keys -> STRING]: ground-truth did:key of each key
  Tokens,          \* abstract token handles presented to from_token
  HeaderAlg,       \* [Tokens -> Algs]: adversary-chosen header "alg"
  KtClaim,         \* [Tokens -> Algs]: adversary-chosen key_type claim
  SubKey,          \* [Tokens -> Keys]: which key's bytes are in `sub`
  IssClaim,        \* [Tokens -> STRING]: adversary-chosen iss
  SigBy,           \* [Tokens -> Keys]: the key that ACTUALLY signed (ground truth)
  SigAlg,          \* [Tokens -> Algs]: the alg ACTUALLY used to sign (ground truth)
  \* mechanism toggles (TRUE = correct/hardened behavior present):
  CheckAlgBinding, \* enforce header_alg == expected_alg(key_type claim)
  CheckSig,        \* enforce signature verification
  CheckIssBinding, \* enforce iss == did(reconstructed pubkey)
  IssAuthoritative,    \* TRUE = buggy build that returns the DID from the iss
                       \* CLAIM instead of the reconstructed/verified pubkey. Rust
                       \* derives the DID from the verified pubkey (mod.rs:308-312),
                       \* so the hardened build sets this FALSE.
  IdentityCurveFromClaim \* TRUE = buggy alg-confusion build that derives the actor
                       \* DID under the kt_claim curve while verifying under the
                       \* header alg. Hardened build derives under the verify curve.

ASSUME Algs = {"EdDSA", "ES256K", "ES256"}
ASSUME KeyCurve \in [Keys -> Algs]
ASSUME DidOf \in [Keys -> STRING]
ASSUME HeaderAlg \in [Tokens -> Algs]
ASSUME KtClaim \in [Tokens -> Algs]
ASSUME SubKey \in [Tokens -> Keys]
ASSUME IssClaim \in [Tokens -> STRING]
ASSUME SigBy \in [Tokens -> Keys]
ASSUME SigAlg \in [Tokens -> Algs]
ASSUME CheckAlgBinding \in BOOLEAN
ASSUME CheckSig \in BOOLEAN
ASSUME CheckIssBinding \in BOOLEAN
ASSUME IssAuthoritative \in BOOLEAN
ASSUME IdentityCurveFromClaim \in BOOLEAN

\* Possible returned values: a real key did, the cross-curve sentinel, or any
\* iss-claim string (reachable only under a buggy IssAuthoritative build).
Outcomes ==
  {"pending", "rejected", "did:crosscurve"}
    \cup {DidOf[k] : k \in Keys}
    \cup {IssClaim[t] : t \in Tokens}

VARIABLES
  result   \* [Tokens -> Outcomes]: "pending" until processed, then "rejected" or the DID

vars == <<result>>

TypeOK ==
  /\ result \in [Tokens -> Outcomes]

Init ==
  result = [t \in Tokens |-> "pending"]

----------------------------------------------------------------------------
\* Mechanism, faithful to from_token's structure.
\*
\* The decode path selects the verification curve from the HEADER alg
\* (decoding.rs dispatch on header_alg -> decode_ed25519 / _secp256k1 / _secp256r1)
\* and reconstructs the pubkey from `sub` under that curve, deriving the actor DID
\* from it. So in the hardened build BOTH the verification curve AND the identity
\* curve are HeaderAlg. The alg-binding check (header == expected_alg(kt_claim))
\* additionally forces the kt_claim to agree. We model the algorithm-confusion bug
\* class as IdentityCurveFromClaim: a build that derives the actor identity (the
\* DID) under the curve named by the kt_claim while the signature is verified under
\* the header alg -- so an attacker can present a low-effort header alg they can
\* sign for, yet be minted as an identity on a different (claimed) curve.

\* Curve used to VERIFY the signature: always the header alg's decode path.
VerifyCurve(t) == HeaderAlg[t]

\* Curve used to derive the actor IDENTITY (DID) from `sub`. Hardened build: the
\* verify curve. Buggy alg-confusion build: the kt_claim curve.
IdentityCurve(t) ==
  IF IdentityCurveFromClaim THEN KtClaim[t] ELSE VerifyCurve(t)

\* did derived from the reconstructed pubkey. did is a pure function of
\* (curve, key-bytes). When the identity curve matches the sub key's real curve,
\* did == DidOf[SubKey[t]]. Otherwise the bytes parse under a different curve and
\* yield a did with a distinct multicodec prefix that equals no real key's did,
\* modeled by the disjoint sentinel "did:crosscurve".
ReconDid(t) ==
  IF IdentityCurve(t) = KeyCurve[SubKey[t]]
  THEN DidOf[SubKey[t]]
  ELSE "did:crosscurve"

\* Signature verification under (pubkey reconstructed on the verify curve, header
\* alg). Per the crypto unforgeability boundary it holds iff the token was truly
\* signed by exactly the key whose bytes are in `sub`, read on the verify curve,
\* using exactly the header alg. Bytes of one key read on a foreign curve are not
\* a key the adversary can sign for, so verification fails there.
SigVerifies(t) ==
  /\ VerifyCurve(t) = KeyCurve[SubKey[t]]   \* sub bytes form a real key on this curve
  /\ SigBy[t] = SubKey[t]
  /\ SigAlg[t] = HeaderAlg[t]

\* expected_alg from the key_type CLAIM (mod.rs:284-288).
ExpectedAlg(t) == KtClaim[t]

AlgBindingOk(t) == (~CheckAlgBinding) \/ (HeaderAlg[t] = ExpectedAlg(t))
SigOk(t)        == (~CheckSig)        \/ SigVerifies(t)
IssBindingOk(t) == (~CheckIssBinding) \/ (IssClaim[t] = ReconDid(t))

Accepts(t) == AlgBindingOk(t) /\ SigOk(t) /\ IssBindingOk(t)

\* On accept, the hardened build returns DID = did(reconstructed/verified pubkey)
\* (mod.rs:308-312). A buggy IssAuthoritative build returns the iss CLAIM verbatim.
ResolvedDid(t) ==
  IF IssAuthoritative THEN IssClaim[t] ELSE ReconDid(t)

----------------------------------------------------------------------------
Process(t) ==
  /\ result[t] = "pending"
  /\ result' = [result EXCEPT ![t] =
       IF Accepts(t) THEN ResolvedDid(t) ELSE "rejected"]

Next == \E t \in Tokens : Process(t)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* INDEPENDENT ORACLE. Defined purely from ground truth (SigBy, SigAlg, KeyCurve,
\* DidOf), NOT from the mechanism's Accepts/ResolvedDid decision. A DID d is
\* "genuinely authenticatable" by token t iff t carries a real signature produced
\* by the key whose did is d, using that key's real curve over this very token.
GenuineForDid(t, d) ==
  \E k \in Keys :
    /\ DidOf[k] = d
    /\ SigBy[t] = k
    /\ SigAlg[t] = KeyCurve[k]

\* CORE PROPERTY: from_token yields DID d for token t only when a genuine
\* signature by the key resolving to d backs that token. No alg/key/iss confusion
\* can manufacture a DID the adversary cannot genuinely sign for.
INV_TokenBindsGenuineDid ==
  \A t \in Tokens :
    (result[t] \notin {"pending", "rejected"}) =>
        GenuineForDid(t, result[t])

\* A returned DID must be a real key's did (never the cross-curve sentinel or junk).
INV_ResolvedDidIsReal ==
  \A t \in Tokens :
    (result[t] \notin {"pending", "rejected"}) =>
        \E k \in Keys : DidOf[k] = result[t]

\* Issuer binding, stated independently of the iss CLAIM: the DID actually
\* returned must equal the did of the key that genuinely signed the token.
INV_ReturnedDidIsSignerDid ==
  \A t \in Tokens :
    (result[t] \notin {"pending", "rejected"}) =>
        result[t] = DidOf[SigBy[t]]
====
