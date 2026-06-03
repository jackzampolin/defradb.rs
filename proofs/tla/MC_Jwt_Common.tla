---- MODULE MC_Jwt_Common ----
\* Shared scenario data for the Jwt slice: three curves, a victim key per curve,
\* an adversary-controlled Ed25519 key, and a battery of attack tokens. Each MC
\* wrapper EXTENDS this and toggles the mechanism checks via its .cfg.
EXTENDS Jwt

\* Keys: one victim per curve, plus the adversary's own Ed25519 key.
mcKeys == {"kEd", "kK1", "kR1", "kAdv"}

mcAlgs == {"EdDSA", "ES256K", "ES256"}

mcKeyCurve ==
  [k \in mcKeys |->
    CASE k = "kEd"  -> "EdDSA"
      [] k = "kK1"  -> "ES256K"
      [] k = "kR1"  -> "ES256"
      [] k = "kAdv" -> "EdDSA"
      [] OTHER      -> "EdDSA"]

\* Ground-truth dids. kAdv is the adversary's genuine identity, distinct from the
\* victims'. None equals the "did:crosscurve" sentinel ReconDid emits on mismatch.
mcDidOf ==
  [k \in mcKeys |->
    CASE k = "kEd"  -> "did:ed"
      [] k = "kK1"  -> "did:k1"
      [] k = "kR1"  -> "did:r1"
      [] k = "kAdv" -> "did:adv"
      [] OTHER      -> "did:ed"]

\* ---- Tokens ---------------------------------------------------------------
\* t_honest      : a correct Ed25519 token for the victim kEd.
\* t_algconf     : algorithm-confusion shape. Header alg EdDSA so the signature is
\*                 verified under the EdDSA decode path against the adversary's own
\*                 key kAdv (sig genuinely by kAdv/EdDSA -> SigVerifies holds). The
\*                 kt_claim is ES256K, disagreeing with the header. A build that
\*                 derives identity from the kt_claim curve (IdentityCurveFromClaim)
\*                 reads kAdv's bytes under ES256K, yielding a did that owns to no
\*                 real key (the cross-curve sentinel). iss is set to that sentinel
\*                 so even iss-binding passes -- only the alg-binding check stops the
\*                 token from being accepted as a garbage/cross-curve DID.
\* t_subswap     : adversary presents victim kK1's bytes in sub with header ES256K
\*                 and iss=did(kK1), but the signature is by kAdv (forgery attempt).
\*                 SigVerifies fails -> only CheckSig catches it. Probes that the
\*                 signature gate has teeth.
\* t_issforge    : adversary genuinely signs with own kAdv (EdDSA), sub=kAdv,
\*                 header EdDSA, kt EdDSA (alg-binding OK, sig OK) but lies iss =
\*                 did(kK1). Only iss binding catches it -> would impersonate kK1.
\* t_crosscurve  : header ES256 (so verifier reads sub under P-256) but sub bytes
\*                 are the Ed25519 victim kEd; kt claim ES256, iss = did:crosscurve.
\*                 Models bytes-of-one-curve parsed as another: recon did is the
\*                 sentinel, sig cannot verify. A buggy iss-trusting build that
\*                 returned iss verbatim would mint a junk DID.
mcTokens ==
  {"t_honest", "t_algconf", "t_subswap", "t_issforge", "t_crosscurve"}

mcHeaderAlg ==
  [t \in mcTokens |->
    CASE t = "t_honest"     -> "EdDSA"
      [] t = "t_algconf"    -> "EdDSA"
      [] t = "t_subswap"    -> "ES256K"
      [] t = "t_issforge"   -> "EdDSA"
      [] t = "t_crosscurve" -> "ES256"
      [] OTHER              -> "EdDSA"]

mcKtClaim ==
  [t \in mcTokens |->
    CASE t = "t_honest"     -> "EdDSA"
      [] t = "t_algconf"    -> "ES256K"   \* mismatch vs header EdDSA
      [] t = "t_subswap"    -> "ES256K"
      [] t = "t_issforge"   -> "EdDSA"
      [] t = "t_crosscurve" -> "ES256"
      [] OTHER              -> "EdDSA"]

mcSubKey ==
  [t \in mcTokens |->
    CASE t = "t_honest"     -> "kEd"
      [] t = "t_algconf"    -> "kAdv"   \* adversary's own key bytes
      [] t = "t_subswap"    -> "kK1"    \* victim's key bytes
      [] t = "t_issforge"   -> "kAdv"
      [] t = "t_crosscurve" -> "kEd"    \* victim Ed bytes read under ES256 path
      [] OTHER              -> "kEd"]

mcIssClaim ==
  [t \in mcTokens |->
    CASE t = "t_honest"     -> "did:ed"
      [] t = "t_algconf"    -> "did:crosscurve" \* matches sentinel so iss-binding passes
      [] t = "t_subswap"    -> "did:k1"
      [] t = "t_issforge"   -> "did:k1"        \* lies about issuer
      [] t = "t_crosscurve" -> "did:crosscurve"
      [] OTHER              -> "did:ed"]

mcSigBy ==
  [t \in mcTokens |->
    CASE t = "t_honest"     -> "kEd"
      [] t = "t_algconf"    -> "kAdv"   \* genuinely signed by adversary's key
      [] t = "t_subswap"    -> "kAdv"   \* forgery: not signed by kK1
      [] t = "t_issforge"   -> "kAdv"
      [] t = "t_crosscurve" -> "kEd"
      [] OTHER              -> "kEd"]

mcSigAlg ==
  [t \in mcTokens |->
    CASE t = "t_honest"     -> "EdDSA"
      [] t = "t_algconf"    -> "EdDSA"
      [] t = "t_subswap"    -> "EdDSA"  \* adversary can only sign with own EdDSA key
      [] t = "t_issforge"   -> "EdDSA"
      [] t = "t_crosscurve" -> "EdDSA"
      [] OTHER              -> "EdDSA"]
====
