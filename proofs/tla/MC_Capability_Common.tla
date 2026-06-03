---- MODULE MC_Capability_Common ----
EXTENDS Capability
\* Shared instance for the explicit-replay capability scenarios.
\*
\* Two peers, two collections, two issuers (one honest, one whose key the
\* adversary may hold), and a curated token universe that witnesses every
\* property: a legit token, a token usable for cross-target/cross-collection
\* replay attempts, an over-cap "eternal" token, and a forgeable token claiming
\* the honest issuer.

mcPeers == {"p1", "p2"}
mcCollections == {"c1", "c2"}
mcIssuers == {"issH", "issX"}
mcHonestIssuers == {"issH"}

mcTokens == {"tGood", "tEternalX", "tForgeX"}

\* tGood    : honest issuer issH, bound src=p1 tgt=p2 coll=c1, expiry 4
\*            (within cap-5 at t0). The one token a correct gate accepts (when
\*            presented via=p1 to verifier (p2,c1)).
\* tEternalX: claims compromised issuer issX, expiry 100 -> over the cap. With a
\*            compromised key the adversary can sign it; the verify-time TTL cap
\*            is what must still reject it.
\* tForgeX  : claims compromised issuer issX, in-TTL, bound to p2/c1, but is
\*            NEVER honestly issued (issX is not an HonestIssuer so IssueToken
\*            cannot mint it). It "verifies" only when issX's key is compromised
\*            or crypto is fully broken -> a forged capability.
mcClaimSrc ==
  [t \in mcTokens |-> "p1"]
mcClaimTgt ==
  [t \in mcTokens |-> "p2"]
mcClaimColl ==
  [t \in mcTokens |-> "c1"]
mcClaimIssuer ==
  [t \in mcTokens |->
     CASE t = "tGood" -> "issH"
       [] OTHER       -> "issX"]
mcClaimExpiry ==
  [t \in mcTokens |->
     CASE t = "tEternalX" -> 100
       [] OTHER           -> 4]

mcMaxTtl == 5
mcMaxClock == 2

\* The adversary's presentation space, enumerated explicitly. `via` is pinned to
\* each token's claimed source (p1); the adversary still chooses the verifier's
\* (target, collection) freely -- including the wrong ones -- to probe binding.
mcAttempts ==
  { Attempt(t, "p1", vtgt, vcoll) :
      t \in mcTokens, vtgt \in mcPeers, vcoll \in mcCollections }
====
