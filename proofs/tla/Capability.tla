---- MODULE Capability ----
\* Explicit-replay capability gate: authorizer-signed, peer+collection-bound,
\* TTL-capped, revocable tokens that authorize live encrypted-replay of one
\* collection from one source peer to one target peer.
\*
\* Anchored in crates/p2p/src/explicit_replay.rs:
\*   - ExplicitReplayCapabilityClaims {source_peer_id, target_peer_id,
\*     collection_id, authorizer_did, expires_at}              (l.34-43)
\*   - generate_capability: caps lifetime at MAX_CAPABILITY_TTL  (l.194-223, 201)
\*   - validate_expiration_cap                                   (l.107-117)
\*   - validate_claims: source/target/collection binding +
\*     expires_at >= now + (expires_at - now <= MAX_CAPABILITY_TTL) (l.128-192)
\*   - verify_capability_with_revocations: validate_claims, then
\*     signature verify against authorizer_did, then deny-list   (l.268-309)
\*   - ExplicitReplayRevocationRegistry deny-list by digest      (l.61-86)
\*
\* Crypto is the ASSUMED boundary, exactly like Kms.tla's ECIES abstraction:
\* a token's signature "verifies" against authorizer_did iff the token was
\* actually issued by that authorizer -- UNLESS that issuer key is compromised
\* (ForgeMode). We do not model ed25519; we model what unforgeability buys.
\*
\* The four Mode constants are the red/green dials. The production code is the
\* tuple (ForgeMode=Unforgeable, TtlMode=CapAtVerify, RevokeMode=DenyRevoked,
\* BindMode=BindTargetCollection). Each weakening is a real bug shape that a
\* nearby (mis)implementation could ship; each has a RED config below.
EXTENDS FiniteSets, Naturals

CONSTANTS
  Peers,            \* transport peer ids (source/target)
  Collections,      \* collection ids
  Issuers,          \* authorizer DIDs that can mint tokens
  Tokens,           \* finite universe of distinct token identities
  ClaimSrc,         \* Tokens -> Peers   (claimed source_peer_id)
  ClaimTgt,         \* Tokens -> Peers   (claimed target_peer_id)
  ClaimColl,        \* Tokens -> Collections
  ClaimIssuer,      \* Tokens -> Issuers (claimed authorizer_did)
  ClaimExpiry,      \* Tokens -> Nat     (claimed expires_at)
  MaxTtl,           \* MAX_CAPABILITY_TTL (seconds)
  MaxClock,         \* bound on the wall clock to keep the state space finite
  HonestIssuers,    \* Issuers whose signing key is NOT compromised
  ForgeMode,        \* "Unforgeable" | "IssuerKeyCompromised" | "FullyForgeable"
  TtlMode,          \* "CapAtVerify" | "CapAtIssueOnly"
  RevokeMode,       \* "DenyRevoked" | "IgnoreRevoked"
  BindMode,         \* "BindTargetCollection" | "BindTargetOnly" | "NoBind"
  Attempts          \* the finite set of presentations the adversary may make
                    \* (token + verifier (via,target,collection)); see wrapper.

ForgeModes == {"Unforgeable", "IssuerKeyCompromised", "FullyForgeable"}
TtlModes   == {"CapAtVerify", "CapAtIssueOnly"}
RevokeModes == {"DenyRevoked", "IgnoreRevoked"}
BindModes  == {"BindTargetCollection", "BindTargetOnly", "NoBind"}

ASSUME ClaimSrc \in [Tokens -> Peers]
ASSUME ClaimTgt \in [Tokens -> Peers]
ASSUME ClaimColl \in [Tokens -> Collections]
ASSUME ClaimIssuer \in [Tokens -> Issuers]
ASSUME ClaimExpiry \in [Tokens -> Nat]
ASSUME MaxTtl \in Nat
ASSUME MaxClock \in Nat
ASSUME HonestIssuers \subseteq Issuers
ASSUME ForgeMode \in ForgeModes
ASSUME TtlMode \in TtlModes
ASSUME RevokeMode \in RevokeModes
ASSUME BindMode \in BindModes

\* A presented attempt: a token offered to a verifier that enforces (tgt, coll)
\* for the connected transport sender peer `via`. `via` is the transport-
\* authenticated sender peer id (libp2p/iroh peer id of the open stream), which
\* validate_claims pins to claims.source_peer_id.
Attempt(tok, via, vtgt, vcoll) ==
  [tok |-> tok, via |-> via, vtgt |-> vtgt, vcoll |-> vcoll]

\* The adversary's presentation space. Enumerated explicitly per scenario rather
\* than as the full peer x peer x collection product, to keep the model finite
\* and fast while still covering the witnessing shapes (legit, cross-target,
\* cross-collection, forged, over-cap, revoked). `via` is always the token's
\* claimed source because the transport authenticates the sender peer id, which
\* validate_claims pins to claims.source_peer_id (so a mismatched via never even
\* reaches the binding/TTL checks -- it is a separate, simpler rejection).
AllAttempts == Attempts

ASSUME Attempts \subseteq
  { Attempt(tok, via, vtgt, vcoll) :
      tok \in Tokens, via \in Peers, vtgt \in Peers, vcoll \in Collections }

VARIABLES
  clock,        \* monotone Nat wall clock at the verifier
  issued,       \* set of Tokens actually minted by their claimed honest issuer
  presented,    \* set of Attempts that have reached a verifier
  revoked,      \* verifier-local deny-list (set of Tokens)
  accepted,     \* set of Attempts the gate AUTHORIZED (replay allowed)
  acceptedWhileRevoked  \* attempts accepted whose token was ALREADY revoked

vars == <<clock, issued, presented, revoked, accepted, acceptedWhileRevoked>>

TypeOK ==
  /\ clock \in 0..MaxClock
  /\ issued \subseteq Tokens
  /\ presented \subseteq AllAttempts
  /\ revoked \subseteq Tokens
  /\ accepted \subseteq AllAttempts
  /\ acceptedWhileRevoked \subseteq AllAttempts

Init ==
  /\ clock = 0
  /\ issued = {}
  /\ presented = {}
  /\ revoked = {}
  /\ accepted = {}
  /\ acceptedWhileRevoked = {}

\* --- Generation cap (generate_capability, l.201 / validate_expiration_cap) ---
\* A minted token's claimed expiry honors the cap relative to its mint time.
\* We mint at the current clock, so issuance requires expiry - clock <= MaxTtl.
WithinCapAt(tok, t) == ClaimExpiry[tok] >= t /\ ClaimExpiry[tok] - t <= MaxTtl

\* Crypto unforgeability boundary. "Signature verifies as issuer I" is modeled
\* as: token was honestly issued, OR I's key is compromised (adversary can sign
\* arbitrary claims as I), OR crypto is fully broken.
SignatureVerifies(tok) ==
  CASE ForgeMode = "Unforgeable"          -> tok \in issued
    [] ForgeMode = "IssuerKeyCompromised" ->
         \/ tok \in issued
         \/ ClaimIssuer[tok] \notin HonestIssuers
    [] ForgeMode = "FullyForgeable"       -> TRUE

\* Binding check the verifier applies (validate_claims source/target/collection).
\* `via` is the transport sender; the source binding pins it regardless of mode.
BindOk(a) ==
  LET tok == a.tok IN
  /\ ClaimSrc[tok] = a.via
  /\ CASE BindMode = "BindTargetCollection" ->
            ClaimTgt[tok] = a.vtgt /\ ClaimColl[tok] = a.vcoll
       [] BindMode = "BindTargetOnly" ->
            ClaimTgt[tok] = a.vtgt
       [] BindMode = "NoBind" -> TRUE

\* TTL enforcement at verify time. CapAtVerify mirrors validate_claims l.183-189
\* re-checking the cap; CapAtIssueOnly is the bug where verify trusts the signed
\* expiry without re-bounding it (only the issuer-side cap exists).
TtlOk(a) ==
  LET tok == a.tok IN
  /\ ClaimExpiry[tok] >= clock              \* not expired (l.176)
  /\ CASE TtlMode = "CapAtVerify" ->
            ClaimExpiry[tok] - clock <= MaxTtl   \* l.183-189
       [] TtlMode = "CapAtIssueOnly" -> TRUE

RevokeOk(a) ==
  CASE RevokeMode = "DenyRevoked"   -> a.tok \notin revoked
    [] RevokeMode = "IgnoreRevoked" -> TRUE

\* The full verify_capability_with_revocations decision.
VerifyAccepts(a) ==
  /\ SignatureVerifies(a.tok)
  /\ BindOk(a)
  /\ TtlOk(a)
  /\ RevokeOk(a)

\* --- Actions ---

Tick ==
  /\ clock < MaxClock
  /\ clock' = clock + 1
  /\ UNCHANGED <<issued, presented, revoked, accepted, acceptedWhileRevoked>>

\* Honest mint: claimed issuer is honest, signs over the exact bound claims, and
\* respects the generation-time cap. This is generate_capability succeeding.
IssueToken(tok) ==
  /\ tok \notin issued
  /\ ClaimIssuer[tok] \in HonestIssuers
  /\ WithinCapAt(tok, clock)
  /\ issued' = issued \cup {tok}
  /\ UNCHANGED <<clock, presented, revoked, accepted, acceptedWhileRevoked>>

\* Adversary presents a token to a verifier of its choosing. Presentation is
\* unconstrained -- anyone can put bytes on the wire claiming any source/target/
\* collection; the gate is what must hold. (Mirrors Kms gossip: receipt is free,
\* usability is gated.)
Present(a) ==
  /\ a \in AllAttempts
  /\ a \notin presented
  /\ presented' = presented \cup {a}
  /\ UNCHANGED <<clock, issued, revoked, accepted, acceptedWhileRevoked>>

\* The capability gate. Authorizes replay iff verify accepts. This is the only
\* path into `accepted`.
Verify(a) ==
  /\ a \in presented
  /\ a \notin accepted
  /\ VerifyAccepts(a)
  /\ accepted' = accepted \cup {a}
  /\ acceptedWhileRevoked' =
       IF a.tok \in revoked
       THEN acceptedWhileRevoked \cup {a}
       ELSE acceptedWhileRevoked
  /\ UNCHANGED <<clock, issued, presented, revoked>>

\* Verifier-local revocation (revoke_capability inserts the digest).
Revoke(tok) ==
  /\ tok \notin revoked
  /\ revoked' = revoked \cup {tok}
  /\ UNCHANGED <<clock, issued, presented, accepted, acceptedWhileRevoked>>

Next ==
  \/ Tick
  \/ \E tok \in Tokens : IssueToken(tok)
  \/ \E a \in AllAttempts : Present(a)
  \/ \E a \in AllAttempts : Verify(a)
  \/ \E tok \in Tokens : Revoke(tok)

Spec == Init /\ [][Next]_vars

\* ---- Properties ----

\* The "honest meaning" of an accepted attempt: it must trace to a real,
\* honestly-issued token bound to exactly this (source via, target, collection),
\* live, and within the TTL cap. This is the ground truth a correct gate enforces
\* and is INDEPENDENT of the gate's own checks -- so a weakened gate that accepts
\* something else is caught.
LegitFor(a) ==
  LET tok == a.tok IN
  /\ tok \in issued
  /\ ClaimIssuer[tok] \in HonestIssuers
  /\ ClaimSrc[tok] = a.via
  /\ ClaimTgt[tok] = a.vtgt
  /\ ClaimColl[tok] = a.vcoll
  /\ ClaimExpiry[tok] >= clock
  /\ ClaimExpiry[tok] - clock <= MaxTtl

\* INV_OnlyLegitAccepted: every authorized replay is backed by a legitimate,
\* peer+collection-bound, in-TTL, honestly-issued token. Forgery, cross-target /
\* cross-collection replay, and TTL escalation under key compromise all violate.
INV_OnlyLegitAccepted ==
  \A a \in accepted : LegitFor(a)

\* INV_TargetBound: an accepted attempt's target/collection equal the token's.
INV_TargetBound ==
  \A a \in accepted :
    /\ ClaimTgt[a.tok] = a.vtgt
    /\ ClaimColl[a.tok] = a.vcoll

\* INV_TtlCapped: no accepted attempt's remaining lifetime exceeds MaxTtl, even
\* if a compromised issuer signed an over-cap expiry.
INV_TtlCapped ==
  \A a \in accepted : ClaimExpiry[a.tok] - clock <= MaxTtl

\* INV_RevokedNeverAccepted: once a token is on the verifier's deny-list, no
\* attempt for it is ever freshly authorized. `acceptedWhileRevoked` records
\* exactly the attempts accepted while their token was already revoked; a correct
\* gate (RevokeMode = "DenyRevoked") keeps this set empty. This is the honest
\* "revoked => later verifies deny" property, immune to the benign accept-then-
\* revoke ordering (which is allowed: the replay already happened pre-revocation).
INV_RevokedNeverAccepted ==
  acceptedWhileRevoked = {}
====
