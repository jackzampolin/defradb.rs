---- MODULE Acp ----
\* ACP tuple replication and revocation-consistency model.
\*
\* The authoritative tuple set represents SourceHub/on-chain ACP or the owner
\* node's local tuple store.  Each node also has a replicated local tuple view
\* and, optionally, a positive access-decision cache.  The property of interest
\* is intentionally post-propagation: once a revocation has reached a node, that
\* node must no longer grant the revoked tuple from either local state or cache.
EXTENDS FiniteSets

CONSTANTS
  Nodes,
  Tuples,
  InitialAuthority,
  InitiallyKnown,
  InitialCache,
  Grantable,
  Revocable,
  NetworkConnected,
  EnforcementMode

Modes == {"CurrentStore", "CacheInvalidated", "StaleCache", "StaleStore"}
CacheModes == {"CacheInvalidated", "StaleCache"}
NodeTuples == Nodes \X Tuples

ASSUME Nodes # {}
ASSUME Tuples # {}
ASSUME InitialAuthority \subseteq Tuples
ASSUME InitiallyKnown \in [Nodes -> SUBSET Tuples]
ASSUME InitialCache \in [Nodes -> SUBSET Tuples]
ASSUME Grantable \subseteq Tuples
ASSUME Revocable \subseteq Tuples
ASSUME Revocable \subseteq InitialAuthority
ASSUME NetworkConnected \in BOOLEAN
ASSUME EnforcementMode \in Modes

VARIABLES
  authority,
  known,
  cache,
  revoked,
  seenRevoke,
  checked

vars == <<authority, known, cache, revoked, seenRevoke, checked>>

TypeOK ==
  /\ authority \subseteq Tuples
  /\ known \in [Nodes -> SUBSET Tuples]
  /\ cache \in [Nodes -> SUBSET Tuples]
  /\ revoked \subseteq Tuples
  /\ seenRevoke \in [Nodes -> SUBSET Tuples]
  /\ checked \subseteq NodeTuples

Init ==
  /\ authority = InitialAuthority
  /\ known = InitiallyKnown
  /\ cache = InitialCache
  /\ revoked = {}
  /\ seenRevoke = [n \in Nodes |-> {}]
  /\ checked = {}

UsesCache == EnforcementMode \in CacheModes

CheckAllowed(n, t) ==
  \/ t \in known[n]
  \/ /\ UsesCache
     /\ t \in cache[n]

Grant(t) ==
  /\ t \in Grantable
  /\ t \notin authority
  /\ t \notin revoked
  /\ authority' = authority \cup {t}
  /\ UNCHANGED <<known, cache, revoked, seenRevoke, checked>>

Revoke(t) ==
  /\ t \in Revocable
  /\ t \in authority
  /\ authority' = authority \ {t}
  /\ revoked' = revoked \cup {t}
  /\ UNCHANGED <<known, cache, seenRevoke, checked>>

ReplicateGrant(n, t) ==
  /\ NetworkConnected
  /\ n \in Nodes
  /\ t \in authority
  /\ t \notin known[n]
  /\ known' = [known EXCEPT ![n] = @ \cup {t}]
  /\ UNCHANGED <<authority, cache, revoked, seenRevoke, checked>>

ReplicateRevoke(n, t) ==
  /\ NetworkConnected
  /\ n \in Nodes
  /\ t \in revoked
  /\ t \notin seenRevoke[n]
  /\ seenRevoke' = [seenRevoke EXCEPT ![n] = @ \cup {t}]
  /\ known' =
      [known EXCEPT ![n] =
        IF EnforcementMode = "StaleStore" THEN @ ELSE @ \ {t}]
  /\ cache' =
      [cache EXCEPT ![n] =
        IF EnforcementMode = "CacheInvalidated" THEN @ \ {t} ELSE @]
  /\ UNCHANGED <<authority, revoked, checked>>

Check(n, t) ==
  /\ n \in Nodes
  /\ t \in Tuples
  /\ CheckAllowed(n, t)
  /\ checked' = checked \cup {<<n, t>>}
  /\ cache' =
      IF UsesCache
      THEN [cache EXCEPT ![n] = @ \cup {t}]
      ELSE cache
  /\ UNCHANGED <<authority, known, revoked, seenRevoke>>

Next ==
  \/ \E t \in Tuples : Grant(t)
  \/ \E t \in Tuples : Revoke(t)
  \/ \E n \in Nodes, t \in Tuples : ReplicateGrant(n, t)
  \/ \E n \in Nodes, t \in Tuples : ReplicateRevoke(n, t)
  \/ \E n \in Nodes, t \in Tuples : Check(n, t)

Fairness ==
  /\ \A t \in Revocable : WF_vars(Revoke(t))
  /\ \A n \in Nodes, t \in Tuples : WF_vars(ReplicateGrant(n, t))
  /\ \A n \in Nodes, t \in Tuples : WF_vars(ReplicateRevoke(n, t))

Spec == Init /\ [][Next]_vars /\ Fairness

\* Safety: after a revoke has propagated to a node, the node cannot still grant
\* the revoked tuple from local replicated state or a cached positive decision.
INV_RevocationConsistent ==
  \A n \in Nodes, t \in Tuples :
    t \in revoked /\ t \in seenRevoke[n] => ~CheckAllowed(n, t)

INV_RevokedNotAuthoritative == revoked \cap authority = {}

\* Liveness under eventual connectivity: every tuple selected for this scenario
\* is eventually revoked, every node eventually observes that revocation, and the
\* revocation remains enforced everywhere.
PROP_RevocationEventuallyEnforced ==
  <>[](\A t \in Revocable :
        /\ t \in revoked
        /\ \A n \in Nodes :
             /\ t \in seenRevoke[n]
             /\ ~CheckAllowed(n, t))
====
