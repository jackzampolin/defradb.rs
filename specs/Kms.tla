---- MODULE Kms ----
\* Request-driven KMS key distribution over pubsub.
\*
\* The model deliberately separates:
\* - request gossip: a node asks for a key by DID;
\* - envelope gossip: any topic peer may receive ciphertext;
\* - usable possession: only a decrypt action can move a key into usable[n].
\*
\* ECIES is abstracted by CryptoMode. The production assumption is
\* CryptoMode = "RecipientOnly": an envelope can be used only by the requester
\* whose ephemeral public key it was encrypted to.
EXTENDS FiniteSets

CONSTANTS
  Nodes,
  DIDs,
  DidOf,
  Keys,
  InitialAuthorized,
  InitiallyHasKey,
  Requesters,
  Grantable,
  Revocable,
  NetworkConnected,
  PolicyMode,       \* "CurrentAuth" | "NoPolicy" | "StaleRequestAuth"
  CryptoMode        \* "RecipientOnly" | "AnyoneCanDecrypt"

NodeKeys == Nodes \X Keys

Request(n, k, snap) == [requester |-> n, key |-> k, snap |-> snap]

AllRequests ==
  { Request(n, k, snap) : n \in Nodes, k \in Keys, snap \in BOOLEAN }

EnvelopeFor(r) == [key |-> r.key, recipient |-> r.requester, request |-> r]

AllEnvelopes == { EnvelopeFor(r) : r \in AllRequests }

VARIABLES
  auth,
  usable,
  activeRequests,
  requestLog,
  envelopes,
  received,
  decryptions,
  revokedBeforeResponse,
  replayedAfterRevoke

vars ==
  << auth, usable, activeRequests, requestLog, envelopes, received,
     decryptions, revokedBeforeResponse, replayedAfterRevoke >>

TypeOK ==
  /\ Nodes # {}
  /\ Keys # {}
  /\ DidOf \in [Nodes -> DIDs]
  /\ InitialAuthorized \in [Keys -> SUBSET DIDs]
  /\ InitiallyHasKey \in [Nodes -> SUBSET Keys]
  /\ Requesters \subseteq Nodes
  /\ Grantable \subseteq NodeKeys
  /\ Revocable \subseteq NodeKeys
  /\ NetworkConnected \in BOOLEAN
  /\ PolicyMode \in {"CurrentAuth", "NoPolicy", "StaleRequestAuth"}
  /\ CryptoMode \in {"RecipientOnly", "AnyoneCanDecrypt"}
  /\ auth \in [Keys -> SUBSET DIDs]
  /\ usable \in [Nodes -> SUBSET Keys]
  /\ activeRequests \subseteq AllRequests
  /\ requestLog \subseteq AllRequests
  /\ envelopes \subseteq AllEnvelopes
  /\ received \in [Nodes -> SUBSET AllEnvelopes]
  /\ decryptions \subseteq (Nodes \X AllEnvelopes)
  /\ revokedBeforeResponse \subseteq NodeKeys
  /\ replayedAfterRevoke \subseteq AllRequests

Init ==
  /\ auth = InitialAuthorized
  /\ usable = InitiallyHasKey
  /\ activeRequests = {}
  /\ requestLog = {}
  /\ envelopes = {}
  /\ received = [n \in Nodes |-> {}]
  /\ decryptions = {}
  /\ revokedBeforeResponse = {}
  /\ replayedAfterRevoke = {}

Authorized(n, k) == DidOf[n] \in auth[k]

MayRespond(r) ==
  CASE PolicyMode = "NoPolicy"          -> TRUE
    [] PolicyMode = "CurrentAuth"       -> Authorized(r.requester, r.key)
    [] PolicyMode = "StaleRequestAuth"  -> r.snap

CanDecrypt(n, e) ==
  CASE CryptoMode = "RecipientOnly"     -> n = e.recipient
    [] CryptoMode = "AnyoneCanDecrypt"  -> TRUE

NoReleasedTo(n, k) ==
  \A e \in envelopes : ~(e.recipient = n /\ e.key = k)

Grant(n, k) ==
  /\ <<n, k>> \in Grantable
  /\ ~Authorized(n, k)
  /\ auth' = [auth EXCEPT ![k] = @ \cup {DidOf[n]}]
  /\ UNCHANGED << usable, activeRequests, requestLog, envelopes, received,
                  decryptions, revokedBeforeResponse, replayedAfterRevoke >>

\* Revocation after a node has the key, or after a response envelope was already
\* released to it, is out of scope for v1 because there is no key rotation.
Revoke(n, k) ==
  /\ <<n, k>> \in Revocable
  /\ Authorized(n, k)
  /\ k \notin usable[n]
  /\ NoReleasedTo(n, k)
  /\ auth' = [auth EXCEPT ![k] = @ \ {DidOf[n]}]
  /\ revokedBeforeResponse' = revokedBeforeResponse \cup {<<n, k>>}
  /\ UNCHANGED << usable, activeRequests, requestLog, envelopes, received,
                  decryptions, replayedAfterRevoke >>

IssueRequest(n, k) ==
  LET r == Request(n, k, Authorized(n, k)) IN
  /\ n \in Requesters
  /\ k \notin usable[n]
  /\ r \notin activeRequests
  /\ activeRequests' = activeRequests \cup {r}
  /\ requestLog' = requestLog \cup {r}
  /\ UNCHANGED << auth, usable, envelopes, received, decryptions,
                  revokedBeforeResponse, replayedAfterRevoke >>

ReplayOldRequest(r) ==
  /\ r \in requestLog
  /\ r.snap
  /\ ~Authorized(r.requester, r.key)
  /\ r.key \notin usable[r.requester]
  /\ r \notin replayedAfterRevoke
  /\ activeRequests' = activeRequests \cup {r}
  /\ replayedAfterRevoke' = replayedAfterRevoke \cup {r}
  /\ UNCHANGED << auth, usable, requestLog, envelopes, received,
                  decryptions, revokedBeforeResponse >>

DenyRequest(r) ==
  /\ r \in activeRequests
  /\ ~MayRespond(r)
  /\ activeRequests' = activeRequests \ {r}
  /\ UNCHANGED << auth, usable, requestLog, envelopes, received, decryptions,
                  revokedBeforeResponse, replayedAfterRevoke >>

Respond(a, r) ==
  LET e == EnvelopeFor(r) IN
  /\ r \in activeRequests
  /\ a \in Nodes
  /\ r.key \in usable[a]
  /\ MayRespond(r)
  /\ e \notin envelopes
  /\ envelopes' = envelopes \cup {e}
  /\ activeRequests' = activeRequests \ {r}
  /\ UNCHANGED << auth, usable, requestLog, received, decryptions,
                  revokedBeforeResponse, replayedAfterRevoke >>

ReceiveEnvelope(n, e) ==
  /\ NetworkConnected
  /\ n \in Nodes
  /\ e \in envelopes
  /\ e \notin received[n]
  /\ received' = [received EXCEPT ![n] = @ \cup {e}]
  /\ UNCHANGED << auth, usable, activeRequests, requestLog, envelopes,
                  decryptions, revokedBeforeResponse, replayedAfterRevoke >>

Decrypt(n, e) ==
  /\ n \in Nodes
  /\ e \in received[n]
  /\ e.key \notin usable[n]
  /\ CanDecrypt(n, e)
  /\ usable' = [usable EXCEPT ![n] = @ \cup {e.key}]
  /\ decryptions' = decryptions \cup {<<n, e>>}
  /\ UNCHANGED << auth, activeRequests, requestLog, envelopes, received,
                  revokedBeforeResponse, replayedAfterRevoke >>

Next ==
  \/ \E n \in Nodes, k \in Keys : Grant(n, k)
  \/ \E n \in Nodes, k \in Keys : Revoke(n, k)
  \/ \E n \in Nodes, k \in Keys : IssueRequest(n, k)
  \/ \E r \in AllRequests : ReplayOldRequest(r)
  \/ \E r \in AllRequests : DenyRequest(r)
  \/ \E a \in Nodes, r \in AllRequests : Respond(a, r)
  \/ \E n \in Nodes, e \in AllEnvelopes : ReceiveEnvelope(n, e)
  \/ \E n \in Nodes, e \in AllEnvelopes : Decrypt(n, e)

Fairness ==
  /\ \A n \in Nodes, k \in Keys : WF_vars(IssueRequest(n, k))
  /\ \A r \in AllRequests : WF_vars(ReplayOldRequest(r))
  /\ \A r \in AllRequests : WF_vars(DenyRequest(r))
  /\ \A a \in Nodes, r \in AllRequests : WF_vars(Respond(a, r))
  /\ \A n \in Nodes, e \in AllEnvelopes : WF_vars(ReceiveEnvelope(n, e))
  /\ \A n \in Nodes, e \in AllEnvelopes : WF_vars(Decrypt(n, e))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Properties ----

\* Safety: a node with a usable plaintext key must be currently authorized.
\* Configs that model revocation exclude "revoked after already holding" by the
\* Revoke guard above; key rotation is not part of v1.
INV_OnlyAuthorizedHasKey ==
  \A n \in Nodes, k \in Keys : k \in usable[n] => Authorized(n, k)

\* Safety: the ECIES decryptability abstraction is recipient-bound.
INV_OnlyIntendedRecipientDecrypts ==
  \A d \in decryptions : d[1] = d[2].recipient

\* Safety: if revocation happened before any response envelope was released to
\* the node, that node never obtains the key.
INV_RevokedCannotObtain ==
  \A nk \in revokedBeforeResponse : nk[2] \notin usable[nk[1]]

\* Safety: replaying a request that was created while authorized must not yield
\* the key after current authorization has been revoked.
INV_NoReplayGrant ==
  \A r \in replayedAfterRevoke : r.key \notin usable[r.requester]

\* Liveness under eventual connectivity/fair pubsub delivery: every currently
\* authorized node eventually reaches a stable state where it holds every key it
\* is authorized for.
INV_AuthorizedEventuallyHasKey ==
  <>[](\A n \in Nodes, k \in Keys : Authorized(n, k) => k \in usable[n])
====
