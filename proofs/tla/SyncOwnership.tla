---- MODULE SyncOwnership ----
\* Stage 3 of #1116: PushLog carries one current head as an idempotent hint.
\* The receiver durably registers missing-DAG work before acknowledging it and
\* owns every paced CAR fetch.  The sender keeps only scope markers and
\* rederives current heads when the retry schedule fires.
\*
\* Policy knobs isolate the useful counterexamples:
\*   SenderMode = "MarkerRederive" | "DocOnlyMarkers" | "PayloadLedger"
\*   RegisterMode = "Durable" | "Volatile"
\*   FlightMode = "SingleFlight" | "Duplicate"
\*   AckGuardMode = "HeadCurrent" | "Unguarded"
\*   ServeMode = "DurableDerived" | "VolatileGrant"
\*   ProviderMode = "OriginBound" | "AuthenticatedHop" |
\*                  "OriginUnroutable" | "RelayOnly"
\*   OriginAuthMode = "TransportBound" | "UnsignedClaim"
\*   FetchMode = "RootThenSelective" | "SelectiveMissing" | "RecursiveFirst"
\*   StreamMode = "DrainResponse" | "CancelOnProgress"
\*   PendingMode = "ScopeCurrent" | "EveryRoot"
\*   MergeMode = "SerializedBatch" | "ParallelWriters"
\*
\* A scope is either a document or a branchable collection.  A monotonically
\* increasing version abstracts the current composite/collection head CID.
\* A receiver chooses OriginBound only when the independently authenticated
\* origin is already transport-routable; otherwise it chooses the authenticated
\* connected hop.  The two green ProviderMode configurations check both sides
\* of that runtime selection without permitting an unroutable payload claim.
\* The receiver retry clock is abstracted by ClaimFetch: every possible trigger
\* must pass through that one action, which admits at most one owner per root.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Docs,
  Cols,
  MaxV,
  Cap,
  SenderMode,
  RegisterMode,
  FlightMode,
  AckGuardMode,
  ServeMode,
  ProviderMode,
  OriginAuthMode,
  FetchMode,
  StreamMode,
  PendingMode,
  MergeMode

Scopes == Docs \cup Cols
Roots == Scopes \X (1..MaxV)

ASSUME Docs # {} /\ Cols # {} /\ Docs \cap Cols = {}
ASSUME MaxV \in Nat /\ MaxV >= 1
ASSUME Cap \in Nat /\ Cap >= 1
ASSUME SenderMode \in {"MarkerRederive", "DocOnlyMarkers", "PayloadLedger"}
ASSUME RegisterMode \in {"Durable", "Volatile"}
ASSUME FlightMode \in {"SingleFlight", "Duplicate"}
ASSUME AckGuardMode \in {"HeadCurrent", "Unguarded"}
ASSUME ServeMode \in {"DurableDerived", "VolatileGrant"}
ASSUME ProviderMode \in {"OriginBound", "AuthenticatedHop", "OriginUnroutable", "RelayOnly"}
ASSUME OriginAuthMode \in {"TransportBound", "UnsignedClaim"}
ASSUME FetchMode \in {"RootThenSelective", "SelectiveMissing", "RecursiveFirst"}
ASSUME StreamMode \in {"DrainResponse", "CancelOnProgress"}
ASSUME PendingMode \in {"ScopeCurrent", "EveryRoot"}
ASSUME MergeMode \in {"SerializedBatch", "ParallelWriters"}

VARIABLES
  localV,        \* [Scopes -> 0..MaxV], current sender head
  dirty,         \* SUBSET Scopes, durable marker-only sender state
  payloadLedger, \* SUBSET Roots, forbidden durable CID/payload delivery state
  inflight,      \* SUBSET Roots, announced hints not yet processed
  pending,       \* SUBSET Roots, durable receiver want registrations
  serveAuth,     \* SUBSET Roots, volatile exact-root CAR grants
  serveScopes,   \* SUBSET Scopes, durable/re-established serving policy
  flights,       \* [Roots -> 0..2], paced CAR fetch owners
  drained,       \* SUBSET Roots, owner retained its CAR through completion
  ready,         \* SUBSET Roots, complete DAGs awaiting the merge writer
  mergeFlights,  \* [Roots -> 0..1], independent merge writers
  mergedV,       \* [Scopes -> 0..MaxV], current receiver head
  crashed        \* BOOLEAN, one receiver crash/restart has happened

vars == <<localV, dirty, payloadLedger, inflight, pending, serveAuth, serveScopes,
          flights, drained, ready, mergeFlights, mergedV, crashed>>

TypeOK ==
  /\ localV \in [Scopes -> 0..MaxV]
  /\ dirty \subseteq Scopes
  /\ payloadLedger \subseteq Roots
  /\ inflight \subseteq Roots
  /\ pending \subseteq Roots
  /\ serveAuth \subseteq Roots
  /\ serveScopes \subseteq Scopes
  /\ Cardinality(pending) <= Cap
  /\ flights \in [Roots -> 0..2]
  /\ drained \subseteq Roots
  /\ ready \subseteq Roots
  /\ mergeFlights \in [Roots -> 0..1]
  /\ mergedV \in [Scopes -> 0..MaxV]
  /\ crashed \in BOOLEAN

Init ==
  /\ localV = [s \in Scopes |-> 0]
  /\ dirty = {}
  /\ payloadLedger = {}
  /\ inflight = {}
  /\ pending = {}
  /\ serveAuth = {}
  /\ serveScopes = {}
  /\ flights = [r \in Roots |-> 0]
  /\ drained = {}
  /\ ready = {}
  /\ mergeFlights = [r \in Roots |-> 0]
  /\ mergedV = [s \in Scopes |-> 0]
  /\ crashed = FALSE

RecordMarker(s) ==
  IF SenderMode = "DocOnlyMarkers" /\ s \in Cols
  THEN dirty
  ELSE IF SenderMode = "PayloadLedger"
       THEN dirty
       ELSE dirty \cup {s}

RecordPayload(s, v) ==
  IF SenderMode = "PayloadLedger"
  THEN {r \in payloadLedger : r[1] # s} \cup {<<s, v>>}
  ELSE payloadLedger

ClearOwned(s, v) ==
  IF AckGuardMode = "HeadCurrent" /\ v # localV[s]
  THEN <<dirty, payloadLedger>>
  ELSE <<dirty \ {s}, payloadLedger \ {<<s, v>>}>>

\* A local write durably records the scope before its live hint is exposed.
Update(s) ==
  /\ localV[s] < MaxV
  /\ LET v == localV[s] + 1
         clearedPayload == {r \in payloadLedger : r[1] # s}
     IN /\ localV' = [localV EXCEPT ![s] = v]
        /\ dirty' = RecordMarker(s)
        /\ payloadLedger' =
             IF SenderMode = "PayloadLedger"
             THEN clearedPayload \cup {<<s, v>>}
             ELSE payloadLedger
        /\ inflight' = inflight \cup {<<s, v>>}
        /\ serveAuth' = serveAuth \cup {<<s, v>>}
        /\ serveScopes' = IF ServeMode = "DurableDerived"
                          THEN serveScopes \cup {s}
                          ELSE serveScopes
  /\ UNCHANGED <<pending, flights, drained, ready, mergeFlights, mergedV, crashed>>

\* Marker retry always rederives localV[s].  There is no stored version to
\* choose.  PayloadLedger is modeled separately as the superseded policy.
ReHintMarker(s) ==
  /\ s \in dirty
  /\ localV[s] >= 1
  /\ inflight' = inflight \cup {<<s, localV[s]>>}
  /\ serveAuth' = serveAuth \cup {<<s, localV[s]>>}
  /\ serveScopes' = IF ServeMode = "DurableDerived"
                    THEN serveScopes \cup {s}
                    ELSE serveScopes
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, flights, drained, ready,
                 mergeFlights, mergedV, crashed>>

\* Current main can retry the CID recorded in its durable delivery ledger.
ReHintPayload(s, v) ==
  /\ <<s, v>> \in payloadLedger
  /\ inflight' = inflight \cup {<<s, v>>}
  /\ serveAuth' = serveAuth \cup {<<s, v>>}
  /\ serveScopes' = IF ServeMode = "DurableDerived"
                    THEN serveScopes \cup {s}
                    ELSE serveScopes
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, flights, drained, mergedV, crashed>>

DropHint(s, v) ==
  /\ <<s, v>> \in inflight
  /\ inflight' = inflight \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, serveAuth, serveScopes,
                 flights, drained, ready, mergeFlights, mergedV, crashed>>

\* A success acknowledgement is emitted only after one of these two actions.
ProcessMerged(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] >= v
  /\ LET cleared == ClearOwned(s, v)
     IN /\ dirty' = cleared[1]
        /\ payloadLedger' = cleared[2]
  /\ inflight' = inflight \ {<<s, v>>}
  /\ serveAuth' = serveAuth \ {<<s, v>>}
  /\ UNCHANGED <<localV, pending, serveScopes, flights, drained, ready,
                 mergeFlights, mergedV, crashed>>

ProcessRegister(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] < v
  /\ LET superseded == {r \in pending : r[1] = s /\ r[2] < v}
         retained == IF PendingMode = "ScopeCurrent"
                     THEN pending \ superseded
                     ELSE pending
     IN /\ (<<s, v>> \in retained \/ Cardinality(retained) < Cap)
        /\ pending' = retained \cup {<<s, v>>}
  /\ inflight' = inflight \ {<<s, v>>}
  /\ <<s, v>> \in serveAuth
  /\ LET cleared == ClearOwned(s, v)
     IN /\ dirty' = cleared[1]
        /\ payloadLedger' = cleared[2]
  /\ UNCHANGED <<localV, serveAuth, serveScopes, flights, drained, ready,
                 mergeFlights, mergedV, crashed>>

\* Overflow is an actionable nack: receiver state is unchanged and sender
\* durable ownership is not cleared.
ProcessNack(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] < v
  /\ <<s, v>> \notin pending
  /\ Cardinality(pending) >= Cap
  /\ inflight' = inflight \ {<<s, v>>}
  /\ serveAuth' = serveAuth \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, serveScopes, flights, drained,
                 ready, mergeFlights, mergedV, crashed>>

\* This is the sole abstract fetch-dispatch seam: retry expiry, connect
\* expedite, and partial progress all coalesce through the same root claim.
ClaimFetch(s, v) ==
  /\ <<s, v>> \in pending
  /\ (<<s, v>> \in serveAuth \/ s \in serveScopes)
  /\ ProviderMode \in {"OriginBound", "AuthenticatedHop"}
  \* RootThenSelective first exercises the bounded rooted serving path at the
  \* actual publisher (which may populate a connected relay), then requests
  \* the still-missing exact frontier. SelectiveMissing is retained as a red
  \* topology probe: exact CIDs alone do not identify a routable provider.
  /\ FetchMode = "RootThenSelective"
  /\ flights[<<s, v>>] < (IF FlightMode = "Duplicate" THEN 2 ELSE 1)
  /\ flights' = [flights EXCEPT ![<<s, v>>] = @ + 1]
  /\ drained' = IF StreamMode = "DrainResponse"
                THEN drained \cup {<<s, v>>}
                ELSE drained
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, serveAuth,
                 serveScopes, ready, mergeFlights, mergedV, crashed>>

CompleteFetch(s, v) ==
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ <<s, v>> \in drained
  /\ ready' = ready \cup {<<s, v>>}
  /\ flights' = [flights EXCEPT ![<<s, v>>] = @ - 1]
  /\ drained' = IF flights[<<s, v>>] = 1
                THEN drained \ {<<s, v>>}
                ELSE drained
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, serveAuth,
                 serveScopes, mergeFlights, mergedV, crashed>>

\* All production entrypoints share one merge-writer owner. The model claims
\* one root at a time; a runtime transaction that claims an ordered batch is a
\* stuttering refinement of this serialized sequence. ParallelWriters retains
\* the old frontend-selected policy as a red counterexample.
ClaimMerge(s, v) ==
  /\ <<s, v>> \in ready
  /\ mergeFlights[<<s, v>>] = 0
  /\ Cardinality({r \in Roots : mergeFlights[r] > 0}) <
       (IF MergeMode = "ParallelWriters" THEN 2 ELSE 1)
  /\ mergeFlights' = [mergeFlights EXCEPT ![<<s, v>>] = 1]
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, serveAuth,
                 serveScopes, flights, drained, ready, mergedV, crashed>>

CompleteMerge(s, v) ==
  /\ <<s, v>> \in ready
  /\ mergeFlights[<<s, v>>] = 1
  /\ mergedV' = [mergedV EXCEPT ![s] = IF v > @ THEN v ELSE @]
  /\ pending' = pending \ {<<s, v>>}
  /\ serveAuth' = serveAuth \ {<<s, v>>}
  /\ ready' = ready \ {<<s, v>>}
  /\ mergeFlights' = [mergeFlights EXCEPT ![<<s, v>>] = 0]
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, serveScopes, flights,
                 drained, crashed>>

Crash ==
  /\ ~crashed
  /\ crashed' = TRUE
  /\ flights' = [r \in Roots |-> 0]
  /\ drained' = {}
  /\ ready' = {}
  /\ mergeFlights' = [r \in Roots |-> 0]
  /\ pending' = IF RegisterMode = "Durable" THEN pending ELSE {}
  /\ serveAuth' = {}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, serveScopes, mergedV>>

Done == \A s \in Scopes : localV[s] = MaxV /\ mergedV[s] = MaxV
Terminating == Done /\ UNCHANGED vars

Next ==
  \/ \E s \in Scopes : Update(s) \/ ReHintMarker(s)
  \/ \E s \in Scopes, v \in 1..MaxV :
       ReHintPayload(s, v) \/ DropHint(s, v) \/ ProcessMerged(s, v)
       \/ ProcessRegister(s, v) \/ ProcessNack(s, v)
       \/ ClaimFetch(s, v) \/ CompleteFetch(s, v) \/ ClaimMerge(s, v)
       \/ CompleteMerge(s, v)
  \/ Crash
  \/ Terminating

Spec == Init /\ [][Next]_vars

FairSpec ==
  Spec /\ \A s \in Scopes :
    /\ WF_vars(ReHintMarker(s))
    /\ SF_vars(\E v \in 1..MaxV : ReHintPayload(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ClaimFetch(s, v))
    /\ SF_vars(\E v \in 1..MaxV : CompleteFetch(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ClaimMerge(s, v))
    /\ SF_vars(\E v \in 1..MaxV : CompleteMerge(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ProcessMerged(s, v) \/ ProcessRegister(s, v))

INV_TypeOK == TypeOK

INV_ObligationConservation ==
  \A s \in Scopes :
    mergedV[s] < localV[s] =>
      \/ s \in dirty
      \/ <<s, localV[s]>> \in payloadLedger
      \/ <<s, localV[s]>> \in inflight
      \/ <<s, localV[s]>> \in pending

INV_SingleFlight == \A r \in Roots : flights[r] <= 1

INV_SingleMergeWriter ==
  Cardinality({r \in Roots : mergeFlights[r] > 0}) <= 1

\* A productive CAR stream is not a completed fetch.  The single receiver
\* owner must retain it until transport completion (or until the DAG itself is
\* proven complete) instead of cancelling after its first arriving block.
INV_FetchOwnerDrainsResponse == \A r \in Roots : flights[r] > 0 => r \in drained

INV_ReceiverQueueBounded == Cardinality(pending) <= Cap

\* A sender has one current durable obligation per document/collection scope.
\* A newer head causally subsumes that sender's older head; retaining both
\* recreates unbounded per-root ownership under a live heartbeat stream.
INV_OnePendingHeadPerScope ==
  \A s \in Scopes : Cardinality({r \in pending : r[1] = s}) <= 1

\* An acknowledged receiver obligation must retain a CAR-serving path across
\* sender restart. A process-local/expiring grant violates this immediately.
INV_PendingServiceable ==
  \A r \in pending : r \in serveAuth \/ r[1] \in serveScopes

\* A durable receiver obligation must retain a provider the transport can
\* actually reach.  Native signed pubsub can bind that provider to the origin.
\* Iroh prefers the independently verified origin when that endpoint is already
\* connected, and otherwise binds it to the authenticated immediate hop.  The
\* signed origin prevents forged hints; the hop fallback preserves recovery in
\* a sparse gossip mesh without treating an unroutable payload ID as a provider.
INV_PendingHasRoutableProvider ==
  ProviderMode \in {"OriginBound", "AuthenticatedHop"} \/ pending = {}

\* A routable-looking peer identifier is not sufficient: Iroh relay metadata
\* authenticates only the last hop, so an origin carried in the payload must
\* be signed by the endpoint key that the identifier names.  Otherwise a
\* forged hint can transfer ownership to a receiver that can never reach the
\* actual DAG holder.
INV_PendingHasAuthenticatedProvider ==
  /\ (OriginAuthMode = "TransportBound" \/ pending = {})
  /\ (ProviderMode # "RelayOnly" \/ pending = {})

\* A recursive historical walk must not occupy the bounded owner before the
\* already-known missing frontier is requested.
INV_KnownFrontierUsesSelective == FetchMode = "RootThenSelective" \/ pending = {}

\* Marker-only durability is a structural protocol property, not merely a
\* storage optimization.  PayloadLedger violates this after the first update.
INV_SenderMarkersOnly == payloadLedger = {}

\* Every marker is a replayable scope and every marker retry is forced by the
\* action definition to announce localV[s], the current head.
INV_MarkersReplayable == dirty \subseteq Scopes

LIVE_EventualCurrency == <>[](\A s \in Scopes : mergedV[s] = localV[s])
====
