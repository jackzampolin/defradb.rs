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
\*   ProviderMode = "OriginBound" | "OriginRebound" | "OriginUnauthorized" |
\*                  "AuthenticatedHop" | "OriginUnroutable" | "RelayOnly"
\*   OriginAuthMode = "TransportBound" | "UnsignedClaim"
\*   FetchMode = "RootThenSelective" | "SelectiveMissing" | "RecursiveFirst"
\*
\* RootThenSelective admits a bounded descendant closure only after the
\* receiver has identified the missing frontier. It models Go's one per-root
\* blockservice session without allowing a recursive historical-root walk to
\* delay already-known missing blocks.
\*   StreamMode = "DrainResponse" | "CancelOnProgress"
\*   CompletionMode = "LatchedDrain" | "EdgeTriggered" |
\*                    "WorkerSaturated" | "BlockingHostEvent" |
\*                    "SharedServeWorkers" | "EagerIdentityLookup" |
\*                    "BusyExhaustion"
\*   PendingMode = "ScopeCurrent" | "EveryRoot"
\*   MergeMode = "SerializedBatch" | "ParallelWriters" |
\*               "DuplicateTerminal"
\*
\* A scope is either a document or a branchable collection.  A monotonically
\* increasing version abstracts the current composite/collection head CID.
\* ProviderMode abstracts the least-qualified peer admitted to a root's fetch
\* rotation, including alternates. A receiver chooses OriginBound only when
\* the independently authenticated origin has a reconnectable transport route,
\* owns the complete linked DAG, and can authorize the receiver at its
\* block-serving boundary. A temporary disconnect is a stuttering interval:
\* it cannot create a fetch owner or a terminal disposition, and fair
\* reconnection re-enables ClaimFetch through the same per-root clock.
\* OriginUnauthorized retains the fleet counterexample where the
\* origin owns every linked block but can serve only universally readable
\* metadata because the receiver has no authenticated ACP identity and no
\* direct replicator grant. An authenticated connected hop is retained as a red
\* mode: gossip relays can possess the head block without possessing any of
\* its linked descendants. Connected peers may become alternates only with
\* positive availability evidence for a CID on the missing frontier.
\* The receiver retry clock is abstracted by ClaimFetch: every possible trigger
\* must pass through that one action, which admits at most one owner per root.
\* Multi-hop explicit replication composes this one-hop machine: only after a
\* receiver completes Merge may that node act as the sender in a downstream
\* instance. It then has the complete DAG and records the downstream scope
\* marker before announcing the same current head. A root-only gossip relay
\* cannot take that transition merely because it forwarded an announcement.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Docs,
  Cols,
  MaxV,
  Cap,
  FetchCap,
  SenderMode,
  RegisterMode,
  FlightMode,
  AckGuardMode,
  ServeMode,
  ProviderMode,
  OriginAuthMode,
  FetchMode,
  StreamMode,
  CompletionMode,
  PendingMode,
  MergeMode

Scopes == Docs \cup Cols
Roots == Scopes \X (1..MaxV)

ASSUME Scopes # {} /\ Docs \cap Cols = {}
ASSUME MaxV \in Nat /\ MaxV >= 1
ASSUME Cap \in Nat /\ Cap >= 1
ASSUME FetchCap \in Nat /\ FetchCap >= 1 /\ FetchCap <= Cap
ASSUME SenderMode \in {"MarkerRederive", "DocOnlyMarkers", "PayloadLedger"}
ASSUME RegisterMode \in {"Durable", "Volatile"}
ASSUME FlightMode \in {"SingleFlight", "Duplicate"}
ASSUME AckGuardMode \in {"HeadCurrent", "Unguarded"}
ASSUME ServeMode \in {"DurableDerived", "VolatileGrant"}
ASSUME ProviderMode \in {"OriginBound", "OriginThenAlternate", "OriginRebound",
                         "OriginUnauthorized", "AuthenticatedHop", "OriginUnroutable",
                         "RelayOnly"}
ASSUME OriginAuthMode \in {"TransportBound", "UnsignedClaim"}
ASSUME FetchMode \in {"RootThenSelective", "SelectiveMissing", "RecursiveFirst"}
ASSUME StreamMode \in {"DrainResponse", "CancelOnProgress"}
ASSUME CompletionMode \in {"LatchedDrain", "EdgeTriggered", "WorkerSaturated",
                            "BlockingHostEvent", "SharedServeWorkers",
                            "EagerIdentityLookup", "BusyExhaustion"}
ASSUME PendingMode \in {"ScopeCurrent", "EveryRoot"}
ASSUME MergeMode \in {"SerializedBatch", "ParallelWriters", "DuplicateTerminal"}

VARIABLES
  localV,        \* [Scopes -> 0..MaxV], current sender head
  dirty,         \* SUBSET Scopes, durable marker-only sender state
  payloadLedger, \* SUBSET Roots, forbidden durable CID/payload delivery state
  inflight,      \* SUBSET Roots, announced hints not yet processed
  pending,       \* SUBSET Roots, durable receiver want registrations
  originBound,   \* SUBSET Roots, roots bound to the qualified origin provider
  qualifiedOffers, \* SUBSET Roots, authenticated direct hints from complete-DAG peers
  qualifiedProvider, \* SUBSET Roots, roots with a routable content-owning provider
  serveAuth,     \* SUBSET Roots, volatile exact-root CAR grants
  serveScopes,   \* SUBSET Scopes, restart-derived rooted pull policy
  flights,       \* [Roots -> 0..2], paced CAR fetch owners
  drained,       \* SUBSET Roots, owner retained its CAR through completion
  ready,         \* SUBSET Roots, complete DAGs awaiting the merge writer
  mergeFlights,  \* [Roots -> 0..1], independent merge writers
  terminalFlights, \* [Roots -> 0..2], durable terminal-cleanup writers
  fetchExhausted, \* SUBSET Roots, terminal provider failures (never local contention)
  lostCompletions, \* SUBSET Roots, transport completion edges no owner can observe
  mergedV,       \* [Scopes -> 0..MaxV], current receiver head
  crashed        \* BOOLEAN, one receiver crash/restart has happened

vars == <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider,
          serveAuth, serveScopes,
          flights, drained, ready, mergeFlights, terminalFlights, fetchExhausted,
          lostCompletions, mergedV, crashed>>

TypeOK ==
  /\ localV \in [Scopes -> 0..MaxV]
  /\ dirty \subseteq Scopes
  /\ payloadLedger \subseteq Roots
  /\ inflight \subseteq Roots
  /\ pending \subseteq Roots
  /\ originBound \subseteq Roots
  /\ qualifiedOffers \subseteq Roots
  /\ qualifiedProvider \subseteq Roots
  /\ serveAuth \subseteq Roots
  /\ serveScopes \subseteq Scopes
  /\ Cardinality(pending) <= Cap
  /\ flights \in [Roots -> 0..2]
  /\ Cardinality({r \in Roots : flights[r] > 0}) <= FetchCap
  /\ drained \subseteq Roots
  /\ ready \subseteq Roots
  /\ mergeFlights \in [Roots -> 0..1]
  /\ terminalFlights \in [Roots -> 0..2]
  /\ fetchExhausted \subseteq Roots
  /\ lostCompletions \subseteq Roots
  /\ mergedV \in [Scopes -> 0..MaxV]
  /\ crashed \in BOOLEAN

Init ==
  /\ localV = [s \in Scopes |-> 0]
  /\ dirty = {}
  /\ payloadLedger = {}
  /\ inflight = {}
  /\ pending = {}
  /\ originBound = {}
  /\ qualifiedOffers = {}
  /\ qualifiedProvider = {}
  /\ serveAuth = {}
  /\ serveScopes = {}
  /\ flights = [r \in Roots |-> 0]
  /\ drained = {}
  /\ ready = {}
  /\ mergeFlights = [r \in Roots |-> 0]
  /\ terminalFlights = [r \in Roots |-> 0]
  /\ fetchExhausted = {}
  /\ lostCompletions = {}
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
        \* DurableDerived abstracts the Go-compatible block-serving boundary:
        \* after restart, the authenticated CAR requester plus either durable
        \* replication configuration or ACP authorization for the exact root
        \* reconstructs serving authority. It does not depend on an eventually
        \* observed gossip-neighbor event.
        /\ serveScopes' = IF ServeMode = "DurableDerived"
                          THEN serveScopes \cup {s}
                          ELSE serveScopes
  /\ UNCHANGED <<pending, originBound, qualifiedOffers, qualifiedProvider, flights, drained, ready, mergeFlights, terminalFlights,
                 fetchExhausted, lostCompletions, mergedV, crashed>>

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
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, originBound, qualifiedOffers, qualifiedProvider, flights, drained, ready,
                 mergeFlights, terminalFlights, fetchExhausted, lostCompletions,
                 mergedV, crashed>>

\* Current main can retry the CID recorded in its durable delivery ledger.
ReHintPayload(s, v) ==
  /\ <<s, v>> \in payloadLedger
  /\ inflight' = inflight \cup {<<s, v>>}
  /\ serveAuth' = serveAuth \cup {<<s, v>>}
  /\ serveScopes' = IF ServeMode = "DurableDerived"
                    THEN serveScopes \cup {s}
                    ELSE serveScopes
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, originBound, qualifiedOffers, qualifiedProvider, flights, drained, ready,
                 mergeFlights, terminalFlights, fetchExhausted, lostCompletions,
                 mergedV, crashed>>

DropHint(s, v) ==
  /\ <<s, v>> \in inflight
  /\ inflight' = inflight \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth, serveScopes,
                 flights, drained, ready, mergeFlights, terminalFlights,
                 fetchExhausted, lostCompletions, mergedV, crashed>>

\* A success acknowledgement is emitted only after one of these two actions.
ProcessMerged(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] >= v
  /\ LET cleared == ClearOwned(s, v)
     IN /\ dirty' = cleared[1]
        /\ payloadLedger' = cleared[2]
  /\ inflight' = inflight \ {<<s, v>>}
  /\ serveAuth' = serveAuth \ {<<s, v>>}
  /\ UNCHANGED <<localV, pending, originBound, qualifiedOffers, qualifiedProvider, serveScopes, flights, drained, ready,
                 mergeFlights, terminalFlights, fetchExhausted, lostCompletions,
                 mergedV, crashed>>

ProcessRegister(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] < v
  /\ LET covered == \E r \in pending : r[1] = s /\ r[2] >= v
         superseded == {r \in pending : r[1] = s /\ r[2] < v}
         retired == IF PendingMode = "ScopeCurrent" THEN superseded ELSE {}
         retained == IF PendingMode = "ScopeCurrent"
                     THEN pending \ superseded
                     ELSE pending
     IN /\ (covered \/ <<s, v>> \in retained \/ Cardinality(retained) < Cap)
        /\ pending' = IF covered THEN retained ELSE retained \cup {<<s, v>>}
        \* A duplicate root keeps the provider that accepted the original
        \* durable transfer. OriginRebound is the countermodel in which a
        \* same-root relay replay overwrites that binding without availability
        \* evidence for the linked DAG.
        /\ originBound' =
             IF covered
             THEN IF ProviderMode = "OriginRebound"
                  THEN originBound \ {<<s, v>>}
                  ELSE originBound
             ELSE (originBound \ retired) \cup
                  (IF ProviderMode \in {"OriginBound", "OriginThenAlternate", "OriginRebound"}
                   THEN {<<s, v>>}
                   ELSE {})
        /\ qualifiedProvider' =
             IF covered
             THEN qualifiedProvider
             ELSE (qualifiedProvider \ retired) \cup
                  (IF ProviderMode \in {"OriginBound", "OriginRebound"}
                   THEN {<<s, v>>}
                   ELSE {})
        /\ qualifiedOffers' = qualifiedOffers \ retired
        \* Supersession invalidates every process-local owner for the retired
        \* durable generation. A stale fetch or merge completion cannot keep a
        \* bounded slot or later discharge the newer root.
        /\ flights' = [r \in Roots |-> IF r \in retired THEN 0 ELSE flights[r]]
        /\ drained' = drained \ retired
        /\ ready' = ready \ retired
        /\ mergeFlights' =
             [r \in Roots |-> IF r \in retired THEN 0 ELSE mergeFlights[r]]
        /\ terminalFlights' =
             [r \in Roots |-> IF r \in retired THEN 0 ELSE terminalFlights[r]]
        /\ lostCompletions' = lostCompletions \ retired
  /\ inflight' = inflight \ {<<s, v>>}
  /\ <<s, v>> \in serveAuth
  /\ LET cleared == ClearOwned(s, v)
     IN /\ dirty' = cleared[1]
        /\ payloadLedger' = cleared[2]
  /\ UNCHANGED <<localV, serveAuth, serveScopes, fetchExhausted, mergedV, crashed>>

\* Overflow is an actionable nack: receiver state is unchanged and sender
\* durable ownership is not cleared.
ProcessNack(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] < v
  /\ <<s, v>> \notin pending
  /\ Cardinality(pending) >= Cap
  /\ inflight' = inflight \ {<<s, v>>}
  /\ serveAuth' = serveAuth \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, pending, originBound, qualifiedOffers, qualifiedProvider, serveScopes, flights, drained,
                 ready, mergeFlights, terminalFlights, fetchExhausted,
                 lostCompletions, mergedV, crashed>>

\* Replaying an already-registered root is an idempotent announcement. The
\* green policy retains the origin chosen by the durable ownership transfer;
\* OriginRebound captures the runtime defect where identical head bytes from
\* another peer replace that origin without linked-DAG availability evidence.
SameRootReannounce(s, v) ==
  /\ <<s, v>> \in pending
  /\ ProviderMode = "OriginRebound"
  /\ originBound' = originBound \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, ready, mergeFlights,
                 terminalFlights, fetchExhausted, lostCompletions, mergedV,
                 crashed>>

\* In a configured A->B->C chain, B may offer A's same root to C only after B
\* completed its own receiver instance, merged the complete DAG, and crossed
\* the authenticated direct-replicator seam to C. This explicit environment
\* action is separate from gossip: an authenticated root-only relay cannot
\* produce a qualified offer. Fairness represents delivery of B's durable
\* downstream marker, not spontaneous provider creation.
OfferQualifiedAlternate(s, v) ==
  /\ <<s, v>> \in pending
  /\ ProviderMode = "OriginThenAlternate"
  /\ <<s, v>> \notin qualifiedProvider
  /\ <<s, v>> \notin qualifiedOffers
  /\ qualifiedOffers' = qualifiedOffers \cup {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound,
                 qualifiedProvider, serveAuth, serveScopes, flights, drained,
                 ready, mergeFlights, terminalFlights, fetchExhausted,
                 lostCompletions, mergedV, crashed>>

\* Durable registration of the authenticated complete-DAG offer extends the
\* recovery rotation without replacing the bound origin or resetting the
\* receiver clock.
AddQualifiedAlternate(s, v) ==
  /\ <<s, v>> \in pending
  /\ ProviderMode = "OriginThenAlternate"
  /\ <<s, v>> \in qualifiedOffers
  /\ <<s, v>> \notin qualifiedProvider
  /\ qualifiedProvider' = qualifiedProvider \cup {<<s, v>>}
  /\ qualifiedOffers' = qualifiedOffers \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound,
                 serveAuth, serveScopes, flights, drained, ready, mergeFlights,
                 terminalFlights, fetchExhausted, lostCompletions, mergedV,
                 crashed>>

HasQualifiedProvider(r) ==
  \/ /\ ProviderMode \in {"OriginBound", "OriginRebound"}
     /\ r \in originBound
  \/ /\ ProviderMode = "OriginThenAlternate"
     /\ r \in qualifiedProvider

\* This is the sole abstract fetch-dispatch seam: retry expiry, connect
\* expedite, and partial progress all coalesce through the same root claim.
\* A configured provider that is temporarily disconnected produces no model
\* transition here: the durable pending root remains conserved and the clock
\* waits for fair reconnection. In particular, unavailability is not a
\* terminal fetch-exhaustion disposition.
ClaimFetch(s, v) ==
  /\ <<s, v>> \in pending
  /\ (<<s, v>> \in serveAuth \/ s \in serveScopes)
  /\ HasQualifiedProvider(<<s, v>>)
  \* RootThenSelective first exercises the bounded rooted serving path at the
  \* actual publisher, then requests a capped descendant closure from the
  \* already-known missing frontier. SelectiveMissing is retained as a red
  \* topology probe: exact CIDs alone do not identify a routable provider.
  /\ FetchMode = "RootThenSelective"
  /\ (flights[<<s, v>>] > 0 \/
      Cardinality({r \in Roots : flights[r] > 0}) < FetchCap)
  /\ flights[<<s, v>>] < (IF FlightMode = "Duplicate" THEN 2 ELSE 1)
  /\ flights' = [flights EXCEPT ![<<s, v>>] = @ + 1]
  /\ drained' = IF StreamMode = "DrainResponse"
                THEN drained \cup {<<s, v>>}
                ELSE drained
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, ready, mergeFlights, terminalFlights, fetchExhausted,
                 lostCompletions, mergedV, crashed>>

\* A provider may fail before the fetch task installs its completion waiter.
\* Production latches that terminal result, releases the owner, and lets the
\* one receiver retry clock pace the next attempt. An edge-triggered handoff
\* loses the result and strands the sole fetch owner until its watchdog fires.
FailFetchBeforeWaiter(s, v) ==
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ lostCompletions' = IF CompletionMode = "EdgeTriggered"
                        THEN lostCompletions \cup {<<s, v>>}
                        ELSE lostCompletions
  /\ flights' = IF CompletionMode = "LatchedDrain"
                THEN [flights EXCEPT ![<<s, v>>] = @ - 1]
                ELSE flights
  /\ drained' = IF CompletionMode = "LatchedDrain" /\ flights[<<s, v>>] = 1
                THEN drained \ {<<s, v>>}
                ELSE drained
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, ready, mergeFlights, terminalFlights, fetchExhausted,
                 mergedV, crashed>>

\* A useful CAR can overlap another response that already owns a shared CID's
\* storage transition.  This is local coalescing, not evidence that the remote
\* provider failed.  Production releases the current fetch lease and leaves
\* the durable root to the same paced retry clock.  The red policy collapses
\* this third completion outcome into terminal provider exhaustion.
ContendedIngest(s, v) ==
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ <<s, v>> \in drained
  /\ fetchExhausted' = IF CompletionMode = "BusyExhaustion"
                       THEN fetchExhausted \cup {<<s, v>>}
                       ELSE fetchExhausted
  /\ flights' = [flights EXCEPT ![<<s, v>>] = @ - 1]
  /\ drained' = IF flights[<<s, v>>] = 1
                THEN drained \ {<<s, v>>}
                ELSE drained
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, ready, mergeFlights, terminalFlights,
                 lostCompletions, mergedV, crashed>>

\* The fleet exposed a second way to make a completed response unobservable:
\* a bounded spawned-worker dispatcher stopped draining its transport channel
\* while every worker was occupied, leaving the CAR completion behind queued
\* requests until the sole fetch owner timed out.  The green bounded scheduler
\* cannot take this transition: it never waits for a request-worker slot,
\* nacks excess requests, and always drains blocks, replies, and completions.
StarveTransportCompletion(s, v) ==
  /\ CompletionMode \in {"WorkerSaturated", "BlockingHostEvent"}
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ <<s, v>> \in drained
  /\ lostCompletions' = lostCompletions \cup {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, ready, mergeFlights,
                 terminalFlights, fetchExhausted, mergedV, crashed>>

\* Completion isolation alone is insufficient when durable ownership
\* registration and recovery serving consume the same bounded worker set.
\* Slow serialized registration can occupy every worker, preventing the
\* provider from accepting the CAR request that discharges an acknowledged
\* receiver obligation. The green dispatcher gives serving its own bounded
\* lane, so admission saturation cannot take this transition.
StarveRecoveryServe(s, v) ==
  /\ CompletionMode = "SharedServeWorkers"
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ <<s, v>> \in drained
  /\ lostCompletions' = lostCompletions \cup {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, ready, mergeFlights,
                 terminalFlights, fetchExhausted, mergedV, crashed>>

\* A durable replicator grant is already sufficient serving authority. An
\* eager implementation that nevertheless waits for an optional reverse DID
\* challenge can consume the entire CAR response budget and strand a valid
\* receiver fetch. Go and the green Rust path short-circuit at the durable
\* replicator decision; ACP identity resolution is only the fallback.
StarveRecoveryAuthorization(s, v) ==
  /\ CompletionMode = "EagerIdentityLookup"
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ <<s, v>> \in drained
  /\ lostCompletions' = lostCompletions \cup {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, ready, mergeFlights,
                 terminalFlights, fetchExhausted, mergedV, crashed>>

CompleteFetch(s, v) ==
  /\ <<s, v>> \in pending
  /\ flights[<<s, v>>] > 0
  /\ <<s, v>> \in drained
  /\ ready' = ready \cup {<<s, v>>}
  /\ flights' = [flights EXCEPT ![<<s, v>>] = @ - 1]
  /\ drained' = IF flights[<<s, v>>] = 1
                THEN drained \ {<<s, v>>}
                ELSE drained
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, mergeFlights, terminalFlights, fetchExhausted,
                 lostCompletions, mergedV, crashed>>

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
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, ready, terminalFlights,
                 fetchExhausted, lostCompletions, mergedV, crashed>>

CompleteMerge(s, v) ==
  /\ <<s, v>> \in ready
  /\ mergeFlights[<<s, v>>] = 1
  /\ mergedV' = [mergedV EXCEPT ![s] = IF v > @ THEN v ELSE @]
  /\ ready' = ready \ {<<s, v>>}
  /\ mergeFlights' = [mergeFlights EXCEPT ![<<s, v>>] = 0]
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, terminalFlights, fetchExhausted,
                 lostCompletions, crashed>>

\* Merge and terminal durable cleanup are separate runtime observations.  A
\* single metadata writer makes repeated completion idempotent instead of
\* letting same-root delete transactions exhaust OCC retries.
ClaimTerminal(s, v) ==
  /\ <<s, v>> \in pending
  /\ mergedV[s] >= v
  /\ terminalFlights[<<s, v>>] <
       (IF MergeMode = "DuplicateTerminal" THEN 2 ELSE 1)
  /\ terminalFlights' = [terminalFlights EXCEPT ![<<s, v>>] = @ + 1]
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, pending, originBound, qualifiedOffers, qualifiedProvider, serveAuth,
                 serveScopes, flights, drained, ready, mergeFlights,
                 fetchExhausted, lostCompletions, mergedV, crashed>>

CompleteTerminal(s, v) ==
  /\ terminalFlights[<<s, v>>] > 0
  /\ pending' = pending \ {<<s, v>>}
  /\ originBound' = originBound \ {<<s, v>>}
  /\ qualifiedOffers' = qualifiedOffers \ {<<s, v>>}
  /\ qualifiedProvider' = qualifiedProvider \ {<<s, v>>}
  /\ serveAuth' = serveAuth \ {<<s, v>>}
  \* Removing the durable generation invalidates its fetch lease. A fetch
  \* claimed during the merge-to-cleanup window cannot retain a global owner
  \* slot after the obligation is terminal.
  /\ flights' = [flights EXCEPT ![<<s, v>>] = 0]
  /\ drained' = drained \ {<<s, v>>}
  /\ ready' = ready \ {<<s, v>>}
  /\ mergeFlights' = [mergeFlights EXCEPT ![<<s, v>>] = 0]
  /\ lostCompletions' = lostCompletions \ {<<s, v>>}
  /\ terminalFlights' = [terminalFlights EXCEPT ![<<s, v>>] = @ - 1]
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, serveScopes,
                 fetchExhausted, mergedV, crashed>>

Crash ==
  /\ ~crashed
  /\ crashed' = TRUE
  /\ flights' = [r \in Roots |-> 0]
  /\ drained' = {}
  /\ ready' = {}
  /\ mergeFlights' = [r \in Roots |-> 0]
  /\ terminalFlights' = [r \in Roots |-> 0]
  /\ lostCompletions' = {}
  /\ pending' = IF RegisterMode = "Durable" THEN pending ELSE {}
  /\ originBound' = IF RegisterMode = "Durable" THEN originBound ELSE {}
  /\ qualifiedOffers' = {}
  /\ qualifiedProvider' = IF RegisterMode = "Durable" THEN qualifiedProvider ELSE {}
  /\ serveAuth' = {}
  /\ UNCHANGED <<localV, dirty, payloadLedger, inflight, serveScopes,
                 fetchExhausted, mergedV>>

Done == \A s \in Scopes : localV[s] = MaxV /\ mergedV[s] = MaxV
Terminating == Done /\ UNCHANGED vars

Next ==
  \/ \E s \in Scopes : Update(s) \/ ReHintMarker(s)
  \/ \E s \in Scopes, v \in 1..MaxV :
       ReHintPayload(s, v) \/ DropHint(s, v) \/ ProcessMerged(s, v)
       \/ ProcessRegister(s, v) \/ ProcessNack(s, v) \/ SameRootReannounce(s, v)
       \/ OfferQualifiedAlternate(s, v) \/ AddQualifiedAlternate(s, v)
       \/ ClaimFetch(s, v) \/ FailFetchBeforeWaiter(s, v)
       \/ ContendedIngest(s, v)
       \/ StarveTransportCompletion(s, v)
       \/ StarveRecoveryServe(s, v)
       \/ StarveRecoveryAuthorization(s, v)
       \/ CompleteFetch(s, v) \/ ClaimMerge(s, v)
       \/ CompleteMerge(s, v) \/ ClaimTerminal(s, v) \/ CompleteTerminal(s, v)
  \/ Crash
  \/ Terminating

Spec == Init /\ [][Next]_vars

FairSpec ==
  Spec /\ \A s \in Scopes :
    /\ WF_vars(ReHintMarker(s))
    /\ SF_vars(\E v \in 1..MaxV : ReHintPayload(s, v))
    /\ SF_vars(\E v \in 1..MaxV : OfferQualifiedAlternate(s, v))
    /\ SF_vars(\E v \in 1..MaxV : AddQualifiedAlternate(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ClaimFetch(s, v))
    /\ SF_vars(\E v \in 1..MaxV : CompleteFetch(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ClaimMerge(s, v))
    /\ SF_vars(\E v \in 1..MaxV : CompleteMerge(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ClaimTerminal(s, v))
    /\ SF_vars(\E v \in 1..MaxV : CompleteTerminal(s, v))
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

INV_FetchOwnersBounded == Cardinality({r \in Roots : flights[r] > 0}) <= FetchCap

INV_SingleMergeWriter ==
  Cardinality({r \in Roots : mergeFlights[r] > 0}) <= 1

INV_SingleTerminalWriter == \A r \in Roots : terminalFlights[r] <= 1

\* A productive CAR stream is not a completed fetch.  The single receiver
\* owner must retain it until transport completion (or until the DAG itself is
\* proven complete) instead of cancelling after its first arriving block.
INV_FetchOwnerDrainsResponse == \A r \in Roots : flights[r] > 0 => r \in drained

\* Completion is state, not an edge: a fast transport failure remains
\* observable even when it races ahead of waiter registration, and a completed
\* CAR response cannot sit behind workers whose progress depends on that same
\* transport loop.
INV_FetchCompletionObservable == lostCompletions = {}

\* A local single-flight collision is not a terminal statement about provider
\* availability. It may only release this claim and re-arm the durable clock.
INV_ContendedIngestDoesNotExhaust == fetchExhausted = {}

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
\* Iroh binds recovery to the independently verified origin only when that
\* endpoint is transport-routable.  The authenticated immediate hop is not
\* sufficient: a gossip relay may hold only the announced root block.
INV_PendingHasRoutableProvider ==
  ProviderMode = "OriginBound" \/ pending = {}

\* An initially unreachable Iroh publisher may remain the immutable ownership
\* origin while a later direct same-root announcer becomes the recovery
\* provider. No fetch may start before that independently qualified candidate
\* is durable, and the candidate survives receiver restart with the obligation.
INV_FetchHasQualifiedProvider ==
  \A r \in Roots : flights[r] > 0 => HasQualifiedProvider(r)

\* Authentication and routability do not prove content availability. Every
\* provider admitted to the fetch rotation must own requested linked content,
\* not merely the head block it relayed in the gossip envelope.
INV_PendingHasCompleteProvider ==
  ProviderMode # "AuthenticatedHop" \/ pending = {}

\* Endpoint authentication and content possession are still insufficient when
\* ACP is enabled: the serving origin must authenticate the requesting peer's
\* Defra identity (or hold an exact-root grant/configured replication scope).
\* Otherwise it can repeatedly return only signature/definition metadata and
\* the durable receiver obligation can never finish.
INV_PendingHasServingAuthorization ==
  ProviderMode # "OriginUnauthorized" \/ pending = {}

\* A routable-looking peer identifier is not sufficient: Iroh relay metadata
\* authenticates only the last hop, so an origin carried in the payload must
\* be signed by the endpoint key that the identifier names.  Otherwise a
\* forged hint can transfer ownership to a receiver that can never reach the
\* actual DAG holder.
INV_PendingHasAuthenticatedProvider ==
  /\ (OriginAuthMode = "TransportBound" \/ pending = {})
  /\ (ProviderMode # "RelayOnly" \/ pending = {})

\* Provider identity is modeled independently from the root tuple. The green
\* binding names the publisher that transferred ownership; the red rebind
\* action replaces it with an authenticated relay that has only the envelope.
ProviderOf(r) == IF r \in originBound THEN "Origin" ELSE "Relay"

\* The provider chosen when ownership is durably transferred belongs to that
\* root generation. Re-announcing identical signed head bytes is idempotent;
\* it cannot replace the origin with a peer that has not demonstrated linked
\* DAG availability.
INV_PendingRetainsBoundProvider ==
  \A r \in pending : ProviderOf(r) = "Origin"

\* A recursive historical-root walk must not occupy the bounded owner before
\* the already-known missing frontier is requested. A bounded descendant
\* closure rooted at that frontier satisfies this invariant.
INV_KnownFrontierUsesSelective == FetchMode = "RootThenSelective" \/ pending = {}

\* Marker-only durability is a structural protocol property, not merely a
\* storage optimization.  PayloadLedger violates this after the first update.
INV_SenderMarkersOnly == payloadLedger = {}

\* Every marker is a replayable scope and every marker retry is forced by the
\* action definition to announce localV[s], the current head.
INV_MarkersReplayable == dirty \subseteq Scopes

LIVE_EventualCurrency == <>[](\A s \in Scopes : mergedV[s] = localV[s])

LIVE_EventualReceiverQuiescence == <>[](pending = {})
====
