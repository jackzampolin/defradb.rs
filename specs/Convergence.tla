---- MODULE Convergence ----
\* DAG delivery convergence under partition/reconnect.  This module is separate
\* from DagReplication.tla: that B3 model covers filtered replication policy; this
\* one covers the distributed delivery machinery that policy depends on.
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
  Nodes,
  Blocks,
  Parents,
  Heads,
  Creator,
  InitialConnected,
  MaxSynced,
  HeadRediscovery,
  AllowRestart

NodePairs == Nodes \X Nodes

ASSUME Creator \in Nodes
ASSUME Heads \subseteq Blocks
ASSUME \A b \in Blocks : Parents[b] \subseteq Blocks
ASSUME InitialConnected \subseteq NodePairs
ASSUME MaxSynced \in Nat /\ MaxSynced > 0
ASSUME HeadRediscovery \in BOOLEAN
ASSUME AllowRestart \in BOOLEAN

\* Terminates because Parents is acyclic in every scenario cfg.
RECURSIVE AncestorsOf(_)
AncestorsOf(b) == Parents[b] \cup UNION { AncestorsOf(p) : p \in Parents[b] }

RequiredBlocks == UNION { {h} \cup AncestorsOf(h) : h \in Heads }

VARIABLES
  have,          \* durable local blockstore
  merged,        \* durable merge marker / accepted local state
  wanted,        \* in-memory pending roots learned from head discovery
  syncing,       \* in-memory DagSyncState.syncing
  synced,        \* bounded in-memory DagSyncState.synced hint
  syncedOrder,   \* FIFO order for synced eviction
  connected,
  restarted

vars == <<have, merged, wanted, syncing, synced, syncedOrder, connected, restarted>>

OrderSet(order) == { order[i] : i \in 1..Len(order) }

SeqNoDup(order) ==
  \A i, j \in 1..Len(order) : i # j => order[i] # order[j]

TypeOK ==
  /\ have       \in [Nodes -> SUBSET Blocks]
  /\ merged     \in [Nodes -> SUBSET Blocks]
  /\ wanted     \in [Nodes -> SUBSET Heads]
  /\ syncing    \in [Nodes -> SUBSET Blocks]
  /\ synced     \in [Nodes -> SUBSET Blocks]
  /\ syncedOrder \in [Nodes -> Seq(Blocks)]
  /\ connected  \subseteq NodePairs
  /\ restarted  \subseteq Nodes
  /\ \A n \in Nodes :
       /\ merged[n] \subseteq have[n]
       /\ synced[n] \subseteq have[n]
       /\ OrderSet(syncedOrder[n]) = synced[n]
       /\ SeqNoDup(syncedOrder[n])
       /\ Cardinality(synced[n]) <= MaxSynced

Init ==
  /\ have       = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ merged     = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ wanted     = [n \in Nodes |-> {}]
  /\ syncing    = [n \in Nodes |-> {}]
  /\ synced     = [n \in Nodes |-> {}]
  /\ syncedOrder = [n \in Nodes |-> <<>>]
  /\ connected  = InitialConnected
  /\ restarted  = {}

Connected(m, n) == <<m, n>> \in connected
FullyConnected == connected = NodePairs

HasConnectedProvider(n, b) ==
  \E m \in Nodes : Connected(m, n) /\ b \in have[m]

\* Head rediscovery abstracts DocSync/BranchableSync after reconnect. Without it,
\* a head announced during a partition can be missed forever even though the
\* transport later reconnects.
DiscoverHead(m, n, h) ==
  /\ HeadRediscovery
  /\ Connected(m, n)
  /\ h \in Heads
  /\ h \in merged[m]
  /\ h \notin merged[n]
  /\ h \notin wanted[n]
  /\ wanted' = [wanted EXCEPT ![n] = @ \cup {h}]
  /\ UNCHANGED <<have, merged, syncing, synced, syncedOrder, connected, restarted>>

FetchTarget(n, b) ==
  \E h \in wanted[n] : b = h \/ b \in AncestorsOf(h)

\* Prepare a fetch for any missing block in a wanted head's causal history.  The
\* synced set is only a duplicate-work hint; it never removes durable blockstore
\* contents and restart clears it.
StartFetch(n, b) ==
  /\ FetchTarget(n, b)
  /\ b \notin have[n]
  /\ b \notin syncing[n]
  /\ b \notin synced[n]
  /\ HasConnectedProvider(n, b)
  /\ syncing' = [syncing EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<have, merged, wanted, synced, syncedOrder, connected, restarted>>

CompleteSynced(n, b) ==
  LET oldOrder == syncedOrder[n]
      oldSynced == synced[n]
      addOrder == IF b \in oldSynced THEN oldOrder ELSE Append(oldOrder, b)
      addSynced == oldSynced \cup {b}
      overLimit == Len(addOrder) > MaxSynced
      newOrder == IF overLimit THEN Tail(addOrder) ELSE addOrder
      newSynced == IF overLimit THEN addSynced \ {Head(addOrder)} ELSE addSynced
  IN
      /\ synced' = [synced EXCEPT ![n] = newSynced]
      /\ syncedOrder' = [syncedOrder EXCEPT ![n] = newOrder]

ReceiveBlock(n, b) ==
  /\ b \in syncing[n]
  /\ HasConnectedProvider(n, b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ syncing' = [syncing EXCEPT ![n] = @ \ {b}]
  /\ CompleteSynced(n, b)
  /\ UNCHANGED <<merged, wanted, connected, restarted>>

Merge(n, b) ==
  /\ b \in have[n]
  /\ b \notin merged[n]
  /\ Parents[b] \subseteq merged[n]
  /\ merged' = [merged EXCEPT ![n] = @ \cup {b}]
  /\ wanted' = [wanted EXCEPT ![n] = @ \ {b}]
  /\ UNCHANGED <<have, syncing, synced, syncedOrder, connected, restarted>>

\* One arbitrary restart per node. It clears in-memory delivery state while
\* preserving durable blockstore and merge state.
Restart(n) ==
  /\ AllowRestart
  /\ n \notin restarted
  /\ wanted' = [wanted EXCEPT ![n] = {}]
  /\ syncing' = [syncing EXCEPT ![n] = {}]
  /\ synced' = [synced EXCEPT ![n] = {}]
  /\ syncedOrder' = [syncedOrder EXCEPT ![n] = <<>>]
  /\ restarted' = restarted \cup {n}
  /\ UNCHANGED <<have, merged, connected>>

SetConnectivity(c) ==
  /\ c \in SUBSET NodePairs
  /\ c # connected
  /\ connected' = c
  /\ UNCHANGED <<have, merged, wanted, syncing, synced, syncedOrder, restarted>>

Next ==
  \/ \E c \in SUBSET NodePairs : SetConnectivity(c)
  \/ \E m \in Nodes, n \in Nodes, h \in Heads : DiscoverHead(m, n, h)
  \/ \E n \in Nodes, b \in Blocks : StartFetch(n, b)
  \/ \E n \in Nodes, b \in Blocks : ReceiveBlock(n, b)
  \/ \E n \in Nodes, b \in Blocks : Merge(n, b)
  \/ \E n \in Nodes : Restart(n)

Fairness ==
  /\ \A m \in Nodes, n \in Nodes, h \in Heads : WF_vars(DiscoverHead(m, n, h))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(StartFetch(n, b))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(ReceiveBlock(n, b))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Merge(n, b))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Properties ----

INV_SyncedFifo ==
  \A n \in Nodes :
    /\ OrderSet(syncedOrder[n]) = synced[n]
    /\ SeqNoDup(syncedOrder[n])
    /\ Cardinality(synced[n]) <= MaxSynced

INV_DurableMerge ==
  \A n \in Nodes :
    /\ merged[n] \subseteq have[n]
    /\ synced[n] \subseteq have[n]

AllConverged == \A n \in Nodes : RequiredBlocks \subseteq merged[n]

EventuallyConnected == <>[]FullyConnected

CONV_EventualConnectivity == EventuallyConnected => <>[]AllConverged

\* Used only by red/no-assumption configs.
CONV_Unconditional == <>[]AllConverged
====
