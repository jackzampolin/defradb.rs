---- MODULE HeadSet ----
\* Concurrent collection-head transitions, abstracting
\* crates/db/src/block/builder/collection.rs (write_collection_block) over the
\* regolith-backed store in crates/storage/src/backends/regolith.
\*
\* THE MECHANISM. A collection's head set is the set of blocks in its DAG that no
\* other block names as a parent. `write_collection_block` maintains it by:
\*   1. scanning the head prefix to read the current heads (collection.rs:27-53),
\*   2. writing a new block whose parents are exactly those heads (:73),
\*   3. writing the new head key (:91),
\*   4. DELETING every old head key it observed (:96-100).
\*
\* Step 4 is the whole subject of this model. Two transactions that run
\* concurrently both observe the same old head and both delete it. That is a
\* write-write conflict on ONE key, and regolith validates the write set at every
\* isolation level, so the second transaction aborts. Relaxing isolation does not
\* help: the failure is identical under ReadCommitted, SnapshotIsolation and
\* Serializable, because none of them permit two transactions to write the same
\* key concurrently.
\*
\* WHAT THE OLD BACKEND DID. The store that regolith replaced carried its own
\* conflict tracker with a carve-out: `IterOptions::with_commutative_set` marked
\* a prefix scan as an observed-remove/add set transition, and the tracker then
\* PERMITTED overlapping keys when both transactions marked the scan that way.
\* regolith has no such notion, so the carve-out had no equivalent and the
\* conflict became real. The flag has since been removed rather than kept as a
\* no-op.
\*
\* WHY THIS MATTERS. Forming sibling heads from concurrent writes is not an
\* optimization, it is what makes the structure a CRDT: two writers that did not
\* see each other must both land and leave two tips for a later merge to join.
\* A model where one of them aborts has changed the data structure's semantics,
\* not merely its throughput.
\*
\* ONE KNOB:
\*   Strategy = "EagerDelete" - the tree as it stands: a writer deletes every head
\*                              it observed. Two concurrent writers write the same
\*                              delete, so one aborts.                      [RED]
\*            = "Derived"     - a writer only ever writes keys derived from its OWN
\*                              block id: its head key, and one supersede marker
\*                              per parent naming itself as the superseder. Write
\*                              sets are disjoint by construction, so nothing can
\*                              conflict. The head set is then DERIVED (a stored
\*                              head is a head iff nothing supersedes it) rather
\*                              than maintained by deletion.                [GREEN]
\*
\* The point of running both is that GREEN alone would not show the fix is
\* load-bearing. RED under EagerDelete is what proves the conflict is real and
\* that the derived model is what removes it.
\*
\* A SECOND KNOB, for the reclamation the derived strategy makes necessary.
\* Nothing deletes a superseded head key on the write path any more, so without
\* a sweep the headstore grows one key per mutation and every append scans all
\* of them. `Prune` is that sweep. It runs in a transaction of its own, which is
\* why it takes no part in WriteSet: folding it into an append would put back
\* the shared write the derived strategy exists to remove.
\*
\*   Reclaim = "Together"    - a head key and every marker against it leave in
\*                             one transaction, so the query can never observe
\*                             one without the other.                     [GREEN]
\*           = "MarkersOnly" - the markers go and the head key stays. The head
\*                             they superseded reads as live again and the head
\*                             set stops matching the DAG.
\*                             INV_HeadsExact fails.                      [RED]
\*
\* The second pair exists for the same reason as the first: "they leave
\* together" is a claim, and a claim has to be able to fail.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Writers,   \* finite set of concurrent writer ids; each writes one block
  Seed,      \* the block id already established as the collection's head
  Strategy,  \* "EagerDelete" | "Derived"
  Reclaim    \* "Together" | "MarkersOnly"

ASSUME Writers # {}
ASSUME Seed \notin Writers
ASSUME Strategy \in {"EagerDelete", "Derived"}
ASSUME Reclaim \in {"Together", "MarkersOnly"}

\* Every block id in the model: the seed plus one per writer.
Blocks == Writers \cup {Seed}

VARIABLES
  pruned,      \* SUBSET Blocks - head keys reclamation has removed
  committed,   \* SUBSET Writers - writers whose transaction committed
  aborted,     \* SUBSET Writers - writers whose transaction hit a write conflict
  observed,    \* [Writers -> SUBSET Blocks] - heads each writer read at snapshot
  snapshotted, \* SUBSET Writers - writers that have taken their snapshot
  headKeys,    \* SUBSET Blocks - stored head keys
  supersedes,  \* SUBSET (Blocks \X Blocks) - <<parent, child>> markers
  parents,     \* [Blocks -> SUBSET Blocks] - each block's recorded parents
  writeLog     \* SUBSET (Writers \X STRING) - which key each committed writer wrote

vars == <<committed, aborted, observed, snapshotted, headKeys, supersedes,
          parents, writeLog, pruned>>

----------------------------------------------------------------------------
\* Key space. A key is modeled as a string tag paired with the block it names,
\* which is all the conflict check needs: two writes conflict iff they name the
\* same key.

HeadKey(b)          == <<"head", b, b>>
SupersedeKey(p, c)  == <<"supersede", p, c>>
DeleteKey(b)        == <<"head", b, b>>   \* a delete writes the same key it removes

\* The set of keys a writer w commits, given what it observed.
WriteSet(w) ==
  IF Strategy = "EagerDelete"
    THEN {HeadKey(w)} \cup {DeleteKey(h) : h \in observed[w]}
    \* Every key here is a function of w, the writer's own block id, so two
    \* distinct writers cannot produce the same key. That is the whole fix.
    ELSE {HeadKey(w)} \cup {SupersedeKey(h, w) : h \in observed[w]}

\* Keys already written by a committed transaction.
CommittedKeys == {k \in {HeadKey(b) : b \in Blocks}
                    \cup {SupersedeKey(p, c) : <<p, c>> \in Blocks \X Blocks}
                    \cup {DeleteKey(b) : b \in Blocks} :
                  \E w \in committed : k \in WriteSet(w)}

\* regolith validates the write set at commit: a transaction aborts if any key it
\* writes was written by a transaction that committed after its snapshot. Both
\* writers snapshot before either commits, so "since its snapshot" is simply
\* "by anyone already committed".
Conflicts(w) == \E k \in WriteSet(w) : \E v \in committed : k \in WriteSet(v)

----------------------------------------------------------------------------
\* The head set a reader observes.
\*
\* EagerDelete maintains it directly: whatever head keys remain.
\* Derived computes it: a stored head key is a head iff nothing supersedes it.

DerivedHeads == {b \in headKeys : ~\E c \in Blocks : <<b, c>> \in supersedes}

Heads == IF Strategy = "EagerDelete" THEN headKeys ELSE DerivedHeads

\* The head set the DAG says it should be: every stored block that no other
\* stored block names as a parent. This is the specification, independent of how
\* either strategy chooses to maintain it.
StoredBlocks == headKeys \cup {c \in Blocks : \E p \in Blocks : <<p, c>> \in supersedes}

DagHeads == {b \in StoredBlocks : ~\E c \in StoredBlocks : b \in parents[c]}

----------------------------------------------------------------------------
Init ==
  /\ pruned      = {}
  /\ committed   = {}
  /\ aborted     = {}
  /\ observed    = [w \in Writers |-> {}]
  /\ snapshotted = {}
  /\ headKeys    = {Seed}
  /\ supersedes  = {}
  /\ parents     = [b \in Blocks |-> {}]
  /\ writeLog    = {}

\* A writer opens its transaction and scans the head prefix.
Snapshot(w) ==
  /\ w \notin snapshotted
  /\ snapshotted' = snapshotted \cup {w}
  /\ observed'    = [observed EXCEPT ![w] = Heads]
  /\ UNCHANGED <<committed, aborted, headKeys, supersedes, parents, writeLog, pruned>>

\* The writer commits. regolith checks the write set first.
\*
\* Every writer snapshots before any of them commits: that is what makes them
\* concurrent, and it is the case the carve-out existed to serve. Without this
\* the model would also explore a sequential interleaving, where the second
\* writer reads the first one's result and no conflict was ever possible.
Commit(w) ==
  /\ snapshotted = Writers
  /\ w \in snapshotted
  /\ w \notin committed
  /\ w \notin aborted
  /\ IF Conflicts(w)
       THEN /\ aborted' = aborted \cup {w}
            /\ UNCHANGED <<committed, headKeys, supersedes, parents, writeLog, pruned>>
       ELSE /\ committed' = committed \cup {w}
            /\ aborted'   = aborted
            \* The block records the parents it was built against.
            /\ parents'   = [parents EXCEPT ![w] = observed[w]]
            /\ headKeys'  = IF Strategy = "EagerDelete"
                              THEN (headKeys \ observed[w]) \cup {w}
                              ELSE headKeys \cup {w}
            /\ supersedes' = IF Strategy = "EagerDelete"
                               THEN supersedes
                               ELSE supersedes \cup {<<h, w>> : h \in observed[w]}
            /\ writeLog'  = writeLog \cup {<<w, "block">>}
            /\ pruned'    = pruned
  /\ UNCHANGED <<observed, snapshotted>>

\* Reclaim one superseded head key. Its own transaction, so it neither aborts a
\* writer nor can be aborted by one: an appending writer's scan is not part of
\* its read set, and the keys removed here were superseded before the sweep
\* began, so nothing an in-flight append writes is touched.
\*
\* Enabled only under the derived strategy, which is the only one that leaves
\* anything behind.
Prune(b) ==
  /\ Strategy = "Derived"
  /\ b \in headKeys
  /\ \E c \in Blocks : <<b, c>> \in supersedes
  /\ b \notin pruned
  /\ pruned' = pruned \cup {b}
  /\ supersedes' = supersedes \ {<<p, c>> \in supersedes : p = b}
  /\ headKeys' = IF Reclaim = "Together" THEN headKeys \ {b} ELSE headKeys
  /\ UNCHANGED <<committed, aborted, observed, snapshotted, parents, writeLog>>

Next ==
  \/ \E w \in Writers : Snapshot(w)
  \/ \E w \in Writers : Commit(w)
  \/ \E b \in Blocks : Prune(b)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----------------------------------------------------------------------------
TypeOK ==
  /\ pruned      \subseteq Blocks
  /\ committed   \subseteq Writers
  /\ aborted     \subseteq Writers
  /\ snapshotted \subseteq Writers
  /\ headKeys    \subseteq Blocks
  /\ supersedes  \subseteq (Blocks \X Blocks)
  /\ committed \cap aborted = {}

\* THE HEADLINE. Every writer's work lands. A writer that did its read, built its
\* block and asked to commit must not be turned away because another writer that
\* it never saw touched the same key.
\*
\* RED under EagerDelete: the second writer deletes the same seed head key and
\* aborts.
\* GREEN under Derived: write sets are disjoint by construction.
INV_NoWriteConflict == aborted = {}

\* The mechanical reason. Two distinct writers never write the same key.
\* This is the property the fix rests on, stated directly so a reader can see
\* that "Derived" is not merely lucky in this configuration.
INV_DisjointWriteSets ==
  \A v, w \in snapshotted :
    v # w => (WriteSet(v) \cap WriteSet(w) = {})

\* Whatever strategy maintains the head set, the head set a reader sees must be
\* the DAG's actual tips: the stored blocks nothing else names as a parent.
\* Holding this is what makes "Derived" a refactor rather than a redefinition.
INV_HeadsExact ==
  (committed = Writers) => (Heads = DagHeads)

\* Siblings survive. Once every writer has committed, each writer's block is a
\* head, and the seed they all superseded is not.
INV_SiblingsPreserved ==
  (committed = Writers) =>
    /\ \A w \in Writers : w \in Heads
    /\ Seed \notin Heads

\* Liveness: every writer eventually reaches a terminal state, and under the
\* derived strategy that state is committed.
EventuallyAllCommit == <>[](committed = Writers)

====
