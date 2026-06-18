---- MODULE MixedFieldMaterialization ----
\* Same-document mixed-field materialization: one LWW field (`name`) and one
\* counter field (`views`). The algebraic product is proved in Lean
\* (`DefraConvergence.MixedField`); this model checks the temporal/materialization
\* hazard #1048 is about.
\*
\* RED: a merge of one field snapshots the whole materialized document and later
\* commits that whole stale snapshot with only its own field updated. A local write
\* to the other field that lands between snapshot and commit is clobbered.
\*
\* GREEN: each field merge commits componentwise against the current materialized
\* document. Merging the counter cannot overwrite the LWW field, and merging the
\* LWW field cannot overwrite the counter field.
EXTENDS Naturals

CONSTANTS
  MergeMode \* "WholeDoc" | "Componentwise"

ASSUME MergeMode \in {"WholeDoc", "Componentwise"}

VARIABLES
  name,          \* Nat abstraction of the LWW winner: 0=seed, 1=remote, 2=local
  views,         \* Nat counter materialized value
  expectedName,  \* oracle LWW winner after committed operations
  expectedViews, \* oracle counter sum after committed operations
  pc,            \* per-operation phase
  snapName,      \* whole-doc name snapshot captured by a remote merge
  snapViews      \* whole-doc views snapshot captured by a remote merge

vars == <<name, views, expectedName, expectedViews, pc, snapName, snapViews>>

Ops == {"localName", "localViews", "remoteName", "remoteViews"}
RemoteOps == {"remoteName", "remoteViews"}
Phases == {"todo", "snap", "done"}

NameJoin(cur, incoming) == IF incoming > cur THEN incoming ELSE cur

TypeOK ==
  /\ name \in Nat
  /\ views \in Nat
  /\ expectedName \in Nat
  /\ expectedViews \in Nat
  /\ pc \in [Ops -> Phases]
  /\ snapName \in [RemoteOps -> Nat]
  /\ snapViews \in [RemoteOps -> Nat]

Init ==
  /\ name = 0
  /\ views = 0
  /\ expectedName = 0
  /\ expectedViews = 0
  /\ pc = [o \in Ops |-> "todo"]
  /\ snapName = [o \in RemoteOps |-> 0]
  /\ snapViews = [o \in RemoteOps |-> 0]

\* Local LWW write. In the live Rust test this is node0 setting name="alice".
LocalName ==
  /\ pc["localName"] = "todo"
  /\ name' = NameJoin(name, 2)
  /\ expectedName' = NameJoin(expectedName, 2)
  /\ pc' = [pc EXCEPT !["localName"] = "done"]
  /\ UNCHANGED <<views, expectedViews, snapName, snapViews>>

\* Local counter write. In the live Rust test this is a node incrementing views.
LocalViews ==
  /\ pc["localViews"] = "todo"
  /\ views' = views + 1
  /\ expectedViews' = expectedViews + 1
  /\ pc' = [pc EXCEPT !["localViews"] = "done"]
  /\ UNCHANGED <<name, expectedName, snapName, snapViews>>

\* Remote merge begins by reading the whole materialized document.
RemoteSnap(o) ==
  /\ o \in RemoteOps
  /\ pc[o] = "todo"
  /\ snapName' = [snapName EXCEPT ![o] = name]
  /\ snapViews' = [snapViews EXCEPT ![o] = views]
  /\ pc' = [pc EXCEPT ![o] = "snap"]
  /\ UNCHANGED <<name, views, expectedName, expectedViews>>

\* Merge a remote LWW field. In WholeDoc mode this can clobber views from the stale
\* snapshot; in Componentwise mode it changes only name.
RemoteNameCommit ==
  /\ pc["remoteName"] = "snap"
  /\ IF MergeMode = "WholeDoc"
       THEN /\ name' = NameJoin(snapName["remoteName"], 1)
            /\ views' = snapViews["remoteName"]
       ELSE /\ name' = NameJoin(name, 1)
            /\ views' = views
  /\ expectedName' = NameJoin(expectedName, 1)
  /\ expectedViews' = expectedViews
  /\ pc' = [pc EXCEPT !["remoteName"] = "done"]
  /\ UNCHANGED <<snapName, snapViews>>

\* Merge a remote counter field. In WholeDoc mode this can clobber name from the
\* stale snapshot; in Componentwise mode it changes only views.
RemoteViewsCommit ==
  /\ pc["remoteViews"] = "snap"
  /\ IF MergeMode = "WholeDoc"
       THEN /\ name' = snapName["remoteViews"]
            /\ views' = snapViews["remoteViews"] + 1
       ELSE /\ name' = name
            /\ views' = views + 1
  /\ expectedName' = expectedName
  /\ expectedViews' = expectedViews + 1
  /\ pc' = [pc EXCEPT !["remoteViews"] = "done"]
  /\ UNCHANGED <<snapName, snapViews>>

Next ==
  \/ LocalName
  \/ LocalViews
  \/ RemoteSnap("remoteName")
  \/ RemoteSnap("remoteViews")
  \/ RemoteNameCommit
  \/ RemoteViewsCommit

Spec == Init /\ [][Next]_vars

\* Headline safety: the materialized mixed document equals the independent oracle.
\* WholeDoc mode violates this by clobbering the non-merged field from a stale
\* snapshot. Componentwise mode preserves it in every interleaving.
INV_Exact ==
  /\ name = expectedName
  /\ views = expectedViews

INV_TypeOK == TypeOK
====
