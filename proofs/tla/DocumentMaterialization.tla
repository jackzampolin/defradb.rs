---- MODULE DocumentMaterialization ----
\* Document/composite materialization status under a delete-vs-active-update race.
\*
\* RED: an active composite update snapshots the whole document while it is visible,
\* a delete commits, then the active update commits its stale snapshot and overwrites
\* the deletion marker back to active.
\*
\* GREEN: status is a component of the materialized document. Active updates may
\* update retained field bytes, but they preserve the current deletion marker.
EXTENDS Naturals

CONSTANTS
  StatusMode \* "Overwrite" | "DeletedAbsorbs"

ASSUME StatusMode \in {"Overwrite", "DeletedAbsorbs"}

VARIABLES
  deleted,         \* Bool: query visibility is derived from this marker
  age,             \* Nat: retained materialized field bytes
  expectedDeleted, \* oracle status after committed operations
  expectedAge,     \* oracle bytes after committed active updates
  pc,              \* per-operation phase
  snapDeleted,     \* active update's whole-document status snapshot
  snapAge          \* active update's whole-document field snapshot

vars == <<deleted, age, expectedDeleted, expectedAge, pc, snapDeleted, snapAge>>

Ops == {"delete", "update"}
Phases == {"todo", "snap", "done"}

TypeOK ==
  /\ deleted \in BOOLEAN
  /\ age \in Nat
  /\ expectedDeleted \in BOOLEAN
  /\ expectedAge \in Nat
  /\ pc \in [Ops -> Phases]
  /\ snapDeleted \in BOOLEAN
  /\ snapAge \in Nat

Init ==
  /\ deleted = FALSE
  /\ age = 30
  /\ expectedDeleted = FALSE
  /\ expectedAge = 30
  /\ pc = [o \in Ops |-> "todo"]
  /\ snapDeleted = FALSE
  /\ snapAge = 30

DeleteCommit ==
  /\ pc["delete"] = "todo"
  /\ deleted' = TRUE
  /\ expectedDeleted' = TRUE
  /\ pc' = [pc EXCEPT !["delete"] = "done"]
  /\ UNCHANGED <<age, expectedAge, snapDeleted, snapAge>>

ActiveUpdateSnap ==
  /\ pc["update"] = "todo"
  /\ snapDeleted' = deleted
  /\ snapAge' = age
  /\ pc' = [pc EXCEPT !["update"] = "snap"]
  /\ UNCHANGED <<deleted, age, expectedDeleted, expectedAge>>

ActiveUpdateCommit ==
  /\ pc["update"] = "snap"
  /\ IF StatusMode = "Overwrite"
       THEN deleted' = snapDeleted
       ELSE deleted' = deleted
  /\ age' = 99
  /\ expectedDeleted' = expectedDeleted
  /\ expectedAge' = 99
  /\ pc' = [pc EXCEPT !["update"] = "done"]
  /\ UNCHANGED <<snapDeleted, snapAge>>

Next ==
  \/ DeleteCommit
  \/ ActiveUpdateSnap
  \/ ActiveUpdateCommit

Spec == Init /\ [][Next]_vars

INV_DeletedMarkerAbsorbs == deleted = expectedDeleted

INV_Exact ==
  /\ deleted = expectedDeleted
  /\ age = expectedAge

INV_TypeOK == TypeOK
====
