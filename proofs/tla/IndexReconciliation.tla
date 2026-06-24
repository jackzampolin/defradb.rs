---- MODULE IndexReconciliation ----
\* Secondary indexes are a derived materialized view of the winning CRDT value.
\*
\* RED: index maintenance saves the new value but does not remove the old key. A
\* partition/replay can converge the document to the LWW winner while stale index
\* entries still point at the same doc.
\*
\* GREEN: each applied winning value reconciles the index to exactly that value,
\* matching db-index's delete-old-then-save-new maintenance proof.
EXTENDS Naturals, FiniteSets

CONSTANTS
  IndexMode \* "SaveOnly" | "DeleteThenSave"

ASSUME IndexMode \in {"SaveOnly", "DeleteThenSave"}

UpdateValues == {20, 99}
Ages == {10, 20, 99}

VARIABLES
  winner, \* Nat abstraction of the materialized LWW winner
  index,  \* subset of Ages whose index key currently points at this doc
  pc      \* per-update delivery state

vars == <<winner, index, pc>>

TypeOK ==
  /\ winner \in Ages
  /\ index \subseteq Ages
  /\ pc \in [UpdateValues -> {"todo", "done"}]

Init ==
  /\ winner = 10
  /\ index = {10}
  /\ pc = [v \in UpdateValues |-> "todo"]

IndexUpdate(old, new) ==
  IF IndexMode = "SaveOnly"
    THEN index \cup {new}
    ELSE (index \ {old}) \cup {new}

ApplyUpdate(v) ==
  /\ v \in UpdateValues
  /\ pc[v] = "todo"
  /\ IF v > winner
       THEN /\ winner' = v
            /\ index' = IndexUpdate(winner, v)
       ELSE /\ winner' = winner
            /\ index' = index
  /\ pc' = [pc EXCEPT ![v] = "done"]

Next == \E v \in UpdateValues : ApplyUpdate(v)

Spec == Init /\ [][Next]_vars

INV_IndexMatchesWinner == index = {winner}

INV_TypeOK == TypeOK
====
