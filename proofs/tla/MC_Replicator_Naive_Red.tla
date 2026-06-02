---- MODULE MC_Replicator_Naive_Red ----
EXTENDS Replicator

\* One pre-existing document with a two-block history. The red behavior starts
\* the push, receives only part of the history, disconnects, then reconnects.
\* Because Mode = "Naive" does not recompute MissingDocs after the first
\* backfill pass, the head block can remain missing forever.
mcDocs == {"docA"}
mcBlocks == {"a0", "a1"}
mcHeads == {"a1"}
mcParents == [b \in mcBlocks |-> CASE b = "a1" -> {"a0"} [] OTHER -> {}]
mcDoc == [b \in mcBlocks |-> "docA"]
====
