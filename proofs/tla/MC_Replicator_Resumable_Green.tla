---- MODULE MC_Replicator_Resumable_Green ----
EXTENDS Replicator

\* docA exists when the replicator is added. docB is created only after the
\* lifecycle reaches Live, exercising both backfill and live-update delivery.
mcDocs == {"docA", "docB"}
mcBlocks == {"a0", "a1", "b0", "b1"}
mcHeads == {"a1", "b1"}
mcParents == [b \in mcBlocks |->
                CASE b = "a1" -> {"a0"}
                  [] b = "b1" -> {"b0"}
                  [] OTHER -> {}]
mcDoc == [b \in mcBlocks |->
           CASE b \in {"a0", "a1"} -> "docA"
             [] OTHER -> "docB"]
====
