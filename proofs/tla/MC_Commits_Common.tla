---- MODULE MC_Commits_Common ----
EXTENDS Commits

mcReaders == {"owner", "eve"}
mcDocs == {"doc"}
mcBlocks == {"create", "update"}
mcDocOfBlock == [b \in mcBlocks |-> "doc"]
mcGrant == [d \in mcDocs |-> {"owner"}]
mcInitialBlockHolders ==
  [r \in mcReaders |-> IF r = "owner" THEN mcBlocks ELSE {}]
====
