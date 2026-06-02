---- MODULE MC_S4 ----
EXTENDS DagReplication
\* S4: SubDoc field-grain filtering. dX history x0 <- x1f <- x2, all DID X.
\* nr (resource-constrained, DID X) FILTERS OUT field block x1f, but head x2 depends
\* on it. nx (DID X) is the Creator/source holding the full chain.
\*   Naive          -> nr can't merge x2 (field-grain #2721): VisibleConverge FAILS.
\*   FullWalkA (A)  -> nr merges x2 but FETCHED x1f: VisibleConverge holds, NoFilteredFetch FAILS.
\*   FilteredMergeB -> nr merges x2 WITHOUT x1f: both hold (the win; relaxes DagComplete).
mcNodes      == {"nx", "nr"}
mcDIDs       == {"X"}
mcDidOf      == [n \in mcNodes |-> "X"]
mcBlocks     == {"x0", "x1f", "x2"}
mcDoc        == [b \in mcBlocks |-> "dX"]
mcParents    == [b \in mcBlocks |-> CASE b = "x1f" -> {"x0"} [] b = "x2" -> {"x1f"} [] OTHER -> {}]
mcHeads      == {"x2"}
mcOwnerWrite == [b \in mcBlocks |-> "none"]
mcCreateOwner == [d \in {"dX"} |-> "X"]
mcRelRef     == [d \in {"dX"} |-> {}]
mcFiltered   == [n \in mcNodes |-> IF n = "nr" THEN {"x1f"} ELSE {}]
====
