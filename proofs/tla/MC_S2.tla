---- MODULE MC_S2 ----
EXTENDS DagReplication
\* S2: WholeDoc + Immutable + FullWalkA. nx (DID X) subscribes only to dX and must
\* converge on it WITHOUT ever receiving dY (dX RelRefs dY). ny is the Creator/source
\* (set in MC_S2.cfg); nx is a non-creator so INV_RelRefSafe is checked non-vacuously.
mcNodes      == {"nx", "ny"}
mcDIDs       == {"X", "Y"}
mcDidOf      == [n \in mcNodes |-> IF n = "nx" THEN "X" ELSE "Y"]
mcBlocks     == {"x0", "x1", "y0"}
mcDoc        == [b \in mcBlocks |-> IF b \in {"x0","x1"} THEN "dX" ELSE "dY"]
mcParents    == [b \in mcBlocks |-> CASE b = "x1" -> {"x0"} [] OTHER -> {}]
mcHeads      == {"x1", "y0"}
mcOwnerWrite == [b \in mcBlocks |-> "none"]        \* immutable: no owner rewrites
mcCreateOwner == [d \in {"dX","dY"} |-> IF d = "dX" THEN "X" ELSE "Y"]
mcRelRef     == [d \in {"dX","dY"} |-> IF d = "dX" THEN {"dY"} ELSE {}]
mcFiltered   == [n \in mcNodes |-> {}]
====
