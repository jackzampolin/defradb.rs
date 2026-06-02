---- MODULE MC_S3 ----
EXTENDS DagReplication
\* S3: WholeDoc + FullWalkA, testing filter-key stability. dX history x0<-x1<-x2.
\* Mutable case: x2 REWRITES owner X->Y. nx (DID X, old owner) already replicated
\* the pre-reassignment blocks {x0,x1}; src (DID S, a neutral source/reassigner)
\* holds the full chain incl x2; ny (DID Y, new owner) starts empty.
\* Under sender-side DID-filtering, the reassignment x2 reaches ny but NOT nx, so
\* both nx (view X) and ny (view Y) end up "owning" dX -> split. Making the key
\* immutable (no owner rewrite) eliminates it.
\* The two cfgs (MC_S3.cfg RED / MC_S3_Fixed.cfg GREEN) differ only in OwnerWrite.
\* Only INV_NoSplitOwnership (+TypeOK) is checked: S3 is about ownership divergence,
\* not convergence; mcRelRef is empty so INV_RelRefSafe doesn't apply here.
mcNodes      == {"nx", "ny", "src"}
mcDIDs       == {"X", "Y", "S"}
mcDidOf      == [n \in mcNodes |-> CASE n = "nx" -> "X" [] n = "ny" -> "Y" [] OTHER -> "S"]
mcBlocks     == {"x0", "x1", "x2"}
mcDoc        == [b \in mcBlocks |-> "dX"]
mcParents    == [b \in mcBlocks |-> CASE b = "x1" -> {"x0"} [] b = "x2" -> {"x1"} [] OTHER -> {}]
mcHeads      == {"x2"}
mcCreateOwner == [d \in {"dX"} |-> "X"]
mcRelRef     == [d \in {"dX"} |-> {}]

\* Mutable: x2 carries an owner rewrite X -> Y.
mcOwnerWrite          == [b \in mcBlocks |-> IF b = "x2" THEN "Y" ELSE "none"]
\* Immutable: no block rewrites the owner.
mcOwnerWriteImmutable == [b \in mcBlocks |-> "none"]

\* Custom initial state: nx has ALREADY replicated the pre-reassignment blocks
\* {x0,x1}; src holds the full chain; ny is empty. (Represents the moment right
\* after reassignment, before delivery — the configuration where the hazard lives.)
InitS3 ==
  /\ have   = [n \in mcNodes |-> CASE n = "src" -> mcBlocks
                                   [] n = "nx"  -> {"x0", "x1"}
                                   [] OTHER     -> {}]
  /\ merged = [n \in mcNodes |-> CASE n = "src" -> mcBlocks
                                   [] n = "nx"  -> {"x0", "x1"}
                                   [] OTHER     -> {}]
  \* wanted is empty for all: nx already completed the fetch/merge cycle for {x0,x1}
  \* (Merge clears wanted), and the other nodes have not started one.
  /\ wanted = [n \in mcNodes |-> {}]

mcFiltered == [n \in mcNodes |-> {}]

SpecS3 == InitS3 /\ [][Next]_vars /\ Fairness
====
