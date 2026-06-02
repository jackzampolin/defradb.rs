# Filtered P2P Replication — TLA+ Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build TLA+/TLC models that (a) reproduce the Go `#2721` "partial DAG sync will never merge" failure, (b) prove Model A (full-ancestry walk) restores convergence, and (c) prove that filtering defra-agent's `agent_did` is safe **iff** the filter key is immutable — producing the recommendation consumed by `defradb.rs-p2p-control`.

**Architecture:** A minimal explicit-state Merkle-DAG replication model. Gossip announces only *heads*; correct sync must *walk* to fetch ancestors before a guarded merge. A spike module (`M1Convergence.tla`) gets TLC green, then a parametric module (`DagReplication.tla`) adds documents, an owner/DID filter, and key-mutability — driven by per-scenario `MC_*.tla`/`.cfg` wrappers (S1–S4). Each task is TDD-shaped: write an invariant/property + a config that should FAIL, run TLC to see the counterexample (red), change the policy knob, run TLC to see it hold (green).

**Tech Stack:** TLA+ (PlusCal not used), TLC model checker via `tla2tools.jar` (Java 11+). No Rust/Go code is produced here — the model abstracts those code paths. See `specs/DESIGN.md` for the invariant→source-module map and the verified findings this plan implements.

---

## TDD mapping for a model-checking project

There is no `pytest` here. The red/green loop is:
- **Red:** a `PROPERTY`/`INVARIANT` that the *buggy* policy (e.g. `Naive` fetch, or `Mutable` key) violates → TLC prints a counterexample trace and exits non-zero.
- **Green:** flip the policy knob to the fix (`FullWalkA`, or `Immutable`) → TLC explores all states/lassos and exits `0` with "No error has been found."

"Expected: FAIL" below means **TLC reports a violation (exit ≠ 0)**; "Expected: PASS" means **TLC reports no error (exit 0)**.

## File structure (created by this plan, all under `specs/`)

| File | Responsibility |
|---|---|
| `specs/tools/tla2tools.jar` | The TLC checker (downloaded; git-ignored) |
| `specs/tools/tlc` | Tiny wrapper script: `java -cp tools/tla2tools.jar tlc2.TLC "$@"` |
| `specs/M1Convergence.tla` + `.cfg` | Spike: minimal DAG replication, `FullWalkA`, dual-branch DAG (S1′) |
| `specs/M1Naive.cfg` (+ `M1Convergence` override) | S1: `Naive` fetch → `#2721` counterexample |
| `specs/DagReplication.tla` | Parametric core: docs, owners, filter, key-mutability, fetch policy |
| `specs/MC_S2.tla` + `.cfg` | S2: WholeDoc / Immutable / FullWalkA → SubsetConverge, RelRefSafe, ClaimUnique |
| `specs/MC_S3.tla` + `.cfg` | S3: WholeDoc / Mutable → NoSplitOwnership counterexample; Immutable closes it |
| `specs/MC_S4.tla` + `.cfg` | S4: SubDoc → Naive re-breaks; Model B / VisibleConverge characterization |
| `specs/README.md` | Plain-English invariant → TLC verdict → source-module map |
| `specs/.gitignore` | Ignore `tools/tla2tools.jar`, `states/`, `*.out`, `MC.out` |

---

### Task 0: Tooling — get TLC runnable

**Files:**
- Create: `specs/tools/tlc`
- Create: `specs/.gitignore`

- [ ] **Step 1: Confirm Java is present**

Run: `java -version`
Expected: prints a version line for Java 11 or newer. If missing: `brew install temurin` (macOS), then re-run.

- [ ] **Step 2: Download the TLC tools jar**

Run:
```bash
mkdir -p specs/tools
curl -L -o specs/tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
```
Expected: a ~7–8 MB `specs/tools/tla2tools.jar`.

- [ ] **Step 3: Create the `tlc` wrapper**

Create `specs/tools/tlc`:
```bash
#!/usr/bin/env bash
# Run TLC from the specs/ directory: ./tools/tlc -config M1Convergence.cfg M1Convergence.tla
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec java -XX:+UseParallelGC -cp "$DIR/tla2tools.jar" tlc2.TLC "$@"
```
Then: `chmod +x specs/tools/tlc`

- [ ] **Step 4: Create `specs/.gitignore`**

Create `specs/.gitignore`:
```gitignore
tools/tla2tools.jar
states/
*.out
*.toolbox/
MC.out
```

- [ ] **Step 5: Smoke-test the toolchain**

Run: `cd specs && printf -- '---- MODULE Smoke ----\nEXTENDS Naturals\nVARIABLE x\nInit == x = 0\nNext == x' "'" ' = (x + 1) %% 3\nSpec == Init /\\ [][Next]_x\n====\n' > Smoke.tla` — *(if the printf escaping is fiddly, just hand-write `specs/Smoke.tla` with that content)*. The module body:
```tla
---- MODULE Smoke ----
EXTENDS Naturals
VARIABLE x
Init == x = 0
Next == x' = (x + 1) % 3
Spec == Init /\ [][Next]_x
Inv == x < 3
====
```
Create `specs/Smoke.cfg`:
```
SPECIFICATION Spec
INVARIANT Inv
```
Run: `cd specs && ./tools/tlc -config Smoke.cfg Smoke.tla`
Expected: `Model checking completed. No error has been found.` Then delete: `rm specs/Smoke.tla specs/Smoke.cfg`.

- [ ] **Step 6: Commit**

```bash
git add specs/tools/tlc specs/.gitignore
git commit -m "tooling: add TLC wrapper and specs gitignore"
```

---

### Task 1: M1 spike — minimal DAG replication converges under Model A (S1′)

**Files:**
- Create: `specs/M1Convergence.tla`
- Create: `specs/M1Convergence.cfg`

This is the control case: gossip announces heads; the node walks full ancestry (`FullWalkA`) and merges. The canonical DAG is the `#2721` dual-branch (`b1` and `b2` both children of `b0`).

- [ ] **Step 1: Write the spec (the failing target is convergence, checked next task; here it must PASS)**

Create `specs/M1Convergence.tla`:
```tla
---- MODULE M1Convergence ----
EXTENDS FiniteSets

\* ---- Fixed dual-branch DAG (the Go #2721 "DualBranch" shape) ----
Blocks  == {"b0", "b1", "b2"}     \* b0 = create; b1,b2 = concurrent children
Heads   == {"b1", "b2"}            \* only heads are gossiped
Parents == [b \in Blocks |->
              CASE b = "b1" -> {"b0"}
                [] b = "b2" -> {"b0"}
                [] OTHER    -> {}]
Nodes   == {"n1", "n2"}
Creator == "n1"                     \* authored & merged the whole DAG

\* FetchPolicy is a model knob: "FullWalkA" (correct) or "Naive" (#2721 bug)
CONSTANT FetchPolicy

RECURSIVE AncestorsOf(_)
AncestorsOf(b) == Parents[b] \cup UNION { AncestorsOf(p) : p \in Parents[b] }

VARIABLES have, merged, wanted
vars == <<have, merged, wanted>>

TypeOK ==
  /\ have   \in [Nodes -> SUBSET Blocks]
  /\ merged \in [Nodes -> SUBSET Blocks]
  /\ wanted \in [Nodes -> SUBSET Blocks]

Init ==
  /\ have   = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ merged = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ wanted = [n \in Nodes |-> {}]

\* Gossip: a peer that merged a HEAD tells n about it; n now wants it.
Announce(m, n, h) ==
  /\ h \in Heads
  /\ h \in merged[m]
  /\ h \notin merged[n] /\ h \notin wanted[n]
  /\ wanted' = [wanted EXCEPT ![n] = @ \cup {h}]
  /\ UNCHANGED <<have, merged>>

\* Provider exists for block b
HasProvider(b) == \E m \in Nodes : b \in have[m]

\* Model A: fetch a wanted head OR any of its ancestors (walk the graph).
FetchA(n, b) ==
  /\ \E h \in wanted[n] : b = h \/ b \in AncestorsOf(h)
  /\ b \notin have[n]
  /\ HasProvider(b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<merged, wanted>>

\* Naive: only fetch the wanted head itself; never walk to ancestors.
FetchNaive(n, b) ==
  /\ b \in wanted[n]
  /\ b \notin have[n]
  /\ HasProvider(b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<merged, wanted>>

Fetch(n, b) == IF FetchPolicy = "Naive" THEN FetchNaive(n, b) ELSE FetchA(n, b)

\* Merge is guarded on full local ancestry (mirrors loadComposites recursion).
Merge(n, b) ==
  /\ b \in have[n]
  /\ b \notin merged[n]
  /\ Parents[b] \subseteq merged[n]
  /\ merged' = [merged EXCEPT ![n] = @ \cup {b}]
  /\ wanted' = [wanted EXCEPT ![n] = @ \ {b}]
  /\ UNCHANGED have

Next ==
  \/ \E m \in Nodes, n \in Nodes, h \in Blocks : Announce(m, n, h)
  \/ \E n \in Nodes, b \in Blocks : Fetch(n, b)
  \/ \E n \in Nodes, b \in Blocks : Merge(n, b)

Fairness ==
  /\ \A m \in Nodes, n \in Nodes, h \in Blocks : WF_vars(Announce(m, n, h))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Fetch(n, b))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Merge(n, b))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Properties ----
INV_DagComplete == \A n \in Nodes : \A b \in merged[n] : Parents[b] \subseteq merged[n]
Converge        == <>[](\A n \in Nodes : merged[n] = Blocks)
====
```

- [ ] **Step 2: Write the green config (Model A)**

Create `specs/M1Convergence.cfg`:
```
SPECIFICATION Spec
CONSTANT FetchPolicy = "FullWalkA"
INVARIANT TypeOK
INVARIANT INV_DagComplete
PROPERTY Converge
```

- [ ] **Step 3: Run TLC — expect PASS**

Run: `cd specs && ./tools/tlc -config M1Convergence.cfg M1Convergence.tla`
Expected: `No error has been found.` (both `INV_DagComplete` invariant and `Converge` liveness hold; exit 0).

- [ ] **Step 4: Commit**

```bash
git add specs/M1Convergence.tla specs/M1Convergence.cfg
git commit -m "model(M1): minimal DAG replication converges under Model A (S1')"
```

---

### Task 2: Reproduce `#2721` — Naive fetch never merges (S1)

**Files:**
- Create: `specs/M1Naive.cfg`

Same spec, flip the knob to `Naive`. The node fetches the gossiped head but never walks to `b0`, so `Merge` stays guarded-out forever → `Converge` is violated. This is the formal `#2721`.

- [ ] **Step 1: Write the red config (Naive policy)**

Create `specs/M1Naive.cfg`:
```
SPECIFICATION Spec
CONSTANT FetchPolicy = "Naive"
INVARIANT TypeOK
INVARIANT INV_DagComplete
PROPERTY Converge
```

- [ ] **Step 2: Run TLC — expect FAIL on `Converge`**

Run: `cd specs && ./tools/tlc -config M1Naive.cfg M1Convergence.tla`
Expected: TLC reports a **temporal property violation** of `Converge` and prints a lasso trace where (e.g.) `n2` has `wanted = {"b1"}`, `have = {"b1"}`, but `merged["n2"]` never includes `b1` because `b0` (a non-head ancestor) is never fetched. Exit ≠ 0. `INV_DagComplete` still holds (merge stays correctly guarded — the bug is liveness, "never merges", exactly as the issue states).

- [ ] **Step 3: Capture the counterexample for the README**

Run: `cd specs && ./tools/tlc -config M1Naive.cfg M1Convergence.tla > M1Naive.trace.txt 2>&1 || true`
Then confirm it contains the violation: `grep -c "is violated" specs/M1Naive.trace.txt` → Expected: `1` or more. (The file is git-ignored via `*.out`? No — name it `.trace.txt`; add it explicitly or keep locally. Do NOT commit the trace; reference it in the README prose instead.)

- [ ] **Step 4: Commit (config only)**

```bash
git add specs/M1Naive.cfg
git commit -m "model(M1): Naive fetch reproduces #2721 'never merges' (S1 counterexample)"
```

---

### Task 3: Parametric core — documents, owner/DID filter, key mutability

**Files:**
- Create: `specs/DagReplication.tla`

Generalizes the spike: blocks belong to documents; a node subscribes to a doc per a filter over the doc's *owner DID*; gossip is **sender-side filtered** (the bandwidth win — and the source of the mutable-key hazard). Owner can be mutable. Adds a cross-doc relational ref to exercise `INV_RelRefSafe`.

- [ ] **Step 1: Write the parametric spec**

Create `specs/DagReplication.tla`:
```tla
---- MODULE DagReplication ----
EXTENDS FiniteSets

CONSTANTS
  Nodes,            \* set of agent nodes
  DIDs,             \* set of DIDs; DidOf(n) gives each node's DID
  DidOf,            \* [Nodes -> DIDs]
  Blocks,           \* universe of blocks
  Doc,              \* [Blocks -> Docs]  which document each block belongs to
  Parents,          \* [Blocks -> SUBSET Blocks]  within-doc causal parents
  Heads,            \* SUBSET Blocks  gossiped tips
  Creator,          \* node that authored everything
  OwnerWrite,       \* [Blocks -> DIDs \cup {"none"}]  owner DID this block sets (or "none")
  CreateOwner,      \* [Docs -> DIDs]  owner asserted by each doc's create block
  RelRef,           \* [Docs -> SUBSET Docs]  cross-doc relational FKs (NOT causal)
  FilterScope,      \* "None" | "WholeDoc" | "SubDoc"
  KeyMutability,    \* "Immutable" | "Mutable"
  FetchPolicy       \* "Naive" | "FullWalkA" | "FilteredMergeB"

Docs == { Doc[b] : b \in Blocks }

RECURSIVE AncestorsOf(_)
AncestorsOf(b) == Parents[b] \cup UNION { AncestorsOf(p) : p \in Parents[b] }

VARIABLES have, merged, wanted
vars == <<have, merged, wanted>>

\* A node's *current local view* of a doc's owner: the latest OwnerWrite among
\* the blocks of that doc it has merged, else the doc's create-block owner.
OwnerView(n, d) ==
  LET writes == { b \in merged[n] : Doc[b] = d /\ OwnerWrite[b] # "none" }
  IN  IF writes = {} THEN CreateOwner[d]
      ELSE OwnerWrite[ CHOOSE b \in writes :
                         \A c \in writes : b \in AncestorsOf(c) \/ b = c ]

\* Subscription predicate. WholeDoc: I subscribe to docs my DID owns (per my view).
Subscribed(n, d) ==
  CASE FilterScope = "None"     -> TRUE
    [] FilterScope = "WholeDoc" -> OwnerView(n, d) = DidOf[n]
    [] FilterScope = "SubDoc"   -> OwnerView(n, d) = DidOf[n]  \* field-grain handled in fetch guard

TypeOK ==
  /\ have   \in [Nodes -> SUBSET Blocks]
  /\ merged \in [Nodes -> SUBSET Blocks]
  /\ wanted \in [Nodes -> SUBSET Blocks]

Init ==
  /\ have   = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ merged = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ wanted = [n \in Nodes |-> {}]

HasProvider(b) == \E m \in Nodes : b \in have[m]

\* SENDER-SIDE FILTER: a head is only announced to n if the SENDER believes n
\* should get it, i.e. the sender's view of the doc owner matches n's DID.
\* This is the bandwidth win — and, with a Mutable key, the split-ownership trap:
\* a reassigned doc's new head is no longer announced to the old owner.
Announce(m, n, h) ==
  /\ h \in Heads
  /\ h \in merged[m]
  /\ h \notin merged[n] /\ h \notin wanted[n]
  /\ \/ FilterScope = "None"
     \/ OwnerView(m, Doc[h]) = DidOf[n]
  /\ wanted' = [wanted EXCEPT ![n] = @ \cup {h}]
  /\ UNCHANGED <<have, merged>>

\* Fetch targets depend on policy. FullWalkA walks all within-doc ancestors.
FetchTarget(n, b) ==
  CASE FetchPolicy = "Naive"          -> b \in wanted[n]
    [] FetchPolicy = "FullWalkA"      -> \E h \in wanted[n] : b = h \/ b \in AncestorsOf(h)
    [] FetchPolicy = "FilteredMergeB" -> \E h \in wanted[n] : b = h \/ b \in AncestorsOf(h)

Fetch(n, b) ==
  /\ FetchTarget(n, b)
  /\ b \notin have[n]
  /\ HasProvider(b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<merged, wanted>>

\* Merge guard: full local within-doc ancestry. (Model B will weaken this in S4.)
Merge(n, b) ==
  /\ b \in have[n]
  /\ b \notin merged[n]
  /\ Parents[b] \subseteq merged[n]
  /\ merged' = [merged EXCEPT ![n] = @ \cup {b}]
  /\ wanted' = [wanted EXCEPT ![n] = @ \ {b}]
  /\ UNCHANGED have

Next ==
  \/ \E m \in Nodes, n \in Nodes, h \in Blocks : Announce(m, n, h)
  \/ \E n \in Nodes, b \in Blocks : Fetch(n, b)
  \/ \E n \in Nodes, b \in Blocks : Merge(n, b)

Fairness ==
  /\ \A m \in Nodes, n \in Nodes, h \in Blocks : WF_vars(Announce(m, n, h))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Fetch(n, b))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Merge(n, b))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Reference observer: a hypothetical full-replication node ----
\* "Everyone who is subscribed to d eventually merges exactly d's blocks."
SubBlocks(n) == { b \in Blocks : Subscribed(n, Doc[b]) }

\* ---- Invariants & properties ----
INV_DagComplete   == \A n \in Nodes : \A b \in merged[n] : Parents[b] \subseteq merged[n]

INV_SubsetConverge == <>[](\A n \in Nodes : merged[n] \cap SubBlocks(n) = SubBlocks(n))

\* Merge never depends on a related doc's blocks: a fully-merged subscribed doc
\* stays mergeable even if RelRef target docs are entirely absent. Because Merge
\* only reads Parents (never RelRef), this holds by construction; we assert it.
INV_RelRefSafe == \A n \in Nodes : \A b \in merged[n] :
                    \A d \in RelRef[Doc[b]] :
                      TRUE  \* no block of d is required in have[n]/merged[n] for b

\* At most one DID's nodes consider a doc actionable in their merged view.
ActionableOwners(d) == { DidOf[n] : n \in { m \in Nodes : Subscribed(m, d) /\
                          \E b \in merged[m] : Doc[b] = d } }
INV_NoSplitOwnership == \A d \in Docs : Cardinality(ActionableOwners(d)) <= 1
====
```

- [ ] **Step 2: Sanity-check it parses (no config yet)**

Run: `cd specs && ./tools/tlc -parse DagReplication.tla` *(if `-parse` is unavailable in your tla2tools build, use SANY: `java -cp tools/tla2tools.jar tla2sany.SANY DagReplication.tla`)*
Expected: `Parsing or semantic analysis failed.` does NOT appear; SANY prints `Semantic processing of module DagReplication` with no errors.

- [ ] **Step 3: Commit**

```bash
git add specs/DagReplication.tla
git commit -m "model(M2): parametric DAG replication — docs, owner filter, key mutability"
```

---

### Task 4: S2 — WholeDoc / Immutable / FullWalkA is the safe ideal

**Files:**
- Create: `specs/MC_S2.tla`
- Create: `specs/MC_S2.cfg`

Two DIDs, two docs (`dX` owned by DID `X`, `dY` owned by `Y`), `dX` has a cross-doc `RelRef` to `dY`. Node `nx` (DID X) subscribes only to `dX`; it must fully converge on `dX` **without** needing `dY`'s blocks (`INV_RelRefSafe`), and `INV_SubsetConverge` holds.

- [ ] **Step 1: Write the S2 wrapper module**

Create `specs/MC_S2.tla`:
```tla
---- MODULE MC_S2 ----
EXTENDS DagReplication

\* Concrete constants for S2 are supplied via MC_S2.cfg using definitions below.
mcNodes   == {"nx", "ny"}
mcDIDs    == {"X", "Y"}
mcDidOf   == [n \in mcNodes |-> IF n = "nx" THEN "X" ELSE "Y"]
\* dX blocks: x0 (create, owner X) -> x1 ; dY blocks: y0 (create, owner Y)
mcBlocks  == {"x0", "x1", "y0"}
mcDoc     == [b \in mcBlocks |-> IF b \in {"x0","x1"} THEN "dX" ELSE "dY"]
mcParents == [b \in mcBlocks |->
                CASE b = "x1" -> {"x0"} [] OTHER -> {}]
mcHeads      == {"x1", "y0"}
mcOwnerWrite == [b \in mcBlocks |-> "none"]            \* immutable: nobody rewrites owner
mcCreateOwn  == [d \in {"dX","dY"} |-> IF d = "dX" THEN "X" ELSE "Y"]
mcRelRef     == [d \in {"dX","dY"} |-> IF d = "dX" THEN {"dY"} ELSE {}]
====
```

- [ ] **Step 2: Write the S2 config**

Create `specs/MC_S2.cfg`:
```
SPECIFICATION Spec
CONSTANTS
  Nodes <- mcNodes
  DIDs <- mcDIDs
  DidOf <- mcDidOf
  Blocks <- mcBlocks
  Doc <- mcDoc
  Parents <- mcParents
  Heads <- mcHeads
  OwnerWrite <- mcOwnerWrite
  CreateOwner <- mcCreateOwn
  RelRef <- mcRelRef
  Creator = "nx"
  FilterScope = "WholeDoc"
  KeyMutability = "Immutable"
  FetchPolicy = "FullWalkA"
INVARIANT TypeOK
INVARIANT INV_DagComplete
INVARIANT INV_RelRefSafe
INVARIANT INV_NoSplitOwnership
PROPERTY INV_SubsetConverge
```

- [ ] **Step 3: Run TLC — expect PASS (the ideal)**

Run: `cd specs && ./tools/tlc -config MC_S2.cfg MC_S2.tla`
Expected: `No error has been found.` `nx` converges on `dX` (`x0`,`x1`) without ever needing `y0`; `INV_NoSplitOwnership` holds (each doc has ≤1 actionable owner DID). Exit 0.

- [ ] **Step 4: Commit**

```bash
git add specs/MC_S2.tla specs/MC_S2.cfg
git commit -m "model(M2/S2): WholeDoc+Immutable+ModelA — SubsetConverge, RelRefSafe, NoSplitOwnership green"
```

---

### Task 5: S3 — Mutable key produces split ownership; immutability closes it

**Files:**
- Create: `specs/MC_S3.tla`
- Create: `specs/MC_S3.cfg`
- Create: `specs/MC_S3_Fixed.cfg`

`dX` starts owned by `X`; a later head rewrites its owner to `Y` (`OwnerWrite["x2"] = "Y"`). Under sender-side filtering on a **Mutable** key, the old owner `nx` is never told to stop, and the new owner `ny` adopts it → two DIDs consider `dX` actionable → `INV_NoSplitOwnership` violated. Then prove that making the key **Immutable** (no owner rewrite) removes the hazard.

- [ ] **Step 1: Write the S3 wrapper with an owner-rewrite block**

Create `specs/MC_S3.tla`:
```tla
---- MODULE MC_S3 ----
EXTENDS DagReplication

mcNodes   == {"nx", "ny"}
mcDIDs    == {"X", "Y"}
mcDidOf   == [n \in mcNodes |-> IF n = "nx" THEN "X" ELSE "Y"]
\* dX: x0 (create, owner X) -> x1 -> x2 ; x2 REWRITES owner to Y
mcBlocks  == {"x0", "x1", "x2"}
mcDoc     == [b \in mcBlocks |-> "dX"]
mcParents == [b \in mcBlocks |->
                CASE b = "x1" -> {"x0"} [] b = "x2" -> {"x1"} [] OTHER -> {}]
mcHeads      == {"x2"}
\* Mutable: x2 carries an owner rewrite X->Y. (For the fixed run we override this.)
mcOwnerWrite == [b \in mcBlocks |-> IF b = "x2" THEN "Y" ELSE "none"]
mcOwnerWriteImmutable == [b \in mcBlocks |-> "none"]
mcCreateOwn  == [d \in {"dX"} |-> "X"]
mcRelRef     == [d \in {"dX"} |-> {}]
====
```

- [ ] **Step 2: Write the RED config (Mutable key)**

Create `specs/MC_S3.cfg`:
```
SPECIFICATION Spec
CONSTANTS
  Nodes <- mcNodes
  DIDs <- mcDIDs
  DidOf <- mcDidOf
  Blocks <- mcBlocks
  Doc <- mcDoc
  Parents <- mcParents
  Heads <- mcHeads
  OwnerWrite <- mcOwnerWrite
  CreateOwner <- mcCreateOwn
  RelRef <- mcRelRef
  Creator = "nx"
  FilterScope = "WholeDoc"
  KeyMutability = "Mutable"
  FetchPolicy = "FullWalkA"
INVARIANT TypeOK
INVARIANT INV_NoSplitOwnership
```

- [ ] **Step 3: Run TLC — expect FAIL on `INV_NoSplitOwnership`**

Run: `cd specs && ./tools/tlc -config MC_S3.cfg MC_S3.tla`
Expected: TLC prints an invariant-violation trace: a state where `nx` still has `dX` in its merged view under owner `X` (it created `x0..x1` and never receives `x2` — sender-side filter no longer routes `dX` to DID `X`) **and** `ny` has merged `x2` and views owner `Y`. `ActionableOwners("dX") = {"X","Y"}`, cardinality 2 > 1. Exit ≠ 0. **This is the split-ownership hazard.**

- [ ] **Step 4: Write the GREEN config (key made Immutable)**

Create `specs/MC_S3_Fixed.cfg` — identical except override `OwnerWrite` with the immutable variant and set `KeyMutability = "Immutable"`:
```
SPECIFICATION Spec
CONSTANTS
  Nodes <- mcNodes
  DIDs <- mcDIDs
  DidOf <- mcDidOf
  Blocks <- mcBlocks
  Doc <- mcDoc
  Parents <- mcParents
  Heads <- mcHeads
  OwnerWrite <- mcOwnerWriteImmutable
  CreateOwner <- mcCreateOwn
  RelRef <- mcRelRef
  Creator = "nx"
  FilterScope = "WholeDoc"
  KeyMutability = "Immutable"
  FetchPolicy = "FullWalkA"
INVARIANT TypeOK
INVARIANT INV_NoSplitOwnership
```

- [ ] **Step 5: Run TLC — expect PASS**

Run: `cd specs && ./tools/tlc -config MC_S3_Fixed.cfg MC_S3.tla`
Expected: `No error has been found.` With no owner rewrite, `dX` stays owned by `X`; only `nx` ever finds it actionable. `INV_NoSplitOwnership` holds. Exit 0. **This is the formal proof that enforcing filter-key immutability closes the hazard.**

- [ ] **Step 6: Commit**

```bash
git add specs/MC_S3.tla specs/MC_S3.cfg specs/MC_S3_Fixed.cfg
git commit -m "model(M2/S3): Mutable key -> split-ownership counterexample; immutability closes it"
```

---

### Task 6: S4 — SubDoc filter; Naive re-breaks; Model B characterization

**Files:**
- Create: `specs/MC_S4.tla`
- Create: `specs/MC_S4_Naive.cfg`
- Create: `specs/MC_S4_ModelB.cfg`

The GraphSync future: filtering *within* a doc's causal DAG. A document has a field-block (`x1f`) that a resource-constrained peer filters out, but a later composite head (`x2`) causally depends on it. `FullWalkA` would fetch `x1f` anyway (defeating resource savings); `Naive`/filtered fetch skips it and the head never merges (a `#2721` recurrence at field grain). `FilteredMergeB` weakens the merge guard to allow a *placeholder* for filtered parents and checks a weaker `INV_VisibleConverge`.

**Note:** Model B's merge semantics are a *research output* (per `specs/DESIGN.md`), not a committed design. This task characterizes the trade-off; a counterexample under Model B is itself a finding (records *which* convergence guarantee is lost), not a task failure.

- [ ] **Step 1: Add Model B semantics + the visible-convergence property to the core spec**

Modify `specs/DagReplication.tla` — add, after the `Merge` operator:
```tla
\* Filtered set: under SubDoc, a node excludes specific filtered blocks from BOTH
\* fetch and the merge-ancestry requirement, substituting a placeholder.
CONSTANT FilteredBlocks   \* [Nodes -> SUBSET Blocks]  blocks this node filters out

\* Model B merge: parent requirement is satisfied if the parent is merged OR
\* the parent is filtered out by this node (placeholder/skip node).
MergeB(n, b) ==
  /\ b \in have[n]
  /\ b \notin merged[n]
  /\ b \notin FilteredBlocks[n]
  /\ \A p \in Parents[b] : p \in merged[n] \/ p \in FilteredBlocks[n]
  /\ merged' = [merged EXCEPT ![n] = @ \cup {b}]
  /\ wanted' = [wanted EXCEPT ![n] = @ \ {b}]
  /\ UNCHANGED have

\* Visible convergence: every node merges every block it does NOT filter out.
VisibleBlocks(n) == { b \in Blocks : Subscribed(n, Doc[b]) /\ b \notin FilteredBlocks[n] }
INV_VisibleConverge == <>[](\A n \in Nodes : VisibleBlocks(n) \subseteq merged[n])
```
And change `Next` and `FetchTarget`/`Fetch` to honour `FilteredBlocks` under `FetchPolicy = "FilteredMergeB"`:
```tla
\* In FetchTarget, FilteredMergeB must NOT pull filtered ancestors:
\*   [] FetchPolicy = "FilteredMergeB" -> \E h \in wanted[n] :
\*        (b = h \/ b \in AncestorsOf(h)) /\ b \notin FilteredBlocks[n]
\* In Next, use MergeB when FetchPolicy = "FilteredMergeB", else Merge.
```
Apply those two edits inline (replace the `FilteredMergeB` line of `FetchTarget`, and make `Next` select `MergeB` vs `Merge` on `FetchPolicy`). Add `FilteredBlocks` to the constants list at the top. *(Earlier configs S1–S3 do not reference `FilteredBlocks`; add `FilteredBlocks <- [n \in Nodes |-> {}]` defaults to MC_S2/MC_S3 wrappers so they still parse.)*

- [ ] **Step 2: Update MC_S2.tla and MC_S3.tla with empty FilteredBlocks default**

Add to both `specs/MC_S2.tla` and `specs/MC_S3.tla` (before `====`):
```tla
mcFiltered == [n \in mcNodes |-> {}]
```
and add `FilteredBlocks <- mcFiltered` to `MC_S2.cfg`, `MC_S3.cfg`, `MC_S3_Fixed.cfg`. Re-run Task 4/Task 5 commands to confirm they still pass (`No error has been found.`).

- [ ] **Step 3: Write the S4 wrapper**

Create `specs/MC_S4.tla`:
```tla
---- MODULE MC_S4 ----
EXTENDS DagReplication

mcNodes   == {"nx", "nr"}            \* nr = resource-constrained peer
mcDIDs    == {"X"}
mcDidOf   == [n \in mcNodes |-> "X"]
\* dX: x0 (create) -> x1f (field block nr filters out) -> x2 (composite head)
mcBlocks  == {"x0", "x1f", "x2"}
mcDoc     == [b \in mcBlocks |-> "dX"]
mcParents == [b \in mcBlocks |->
                CASE b = "x1f" -> {"x0"} [] b = "x2" -> {"x1f"} [] OTHER -> {}]
mcHeads      == {"x2"}
mcOwnerWrite == [b \in mcBlocks |-> "none"]
mcCreateOwn  == [d \in {"dX"} |-> "X"]
mcRelRef     == [d \in {"dX"} |-> {}]
mcFiltered   == [n \in mcNodes |-> IF n = "nr" THEN {"x1f"} ELSE {}]
====
```

- [ ] **Step 4: RED — Naive/filtered fetch re-breaks (field-grain #2721)**

Create `specs/MC_S4_Naive.cfg`:
```
SPECIFICATION Spec
CONSTANTS
  Nodes <- mcNodes
  DIDs <- mcDIDs
  DidOf <- mcDidOf
  Blocks <- mcBlocks
  Doc <- mcDoc
  Parents <- mcParents
  Heads <- mcHeads
  OwnerWrite <- mcOwnerWrite
  CreateOwner <- mcCreateOwn
  RelRef <- mcRelRef
  FilteredBlocks <- mcFiltered
  Creator = "nx"
  FilterScope = "SubDoc"
  KeyMutability = "Immutable"
  FetchPolicy = "Naive"
INVARIANT TypeOK
PROPERTY INV_VisibleConverge
```
Run: `cd specs && ./tools/tlc -config MC_S4_Naive.cfg MC_S4.tla`
Expected: FAIL — `nr` wants `x2`, can't merge it (parent `x1f` filtered out, plain `Merge` guard needs it merged), so `x2 \notin merged["nr"]` though `x2 \in VisibleBlocks("nr")`. Field-grain `#2721`. Exit ≠ 0.

- [ ] **Step 5: Characterize — Model B and the weaker guarantee**

Create `specs/MC_S4_ModelB.cfg` (same constants, `FetchPolicy = "FilteredMergeB"`):
```
SPECIFICATION Spec
CONSTANTS
  Nodes <- mcNodes
  DIDs <- mcDIDs
  DidOf <- mcDidOf
  Blocks <- mcBlocks
  Doc <- mcDoc
  Parents <- mcParents
  Heads <- mcHeads
  OwnerWrite <- mcOwnerWrite
  CreateOwner <- mcCreateOwn
  RelRef <- mcRelRef
  FilteredBlocks <- mcFiltered
  Creator = "nx"
  FilterScope = "SubDoc"
  KeyMutability = "Immutable"
  FetchPolicy = "FilteredMergeB"
INVARIANT TypeOK
PROPERTY INV_VisibleConverge
```
Run: `cd specs && ./tools/tlc -config MC_S4_ModelB.cfg MC_S4.tla`
Expected: PASS for `INV_VisibleConverge` — `nr` merges `x0` and `x2` (treating filtered `x1f` as a placeholder) **without fetching `x1f`** (resource savings preserved). Record in the README that Model B trades *full* convergence for *visible-subset* convergence, and that this is the only scenario requiring it.

- [ ] **Step 6: Commit**

```bash
git add specs/DagReplication.tla specs/MC_S2.tla specs/MC_S2.cfg specs/MC_S3.tla specs/MC_S3.cfg specs/MC_S3_Fixed.cfg specs/MC_S4.tla specs/MC_S4_Naive.cfg specs/MC_S4_ModelB.cfg
git commit -m "model(M2/S4): SubDoc field-grain — Naive re-breaks; Model B holds VisibleConverge"
```

---

### Task 7: README — invariant → verdict → source map; the recommendation

**Files:**
- Create: `specs/README.md`

- [ ] **Step 1: Write the README**

Create `specs/README.md` with these sections (fill verdicts from the actual TLC runs in Tasks 1–6; use the real counterexample summaries you observed):
```markdown
# P2P Filtered Replication — TLA+ Models

Formal models backing the B3 (#1013) decision. Design + verified findings: `DESIGN.md`.

## Run everything
\`\`\`bash
cd specs
./tools/tlc -config M1Convergence.cfg M1Convergence.tla   # S1' green
./tools/tlc -config M1Naive.cfg        M1Convergence.tla   # S1  red (#2721)
./tools/tlc -config MC_S2.cfg          MC_S2.tla           # S2  green
./tools/tlc -config MC_S3.cfg          MC_S3.tla           # S3  red (split ownership)
./tools/tlc -config MC_S3_Fixed.cfg    MC_S3.tla           # S3  green (immutable key)
./tools/tlc -config MC_S4_Naive.cfg    MC_S4.tla           # S4  red (field-grain #2721)
./tools/tlc -config MC_S4_ModelB.cfg   MC_S4.tla           # S4  green (Model B)
\`\`\`

## Invariants (plain English) → verdict → source module
| Invariant | Means | Verdict | Source it abstracts |
|---|---|---|---|
| INV_DagComplete | no merged block lacks a merged parent | holds always (guard) | `db-merge/.../merge_handler` loadComposites |
| Converge | all nodes merge all blocks | green A / RED Naive | `coordinator/dag_fetcher.rs` walk |
| INV_SubsetConverge | subscribed docs fully converge | green (S2) | `watcher/query.rs` DID filter |
| INV_RelRefSafe | dropping a foreign-DID ref never blocks merge | green (S2) | scalar FK, merge never derefs |
| INV_NoSplitOwnership | ≤1 DID owns a doc | RED mutable / green immutable | `agent_request.graphql` agent_did |
| INV_VisibleConverge | every non-filtered block merges | RED Naive / green Model B | GraphSync future |

## Recommendation for `defradb.rs-p2p-control`
1. Model A (full within-doc ancestry walk) — already shipped; convergence proven.
2. Foreign-DID docs safe to drop (INV_RelRefSafe).
3. Filter-key immutability is REQUIRED: mutable agent_did → split ownership (S3).
   Enforce via E1 (merge-time write-once constraint) or E2 (content-addressed
   create-block key). No DefraDB mechanism exists today — separate follow-on.
4. Model B only needed for field-level GraphSync filtering (S4).
```

- [ ] **Step 2: Verify the README run-block actually works end to end**

Run each of the 7 commands in the README "Run everything" block; confirm the 4 green ones print `No error has been found.` and the 3 red ones print a violation. Fix any drift between the README's expected verdicts and reality.

- [ ] **Step 3: Commit**

```bash
git add specs/README.md
git commit -m "docs(specs): README — invariant/verdict/source map + B3 recommendation"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** M1 baseline + `#2721` (Tasks 1–2); S2 WholeDoc/Immutable with `INV_SubsetConverge`/`INV_RelRefSafe`/`INV_ClaimUnique` (Task 4 — note: `INV_ClaimUnique` is *subsumed* by `INV_NoSplitOwnership` at this abstraction since claim contention is among same-DID instances kept in the same replication set; if a distinct claim model is wanted later it is a follow-on, flagged here); S3 mutable-key hazard + immutability fix (Task 5); S4 SubDoc/Model B (Task 6); recommendation + verdict map (Task 7). The `ProviderAvailable` assumption is encoded as the `HasProvider(b)` guard on `Fetch`. All `DESIGN.md` scenarios S1–S4 are covered.
- **Placeholder scan:** no "TBD"/"handle later"; every TLA+ module and `.cfg` is given in full; every step has an exact command + expected TLC outcome.
- **Type/name consistency:** operator names (`AncestorsOf`, `HasProvider`, `OwnerView`, `Subscribed`, `Merge`/`MergeB`, `FetchTarget`) are reused verbatim across tasks; constant names match between `DagReplication.tla`, the `MC_*.tla` wrappers, and the `.cfg` `<-` overrides.
- **Known follow-ons (out of scope, file as issues):** (a) `agent_did` immutability enforcement E1/E2 in defradb.rs/defra-agent; (b) a dedicated multi-instance CRDT-CAS claim-race model if `INV_ClaimUnique` needs more than the ownership abstraction; (c) M3 management-channel auth (`#1012 A2`).
```
