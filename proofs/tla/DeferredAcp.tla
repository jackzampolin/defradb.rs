---- MODULE DeferredAcp ----
\* Deferred-ACP overlay consistency, abstracting
\* crates/query/src/txn/primitives/context.rs (DeferredAcpMutations: projected_registrations
\* + commit-time hooks, check_doc_access_with_overlay / is_doc_registered_with_overlay)
\* and crates/db/src/txn_registry.rs (one Arc<DeferredAcpMutations> per txn, hooks wired
\* to db_txn.on_success_async => run only on commit, never on discard/rollback).
\* Anchors are in DeferredAcp_DESIGN.md.
\*
\* THE MECHANISM.  An explicit DefraDB transaction buffers ACP register/unregister writes
\* as commit-time hooks AND maintains a txn-LOCAL projected_registrations map. Access checks
\* within the txn read that projection FIRST (check_doc_access_with_overlay): a not-yet-
\* committed Registered{owner} already gates reads to the owner, a projected Unregistered
\* opens reads. The real ACP writes (the hooks) apply only after the storage txn commits
\* (on_success_async); on rollback (discard) the hooks never run and committed ACP is
\* untouched. Each txn owns its OWN DeferredAcpMutations (new Arc per begin()), so one txn's
\* projection is invisible to another.
\*
\* THE PROPERTY (fail-closed across projected -> committed):
\*   (1) Isolation: a read in txn T consults only T's own projection, never the other txn's
\*       uncommitted projection.
\*   (2) Atomicity on commit: committing T applies EXACTLY T's projected registrations to
\*       the committed ACP state (all hooks, atomically w.r.t. observers).
\*   (3) No-op on rollback: rolling back T changes committed ACP not at all.
\*   (4) Fail-CLOSED: whenever the overlay GRANTS a read to reader r for doc d inside txn T,
\*       the COMMITTED ACP state that T's own commit would produce also grants it. An
\*       in-flight projection never grants access the committed state would deny.
\*
\* INDEPENDENT ORACLE.  Correctness is judged from `committed` -- the actually-committed ACP
\* registration state -- NOT from the overlay's own decision. `Grant(state, d, r)` is the
\* ground-truth ACP rule (Unregistered => anyone; Registered{o} => only o). The prospective
\* committed state for a txn T is `committed` overlaid with T's OWN projection (because T's
\* commit hooks produce exactly that). Every granted read is recorded with the txn that
\* produced it; the invariant re-derives ground truth and compares. The overlay cannot fake
\* this by agreeing with itself.
\*
\* Three knobs select the correct mechanism vs. adversary variants:
\*   IsolationMode = "PerTxn" - real: each txn reads only its own projection            [GREEN]
\*                 = "Shared"  - bug: txns share one projection map (one global Arc) so a
\*                               txn sees another's uncommitted registration             [RED]
\*   RollbackMode  = "NoHooks" - real: discard() never fires on_success_async hooks      [GREEN]
\*                 = "RunHooks"- bug: rollback applies the buffered hooks anyway          [RED]
\*   OwnerCheck    = "Strict"  - real: Registered{o} grants only to o                     [GREEN]
\*                 = "Any"     - bug: Registered grants to any authenticated identity     [RED]
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Txns,            \* finite set of concurrent transaction ids (model with 2)
  Docs,            \* finite set of document ids
  Idents,          \* authenticated identities that can own / read docs
  Anon,            \* the anonymous (unauthenticated) identity, Anon \notin Idents
  InitCommitted,   \* [Docs -> Reg]  committed ACP state before any txn acts
  MaxOps,          \* bound on register/unregister projection ops per txn
  IsolationMode,   \* "PerTxn" | "Shared"
  RollbackMode,    \* "NoHooks" | "RunHooks"
  OwnerCheck       \* "Strict" | "Any"

ASSUME Txns # {} /\ Docs # {} /\ Idents # {}
ASSUME Anon \notin Idents
ASSUME MaxOps \in Nat /\ MaxOps >= 1
ASSUME IsolationMode \in {"PerTxn", "Shared"}
ASSUME RollbackMode  \in {"NoHooks", "RunHooks"}
ASSUME OwnerCheck    \in {"Strict", "Any"}

\* All readers: authenticated identities plus the anonymous principal.
Readers == Idents \cup {Anon}

\* ---- ACP registration values (ProjectedDocRegistration / committed registration) -------
\* Unregistered: [tag |-> "U"].  Registered{owner}: [tag |-> "R", owner |-> id].
Unreg == [tag |-> "U"]
Reg(o) == [tag |-> "R", owner |-> o]
Regs == {Unreg} \cup {Reg(o) : o \in Idents}

ASSUME InitCommitted \in [Docs -> Regs]

\* No projection yet for a (txn,doc): the overlay falls through to committed state.
NoProj == [tag |-> "none"]
ProjVals == Regs \cup {NoProj}

VARIABLES
  committed,   \* [Docs -> Regs]            ORACLE: the actually-committed ACP state
  proj,        \* [Txns -> [Docs -> ProjVals]]  txn-local projected_registrations
  status,      \* [Txns -> {"active","committed","rolledback"}]  per-txn lifecycle
  ops,         \* [Txns -> Nat]             projection ops spent (bounds the state space)
  reads,       \* recorded granted reads: set of [txn,doc,reader] the overlay GRANTED
  rbDirtied    \* ghost history bit: a Rollback action actually mutated committed ACP

vars == << committed, proj, status, ops, reads, rbDirtied >>

TxnStatus == {"active", "committed", "rolledback"}

TypeOK ==
  /\ committed \in [Docs -> Regs]
  /\ proj      \in [Txns -> [Docs -> ProjVals]]
  /\ status    \in [Txns -> TxnStatus]
  /\ ops       \in [Txns -> 0..MaxOps]
  \* each recorded read carries the ground-truth oracle verdict captured AT READ TIME:
  \*   oracleProspective - Grant over (committed (+) this txn's own projection) then
  \*   oracleCommittedNow - Grant over the bare committed state at read time
  /\ reads     \subseteq [ txn : Txns, doc : Docs, reader : Readers,
                           oracleProspective : BOOLEAN, oracleCommittedNow : BOOLEAN,
                           ownHadProj : BOOLEAN ]
  /\ rbDirtied \in BOOLEAN

Init ==
  /\ committed = InitCommitted
  /\ proj      = [t \in Txns |-> [d \in Docs |-> NoProj]]
  /\ status    = [t \in Txns |-> "active"]
  /\ ops       = [t \in Txns |-> 0]
  /\ reads     = {}
  /\ rbDirtied = FALSE

\* ---- The ground-truth ACP rule (the oracle) -------------------------------------------
\* Independent of the overlay mechanism: Unregistered => anyone may read; Registered{o} =>
\* only o (an authenticated identity equal to the owner). Mirrors DocumentACP.check_doc_access.
Grant(state, d, r) ==
  CASE state[d].tag = "U" -> TRUE
    [] OTHER              -> (r \in Idents /\ r = state[d].owner)

\* The committed state that txn T's OWN commit would produce: committed, overlaid with
\* every doc T has projected (its hooks apply exactly its projection). This is what the
\* fail-closed oracle is judged against -- derived from ground truth + T's intent, NOT from
\* the overlay's decision.
ProspectiveCommitted(t) ==
  [d \in Docs |-> IF proj[t][d] = NoProj THEN committed[d] ELSE proj[t][d]]

\* ---- The overlay gate (the MECHANISM under test) --------------------------------------
\* check_doc_access_with_overlay: which projection map does a read in txn t consult?
\*   PerTxn (real): exactly t's own proj.
\*   Shared (bug):  a single global map -- if ANY active txn has projected this doc, that
\*                  projection is seen (models one shared Arc<DeferredAcpMutations>).
\* When the consulted projection has an entry, the overlay decides from it; otherwise it
\* falls through to committed ACP.
EffectiveProj(t, d) ==
  IF IsolationMode = "PerTxn"
  THEN proj[t][d]
  ELSE \* "Shared": pick this txn's own entry if present, else any other active txn's entry
    IF proj[t][d] # NoProj
    THEN proj[t][d]
    ELSE LET others == { u \in Txns : u # t /\ status[u] = "active" /\ proj[u][d] # NoProj }
         IN IF others = {} THEN NoProj
            ELSE proj[ CHOOSE u \in others : TRUE ][d]

\* The overlay's grant decision (OwnerCheck selects the strict-owner vs. buggy any rule for
\* a projected Registered; the committed fall-through always uses the strict ground-truth).
OverlayGrant(t, d, r) ==
  LET e == EffectiveProj(t, d) IN
  IF e = NoProj
  THEN Grant(committed, d, r)                       \* fall through to acp.check_doc_access
  ELSE CASE e.tag = "U" -> TRUE                       \* projected Unregistered: open
         [] OTHER ->                                  \* projected Registered{owner}
              IF OwnerCheck = "Strict"
              THEN (r \in Idents /\ r = e.owner)       \* real: owner-only
              ELSE (r \in Idents)                      \* bug: any authenticated identity

\* ======================================================================================
\* ACTIONS
\* ======================================================================================

\* schedule_register_doc_object: set txn-local projection to Registered{owner} and (in the
\* model) buffer the hook implicitly via the projection (commit replays the projection).
Register(t, d, o) ==
  /\ status[t] = "active"
  /\ ops[t] < MaxOps
  /\ proj' = [proj EXCEPT ![t][d] = Reg(o)]
  /\ ops'  = [ops  EXCEPT ![t] = @ + 1]
  /\ UNCHANGED << committed, status, reads, rbDirtied >>

\* schedule_unregister_doc_object: set txn-local projection to Unregistered.
Unregister(t, d) ==
  /\ status[t] = "active"
  /\ ops[t] < MaxOps
  /\ proj' = [proj EXCEPT ![t][d] = Unreg]
  /\ ops'  = [ops  EXCEPT ![t] = @ + 1]
  /\ UNCHANGED << committed, status, reads, rbDirtied >>

\* A read through the overlay gate. We only RECORD reads the overlay GRANTED; the oracle
\* invariants then check those grants against ground truth. (Denied reads are safe by
\* construction -- fail-closed concerns over-granting.)
Read(t, d, r) ==
  /\ status[t] = "active"
  /\ OverlayGrant(t, d, r)
  /\ reads' = reads \cup {[ txn |-> t, doc |-> d, reader |-> r,
                            \* independent oracle verdicts captured at the read instant
                            oracleProspective  |-> Grant(ProspectiveCommitted(t), d, r),
                            oracleCommittedNow |-> Grant(committed, d, r),
                            \* did THIS txn have its own projection for d at read time?
                            ownHadProj |-> proj[t][d] # NoProj ]}
  /\ UNCHANGED << committed, proj, status, ops, rbDirtied >>

\* Commit: run all hooks atomically -- apply T's projection to committed ACP -- then mark
\* committed and DROP the projection (hooks are drained; the txn is consumed).
Commit(t) ==
  /\ status[t] = "active"
  /\ committed' = [d \in Docs |->
                     IF proj[t][d] = NoProj THEN committed[d] ELSE proj[t][d]]
  /\ status' = [status EXCEPT ![t] = "committed"]
  /\ proj'   = [proj   EXCEPT ![t] = [d \in Docs |-> NoProj]]
  /\ UNCHANGED << ops, reads, rbDirtied >>

\* Rollback (discard): NoHooks (real) -- committed ACP untouched; RunHooks (bug) -- the
\* buffered hooks fire anyway, mutating committed ACP though the txn aborted.
Rollback(t) ==
  LET newCommitted == IF RollbackMode = "RunHooks"
                      THEN [d \in Docs |->
                              IF proj[t][d] = NoProj THEN committed[d] ELSE proj[t][d]]
                      ELSE committed
  IN
  /\ status[t] = "active"
  /\ committed' = newCommitted
  /\ status' = [status EXCEPT ![t] = "rolledback"]
  /\ proj'   = [proj   EXCEPT ![t] = [d \in Docs |-> NoProj]]
  \* ghost: did this rollback actually mutate committed ACP? (real code: never)
  /\ rbDirtied' = (rbDirtied \/ (newCommitted # committed))
  /\ UNCHANGED << ops, reads >>

Next ==
  \/ \E t \in Txns, d \in Docs, o \in Idents : Register(t, d, o)
  \/ \E t \in Txns, d \in Docs              : Unregister(t, d)
  \/ \E t \in Txns, d \in Docs, r \in Readers : Read(t, d, r)
  \/ \E t \in Txns : Commit(t)
  \/ \E t \in Txns : Rollback(t)

\* Stutter once every txn has finished, so TLC does not flag deadlock on a done schedule.
Done == \A t \in Txns : status[t] # "active"
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* ======================================================================================
\* INVARIANTS (safety) -- all judged against the independent `committed` oracle
\* ======================================================================================
INV_TypeOK == TypeOK

\* ---- (4) FAIL-CLOSED: the headline -----------------------------------------------------
\* Every read the overlay GRANTED must, at the instant it was granted, also be granted by the
\* ground-truth oracle over the committed state that this txn's OWN commit would produce
\* (committed (+) the txn's own projection). The verdict `oracleProspective` was computed in
\* the Read action by the independent `Grant` function -- NOT by the overlay's OverlayGrant.
\* An in-flight projection that grants what its own prospective committed state would deny is
\* a fail-OPEN over-grant. RED under OwnerCheck="Any" (projected Registered granted to a
\* stranger) and under IsolationMode="Shared" (a grant justified only by a SIBLING's
\* projection -- absent from this txn's own prospective state, so its oracle verdict is FALSE).
INV_FailClosedActive ==
  \A x \in reads : x.oracleProspective

\* ---- (1) ISOLATION -------------------------------------------------------------------
\* When a txn had NO projection of its own for the doc, the overlay must fall through to the
\* committed ACP state (is_doc_registered_with_overlay / check_doc_access_with_overlay return
\* the projected entry ONLY if present, else defer to acp.*). So any read GRANTED with
\* ownHadProj=FALSE must be granted by the BARE committed state -- it can never be justified by
\* a SIBLING txn's uncommitted projection. RED under IsolationMode="Shared": a txn with no own
\* projection is granted off a sibling's buffered Unregister, but committed still denies.
INV_NoCrossTxnLeak ==
  \A x \in reads : (~x.ownHadProj) => x.oracleCommittedNow


\* ---- (3) ROLLBACK IS A NO-OP ----------------------------------------------------------
\* A rolled-back txn must change committed ACP not at all. The ghost bit rbDirtied is set
\* exactly when a Rollback action mutates committed; the real mechanism (discard never fires
\* on_success_async) keeps it FALSE forever. This has teeth independent of WHO rolled back or
\* WHAT they projected: any committed-write on a rollback path trips it. RED under
\* RollbackMode="RunHooks".
INV_RollbackNoOp == ~rbDirtied

\* ======================================================================================
\* VACUITY PROBES (used only as NEGATED reachability checks; asserting them as invariants
\* forces TLC to exhibit the interesting state as a counterexample -> proves it reachable).
\* ======================================================================================
\* A read was actually granted through a projection (not just the committed fall-through).
SomeProjectedReadGranted ==
  \E x \in reads :
    LET t == x.txn IN proj[t][x.doc] # NoProj \/ status[t] = "committed"
NoProjectedReadGranted == ~SomeProjectedReadGranted

\* Committed ACP actually changed from the initial state (a commit took effect).
CommittedChanged == \E d \in Docs : committed[d] # InitCommitted[d]
NoCommittedChange == ~CommittedChanged

\* Both txns reached non-active terminal states (full lifecycle exercised).
BothFinished == \A t \in Txns : status[t] # "active"
NotBothFinished == ~BothFinished
====
