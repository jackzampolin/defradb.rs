---- MODULE Nac ----
\* ===========================================================================
\* NAC lifecycle privilege-escalation safety.
\*
\* Models the Node Access Control (NAC) lifecycle state machine and the
\* privilege-escalation hazards that live across the
\*   Enabled -> DisabledTemporarily -> re-enable
\* window. This is a DISTINCT concern from the management-channel AUTH GATE
\* (proofs/tla/Auth.tla), which models the per-request signature/JWT + NAC
\* permission gate at the HTTP/transport boundary. THIS slice models the
\* node-scoped lifecycle status machine, the write-block-while-disabled rule,
\* the persisted-disabled-flag survives-restart rule, and the
\* live-is_admin vs persisted-is_admin asymmetry that re-enable relies on.
\*
\* ---------------------------------------------------------------------------
\* SOURCE ANCHORS (Rust, this repo)
\*   crates/acp/src/nac/node_acp/mod.rs
\*     :33   DISABLED_RELATION sentinel ("_disabled") persisted in the store
\*     :96-143 load(): on startup, status := DisabledTemporarily iff the
\*             persisted _disabled relationship is present, else Enabled.
\*             (== Restart action recovering status from disk_disabled)
\*   crates/acp/src/nac/node_acp/lifecycle.rs
\*     :74-108 disable(): Enabled -> DisabledTemporarily, and PERSISTS the
\*             _disabled relationship (88-96) so it survives restart.
\*     :120-155 re_enable(): DisabledTemporarily -> Enabled, DELETES the
\*             persisted _disabled relationship (132-144).
\*     :171-197 purge(): -> NotConfigured, deletes all node relationships.
\*   crates/acp/src/nac/node_acp/operations.rs
\*     :72-79  is_admin(): when status != Enabled returns Ok(true) for EVERYONE
\*             (the "live" permissive check).
\*     :87-104 is_admin_persisted(): checks the STORED owner/admin relations
\*             REGARDLESS of status (the persisted ground-truth check).
\*     :110-117,179-186,238-250,307-319 add_admin / remove_admin /
\*             add_permission_grant / remove_permission_grant: each REJECTS
\*             with InvalidPolicy when status == DisabledTemporarily
\*             ("cannot modify relationships while NAC is disabled").
\*   crates/db-nac/src/lib.rs
\*     :235-244 NacManager::disable(): auth via is_admin (LIVE).
\*     :247-256 NacManager::re_enable(): auth via is_admin_persisted (STORED).
\*             ** This asymmetry is the security crux: while disabled, live
\*                is_admin is true for everyone, so re_enable MUST use the
\*                persisted check or any actor could re-enable then escalate.
\*     :259-272 purge(): requires dev_mode && is_admin.
\*
\* SOURCE ANCHORS (Go, origin/develop -- the live upstream, not the checkout)
\*   internal/db/acp/nac.go :44-61  NodeACP kept Start()ed even while disabled,
\*             specifically so re-enable authorization can be checked against
\*             persisted relations (the Go comment states this rationale).
\*   internal/db/acp/check.go :172-176  CheckNodeOperationAccess:
\*             if Status != NACEnabled && perm != NodeReEnableNACPerm: return nil
\*             (unrestricted while disabled) -- EXCEPT re-enable, which falls
\*             through to the real persisted ACP check. This is the exact Go
\*             analog of the Rust is_admin / is_admin_persisted asymmetry.
\*   internal/db/db_nac.go
\*     :107-128 DisableNAC: guards NotConfigured/already-disabled, checks
\*             NodeDisableNACPerm, sets DisabledTemporarily, saveNodeACPDesc.
\*     :73-95  ReEnableNAC: guards, checks NodeReEnableNACPerm, sets Enabled,
\*             saveNodeACPDesc.
\*     :189-281 add/deleteNACActorRelationship: REJECT with
\*             ErrACPOperationButACPNotAvailable when Status != NACEnabled
\*             (write-block while disabled).
\*     :384-450 fetchNodeACPDesc / saveNodeACPDesc: status persisted to the
\*             systemstore as JSON (== disk_disabled survives restart).
\*
\* ---------------------------------------------------------------------------
\* PROPERTY (what is proven)
\*   INV_NoNonAdminMutatesAdminSet  -- the headline. Across the whole lifecycle
\*     no actor that is NOT a ground-truth admin (owner or persisted admin)
\*     ever causes a change to the protected admin set. The oracle for "admin"
\*     is the PERSISTED relationship set (admins_persisted), NOT the runtime
\*     is_admin decision -- so a buggy live-permissive check cannot make this
\*     vacuously true.
\*   INV_NoWriteWhileDisabled  -- no admin-set mutation is applied while
\*     status = DisabledTemporarily.
\*   INV_DisabledPersistsAcrossRestart  -- whenever status = DisabledTemporarily
\*     the persisted disk flag is set, so a restart recovers Disabled (re-enable
\*     cannot be silently skipped by bouncing the node).
\*   INV_ReEnableNeedsPersistedAdmin  -- a transition out of Disabled into
\*     Enabled was authorized by an actor who is a ground-truth (persisted)
\*     admin.
\*
\* RED / GREEN
\*   GREEN  MC_Nac_Green                 Mode=Correct: all four hold.
\*   RED    MC_Nac_Red_WriteWhileDisabled Mode=BugWriteNotBlocked: the
\*          write-block is removed; while disabled is_admin is permissive, so a
\*          non-admin mutates the admin set -> INV_NoNonAdminMutatesAdminSet
\*          AND INV_NoWriteWhileDisabled both break (real counterexample).
\*   RED    MC_Nac_Red_ReEnableLive      Mode=BugReEnableLive: re_enable uses
\*          the LIVE is_admin (true for everyone while disabled) instead of the
\*          persisted check; a non-admin re-enables, then mutates the admin set
\*          as the de-facto controller -> INV_ReEnableNeedsPersistedAdmin and
\*          INV_NoNonAdminMutatesAdminSet break.
\*   RED    MC_Nac_Red_NoPersist         Mode=BugNoPersist: disable() does not
\*          persist the flag; a restart recovers Enabled instead of Disabled,
\*          and the actor that disabled never had to be re-authorized to come
\*          back -> INV_DisabledPersistsAcrossRestart breaks.
\*
\* RUN
\*   cd proofs/tla
\*   ./tools/tlc -metadir states/b_nac_green -config MC_Nac_Green.cfg MC_Nac_Green.tla
\*   ./tools/tlc -metadir states/b_nac_wd    -config MC_Nac_Red_WriteWhileDisabled.cfg MC_Nac_Red_WriteWhileDisabled.tla
\*   ./tools/tlc -metadir states/b_nac_rel   -config MC_Nac_Red_ReEnableLive.cfg MC_Nac_Red_ReEnableLive.tla
\*   ./tools/tlc -metadir states/b_nac_np    -config MC_Nac_Red_NoPersist.cfg MC_Nac_Red_NoPersist.tla
\* ===========================================================================
EXTENDS FiniteSets, Naturals

CONSTANTS
  Owner,          \* the bootstrap owner DID (ground-truth admin, immovable)
  Admins0,        \* set of non-owner DIDs that start as persisted admins
  NonAdmins,      \* set of DIDs that are never legitimately admin
  MaxMutations,   \* bound on admin-set mutations (keeps state finite)
  Mode            \* "Correct" | "BugWriteNotBlocked" | "BugReEnableLive" | "BugNoPersist"

Actors == {Owner} \cup Admins0 \cup NonAdmins
Modes  == {"Correct", "BugWriteNotBlocked", "BugReEnableLive", "BugNoPersist"}
Statuses == {"NotConfigured", "Enabled", "DisabledTemporarily"}

ASSUME Owner \notin Admins0
ASSUME Owner \notin NonAdmins
ASSUME Admins0 \cap NonAdmins = {}
ASSUME Mode \in Modes
ASSUME MaxMutations \in Nat

VARIABLES
  status,             \* current NacStatus (runtime, in-memory)
  disk_disabled,      \* persisted _disabled flag (survives restart)
  admins_persisted,   \* the protected admin set as PERSISTED relationships.
                      \* This is the GROUND TRUTH / oracle for is_admin_persisted.
  configured,         \* TRUE once enabled at least once and not purged
  mutations,          \* count of admin-set mutations performed (bound)
  dirty_by_nonadmin,  \* TRUE iff a non-ground-truth-admin ever changed admins_persisted
  bad_reenable,       \* TRUE iff a Disabled->Enabled transition was authorized by a non-admin
  wrote_while_disabled \* TRUE iff an admin-set mutation was applied while DisabledTemporarily

vars == <<status, disk_disabled, admins_persisted, configured,
          mutations, dirty_by_nonadmin, bad_reenable, wrote_while_disabled>>

\* --------------------------------------------------------------------------
\* Ground truth: an actor is a (persisted) admin iff it is the Owner or appears
\* in the persisted admin set. This mirrors is_admin_persisted (operations.rs
\* :87-104) and is INDEPENDENT of `status` -- it is the oracle the invariants
\* are stated against, so green cannot be vacuous.
IsAdminGT(a) == (a = Owner) \/ (a \in admins_persisted)

\* Live is_admin (operations.rs :72-79): permissive whenever status != Enabled.
\* This is the dangerous check; correct code never uses it to gate re-enable or
\* writes-while-disabled.
IsAdminLive(a) == (status # "Enabled") \/ IsAdminGT(a)

TypeOK ==
  /\ status \in Statuses
  /\ disk_disabled \in BOOLEAN
  /\ admins_persisted \in SUBSET (Admins0 \cup NonAdmins)
  /\ configured \in BOOLEAN
  /\ mutations \in 0..MaxMutations
  /\ dirty_by_nonadmin \in BOOLEAN
  /\ bad_reenable \in BOOLEAN
  /\ wrote_while_disabled \in BOOLEAN

Init ==
  /\ status = "NotConfigured"
  /\ disk_disabled = FALSE
  /\ admins_persisted = Admins0
  /\ configured = FALSE
  /\ mutations = 0
  /\ dirty_by_nonadmin = FALSE
  /\ bad_reenable = FALSE
  /\ wrote_while_disabled = FALSE

\* --------------------------------------------------------------------------
\* Lifecycle transitions.

\* enable(): NotConfigured -> Enabled. Owner is bootstrapped (already in GT via
\* IsAdminGT). Idempotent in code; here we only allow the meaningful transition.
Enable ==
  /\ status = "NotConfigured"
  /\ status' = "Enabled"
  /\ configured' = TRUE
  /\ disk_disabled' = FALSE
  /\ UNCHANGED <<admins_persisted, mutations, dirty_by_nonadmin, bad_reenable,
                 wrote_while_disabled>>

\* disable(): Enabled -> DisabledTemporarily. Persists the disabled flag UNLESS
\* the BugNoPersist variant drops the persistence. Auth is via LIVE is_admin
\* (lib.rs:236), which while Enabled equals the persisted check -- sound here.
Disable(a) ==
  /\ status = "Enabled"
  /\ IsAdminLive(a)                     \* enforced by NacManager::disable
  /\ status' = "DisabledTemporarily"
  /\ disk_disabled' = (Mode # "BugNoPersist")
  /\ UNCHANGED <<admins_persisted, configured, mutations,
                 dirty_by_nonadmin, bad_reenable, wrote_while_disabled>>

\* re_enable(): DisabledTemporarily -> Enabled. Correct code authorizes via the
\* PERSISTED admin check (lib.rs:248). The BugReEnableLive variant authorizes
\* via the LIVE check instead, which is true for everyone while disabled.
ReEnableAuth(a) ==
  IF Mode = "BugReEnableLive" THEN IsAdminLive(a) ELSE IsAdminGT(a)

ReEnable(a) ==
  /\ status = "DisabledTemporarily"
  /\ ReEnableAuth(a)
  /\ status' = "Enabled"
  /\ disk_disabled' = FALSE
  /\ bad_reenable' = (bad_reenable \/ ~IsAdminGT(a))
  /\ UNCHANGED <<admins_persisted, configured, mutations, dirty_by_nonadmin,
                 wrote_while_disabled>>

\* Restart: in-memory status is recovered from the persisted flag (mod.rs load,
\* :96-143; Go fetchNodeACPDesc). Models a node bounce. configured nodes only.
Restart ==
  /\ configured
  /\ status \in {"Enabled", "DisabledTemporarily"}
  /\ status' = IF disk_disabled THEN "DisabledTemporarily" ELSE "Enabled"
  /\ UNCHANGED <<disk_disabled, admins_persisted, configured,
                 mutations, dirty_by_nonadmin, bad_reenable, wrote_while_disabled>>

\* --------------------------------------------------------------------------
\* The protected admin-set mutations (add_admin / remove_admin).
\* Mechanism gate:
\*   1. write-block while disabled (operations.rs:113 etc.) UNLESS BugWriteNotBlocked
\*   2. requestor must pass is_admin (LIVE). While Enabled this is the persisted
\*      check; while disabled (only reachable in the bug variant) it is permissive.
WriteAllowedByStatus ==
  \/ status = "Enabled"
  \/ (Mode = "BugWriteNotBlocked" /\ status = "DisabledTemporarily")

AddAdmin(req, tgt) ==
  /\ mutations < MaxMutations
  /\ tgt \in (Admins0 \cup NonAdmins)
  /\ tgt \notin admins_persisted
  /\ WriteAllowedByStatus
  /\ IsAdminLive(req)
  /\ admins_persisted' = admins_persisted \cup {tgt}
  /\ mutations' = mutations + 1
  /\ dirty_by_nonadmin' = (dirty_by_nonadmin \/ ~IsAdminGT(req))
  /\ wrote_while_disabled' = (wrote_while_disabled \/ status = "DisabledTemporarily")
  /\ UNCHANGED <<status, disk_disabled, configured, bad_reenable>>

RemoveAdmin(req, tgt) ==
  /\ mutations < MaxMutations
  /\ tgt \in admins_persisted
  /\ WriteAllowedByStatus
  /\ IsAdminLive(req)
  /\ admins_persisted' = admins_persisted \ {tgt}
  /\ mutations' = mutations + 1
  /\ dirty_by_nonadmin' = (dirty_by_nonadmin \/ ~IsAdminGT(req))
  /\ wrote_while_disabled' = (wrote_while_disabled \/ status = "DisabledTemporarily")
  /\ UNCHANGED <<status, disk_disabled, configured, bad_reenable>>

Next ==
  \/ Enable
  \/ \E a \in Actors : Disable(a)
  \/ \E a \in Actors : ReEnable(a)
  \/ Restart
  \/ \E req \in Actors, tgt \in (Admins0 \cup NonAdmins) : AddAdmin(req, tgt)
  \/ \E req \in Actors, tgt \in (Admins0 \cup NonAdmins) : RemoveAdmin(req, tgt)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* --------------------------------------------------------------------------
\* INVARIANTS

\* Headline. Stated against the GROUND-TRUTH admin oracle (IsAdminGT applied at
\* the moment of mutation, captured in dirty_by_nonadmin), NOT against the
\* runtime is_admin decision. No non-admin ever mutated the protected set.
INV_NoNonAdminMutatesAdminSet == ~dirty_by_nonadmin

\* No admin-set mutation was ever applied while NAC was temporarily disabled.
\* wrote_while_disabled is a history bit set by AddAdmin/RemoveAdmin exactly when
\* a mutation actually fires with status = DisabledTemporarily. This has teeth
\* independent of who the writer was: the BugWriteNotBlocked variant lets a
\* mutation through while disabled and trips this even if the writer is an admin.
INV_NoWriteWhileDisabled == ~wrote_while_disabled

\* Whenever runtime status is DisabledTemporarily, the persisted flag is set, so
\* a restart recovers Disabled. The escalation window cannot be skipped by
\* bouncing the node.
INV_DisabledPersistsAcrossRestart ==
  (status = "DisabledTemporarily") => disk_disabled

\* Any Disabled->Enabled transition was authorized by a ground-truth admin.
INV_ReEnableNeedsPersistedAdmin == ~bad_reenable
====
