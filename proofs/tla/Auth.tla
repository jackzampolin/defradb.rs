---- MODULE Auth ----
\* Management-channel authorization state machine.
\* The model separates transport/entry-point admission from actor-DID
\* authorization. Only ActorGate represents a fresh signature/JWT verified
\* actor plus a current NAC permission check.
EXTENDS FiniteSets

CONSTANTS
  Requests,
  Actors,
  Permissions,
  EntryPoints,
  EntryOf,
  PresentedActor,
  RequiredPermission,
  InitialCredential,
  InitialGrants,
  MutableGrantPairs,
  GateByEntry,
  EntryCanMutate,
  AuthzMode

Statuses == {"unverified", "verified", "authorized", "executed", "rejected"}
CredentialStates == {"absent", "invalid", "valid", "expired", "revoked", "replayed"}
AuthCredentialStates == CredentialStates \cup {"none"}
GateKinds == {"ActorGate", "DidOnlyGate", "PeerGate", "NoGate"}
AuthzModes == {"Strict", "TokenOnly", "CachedGrant"}

ASSUME EntryOf \in [Requests -> EntryPoints]
ASSUME PresentedActor \in [Requests -> Actors]
ASSUME RequiredPermission \in [Requests -> Permissions]
ASSUME InitialCredential \in [Requests -> CredentialStates]
ASSUME InitialGrants \in SUBSET (Actors \X Permissions)
ASSUME MutableGrantPairs \in SUBSET (Actors \X Permissions)
ASSUME GateByEntry \in [EntryPoints -> GateKinds]
ASSUME EntryCanMutate \in [EntryPoints -> BOOLEAN]
ASSUME AuthzMode \in AuthzModes

VARIABLES
  status,
  credential,
  grants,
  verified_fresh,
  cached_perm_ok,
  auth_cred,
  auth_perm_ok

vars == <<status, credential, grants, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

TypeOK ==
  /\ status \in [Requests -> Statuses]
  /\ credential \in [Requests -> CredentialStates]
  /\ grants \in SUBSET (Actors \X Permissions)
  /\ verified_fresh \in [Requests -> BOOLEAN]
  /\ cached_perm_ok \in [Requests -> BOOLEAN]
  /\ auth_cred \in [Requests -> AuthCredentialStates]
  /\ auth_perm_ok \in [Requests -> BOOLEAN]

Init ==
  /\ status = [r \in Requests |-> "unverified"]
  /\ credential = InitialCredential
  /\ grants = InitialGrants
  /\ verified_fresh = [r \in Requests |-> FALSE]
  /\ cached_perm_ok = [r \in Requests |-> FALSE]
  /\ auth_cred = [r \in Requests |-> "none"]
  /\ auth_perm_ok = [r \in Requests |-> FALSE]

MutatingRequest(r) == EntryCanMutate[EntryOf[r]]
ActorGate(r) == GateByEntry[EntryOf[r]] = "ActorGate"
FreshCredential(r) == credential[r] = "valid"
HasRequiredPermission(r) == <<PresentedActor[r], RequiredPermission[r]>> \in grants

Verify(r) ==
  /\ status[r] = "unverified"
  /\ MutatingRequest(r)
  /\ ActorGate(r)
  /\ FreshCredential(r)
  /\ status' = [status EXCEPT ![r] = "verified"]
  /\ verified_fresh' = [verified_fresh EXCEPT ![r] = TRUE]
  /\ cached_perm_ok' = [cached_perm_ok EXCEPT ![r] = HasRequiredPermission(r)]
  /\ UNCHANGED <<credential, grants, auth_cred, auth_perm_ok>>

RejectAtGate(r) ==
  /\ status[r] = "unverified"
  /\ MutatingRequest(r)
  /\ ActorGate(r)
  /\ ~FreshCredential(r)
  /\ status' = [status EXCEPT ![r] = "rejected"]
  /\ UNCHANGED <<credential, grants, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

AuthzAllowed(r) ==
  CASE AuthzMode = "Strict" ->
        FreshCredential(r) /\ HasRequiredPermission(r)
    [] AuthzMode = "TokenOnly" ->
        FreshCredential(r)
    [] AuthzMode = "CachedGrant" ->
        verified_fresh[r] /\ cached_perm_ok[r]
    [] OTHER -> FALSE

AuthorizeAfterVerify(r) ==
  /\ status[r] = "verified"
  /\ AuthzAllowed(r)
  /\ status' = [status EXCEPT ![r] = "authorized"]
  /\ auth_cred' = [auth_cred EXCEPT ![r] = credential[r]]
  /\ auth_perm_ok' = [auth_perm_ok EXCEPT ![r] = HasRequiredPermission(r)]
  /\ UNCHANGED <<credential, grants, verified_fresh, cached_perm_ok>>

RejectAtAuthz(r) ==
  /\ status[r] = "verified"
  /\ ~AuthzAllowed(r)
  /\ status' = [status EXCEPT ![r] = "rejected"]
  /\ UNCHANGED <<credential, grants, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

\* Non-actor gates represent the bug shape: a mutating entry point can authorize
\* without proving a fresh actor-DID token. PeerGate models transport PeerID alone.
BypassAuthorize(r) ==
  /\ status[r] = "unverified"
  /\ MutatingRequest(r)
  /\ ~ActorGate(r)
  /\ status' = [status EXCEPT ![r] = "authorized"]
  /\ auth_cred' = [auth_cred EXCEPT ![r] = credential[r]]
  /\ auth_perm_ok' = [auth_perm_ok EXCEPT ![r] = HasRequiredPermission(r)]
  /\ UNCHANGED <<credential, grants, verified_fresh, cached_perm_ok>>

Execute(r) ==
  /\ status[r] = "authorized"
  /\ status' = [status EXCEPT ![r] = "executed"]
  /\ UNCHANGED <<credential, grants, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

ExpireCredential(r) ==
  /\ credential[r] = "valid"
  /\ status[r] \in {"unverified", "verified"}
  /\ credential' = [credential EXCEPT ![r] = "expired"]
  /\ UNCHANGED <<status, grants, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

ReplayCredential(r) ==
  /\ credential[r] = "valid"
  /\ status[r] \in {"unverified", "verified"}
  /\ credential' = [credential EXCEPT ![r] = "replayed"]
  /\ UNCHANGED <<status, grants, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

RevokeCredential(r) ==
  /\ credential[r] \in {"valid", "replayed"}
  /\ status[r] \in {"unverified", "verified"}
  /\ credential' = [credential EXCEPT ![r] = "revoked"]
  /\ grants' = grants \ {gp \in grants : gp[1] = PresentedActor[r]}
  /\ UNCHANGED <<status, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

GrantPermission(a, p) ==
  /\ <<a, p>> \in MutableGrantPairs
  /\ <<a, p>> \notin grants
  /\ grants' = grants \cup {<<a, p>>}
  /\ UNCHANGED <<status, credential, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

RevokePermission(a, p) ==
  /\ <<a, p>> \in MutableGrantPairs
  /\ <<a, p>> \in grants
  /\ grants' = grants \ {<<a, p>>}
  /\ UNCHANGED <<status, credential, verified_fresh, cached_perm_ok, auth_cred, auth_perm_ok>>

Next ==
  \/ \E r \in Requests : Verify(r)
  \/ \E r \in Requests : RejectAtGate(r)
  \/ \E r \in Requests : AuthorizeAfterVerify(r)
  \/ \E r \in Requests : RejectAtAuthz(r)
  \/ \E r \in Requests : BypassAuthorize(r)
  \/ \E r \in Requests : Execute(r)
  \/ \E r \in Requests : ExpireCredential(r)
  \/ \E r \in Requests : ReplayCredential(r)
  \/ \E r \in Requests : RevokeCredential(r)
  \/ \E a \in Actors, p \in Permissions : GrantPermission(a, p)
  \/ \E a \in Actors, p \in Permissions : RevokePermission(a, p)

Spec == Init /\ [][Next]_vars

\* No node-config mutation may execute unless the request previously passed a
\* fresh actor-DID verification. PeerID-only bypasses fail here.
INV_NoMutationWithoutVerifiedActor ==
  \A r \in Requests : status[r] = "executed" => verified_fresh[r]

\* Authorization-time credential must be fresh. The snapshot avoids false
\* failures when a token expires after a correct authorization.
INV_NoStaleReplay ==
  \A r \in Requests :
    status[r] \in {"authorized", "executed"} => auth_cred[r] = "valid"

\* Authorization-time NAC check must cover the exact permission required by the
\* mutation, not just any permission held by the actor.
INV_PermissionScoped ==
  \A r \in Requests : status[r] = "executed" => auth_perm_ok[r]

\* Static entry-point property: anything that can trigger a management mutation
\* must use the actor-DID gate, not PeerID, DID-string only, or no gate.
INV_AllEntryPointsGated ==
  \A e \in EntryPoints : EntryCanMutate[e] => GateByEntry[e] = "ActorGate"
====
