# Management-Channel Auth: TLA+ Model Design

Date: 2026-06-02. Scope: #1012 A2 security counterpart to the B3 replication
model. The model asks whether a remote actor can mutate node configuration
without a fresh, scope-correct actor-DID authorization.

The requested `brainstorming` skill was not available in this Codex session, so
this document records the equivalent design pass: properties, state machine,
adversary model, entry-point assumptions, and TLC runs.

## Grounded Facts

| Concern | Source | Model interpretation |
|---|---|---|
| HTTP auth middleware | `crates/http/src/auth_middleware.rs` | Matched routes are classified by `route_permission`; non-exempt routes parse an Authorization bearer token and insert the DID before handler dispatch. `Required` routes call `require_permission` in middleware. |
| Token identity | `crates/http/src/identity_extractor.rs` | Bearer tokens are parsed, signature-checked, expiry/not-before/audience-checked, and reduced to an actor DID. Invalid or expired tokens do not become identities. |
| NAC permission check | `crates/http/src/nac_guard.rs` | `require_permission` checks the current NAC grant set for the exact `NodePermission`, with wildcard fallback only for anonymous requests when explicitly granted. |
| P2P collection mutations | `crates/http/src/handlers/p2p/collections.rs`; `crates/http/src/route_permissions.rs` | Add/delete P2P collections require `P2pCollectionAdd` / `P2pCollectionDelete` in route table and handler. |
| P2P replicator mutations | `crates/http/src/handlers/p2p/replicators.rs`; `crates/http/src/route_permissions.rs` | Add/delete replicators require `P2pReplicatorAdd` / `P2pReplicatorDelete` in route table and handler. The handler passes the authenticated authorizer DID into explicit replay capability checks where applicable. |
| DAC policy mutation | `crates/http/src/handlers/acp.rs`; `crates/http/src/route_permissions.rs` | `POST /api/v0/acp/policy` requires `DacPolicyAdd`; the handler also requires a non-empty authenticated creator DID. |
| NAC grant/revoke mutation | `crates/http/src/handlers/nac.rs`; `crates/http/src/route_permissions.rs` | Relationship add/delete routes require `NacRelationAdd` / `NacRelationDelete`. Dynamic enable/disable/re-enable require authenticated DID and delegate admin checks to NAC itself. |
| P2P sync ingress | `crates/p2p/src/iroh/endpoint_streams.rs`; `crates/p2p/src/sync/coordinator/authorizer.rs`; `crates/p2p/src/sync/coordinator/access.rs` | Iroh stream dispatch authenticates transport-level signed P2P messages for some protocols and authorizes sync by PeerID/replicator state. This is not an actor-DID management authorization gate. The green model treats these sync streams as non-mutating for node configuration; the red PeerID-only run flips a hypothetical management stream to mutating and demonstrates the failure. |
| FFI / embedded P2P and DAC paths | `crates/ffi/src/nac_check.rs`; `crates/ffi/src/p2p/*.rs`; `crates/ffi/src/acp/dac.rs`; `crates/p2p-adapter/src/{libp2p,iroh}.rs` | FFI wrappers check NAC with a caller-supplied DID string before mutating. The underlying adapters do not perform actor/JWT auth themselves. This model scopes the green proof to remote HTTP management paths; an embedded direct caller must be considered trusted local code or wrapped by an equivalent gate. |

No `check_node_access` symbol exists in this worktree; the source-level
primitive is represented by `require_permission` and `NodeAcpOperations::
check_permission`.

## Model

`Auth.tla` is a standalone control-state model. It does not depend on the DAG
replication model because the security question is about request authorization,
not block ancestry.

Each request carries:

- a state in `{unverified, verified, authorized, executed, rejected}`;
- an entry point;
- a presented actor DID;
- a required node permission;
- a credential state in `{absent, invalid, valid, expired, revoked, replayed}`.

The node carries a mutable NAC grant set `grants`, modeled as actor/permission
pairs. A separate environment action can grant, revoke, expire, or replay
credentials to represent another already-authorized management operation or time
passing between verification and authorization. `MutableGrantPairs` bounds
arbitrary grant/revoke exploration per scenario so TLC checks stay small; stale
credential revocation is still modeled directly.

Entry points carry two static attributes:

- `EntryCanMutate[e]`: whether this entry point can trigger a node-config
  mutation in this scenario.
- `GateByEntry[e]`: one of `ActorGate`, `DidOnlyGate`, `PeerGate`, or `NoGate`.

Only `ActorGate` is the remote management gate proved by this model: it requires
a fresh signature-verified actor-DID token and then checks the current NAC grant
for the specific permission. `DidOnlyGate` is useful for documenting FFI/local
paths; it is not equivalent to HTTP JWT verification.

## Invariants

- `INV_NoMutationWithoutVerifiedActor`: if a request reaches `executed`, it must
  have passed a fresh actor-DID verification. PeerID-only execution violates
  this.
- `INV_NoStaleReplay`: if a request reaches `authorized` or `executed`, the
  credential used at authorization time must have been `valid`, not expired,
  revoked, invalid, absent, or replayed.
- `INV_PermissionScoped`: if a request reaches `executed`, the actor must have
  held the exact required node permission at authorization time.
- `INV_AllEntryPointsGated`: every entry point marked as able to trigger a
  management mutation must use `ActorGate`.

The model snapshots the credential and permission verdict at authorization time.
That matters: a token or grant expiring after a correct authorization should not
retroactively make the earlier authorization unsafe.

## TLC Runs

Red runs:

- `MC_Auth_Red_PeerOnly`: a hypothetical Iroh management stream is marked as a
  mutating entry point but is only PeerID-gated. TLC reaches `executed` from an
  absent actor token, violating `INV_NoMutationWithoutVerifiedActor`.
- `MC_Auth_Red_Stale`: an actor verifies while valid, then the credential
  expires or is revoked. A cached authorization model still authorizes the request,
  violating `INV_NoStaleReplay`.
- `MC_Auth_Red_WrongScope`: a token is valid, but the actor has a different
  node permission. A token-only authorization model executes the mutation,
  violating `INV_PermissionScoped`.

Green run:

- `MC_Auth_Green`: HTTP P2P collection, P2P replicator, DAC policy, and NAC
  relationship mutations are all `ActorGate` entry points in the static
  entry-point table. The dynamic requests include one valid authorized request,
  one absent-token request, and one wrong-scope request; credential expiry,
  replay, and revocation can occur before authorization. Iroh sync streams and
  embedded-direct adapter calls are present as non-remote-management entries, so
  their PeerID/DID-only gates do not satisfy `INV_AllEntryPointsGated` unless
  they are kept out of the mutating remote-management surface. The strict gate
  rechecks token freshness and the current permission at authorization time; all
  invariants hold.

## Recommendation

For remote node configuration management, keep the current HTTP route-table plus
handler-level NAC gates: no node-config mutation should execute without a fresh,
scope-correct, non-revoked actor-DID.

The source review did not find a current Iroh stream that mutates node
configuration. If one is added, `endpoint_streams.rs` must not rely on PeerID or
transport signatures alone; it needs an actor-DID gate equivalent to the HTTP
path. Likewise, any embedded or FFI surface that is exposed to untrusted callers
must be wrapped by a gate equivalent to `ActorGate`, not only a raw adapter call.
