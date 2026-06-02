# Design: Authenticated P2P Management Channel (#1012)

Status: approved design · Branch `feat/p2p-control-channel` · 2026-06-02

> Scope: this spec covers **#1012 only** (A1 channel + A2 auth/NAC + A3 peer
> connect/remove). #1013 (filtered replication) and #515 (SourceHub registry)
> are explicitly out of scope and get their own work. B3 (filtered-DAG
> completeness) is modeled separately in the `defradb.rs-p2p-tla` worktree.

## Problem

The P2P wire carries only data protocols (`rep`, `rep_se`, `se_query`, `car`,
`ident`). Node administration — add/remove/list replicators, P2P collections,
P2P documents, and peer connect — is reachable **only over HTTP** `/p2p/*`. A
node can dial a peer over P2P but cannot ask it to subscribe a collection or
install a reverse replicator without also reaching its HTTP endpoint. This
blocks P2P-first deployments (`defra-agent#107`, `#180`) where only the DefraDB
P2P port is exposed.

## Goal

Add an **authenticated** request/response management channel over P2P (both iroh
ALPN and libp2p protocol IDs) that invokes the existing coordinator management
methods, gated by the existing Node Access Control (NAC) engine keyed on the
**remote actor's DID** — not the PeerID.

## Where Go points (the grounding constraint)

The design mirrors Go's SE channel architecture, because that is the one real
precedent in the codebase for a multi-operation request/response surface:

- Go's `CommChannel` (`internal/db/p2p/protocol/comm_channel.go`) is generic over
  **one concrete `[Req, Reply]` pair per channel**. It never multiplexes verbs
  through an enum field — every existing channel is single-purpose.
- SE registers **two** channels (`se/coordinator.go:81-90`):
  `rep_se` (store, ack reply) and `se_query` (query, typed-data reply). The split
  is **mutate vs. query**, carved on **reply shape**.
- The Rust repo already mirrors this grain: one ALPN req/resp pair per concrete
  message shape (`docsync`, `branchable`, `car`, `se`, `se_query`, `twostream`),
  plus a `SeQueryCorrelator` doing `HashMap<message_id, oneshot::Sender>`
  request/reply correlation (`crates/p2p/src/se_correlator.rs`).

Management replies are heterogeneous (mutations → ack; lists → typed data), so a
single grab-bag reply is exactly what the SE split exists to avoid. Therefore we
build **two channels**, not one mega-enum and not eleven per-verb channels. The
verb enum lives **only on the request side** (where Go has no counter-precedent —
only an absence), and each channel keeps a single homogeneous reply type (the
rule Go actually enforces).

## A1 — Channel shape & wire protocol

Two request/response channels, structured exactly like SE:

| Channel | iroh ALPN | libp2p protocol | Reply shape |
|---|---|---|---|
| `manage` (mutate) | `/defra-iroh/manage/0.1/req` + `/resp` | `/defradb/manage_req/0.0.1` + `_resp` | uniform ack/error |
| `manage_query` (read) | `/defra-iroh/manage-query/0.1/req` + `/resp` | `/defradb/manage_query_req/0.0.1` + `_resp` | typed list payload |

Components (following the SE pattern at each layer):

- **ALPNs** added in `crates/p2p/src/iroh/protocols.rs` and listed in `ALL_ALPNS`.
- **libp2p protocol IDs** added in `crates/p2p/src/protocol.rs` (Go-compatible
  naming so a future Go peer interops).
- **Message types** in new `crates/p2p/src/message/manage.rs`:
  - `ManageRequest { MetaData, auth_token: Vec<u8>, op: ManageMutateOp }`
  - `ManageReply { MetaData, result: ManageResult }`
  - `ManageQueryRequest { MetaData, auth_token: Vec<u8>, op: ManageQueryOp }`
  - `ManageQueryReply { MetaData, result: ManageQueryResult }`
  - All impl the `Message` trait; CBOR-encoded like the SE messages.
- **Correlation:** reuse the `SeQueryCorrelator` shape as a `ManageCorrelator`
  (one per channel, or a generic correlator parameterized by reply type), matched
  by `message_id`, with the `PendingSeQuery` drop-guard cleanup on timeout/error.
- **Dispatch:** new arms in `crates/p2p/src/iroh/endpoint_streams.rs`
  `dispatch_stream` for each req/resp ALPN, replacing part of the unknown-ALPN
  drop at `:434`. Each `*_req` arm decodes → handles → replies; each `*_resp` arm
  delivers to the correlator.
- **Processors:** one per channel, mirroring `seStoreProcessor` /
  `seQueryProcessor` (`se/coordinator_protocol.rs` equivalent). The processor
  decodes the op, runs auth/NAC (A2), then dispatches to the **existing**
  coordinator method — no new business logic.

## A1/A3 — Verb set, dispatch & permission mapping

`manage` (mutate) → `ManageMutateOp`:

| Op | Coordinator call | NodePermission |
|---|---|---|
| `ReplicatorAdd{peer, addrs, collections}` | `create_replicator` | `P2pReplicatorAdd` |
| `ReplicatorDelete{peer, collections?}` | `delete_replicator` / `remove_replicator_collections` | `P2pReplicatorDelete` |
| `CollectionAdd{ids}` | `subscribe_collection` | `P2pCollectionAdd` |
| `CollectionRemove{ids}` | `unsubscribe_collection` | `P2pCollectionDelete` |
| `DocumentAdd{ids}` | `subscribe_document` | `P2pDocumentAdd` |
| `DocumentRemove{ids}` | `unsubscribe_document` | `P2pDocumentDelete` |
| `PeerConnect{addrs}` (A3) | transport connect | `P2pPeerConnect` |
| `PeerRemove{peer}` (A3) | transport disconnect | `P2pPeerConnect` |

`manage_query` (read) → `ManageQueryOp`:

| Op | Coordinator call | NodePermission | Reply payload |
|---|---|---|---|
| `ReplicatorList` | `list_replicators` | `P2pReplicatorList` | `Vec<ReplicatorInfo>` |
| `CollectionList` | `get_subscribed_collections` | `P2pCollectionList` | `Vec<String>` |
| `DocumentList` | `get_subscribed_documents` (new) | `P2pDocumentList` | `Vec<String>` |

Notes:

- These are the **exact same `NodePermission::P2p*` variants** the HTTP routes map
  to (`crates/http/src/route_permissions.rs`). One permission vocabulary, two
  transports. The op→permission mapping is a small self-contained `match` in the
  processor, parallel to `route_permissions.rs`.
- **`PeerRemove` reuses `P2pPeerConnect`** to keep the NAC policy surface minimal;
  connect/disconnect are the same peer-management capability and disconnect is
  strictly less dangerous. Splittable into a dedicated `P2pPeerDisconnect` later
  if least-privilege demands.
- **`get_subscribed_documents`** is a small net-new coordinator getter mirroring
  `get_subscribed_collections` (`crates/p2p/src/sync/coordinator/subscriptions.rs`).
  Everything else reuses existing coordinator methods.

## A2 — Actor-DID auth + NAC enforcement

The wire authenticates a **PeerID, not an actor DID**. Node admin must not be
authorized by PeerID alone. Go's only existing mechanism for "verified actor DID
over P2P" is `hasAccess()` (`internal/db/p2p/p2p.go:374-407`): it fetches the
peer's identity via the IdentityProtocol, verifies a signed JWT auth token,
extracts the DID, and runs the access check. We apply that same pattern to
inbound management requests. The Rust repo already has the matching primitives:
`TokenIdentity` JWT (`crates/identity`), `IdentityResponse` carrying a JWT
(`crates/p2p/src/message/identity.rs`), and `nac.check_permission(&Did,
NodePermission)` (`crates/acp/src/nac/node_acp/operations.rs:17`).

Two layers, each playing the role its Go counterpart plays:

1. **Transport/integrity** — per-message `MetaData` signature with the **host
   key** (like Go's `CommChannel`). Authenticates the *sending PeerID*; signature
   cleared before signing.
2. **Actor identity** — an embedded JWT `auth_token` (the existing
   `TokenIdentity` JWT, same one HTTP uses) → `verify_auth_token` → actor `Did`
   from `iss`. This supplies the DID the NAC check runs against.

Per-request flow in the processor:

1. **Bounded read** (`MAX_MANAGE_MSG_SIZE`) → decode.
2. **Verify `MetaData` host-key signature** (authenticates sender PeerID).
3. **Verify JWT `auth_token`** → extract actor `Did` from `iss`.
4. **Replay binding:** require `aud == this node's identity` (its PeerID/node-DID)
   and enforce `exp`/`nbf`. A token captured by node X cannot be replayed at node
   Y; the window is bounded. The caller knows the target DID because it is dialing
   that specific peer, so it mints the token for that audience.
5. **Authorize:** `nac.check_permission(&actor_did, perm)?`, op→permission mapped.
6. **Execute:** dispatch to the existing coordinator method.
7. **Reply:** `ManageResult::Ok(payload)` or `ManageResult::Err(Unauthorized)` —
   NAC denial returns a clean error, never a panic.

### Wiring NAC into the P2P layer (no HTTP refactor)

HTTP keeps its existing `auth_middleware` → `require_permission` →
`check_permission` path untouched. The manage processor calls the same
transport-agnostic core primitive directly. To reach it without `crates/p2p`
taking a heavy dependency on `crates/acp` (circular-dep risk), inject a **minimal
trait**, exactly as `MergeHandler` is injected into `loop_runner`:

```rust
trait NodeAccessCheck {
    async fn check_permission(&self, did: &Did, perm: NodePermission) -> Result<bool>;
}
```

Implemented by the NAC engine, passed as `Arc<dyn NodeAccessCheck>` into the
manage processor. When NAC is disabled the impl returns `Ok(true)` (parity with
today's behavior). This is the #633 co-design seam: NAC enforcement becomes a
service-layer invariant reachable from any transport, while the existing HTTP
enforcement stays as-is.

## Hardening (port the Go fixes, not the bugs)

- **Bounded reads** (`defradb#4718`): cap every manage stream read at
  `MAX_MANAGE_MSG_SIZE` before decode (like `MAX_MESSAGE_SIZE`/`MAX_CAR_SIZE` in
  `read_message`). Prevents unbounded `ReadAll` OOM.
- **Signature cleared before signing** (`defradb#4728`, `#4719`): the `MetaData`
  sign/verify must null the signature field before CBOR-marshaling, matching
  `message.go:254` / `signAndSetMetaData`. We reuse the shared `Message`-trait
  signing helpers; verify they already clear, and add a regression test.
- **Correlator cleanup on timeout:** drop the `oneshot` sender from the map on
  timeout/error so pending-request entries cannot leak (reuse the
  `PendingSeQuery` drop-guard pattern).

## Sequencing rule

A1 and A2 land **together**. The channel is never registered/enabled without the
A2 check in place. Shipping A1 alone is unauthenticated remote node-config
mutation — the exact failure the issue exists to prevent.

## Testing

Mirrors how SE / p2p are tested today.

- **Unit (`crates/p2p`):** CBOR round-trip for both request/reply pairs and both
  op enums (Go byte-compat on the `MetaData` envelope); signature
  clear-before-sign regression; bounded-read rejection over
  `MAX_MANAGE_MSG_SIZE`; correlator cleanup-on-timeout.
- **Auth/NAC unit:** wrong `aud` → rejected (replay); expired / `nbf` → rejected;
  valid token + insufficient permission → `Unauthorized`; valid + permitted →
  executes; NAC-disabled → allowed (parity with today).
- **Integration (`tools/integration-test`):** new `management` module under
  `--test p2p` **and `--test p2p_iroh`**. Node A dials node B *over the P2P manage
  channel only* (no HTTP to B), then exercises: add→list→remove replicator;
  collection subscribe/list/unsubscribe; document subscribe/list/unsubscribe; peer
  connect/remove. Run on **both transports** — iroh is primary (defra-agent runs
  Iroh in prod), libp2p for Go-parity.

## Files touched (summary)

- `crates/p2p/src/iroh/protocols.rs` — new ALPNs + `ALL_ALPNS`.
- `crates/p2p/src/protocol.rs` — new libp2p protocol IDs.
- `crates/p2p/src/message/manage.rs` — new message types + op enums.
- `crates/p2p/src/message/mod.rs` — module export.
- `crates/p2p/src/iroh/endpoint_streams.rs` — dispatch arms.
- `crates/p2p/src/*correlator*` — `ManageCorrelator` (reuse `SeQueryCorrelator`).
- manage processors (new, alongside the SE processor equivalents).
- `crates/p2p/src/sync/coordinator/subscriptions.rs` — `get_subscribed_documents`.
- NAC injection: `NodeAccessCheck` trait + `Arc<dyn NodeAccessCheck>` wiring;
  impl in the NAC engine crate.
- `tools/integration-test/tests/p2p*` — new `management` module.

## Out of scope

- #1013 filtered replication (B1–B5); B3 modeled in `defradb.rs-p2p-tla`.
- #515 SourceHub registry-driven topology (reference only).
- HTTP `/p2p/*` refactor — left untouched by design.
