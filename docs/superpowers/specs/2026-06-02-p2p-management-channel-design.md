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
  drop at `:434`. Each arm **only decodes → verifies signature → emits a
  `TransportEvent`** (it does *not* serve or correlate inline — that happens in
  the runtime handler, exactly like SE query).
- **Serve in `crates/p2p-adapter`:** the request handler
  (`p2p-adapter/src/manage/serve.rs`) verifies the actor token, checks NAC, and
  dispatches through the **existing `defra_http::P2POperations` controller** —
  the same transport-agnostic surface the HTTP handlers use, with
  transport-correct address parsing/dial for **both libp2p and iroh**. The
  runtime event loops (`embedded/src/node_tasks.rs`, `cli/.../server_p2p.rs`)
  route the four events, **sign the reply** (`sign_with_transport`, once, at the
  call site), send it, and `deliver` replies to the correlators.

> **Revised twice after code review (see plan doc). Net architecture:**
> (1) replies are signed before send — receivers verify; (2) **reuse existing
> seams** — auth via the object-safe `db::NacManagerApi::check_permission`
> (already held next to the p2p host; no new adapter), ops via the
> `defra_http::P2POperations` controller (already implemented transport-correctly
> in `p2p-adapter` for libp2p + iroh; no inline `parse_multiaddr`, which would
> break iroh — the primary target); (3) the serve handler lives in `p2p-adapter`
> (deps `db` + `defra-http` + `p2p` + `acp`, plus `identity`), **`db-merge` is
> untouched**; (4) ops map directly to `acp::NodePermission` (no p2p-local
> permission enum); (5) `PeerRemove` and `DocumentList` deferred.

## A1/A3 — Verb set, dispatch & permission mapping

`manage` (mutate) → `ManageMutateOp`:

| Op | `P2POperations` call | `acp::NodePermission` |
|---|---|---|
| `ReplicatorAdd{addresses, collection_ids}` | `add_replicator(collection_ids, addr, [], actor_did)` | `P2pReplicatorAdd` |
| `ReplicatorDelete{addresses, collection_ids}` | `remove_replicator(collection_ids, addr)` | `P2pReplicatorDelete` |
| `CollectionAdd{ids}` | `add_collections(ids)` | `P2pCollectionAdd` |
| `CollectionRemove{ids}` | `remove_collections(ids)` | `P2pCollectionDelete` |
| `DocumentAdd{docs}` | `add_documents(docs)` | `P2pDocumentAdd` |
| `DocumentRemove{docs}` | `remove_documents(docs)` | `P2pDocumentDelete` |
| `PeerConnect{address}` (A3) | `connect_peer(address)` | `P2pPeerConnect` |
| ~~`PeerRemove`~~ **(deferred)** | needs `P2PTransport::disconnect` (absent) | — |

`manage_query` (read) → `ManageQueryOp`:

| Op | `P2POperations` call | `acp::NodePermission` | Reply payload |
|---|---|---|---|
| `ReplicatorList` | `get_replicators()` | `P2pReplicatorList` | `Vec<ReplicatorInfo>` |
| `CollectionList` | `get_collections()` | `P2pCollectionList` | `Vec<String>` |
| ~~`DocumentList`~~ **(deferred)** | no listable doc-subscription state | — | — |

Notes:

- These are the **exact same `acp::NodePermission::P2p*` variants** the HTTP routes
  map to (`crates/http/src/route_permissions.rs`) — and the **same
  `P2POperations` controller** those routes dispatch through. One permission
  vocabulary, one ops surface, two transports. `p2p` already depends on `acp`, so
  `op.permission()` returns `acp::NodePermission` directly — no p2p-local
  permission enum.
- **`PeerRemove` deferred:** `P2PTransport` exposes `dial` but no `disconnect`
  primitive; adding one touches the trait + libp2p + iroh + the test mock. Ship
  `PeerConnect`; track `PeerRemove` + the disconnect primitive as a follow-up.
- **`DocumentList` deferred:** `subscribe_document`/`unsubscribe_document` only
  delegate to the broadcaster — there is no listable document-subscription set
  (unlike collections). Building enumeration storage around per-document P2P
  invests in the non-scaling primitive #1013 replaces. Ship `DocumentAdd`/
  `DocumentRemove`; track `DocumentList` under #1013.

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

Per-request flow (transport layer steps 1–2; serve-handler steps 3–7):

1. **Bounded read** (`MAX_MANAGE_MSG_SIZE`) → decode *(transport/iroh handler)*.
2. **Verify `MetaData` host-key signature** (`verify_message`, authenticates
   sender PeerID) → emit `TransportEvent::Manage*` *(transport handler)*.
3. **Verify JWT `auth_token`** (`from_token` → `verify_auth_token`) → actor `Did`
   via `TokenIdentity::did()` *(serve handler)*.
4. **Replay binding:** require `aud == serving node's PeerID`
   (`transport.local_peer_id()`) and enforce `exp`/`nbf`. A token captured by node
   X cannot be replayed at node Y; the window is bounded. The caller knows the
   target PeerID because it is dialing that specific peer, so it mints the token
   for that audience.
5. **Authorize:** `nac.check_permission(&actor_did, op.permission())?` via
   `db::NacManagerApi` *(serve handler)*.
6. **Execute:** dispatch through the `defra_http::P2POperations` controller
   (transport-correct for libp2p + iroh) *(serve handler)*.
7. **Reply:** `build_manage_reply` returns `ManageReply`/`ManageQueryReply`
   (Ok payload or `Err` message, unsigned); the runtime wiring **signs it once**
   (`sign_with_transport`) and sends. NAC denial returns a clean `unauthorized`
   reply, never a panic.

### Wiring NAC (no HTTP refactor, no new adapter)

HTTP keeps its existing `auth_middleware` → `require_permission` →
`check_permission` path untouched. The serve handler (in `p2p-adapter`) calls the
**already-existing object-safe** `db::NacManagerApi::check_permission(&Did,
NodePermission) -> Result<bool>` (`crates/db-nac/src/lib.rs:72,77`), which the
node already holds as `Arc<dyn NacManagerApi>` next to the p2p host. No new NAC
trait or adapter is built — an earlier draft's `ManageAccessCheck`/`NacAccess<S>`
was both redundant (this API exists) and unbuildable (the node never exposes a
bare `NodeACP<S>`). When NAC is disabled `check_permission` returns `Ok(true)`
(parity). This is the #633 co-design seam: NAC enforcement is reachable from any
transport via the same `NacManagerApi`, while HTTP enforcement stays as-is.

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

A1 and A2 land **together**. The invariant is **no state-mutating consumer before
the auth check exists**: the transport handlers may register ALPNs and emit
events early (decode → verify → emit is side-effect-free, no consumer), but the
serve handler that mutates node state is added with — and gated by — the A2 auth
path. Shipping a serving handler without A2 is unauthenticated remote node-config
mutation — the exact failure the issue exists to prevent.

## Testing

Mirrors how SE / p2p are tested today.

- **Unit (`crates/p2p`):** CBOR self-round-trip for both request/reply pairs and
  both op enums (only the `MetaData` envelope is Go-byte-compatible; op enums are
  Rust-native); signature clear-before-sign is inherited from the shared
  `signing`/`verify_message` path; bounded reads are inherited from the existing
  `read_cbor_message`/`read_message` helpers; correlator cleanup-on-timeout.
- **Auth/NAC unit (`crates/db-merge`):** wrong `aud` → rejected (replay); expired
  / `nbf` → rejected; valid token + insufficient permission → `unauthorized`
  reply with no side effects; valid + permitted → executes; NAC-disabled →
  allowed (parity with today).
- **Integration (`tools/integration-test`):** new `management` module under
  `--test p2p` **and `--test p2p_iroh`**. Node A drives node B *over the P2P manage
  channel only* (no HTTP to B): add→list replicator; collection
  subscribe/list/unsubscribe; document subscribe/unsubscribe; peer connect; plus
  an unauthorized-actor denial. Run on **both transports** — iroh is primary
  (defra-agent runs Iroh in prod), libp2p for Go-parity.

## Files touched (summary)

- `crates/p2p/src/iroh/protocols.rs` — new ALPNs + `ALL_ALPNS` + `MAX_MANAGE_MSG_SIZE`.
- `crates/p2p/src/protocol.rs` — new libp2p protocol IDs.
- `crates/p2p/src/message/manage.rs` (+ `mod.rs`) — message types, op enums,
  `op.permission() -> acp::NodePermission`.
- `crates/p2p/src/manage_correlator.rs` (+ `lib.rs`) — reply correlators (copy of
  `se_correlator.rs`).
- `crates/p2p/src/transport.rs` — `TransportEvent::Manage*` variants + four
  `send_manage_*` trait methods (default-unsupported).
- `crates/p2p/src/two_stream/{event.rs,handler/manage.rs}` — event variants +
  libp2p handler (decode/verify/emit).
- `crates/p2p/src/iroh/{endpoint_streams.rs,transport.rs,command.rs}` — ALPN
  dispatch (emit events) + iroh send impls.
- `crates/p2p/src/host/{libp2p_transport.rs,p2p_host/two_stream.rs}` — libp2p
  send impls + event mapping.
- `crates/p2p/src/error.rs` — `Error::Unauthorized`.
- `crates/p2p-adapter/src/manage/{auth.rs,serve.rs,client.rs}` (+ `Cargo.toml`
  adds `identity`) — token verification, serve handlers dispatching through
  `P2POperations` + `db::NacManagerApi`, and the `ManageClient` requester.
- `crates/embedded/src/{node_p2p.rs,node_tasks.rs}`, `crates/cli/.../server_p2p.rs`
  — construct correlators; thread the existing controller + NAC handle; route the
  four manage events (sign reply once at the call site).
- `tools/integration-test/tests/p2p*` — new `management` module.
- **Untouched:** `crates/db-merge`, `crates/http`, the `P2PTransport`/coordinator
  op signatures.

## Out of scope

- #1013 filtered replication (B1–B5); B3 modeled in `defradb.rs-p2p-tla`.
- #515 SourceHub registry-driven topology (reference only).
- HTTP `/p2p/*` refactor — left untouched by design.
