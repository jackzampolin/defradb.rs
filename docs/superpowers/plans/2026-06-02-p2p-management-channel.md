# P2P Management Channel Implementation Plan (#1012)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add an authenticated P2P request/response management channel so a peer can add/remove/list replicators, add/remove/list P2P collections, add/remove P2P documents, and connect peers over the P2P wire — gated by the existing NAC engine keyed on the remote actor's DID.

**Architecture:** Two channels mirroring the SE store/query split — `manage` (mutations → ack reply) and `manage_query` (reads → typed reply). The **p2p transport layers** (`two_stream` handler, iroh `endpoint_streams`, the `P2PTransport` trait) only **decode → verify signature → emit a `TransportEvent`** — exactly like SE query. The **serve logic lives in `crates/p2p-adapter`** (`manage` module): it verifies an embedded actor JWT (audience-bound to the serving node's PeerID), calls `db::NacManagerApi::check_permission`, and dispatches through the **existing transport-agnostic `P2POperations` controller** (which already parses addresses + dials correctly for both libp2p and iroh), then the runtime wiring signs the reply and sends it. The runtime event loops (`embedded/src/node_tasks.rs`, `cli/.../server_p2p.rs`) route the four new events to the serve handler and the reply correlators. HTTP `/p2p/*` is untouched.

**Tech Stack:** Rust, libp2p + iroh, `serde`/`serde_cbor`, `tokio`, `identity` (`from_token`/`verify_auth_token`/`new_token`), `db`/`db-nac` (`NacManagerApi`), `defra-http` (`P2POperations` trait), `p2p-adapter` (the `P2POperations` impls).

### Why this shape (verified seams — file:line)

- **Reuse `db::NacManagerApi`, do not build a new NAC adapter.** It is already object-safe `#[async_trait]` with `async fn check_permission(&self, &Did, NodePermission) -> Result<bool>` (`crates/db-nac/src/lib.rs:72,77`), held as `Arc<dyn NacManagerApi>` next to the p2p host, re-exported by `db` (`crates/db/src/lib.rs:150`). The node never exposes a bare `NodeACP<S>`, so a `NacAccess<S>` blanket impl is unbuildable. NAC-disabled → `Ok(true)` (parity).
- **Reuse the `P2POperations` controller, do not inline address parsing.** Defined in `crates/http/src/router/traits.rs:52`; methods used: `local_peer_id() -> String` (:54), `connect_peer(&str)` (:74), `get_replicators() -> Vec<ReplicatorInfo>` (:86), `add_replicator(Vec<String>, Option<&str>, Vec<ExplicitReplayCapabilityInput>, Option<&str>)` (:89), `remove_replicator(Vec<String>, Option<&str>)` (:98), `get_collections()`/`add_collections(Vec<String>)`/`remove_collections(Vec<String>)` (:105-111), `add_documents(Vec<P2pDocumentRequest>)`/`remove_documents(...)` (:117-120). Transport-correct impls live in `crates/p2p-adapter/src/{libp2p.rs,iroh.rs}` (e.g. `connect_peer` libp2p:166 / iroh:211; `add_replicator` libp2p:223 / iroh:269). The controller is built as `Arc<dyn defra_http::P2POperations>` in `crates/embedded/src/node_p2p.rs:246,467`. **This is the iroh-correct path** (defra-agent runs Iroh in prod); inline libp2p `parse_multiaddr` would break iroh.
- **`p2p-adapter` is the serve handler's home.** It already deps `acp`, `db`, `defra-http`, `p2p` (`crates/p2p-adapter/Cargo.toml:12-21`). Add `identity` for token verification. `db-merge` is **not** touched (it cannot reach `P2POperations`).
- **`P2pDocumentRequest { collection: String, doc_id: String }`** (`http/src/router/traits.rs:158`).
- SE serve+correlate runtime template: `crates/embedded/src/node_tasks.rs:88-102` (libp2p) + `:172-189` (iroh); `crates/cli/.../server_p2p.rs:377,387,794,804`. Correlator constructed/cloned: `crates/embedded/src/node_p2p.rs:171-179`.
- iroh dispatch verifies with `verify_iroh_message(&msg)?` + `ensure_iroh_signed_sender(peer_id, msg.sender_id.as_str())?` (`crates/p2p/src/iroh/endpoint_streams.rs:451,440`), NOT `verify_message`. libp2p two-stream uses `crate::verify_message` + `ensure_transport_sender` (`two_stream/handler/se_query.rs:93-94`).
- **Two `PeerId` types:** `p2p::PeerId` = `libp2p::PeerId` (`lib.rs:165`); the transport newtype is `p2p::transport::PeerId` (`PeerId::new(String)`/`as_str()`, `transport.rs:23-57`). **All manage code uses `p2p::transport::{PeerId, PeerAddr}`.** Never `.parse()`.
- Imports that must be from crate roots (the inner modules are private): `use identity::{from_token, verify_auth_token, new_token, Did, Identity};` (`identity/src/lib.rs:30` re-exports `token::{...}`; `mod token` is private). `use p2p::message::Message;` (`p2p/src/message/mod.rs:38` `mod traits;` is private; `Message` re-exported at `:51`). Error variant is `Error::InvalidMultiaddr(String)` (`p2p/src/error.rs:78`); `InvalidPeerId(String)` (:114); **add `Unauthorized`** (Task 4.1).
- identity API: `from_token(&[u8]) -> Result<TokenIdentity>`; `verify_auth_token(&TokenIdentity, expected_audience: &str) -> Result<()>` (enforces `aud` membership + `exp`/`nbf`); DID via `Identity::did(&self) -> Result<Did>`; `new_token(&impl FullIdentity, Duration, audience: Option<String>, authorized_account: Option<String>) -> Result<Vec<u8>>`.

### Scope (v1)

In: ReplicatorAdd/Delete/List, CollectionAdd/Remove/List, DocumentAdd/Remove, PeerConnect. **Deferred:** `PeerRemove` (no `P2PTransport::disconnect` primitive) and `DocumentList` (no listable doc-subscription state; #1013 supersedes per-document P2P). No Go wire-compat for the op enums (no Go management channel exists); only the `MetaData` envelope is Go-byte-compatible (required for the shared `signing`/`verify_message` path). Tests assert CBOR self-round-trip.

### Sequencing invariant

**No state-mutating consumer before the auth check exists.** Phases 0–3 register protocols and emit **unconsumed** `TransportEvent::Manage*` events (decode→verify→emit is side-effect-free). The serve handler is added in Phase 5 and routed (enabled) only in Phase 6.

**Read first:** `message/se.rs`, `two_stream/handler/se_query.rs`, `se_correlator.rs`, `iroh/endpoint_streams.rs`, `iroh/protocols.rs`, `embedded/src/node_tasks.rs`, `embedded/src/node_p2p.rs`, `p2p-adapter/src/{libp2p.rs,iroh.rs}`, `http/src/router/traits.rs`, `db-nac/src/lib.rs`, `signing.rs`.

**Spec:** `docs/superpowers/specs/2026-06-02-p2p-management-channel-design.md`.

---

## Phase 0: Message types (crate: `p2p`)

### Task 0.1: Op enums + `permission()` mapping

**Files:** Create `crates/p2p/src/message/manage.rs`; Modify `crates/p2p/src/message/mod.rs`.

- [ ] **Step 1: Write the failing test** — create `manage.rs`:

```rust
//! P2P management channel message types.
//!
//! Two request/reply pairs mirroring `se.rs` (the SE store/query split). The
//! `MetaData` envelope fields are byte-identical to `se.rs` for the shared
//! `signing`/`verify_message` path. The op enums are Rust-native (no Go peer).

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes};
use super::Message;
use crate::protocol::MESSAGE_VERSION;

/// A document reference for P2P document ops (maps to `P2pDocumentRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManageDocRef {
    #[serde(rename = "Collection")]
    pub collection: String,
    #[serde(rename = "DocID")]
    pub doc_id: String,
}

/// Mutating management operations (ack reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageMutateOp {
    ReplicatorAdd {
        #[serde(rename = "Addresses")]
        addresses: Vec<String>,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
    },
    ReplicatorDelete {
        #[serde(rename = "Addresses", default)]
        addresses: Vec<String>,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
    },
    CollectionAdd { #[serde(rename = "CollectionIDs")] collection_ids: Vec<String> },
    CollectionRemove { #[serde(rename = "CollectionIDs")] collection_ids: Vec<String> },
    DocumentAdd { #[serde(rename = "Docs")] docs: Vec<ManageDocRef> },
    DocumentRemove { #[serde(rename = "Docs")] docs: Vec<ManageDocRef> },
    PeerConnect { #[serde(rename = "Address")] address: String },
}

/// Read-only management operations (typed reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryOp { ReplicatorList, CollectionList }

/// Typed payload for a `manage_query` reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryResult {
    Replicators { #[serde(rename = "Replicators")] replicators: Vec<crate::replicator::ReplicatorInfo> },
    Strings { #[serde(rename = "Values")] values: Vec<String> },
}

impl ManageMutateOp {
    pub fn permission(&self) -> acp::NodePermission {
        use acp::NodePermission as P;
        match self {
            ManageMutateOp::ReplicatorAdd { .. } => P::P2pReplicatorAdd,
            ManageMutateOp::ReplicatorDelete { .. } => P::P2pReplicatorDelete,
            ManageMutateOp::CollectionAdd { .. } => P::P2pCollectionAdd,
            ManageMutateOp::CollectionRemove { .. } => P::P2pCollectionDelete,
            ManageMutateOp::DocumentAdd { .. } => P::P2pDocumentAdd,
            ManageMutateOp::DocumentRemove { .. } => P::P2pDocumentDelete,
            ManageMutateOp::PeerConnect { .. } => P::P2pPeerConnect,
        }
    }
}

impl ManageQueryOp {
    pub fn permission(&self) -> acp::NodePermission {
        use acp::NodePermission as P;
        match self {
            ManageQueryOp::ReplicatorList => P::P2pReplicatorList,
            ManageQueryOp::CollectionList => P::P2pCollectionList,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutate_op_cbor_round_trip() {
        let op = ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] };
        assert_eq!(op, serde_cbor::from_slice(&serde_cbor::to_vec(&op).unwrap()).unwrap());
    }
    #[test]
    fn ops_map_to_permissions() {
        use acp::NodePermission as P;
        assert_eq!(ManageMutateOp::PeerConnect { address: "x".into() }.permission(), P::P2pPeerConnect);
        assert_eq!(ManageQueryOp::ReplicatorList.permission(), P::P2pReplicatorList);
    }
}
```

Add to `message/mod.rs` (mirror `mod se;`): `mod manage;` + `pub use manage::{ManageDocRef, ManageMutateOp, ManageQueryOp, ManageQueryResult};`.

- [ ] **Step 2:** `cargo test -p p2p message::manage` → FAIL then PASS once wired. Confirm `acp::NodePermission` variant idents with `grep -n "P2p" crates/acp/src/nac/permission.rs`.
- [ ] **Step 3:** (impl is in Step 1).
- [ ] **Step 4:** `cargo test -p p2p message::manage` → PASS.
- [ ] **Step 5:** `git add crates/p2p/src/message/manage.rs crates/p2p/src/message/mod.rs && git commit -m "feat(p2p): manage op enums + acp permission mapping"`

### Task 0.2: Request/reply envelopes

**Files:** Modify `crates/p2p/src/message/manage.rs`, `message/mod.rs`.

Copy the `QuerySEArtifactsRequest`/`PushSEArtifactsReply` shapes from `se.rs` (six `MetaData` fields, serde attrs, `impl Message`). Requests add `auth_token: Vec<u8>` + `op`; mutate reply is ack-only; query reply has `result: Option<ManageQueryResult>`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn request_round_trip_and_trait() {
    let mut req = ManageRequest::new(ManageMutateOp::DocumentRemove { docs: vec![] }, b"jwt".to_vec());
    req.set_message_id("mid-1".into());
    let back: ManageRequest = serde_cbor::from_slice(&serde_cbor::to_vec(&req).unwrap()).unwrap();
    assert_eq!(back.message_id(), "mid-1");
    assert_eq!(back.auth_token, b"jwt");
}
#[test]
fn replies_build() {
    assert!(ManageReply::success("m").err_message().is_none());
    assert_eq!(ManageReply::error("m", "unauthorized").err_message(), Some("unauthorized"));
    let q = ManageQueryReply::success("m", ManageQueryResult::Strings { values: vec!["c".into()] });
    assert!(matches!(q.result, Some(ManageQueryResult::Strings { .. })));
}
```

- [ ] **Step 2:** `cargo test -p p2p message::manage::tests::request_round_trip_and_trait` → FAIL.
- [ ] **Step 3: Write minimal implementation** — `ManageRequest` (six fields + `auth_token` (`#[serde(rename="AuthToken", with="serde_bytes")]`) + `op: ManageMutateOp`), `new(op, auth_token)`. `ManageReply` = `PushSEArtifactsReply` renamed (`success(id)`/`error(id,err)`). `ManageQueryRequest` = request with `op: ManageQueryOp`. `ManageQueryReply` = six fields + `result: Option<ManageQueryResult>` (`#[serde(rename="Result", skip_serializing_if="Option::is_none", default)]`) with `success(id, result)`/`error(id, err)`. Copy each `impl Message` block from `se.rs` (renamed). Export the four from `mod.rs`.
- [ ] **Step 4:** `cargo test -p p2p message::manage` → PASS.
- [ ] **Step 5:** `git commit -m "feat(p2p): manage request/reply envelopes"`

---

## Phase 1: Protocol IDs & ALPNs (crate: `p2p`)

### Task 1.1

**Files:** Modify `crates/p2p/src/protocol.rs`, `crates/p2p/src/iroh/protocols.rs`.

- [ ] **Step 1:** test in `iroh/protocols.rs`:

```rust
#[test]
fn manage_alpns_registered() {
    for a in [ALPN_MANAGE_REQ, ALPN_MANAGE_RESP, ALPN_MANAGE_QUERY_REQ, ALPN_MANAGE_QUERY_RESP] {
        assert!(ALL_ALPNS.contains(&a));
    }
}
```

- [ ] **Step 2:** `cargo test -p p2p manage_alpns_registered` → FAIL.
- [ ] **Step 3:** `iroh/protocols.rs` (mirror SE query ALPNs :29-33; append all four to `ALL_ALPNS`):

```rust
pub const ALPN_MANAGE_REQ: &[u8] = b"/defra-iroh/manage/0.1/req";
pub const ALPN_MANAGE_RESP: &[u8] = b"/defra-iroh/manage/0.1/resp";
pub const ALPN_MANAGE_QUERY_REQ: &[u8] = b"/defra-iroh/manage-query/0.1/req";
pub const ALPN_MANAGE_QUERY_RESP: &[u8] = b"/defra-iroh/manage-query/0.1/resp";
pub const MAX_MANAGE_MSG_SIZE: usize = 4 * 1024 * 1024;
```

`protocol.rs` (mirror `SE_QUERY_REQUEST_PROTOCOL` :55-59 + any `StreamProtocol` helpers):

```rust
pub const MANAGE_REQUEST_PROTOCOL: &str = "/defradb/manage_req/0.0.1";
pub const MANAGE_RESPONSE_PROTOCOL: &str = "/defradb/manage_resp/0.0.1";
pub const MANAGE_QUERY_REQUEST_PROTOCOL: &str = "/defradb/manage_query_req/0.0.1";
pub const MANAGE_QUERY_RESPONSE_PROTOCOL: &str = "/defradb/manage_query_resp/0.0.1";
```

- [ ] **Step 4:** `cargo test -p p2p manage_alpns_registered` → PASS.
- [ ] **Step 5:** `git commit -m "feat(p2p): manage protocol IDs, ALPNs, size cap"`

---

## Phase 2: Correlators, events, transport-trait send methods (crate: `p2p`)

### Task 2.1: Reply correlators

**Files:** Create `crates/p2p/src/manage_correlator.rs`; Modify `crates/p2p/src/lib.rs`.

- [ ] **Step 1:** copy `se_correlator.rs`'s tests, using `ManageReply::success("msg-1")`.
- [ ] **Step 2:** `cargo test -p p2p manage_correlator` → FAIL.
- [ ] **Step 3:** copy `se_correlator.rs` → `manage_correlator.rs`; replace `QuerySEArtifactsReply`→`ManageReply`, `SeQueryCorrelator`→`ManageCorrelator`, `PendingSeQuery`→`PendingManage`. Add a parallel `ManageQueryCorrelator`/`PendingManageQuery` over `ManageQueryReply`. In `lib.rs`: `mod manage_correlator; pub use manage_correlator::{ManageCorrelator, ManageQueryCorrelator};`.
- [ ] **Step 4:** `cargo test -p p2p manage_correlator` → PASS.
- [ ] **Step 5:** `git commit -m "feat(p2p): manage reply correlators"`

### Task 2.2: Event variants

**Files:** Modify `crates/p2p/src/transport.rs` (`TransportEvent`), `crates/p2p/src/two_stream/event.rs` (`TwoStreamEvent`).

- [ ] **Step 1:** `grep -rn "SEQueryRequest\|SEQueryReply" crates/p2p/src/transport.rs crates/p2p/src/two_stream/event.rs`.
- [ ] **Step 2:** test in `two_stream/event.rs`:

```rust
#[test]
fn manage_variants_exist() {
    fn _a(e: TwoStreamEvent) -> bool {
        matches!(e, TwoStreamEvent::ManageRequest { .. } | TwoStreamEvent::ManageReply { .. }
            | TwoStreamEvent::ManageQueryRequest { .. } | TwoStreamEvent::ManageQueryReply { .. })
    }
    let _ = _a;
}
```
`cargo test -p p2p manage_variants_exist` → FAIL.

- [ ] **Step 3:** add four variants to BOTH enums, mirroring `SEQueryRequest{peer_id, request}`/`SEQueryReply{peer_id, reply}`. **`TransportEvent` uses `crate::transport::PeerId`; `TwoStreamEvent` uses `libp2p::PeerId`** — match each enum's existing SE variant exactly.
- [ ] **Step 4:** `cargo test -p p2p manage_variants_exist` + `cargo build -p p2p` → PASS/clean.
- [ ] **Step 5:** `git commit -m "feat(p2p): manage transport + two-stream event variants"`

### Task 2.3: `P2PTransport` send methods (default unsupported)

**Files:** Modify `crates/p2p/src/transport.rs`.

- [ ] **Step 2:** `grep -n "fn send_se_query_request\|fn send_se_query_response" crates/p2p/src/transport.rs`.
- [ ] **Step 3:** add four trait methods mirroring `send_se_query_request`/`send_se_query_response` (default body `Err(Error::Transport("… not supported".into()))`): `send_manage_request(&self, &PeerId, ManageRequest)`, `send_manage_response(&self, &PeerId, ManageReply)`, `send_manage_query_request(&self, &PeerId, ManageQueryRequest)`, `send_manage_query_response(&self, &PeerId, ManageQueryReply)`.
- [ ] **Step 4:** `cargo build -p p2p` → clean.
- [ ] **Step 5:** `git commit -m "feat(p2p): P2PTransport manage send methods (default unsupported)"`

---

## Phase 3: Transport handlers — decode → verify → emit (crate: `p2p`)

### Task 3.1: Hoist `read_cbor_message`

**Files:** Modify `crates/p2p/src/two_stream/handler/se_query.rs`, `crates/p2p/src/two_stream/handler/mod.rs`.

- [ ] **Step 3:** move the private `read_cbor_message` fn from `se_query.rs:137` to `handler/mod.rs` as `pub(super) async fn read_cbor_message<T>(...)`, update `se_query.rs` to call it. (One bounded-read helper, reused — no duplication.)
- [ ] **Step 4:** `cargo build -p p2p` clean; `cargo test -p p2p two_stream` green.
- [ ] **Step 5:** `git commit -m "refactor(p2p): hoist read_cbor_message to handler module"`

### Task 3.2: libp2p two-stream manage handler

**Files:** Create `crates/p2p/src/two_stream/handler/manage.rs`; Modify `handler/mod.rs`.

- [ ] **Step 1:** decode test (full flow in Phase 7):

```rust
#[tokio::test]
async fn manage_request_decodes() {
    use crate::message::{ManageMutateOp, ManageRequest};
    let req = ManageRequest::new(ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] }, b"t".to_vec());
    let back: ManageRequest = serde_cbor::from_slice(&serde_cbor::to_vec(&req).unwrap()).unwrap();
    assert!(matches!(back.op, ManageMutateOp::CollectionAdd { .. }));
}
```

- [ ] **Step 2:** `cargo test -p p2p two_stream::handler::manage` → FAIL.
- [ ] **Step 3:** copy `se_query.rs` → `manage.rs`; for each channel provide the four methods mirroring SE (`send_*_request_fire_and_forget`, `send_*_response`, `handle_*_request_stream`, `handle_*_response_stream`). Handlers call `super::read_cbor_message::<T>(peer_id, stream, max_msg_size, t, "manage …")`, then `crate::verify_message(&msg)?` + `ensure_transport_sender(&peer_id, &msg)?`, then emit the event. Add the four `*_protocol()` helpers (`StreamProtocol::new(crate::protocol::MANAGE_REQUEST_PROTOCOL)` etc.). Declare `mod manage;` beside `mod se_query;`.
- [ ] **Step 4:** `cargo test -p p2p two_stream::handler::manage` → PASS.
- [ ] **Step 5:** `git commit -m "feat(p2p): libp2p two-stream manage handler"`

### Task 3.3: libp2p registration/routing + send impls + event mapping

**Files:** the two-stream protocol registration site; `crates/p2p/src/host/p2p_host/two_stream.rs` (:279-296 SE mapping); `crates/p2p/src/host/libp2p_transport.rs` (:441-445).

- [ ] **Step 1–2:** `grep -rn "SE_QUERY_REQUEST_PROTOCOL\|handle_se_query_request_stream\|SEQueryRequest\|send_se_query" crates/p2p/src/host crates/p2p/src/two_stream`.
- [ ] **Step 3:** at each SE-query site, add the four manage parallels: register inbound `MANAGE_*` protocols → route to Task 3.2 handlers (pass `protocols::MAX_MANAGE_MSG_SIZE as u64` + existing timeout); map `TwoStreamEvent::Manage*` → `TransportEvent::Manage*` in `two_stream.rs`; implement `send_manage_*` trait methods in `libp2p_transport.rs` (sign with host keypair, call the fire-and-forget senders) mirroring `send_se_query_*`. **No consumer yet** (Phase 6) — side-effect-free.
- [ ] **Step 4:** `cargo build -p p2p` → clean.
- [ ] **Step 5:** `git commit -m "feat(p2p): libp2p manage registration/routing/send"`

### Task 3.4: iroh dispatch + send impls

**Files:** `crates/p2p/src/iroh/endpoint_streams.rs`, `crates/p2p/src/iroh/transport.rs`, `crates/p2p/src/iroh/command.rs`.

- [ ] **Step 1–2:** `grep -n "ALPN_SE_QUERY_REQ\|ALPN_SE_QUERY_RESP\|verify_iroh_message\|ensure_iroh_signed_sender\|SEQueryRequest" crates/p2p/src/iroh/endpoint_streams.rs crates/p2p/src/iroh/transport.rs`.
- [ ] **Step 3:** in `dispatch_stream`, add four arms mirroring SE query (:388-433): `ALPN_MANAGE_REQ` → `read_message::<ManageRequest>(&mut recv, protocols::MAX_MANAGE_MSG_SIZE)`, `verify_iroh_message(&request)?`, `ensure_iroh_signed_sender(&peer_id, request.sender_id.as_str())?`, **emit `TransportEvent::ManageRequest`**. `ALPN_MANAGE_RESP` → read `ManageReply`, verify, **emit `TransportEvent::ManageReply`** (no correlator here). Same for the two query ALPNs. Implement the iroh `send_manage_*` trait methods (sign + open ALPN stream + `write_message`) mirroring the iroh SE query send path.
- [ ] **Step 4:** `cargo build -p p2p` → clean.
- [ ] **Step 5:** `git commit -m "feat(p2p): iroh manage dispatch (emit events) + send"`

---

## Phase 4: Error variant + token verification

### Task 4.1: `Error::Unauthorized` (crate: `p2p`)

**Files:** `crates/p2p/src/error.rs`.

- [ ] **Step 1:** `#[test] fn unauthorized_displays() { assert_eq!(Error::Unauthorized("nope".into()).to_string(), "unauthorized: nope"); }` → FAIL.
- [ ] **Step 3:** add `#[error("unauthorized: {0}")] Unauthorized(String),`.
- [ ] **Step 4–5:** test PASS; `git commit -m "feat(p2p): Error::Unauthorized variant"`.

### Task 4.2: `verify_actor_token` (crate: `p2p-adapter`)

**Files:** Create `crates/p2p-adapter/src/manage/mod.rs` + `crates/p2p-adapter/src/manage/auth.rs`; Modify `crates/p2p-adapter/src/lib.rs`, `crates/p2p-adapter/Cargo.toml` (add `identity = { path = "../identity" }`).

- [ ] **Step 1: Write the failing test** — confirm a `FullIdentity` test builder: `grep -rn "FullIdentity\|fn test_identity\|new_token" crates/identity/src`. Then:

```rust
#[test]
fn rejects_wrong_audience() {
    let (token, _did) = mint_token_for("12D3KooW-OTHER");
    assert!(verify_actor_token(&token, "12D3KooW-THIS").is_err());
}
#[test]
fn accepts_matching_audience_returns_did() {
    let (token, did) = mint_token_for("12D3KooW-THIS");
    assert_eq!(verify_actor_token(&token, "12D3KooW-THIS").unwrap(), did);
}
```
(`mint_token_for(aud)` builds a `FullIdentity` and calls `identity::new_token(&id, std::time::Duration::from_secs(300), Some(aud.into()), None)`, returning `(Vec<u8>, Did)` — base it on identity's own token tests.)

- [ ] **Step 2:** `cargo test -p p2p-adapter manage::auth` → FAIL.
- [ ] **Step 3:**

```rust
use identity::{from_token, verify_auth_token, Did, Identity};

/// Verify an actor JWT, requiring `aud == expected_audience`; return its DID.
pub fn verify_actor_token(token: &[u8], expected_audience: &str) -> Result<Did, String> {
    let ti = from_token(token).map_err(|e| format!("invalid actor token: {e}"))?;
    verify_auth_token(&ti, expected_audience).map_err(|e| format!("actor token rejected: {e}"))?;
    ti.did().map_err(|e| format!("token has no DID: {e}"))
}
```
Register `pub mod manage;` in `lib.rs`, with `pub mod auth;` in `manage/mod.rs`.

- [ ] **Step 4:** `cargo test -p p2p-adapter manage::auth` → PASS.
- [ ] **Step 5:** `git commit -m "feat(p2p-adapter): verify_actor_token with audience binding"`

---

## Phase 5: Serve handlers (crate: `p2p-adapter`)

### Task 5.1: `build_manage_reply` / `build_manage_query_reply`

**Files:** Create `crates/p2p-adapter/src/manage/serve.rs`; Modify `crates/p2p-adapter/src/manage/mod.rs`.

The serve fns take the existing controller (`&dyn defra_http::P2POperations`) + NAC (`&dyn db::NacManagerApi`). Audience = the serving node's PeerID (`controller.local_peer_id().await?`). Dispatch goes through the controller (transport-correct for libp2p + iroh). Replies are returned **unsigned** (signed once at the call site in Phase 6).

- [ ] **Step 1: Write the failing test**

```rust
use async_trait::async_trait;
use identity::Did;
use p2p::message::{ManageMutateOp, ManageRequest, Message};

// Minimal mock controller recording calls; deny-all NAC.
struct MockOps { collections: std::sync::Mutex<Vec<String>> }
#[async_trait]
impl defra_http::P2POperations for MockOps {
    async fn local_peer_id(&self) -> defra_http::P2PResult<String> { Ok("12D3KooW-THIS".into()) }
    async fn add_collections(&self, c: Vec<String>) -> defra_http::P2PResult<()> { self.collections.lock().unwrap().extend(c); Ok(()) }
    // ... other methods: unimplemented!() or minimal Ok defaults ...
}
struct DenyNac;
#[async_trait]
impl db::NacManagerApi for DenyNac {
    async fn check_permission(&self, _: &Did, _: acp::NodePermission) -> db::Result<bool> { Ok(false) }
    // ... other NacManagerApi methods minimal ...
}

#[tokio::test]
async fn unauthorized_rejected_before_side_effects() {
    let ops = MockOps { collections: Default::default() };
    let token = crate::manage::auth::mint_token_for("12D3KooW-THIS"); // reuse test helper
    let req = ManageRequest::new(ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] }, token);
    let reply = build_manage_reply(&ops, &DenyNac, req).await;
    assert_eq!(reply.err_message(), Some("unauthorized"));
    assert!(ops.collections.lock().unwrap().is_empty());
}
```

> Implementing the full `P2POperations`/`NacManagerApi` mocks is verbose. Confirm the complete method lists (`sed -n '52,135p' crates/http/src/router/traits.rs`, `crates/db-nac/src/lib.rs:72-105`) and stub the unused ones with `unimplemented!()`. This is acceptable test scaffolding.

- [ ] **Step 2:** `cargo test -p p2p-adapter manage::serve` → FAIL.
- [ ] **Step 3: Write minimal implementation**

```rust
use defra_http::{P2POperations, P2pDocumentRequest};
use p2p::message::{
    ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryRequest, ManageQueryResult,
    ManageReply, ManageRequest,
};
use super::auth::verify_actor_token;

pub async fn build_manage_reply(ops: &dyn P2POperations, nac: &dyn db::NacManagerApi, request: ManageRequest) -> ManageReply {
    let mid = request.message_id.clone();
    match authorize_and_apply(ops, nac, &request).await {
        Ok(()) => ManageReply::success(&mid),
        Err(e) => ManageReply::error(&mid, &e),
    }
}

async fn authorize_and_apply(ops: &dyn P2POperations, nac: &dyn db::NacManagerApi, request: &ManageRequest) -> Result<(), String> {
    let audience = ops.local_peer_id().await.map_err(|e| e.to_string())?;
    let actor = verify_actor_token(&request.auth_token, &audience)?;
    if !nac.check_permission(&actor, request.op.permission()).await.map_err(|e| e.to_string())? {
        return Err("unauthorized".into());
    }
    let did_str = actor.to_string();
    match &request.op {
        ManageMutateOp::ReplicatorAdd { addresses, collection_ids } => {
            ops.add_replicator(collection_ids.clone(), addresses.first().map(|s| s.as_str()), vec![], Some(did_str.as_str()))
                .await.map_err(|e| e.to_string())
        }
        ManageMutateOp::ReplicatorDelete { addresses, collection_ids } => {
            ops.remove_replicator(collection_ids.clone(), addresses.first().map(|s| s.as_str())).await.map_err(|e| e.to_string())
        }
        ManageMutateOp::CollectionAdd { collection_ids } => ops.add_collections(collection_ids.clone()).await.map_err(|e| e.to_string()),
        ManageMutateOp::CollectionRemove { collection_ids } => ops.remove_collections(collection_ids.clone()).await.map_err(|e| e.to_string()),
        ManageMutateOp::DocumentAdd { docs } => ops.add_documents(to_doc_reqs(docs)).await.map_err(|e| e.to_string()),
        ManageMutateOp::DocumentRemove { docs } => ops.remove_documents(to_doc_reqs(docs)).await.map_err(|e| e.to_string()),
        ManageMutateOp::PeerConnect { address } => ops.connect_peer(address).await.map_err(|e| e.to_string()),
    }
}

fn to_doc_reqs(docs: &[p2p::message::ManageDocRef]) -> Vec<P2pDocumentRequest> {
    docs.iter().map(|d| P2pDocumentRequest { collection: d.collection.clone(), doc_id: d.doc_id.clone() }).collect()
}

pub async fn build_manage_query_reply(ops: &dyn P2POperations, nac: &dyn db::NacManagerApi, request: ManageQueryRequest) -> ManageQueryReply {
    let mid = request.message_id.clone();
    let run = async {
        let audience = ops.local_peer_id().await.map_err(|e| e.to_string())?;
        let actor = verify_actor_token(&request.auth_token, &audience)?;
        if !nac.check_permission(&actor, request.op.permission()).await.map_err(|e| e.to_string())? {
            return Err("unauthorized".to_string());
        }
        Ok(match request.op {
            ManageQueryOp::ReplicatorList => ManageQueryResult::Replicators { replicators: ops.get_replicators().await.map_err(|e| e.to_string())? },
            ManageQueryOp::CollectionList => ManageQueryResult::Strings { values: ops.get_collections().await.map_err(|e| e.to_string())? },
        })
    };
    match run.await {
        Ok(result) => ManageQueryReply::success(&mid, result),
        Err(e) => ManageQueryReply::error(&mid, &e),
    }
}
```

> Confirm `P2pDocumentRequest` is exported from `defra_http` (`grep -n "pub use.*P2pDocumentRequest\|pub struct P2pDocumentRequest" crates/http/src`); if it lives behind a module, adjust the path. Confirm `db::Result`/`NacManagerApi` import paths (`grep -n "pub use" crates/db/src/lib.rs`). `actor.to_string()` gives the DID string for `expected_authorizer_did` (mirrors `replicators.rs:128`).

- [ ] **Step 4:** `cargo test -p p2p-adapter manage::serve` → PASS.
- [ ] **Step 5:** `git commit -m "feat(p2p-adapter): manage serve handlers (auth + NAC + controller dispatch)"`

---

## Phase 6: Enable the channel + requester (crates: `embedded`, `cli`, `p2p-adapter`)

### Task 6.1: Construct + thread correlators and handles

**Files:** `crates/embedded/src/node_p2p.rs` (mirror SE correlator :171-179), `crates/cli/src/commands/start/server_p2p.rs`.

- [ ] **Step 3:** where `SeQueryCorrelator::new()` is built and cloned into transport + event loop, add `ManageCorrelator::new()` + `ManageQueryCorrelator::new()` the same way. Thread the existing `Arc<dyn defra_http::P2POperations>` controller (already built at `node_p2p.rs:246`) and the `Arc<dyn db::NacManagerApi>` NAC handle into the event handler.
- [ ] **Step 4:** `cargo build -p embedded -p cli` → clean.
- [ ] **Step 5:** `git commit -m "feat(embedded,cli): construct manage correlators + thread controller/nac"`

### Task 6.2: Route manage events (enables the channel)

**Files:** `crates/embedded/src/node_tasks.rs` (:88-102, :172-189), `crates/cli/src/commands/start/server_p2p.rs` (:377-387, :794-804).

- [ ] **Step 3:** in every SE-query event match (libp2p + iroh, embedded + cli), add the four manage arms:

```rust
TransportEvent::ManageRequest { peer_id, request } => {
    let mut reply = p2p_adapter::manage::serve::build_manage_reply(controller.as_ref(), nac.as_ref(), request).await;
    if p2p::signing::sign_with_transport(&transport, &mut reply).is_ok() {
        let _ = transport.send_manage_response(&peer_id, reply).await;
    }
}
TransportEvent::ManageQueryRequest { peer_id, request } => {
    let mut reply = p2p_adapter::manage::serve::build_manage_query_reply(controller.as_ref(), nac.as_ref(), request).await;
    if p2p::signing::sign_with_transport(&transport, &mut reply).is_ok() {
        let _ = transport.send_manage_query_response(&peer_id, reply).await;
    }
}
TransportEvent::ManageReply { reply, .. } => { manage_correlator.deliver(reply); }
TransportEvent::ManageQueryReply { reply, .. } => { manage_query_correlator.deliver(reply); }
```

(The reply is signed here once — `build_*` returns it unsigned; do not sign inside serve. This is the review-#1 fix; receivers `verify_message`/`verify_iroh_message`.)

- [ ] **Step 4:** `cargo build -p embedded -p cli` → clean.
- [ ] **Step 5:** `git commit -m "feat: enable manage channel — serve + correlate wiring (libp2p + iroh)"`

### Task 6.3: Requester API

**Files:** Create `crates/p2p-adapter/src/manage/client.rs`; Modify `manage/mod.rs`. (Mirror the SE requester — `grep -rn "SeQueryCorrelator\|register(" crates/db-merge/src crates/p2p-adapter/src`.)

- [ ] **Step 3:** a `ManageClient` holding `transport` + the two correlators, with `manage(&self, peer_id: &PeerId, op: ManageMutateOp, auth_token: Vec<u8>) -> Result<ManageReply>` and `manage_query(...)`:
  - build `ManageRequest::new(op, auth_token)`; `p2p::signing::sign_message(keypair, &mut req)?` (sets message_id) or `sign_with_transport`;
  - `let mut pending = self.manage_correlator.register(req.message_id.clone());`
  - `self.transport.send_manage_request(peer_id, req).await?;`
  - `tokio::time::timeout(REQUEST_TIMEOUT, pending.recv()).await` → map timeout → `Error::ResponseTimeout`.
- [ ] **Step 4:** `cargo build` workspace → clean.
- [ ] **Step 5:** `git commit -m "feat(p2p-adapter): manage channel requester (ManageClient)"`

---

## Phase 7: Integration tests (both transports)

### Task 7.1: `--test p2p` management module

**Files:** Create `tools/integration-test/tests/p2p/management.rs`; Modify the `--test p2p` module root.

- [ ] **Step 1:** model on `tools/integration-test/tests/p2p/replication.rs`. Node A drives B over P2P only; A mints tokens with `aud = B.peer_id()`:

```rust
#[tokio::test]
async fn replicator_add_then_list_over_p2p() {
    let net = TwoNodeNet::start().await;
    let (a, b) = (net.node_a(), net.node_b());
    let token = a.actor_token_for(b.peer_id().as_str());
    let reply = a.manage_to(b.peer_id(), ManageMutateOp::ReplicatorAdd {
        addresses: a.listen_multiaddrs(), collection_ids: vec![b.collection_id("Users").await],
    }, token.clone()).await.unwrap();
    assert!(reply.err_message().is_none());
    let listed = a.manage_query_to(b.peer_id(), ManageQueryOp::ReplicatorList, token).await.unwrap();
    assert!(matches!(listed.result, Some(ManageQueryResult::Replicators { .. })));
}
#[tokio::test]
async fn denied_for_unauthorized_actor() {
    let net = TwoNodeNet::start_with_nac().await; // B NAC-enabled, A not owner/admin
    let reply = net.node_a().manage_to(net.node_b().peer_id(),
        ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] },
        net.node_a().actor_token_for(net.node_b().peer_id().as_str())).await.unwrap();
    assert_eq!(reply.err_message(), Some("unauthorized"));
}
```

- [ ] **Step 2–4:** add harness helpers (`manage_to`/`manage_query_to`/`actor_token_for`) wrapping `ManageClient` + the harness identity helpers; iterate to PASS.
- [ ] **Step 5:** `git commit -m "test(integration): p2p management channel over libp2p"`

### Task 7.2: `--test p2p_iroh` mirror

**Files:** Create `tools/integration-test/tests/p2p_iroh/management.rs`; Modify the root.

- [ ] Copy Task 7.1 for iroh (primary target). `cargo test -p integration-test --test p2p_iroh -- management::` → PASS. Commit `test(integration): p2p management channel over iroh`.

---

## Phase 8: Final gate

- [ ] `cargo fmt --all`
- [ ] `cargo clippy --all -- -D warnings` (fix all)
- [ ] `cargo test -p p2p -p p2p-adapter` green
- [ ] `cargo test -p integration-test --test p2p` and `--test p2p_iroh` green
- [ ] `cargo test -p acp` green (NAC unaffected)
- [ ] `git commit -m "chore: fmt + clippy clean for management channel"`

---

## Self-review notes (for the implementer)

- **Reuse, don't rebuild:** auth = `db::NacManagerApi::check_permission` (object-safe, already held next to the host); ops = `defra_http::P2POperations` controller (transport-correct for libp2p AND iroh). No new NAC adapter, no `ManageOps` trait, no `db-merge` changes, no `get_subscribed_documents`.
- **Replies signed once, in the runtime wiring** (Task 6.2), not inside `build_*` — receivers verify (`verify_message` libp2p / `verify_iroh_message` iroh). Don't double-sign.
- **iroh is the primary target** — dispatch through `P2POperations` so addresses parse/dial correctly per transport; never inline libp2p `parse_multiaddr`.
- **PeerId discipline:** manage code uses `p2p::transport::{PeerId, PeerAddr}` (string newtype: `PeerId::new`/`as_str`), never `p2p::PeerId` (= `libp2p::PeerId`) and never `.parse()`.
- **Imports from crate roots:** `identity::{from_token, verify_auth_token, new_token, Did, Identity}`, `p2p::message::Message`. Error variants: `Unauthorized` (added), `InvalidMultiaddr`, `InvalidPeerId`, `Transport`. No `Did::default()`, `Other`, `InvalidInput`, `InvalidMultiaddress`.
- **Sequencing:** Phases 0–3 emit unconsumed events (side-effect-free); Phase 6 enables the serve consumer with auth in place.
- **serde_cbor + `#[serde(tag = "Kind")]`** on the op enums is verified to round-trip (internally-tagged unit + struct variants, nested, behind `Option`). Only the `MetaData` envelope needs Go-byte-compat; op enums are Rust-native.
- **Deferred:** `PeerRemove` (needs `P2PTransport::disconnect`), `DocumentList` (needs doc-subscription storage / #1013). Open follow-up issues.