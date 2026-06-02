# P2P Management Channel Implementation Plan (#1012)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an authenticated P2P request/response management channel so a peer can add/remove/list replicators, add/remove/list P2P collections, add/remove P2P documents, and connect peers over the P2P wire — gated by the existing NAC engine keyed on the remote actor's DID.

**Architecture:** Two request/response channels mirroring the existing SE store/query split — `manage` (mutations → ack reply) and `manage_query` (reads → typed reply). The transport layers (`two_stream` handler, iroh `endpoint_streams`, the `P2PTransport` trait) only **decode → verify signature → emit a `TransportEvent`** — exactly like SE query. The **serve + correlate + authorize logic lives in the assembly layer** (`crates/db-merge/src/manage/serve.rs`, mirroring `crates/db-merge/src/se/serve.rs`), wired into the runtime event loop (`crates/embedded/src/node_tasks.rs`, `crates/cli/.../server_p2p.rs`). The serve handler verifies an embedded actor JWT (audience-bound to the serving node's PeerID), calls `NodeACP::check_permission`, dispatches to the existing coordinator/transport operations, signs the reply, and sends it. HTTP `/p2p/*` is left untouched.

**Tech Stack:** Rust, libp2p (`Stream`/stream-control), iroh (ALPN/QUIC), `serde`/`serde_cbor`, `tokio`, `identity` crate (`from_token`/`verify_auth_token`/`new_token`, `TokenIdentity`), `acp` crate (`NodeACP::check_permission`, `acp::NodePermission`).

### Scope decisions (from review)

- **Defer `PeerRemove`.** `P2PTransport` has no `disconnect` primitive (only `dial`); adding one touches the trait + libp2p + iroh + the test mock. Ship `PeerConnect` (uses existing dial); track `PeerRemove` as a follow-up. (A3's `AddPeer` ships; `RemovePeer` defers.)
- **Defer `DocumentList`.** `subscribe_document`/`unsubscribe_document` only delegate to the broadcaster (`subscriptions.rs:62-64,95-97`); there is no listable document-subscription set (unlike collections). Building enumeration storage around per-document P2P invests in the non-scaling primitive #1013 replaces. Ship `DocumentAdd`/`DocumentRemove` (one-line broadcaster delegations, existing perms); track `DocumentList` + the scalable replacement under #1013.
- **No Go wire-compat for the op payload.** There is no Go management channel, so only the shared `MetaData` envelope (`Version`/`MessageID`/`SenderID`/`Pubkey`/`Signature`/`ErrMessage`) is Go-byte-compatible (it must be, for the shared `signing`/`verify_message` path). The `Manage*Op` enums are Rust-native and versioned via the `Version` field. Tests assert CBOR self-round-trip, not Go fixtures.

### Verified facts this plan depends on (file:line)

- `crates/p2p/Cargo.toml:19` — **p2p already depends on acp**, so p2p may name `acp::NodePermission` directly. (No p2p-local permission enum.)
- `crates/db-merge/Cargo.toml:58-61` — db-merge depends on `identity`, `acp`, `p2p`. The serve handler + NAC check + token verification all live here. (Avoids the p2p↔acp cycle: the adapter is NOT in acp.)
- SE serve template: `crates/db-merge/src/se/serve.rs:25-54` — builds reply, `p2p::signing::sign_with_transport(transport, &mut reply)` (line 46), `transport.send_se_query_response(&peer_id, reply)` (line 51).
- Runtime routing template: `crates/embedded/src/node_tasks.rs:88-101` — `TransportEvent::SEQueryRequest{peer_id,request}` → serve; `TransportEvent::SEQueryReply{reply,..}` → `se_correlator.deliver(reply)`. Iroh variants at `node_tasks.rs:172,184`. CLI variants at `crates/cli/.../server_p2p.rs:377,387,794,804`.
- Correlator construction: `crates/embedded/src/node_p2p.rs:171-179` — `SeQueryCorrelator::new()`, cloned into transport + event loop.
- `P2PTransport` trait (`crates/p2p/src/transport.rs`): methods have **default impls returning `Error::Transport("not supported")`** (e.g. `send_se_query_request` :370, `send_se_query_response` :380). `dial(&self, &PeerId, Vec<PeerAddr>)` :254. `create_replicator(&self, &PeerId, Vec<String>)` :403 (**no addresses**). `PeerId` is a **string newtype** (`PeerId::new(String)`, `as_str()`) at :23-57 — NOT `libp2p::PeerId`, no `.parse()`.
- Coordinator: `create_replicator(&self, &PeerId, Vec<String>, auto_subscribe: bool)` (`replicators.rs:13`). HTTP derives the peer/addr from the request multiaddr; the manage handler mirrors the HTTP facade flow (`crates/http/src/handlers/p2p/replicators.rs:109-141`, `peers.rs:140-157` `connect_peer(&multiaddr_str)`).
- `acp::NodePermission` P2p variants (`crates/acp/src/nac/permission.rs:114-157`): `P2pPeerConnect`, `P2pReplicatorAdd/Delete/List`, `P2pCollectionAdd/Delete/List`, `P2pDocumentAdd/Delete/List` (+ others). Exported as `acp::NodePermission` (`crates/acp/src/lib.rs:65`).
- `acp`: `NodeACP::check_permission(&self, &Did, NodePermission) -> Result<bool>` (`node_acp/operations.rs:17`) — returns `Ok(true)` when NAC disabled.
- identity: `from_token(&[u8]) -> Result<TokenIdentity>` (`token/mod.rs:263`), `verify_auth_token(&TokenIdentity, expected_audience: &str) -> Result<()>` (`:179`), DID via the `Identity` trait `fn did(&self) -> Result<Did>` (`token/identity.rs`), `new_token(&I, Duration, audience: Option<String>, authorized_account: Option<String>) -> Result<Vec<u8>>` (`:66`).
- `p2p::error::Error` (`crates/p2p/src/error.rs`): has `Transport`, `InvalidPeerId(String)` (:113), `InvalidMultiaddress(String)` (:77); **no `Unauthorized`, no `Other`, no `InvalidInput`** — add `Unauthorized` (Task 4.1), reuse the others.

### Sequencing invariant (softened per review)

**No state-mutating consumer is wired before the authorizing serve handler exists.** Phases 0–3 add message types, ALPNs/protocol registration, and transport handlers that only decode → verify → **emit an event with no consumer** (events fall through / are logged). The serve handler that mutates node state is added in Phase 5 and only *enabled* (routed to) in Phase 6. Accepting an ALPN and emitting an unconsumed event is side-effect-free, so adding the four ALPNs to `ALL_ALPNS` in Phase 2 is safe.

**Reference templates to read first:** `message/se.rs`, `two_stream/handler/se_query.rs`, `se_correlator.rs`, `db-merge/src/se/serve.rs`, `embedded/src/node_tasks.rs`, `embedded/src/node_p2p.rs`, `iroh/protocols.rs`, `iroh/endpoint_streams.rs`, `signing.rs`.

**Design spec:** `docs/superpowers/specs/2026-06-02-p2p-management-channel-design.md`.

---

## Phase 0: Message types

### Task 0.1: Op enums + `permission()` mapping

**Files:**
- Create: `crates/p2p/src/message/manage.rs`
- Modify: `crates/p2p/src/message/mod.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/p2p/src/message/manage.rs`:

```rust
//! P2P management channel message types.
//!
//! Two request/reply pairs mirroring `se.rs` (the SE store/query split):
//! `Manage*` (mutations, ack reply) and `ManageQuery*` (reads, typed reply).
//! The `MetaData` envelope fields are byte-identical to `se.rs` for the shared
//! `signing`/`verify_message` path. The op enums are Rust-native (no Go peer).

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes};
use super::traits::Message;
use crate::protocol::MESSAGE_VERSION;

/// Mutating management operations (ack reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageMutateOp {
    /// Install a replicator. `addresses` are dialable multiaddrs (peer ID embedded),
    /// mirroring the HTTP replicator-add request.
    ReplicatorAdd {
        #[serde(rename = "Addresses")]
        addresses: Vec<String>,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
    },
    ReplicatorDelete {
        #[serde(rename = "PeerID")]
        peer_id: String,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
    },
    CollectionAdd {
        #[serde(rename = "CollectionIDs")]
        collection_ids: Vec<String>,
    },
    CollectionRemove {
        #[serde(rename = "CollectionIDs")]
        collection_ids: Vec<String>,
    },
    DocumentAdd {
        #[serde(rename = "DocIDs")]
        doc_ids: Vec<String>,
    },
    DocumentRemove {
        #[serde(rename = "DocIDs")]
        doc_ids: Vec<String>,
    },
    /// Connect to a peer by multiaddr (mirrors HTTP connect_peer).
    PeerConnect {
        #[serde(rename = "Address")]
        address: String,
    },
}

/// Read-only management operations (typed reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryOp {
    ReplicatorList,
    CollectionList,
}

/// Typed payload for a `manage_query` reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryResult {
    Replicators {
        #[serde(rename = "Replicators")]
        replicators: Vec<crate::replicator::ReplicatorInfo>,
    },
    Strings {
        #[serde(rename = "Values")]
        values: Vec<String>,
    },
}

impl ManageMutateOp {
    /// The NAC permission required to perform this op.
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
        let op = ManageMutateOp::CollectionAdd { collection_ids: vec!["bafy-col".into()] };
        let bytes = serde_cbor::to_vec(&op).unwrap();
        assert_eq!(op, serde_cbor::from_slice::<ManageMutateOp>(&bytes).unwrap());
    }

    #[test]
    fn ops_map_to_expected_permissions() {
        use acp::NodePermission as P;
        assert_eq!(ManageMutateOp::DocumentAdd { doc_ids: vec![] }.permission(), P::P2pDocumentAdd);
        assert_eq!(ManageQueryOp::ReplicatorList.permission(), P::P2pReplicatorList);
    }
}
```

Add to `crates/p2p/src/message/mod.rs` (mirror the `mod se;` block):

```rust
mod manage;
pub use manage::{ManageMutateOp, ManageQueryOp, ManageQueryResult};
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p message::manage`
Expected: FAIL/compile-error until the file + mod wiring exist; then PASS.

> If `acp::NodePermission` is not in scope, confirm the import path with `grep -n "NodePermission" crates/acp/src/lib.rs` and use `acp::NodePermission`. Confirm the exact variant idents with `grep -n "P2p" crates/acp/src/nac/permission.rs`.

- [ ] **Step 3:** (implementation is in Step 1's file).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p message::manage`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/message/manage.rs crates/p2p/src/message/mod.rs
git commit -m "feat(p2p): manage op enums + acp permission mapping"
```

### Task 0.2: Request/reply envelopes (both channels)

**Files:**
- Modify: `crates/p2p/src/message/manage.rs`, `crates/p2p/src/message/mod.rs`

The four envelope structs are mechanically identical to `QuerySEArtifactsRequest` / `PushSEArtifactsReply` in `se.rs` (same six `MetaData` fields, same serde attrs, same `impl Message`). **Copy the `se.rs` template; change names and payload fields.** Requests add `auth_token: Vec<u8>` (the actor JWT) + `op`. The mutate reply is ack-only (`PushSEArtifactsReply` shape). The query reply carries `result: Option<ManageQueryResult>`.

- [ ] **Step 1: Write the failing test**

```rust
use super::super::traits::Message;

#[test]
fn manage_request_round_trip_and_trait() {
    let mut req = ManageRequest::new(
        ManageMutateOp::DocumentRemove { doc_ids: vec!["bae-1".into()] },
        b"jwt".to_vec(),
    );
    req.set_message_id("mid-1".into());
    let back: ManageRequest = serde_cbor::from_slice(&serde_cbor::to_vec(&req).unwrap()).unwrap();
    assert_eq!(back.message_id(), "mid-1");
    assert_eq!(back.auth_token, b"jwt");
    assert!(matches!(back.op, ManageMutateOp::DocumentRemove { .. }));
}

#[test]
fn manage_reply_success_and_error() {
    assert!(ManageReply::success("mid-1").err_message().is_none());
    assert_eq!(ManageReply::error("mid-1", "unauthorized").err_message(), Some("unauthorized"));
}

#[test]
fn manage_query_reply_carries_typed_result() {
    let reply = ManageQueryReply::success("mid-q", ManageQueryResult::Strings { values: vec!["c1".into()] });
    let back: ManageQueryReply = serde_cbor::from_slice(&serde_cbor::to_vec(&reply).unwrap()).unwrap();
    match back.result {
        Some(ManageQueryResult::Strings { values }) => assert_eq!(values, vec!["c1"]),
        other => panic!("unexpected: {other:?}"),
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p message::manage::tests::manage_request_round_trip_and_trait`
Expected: FAIL — types not found.

- [ ] **Step 3: Write minimal implementation**

Add `ManageRequest` (six MetaData fields from `QuerySEArtifactsRequest` + the two new fields):

```rust
    /// Signed actor auth token (JWT). Authenticates the actor DID for NAC.
    #[serde(rename = "AuthToken", with = "serde_bytes")]
    pub auth_token: Vec<u8>,
    /// The management operation to perform.
    #[serde(rename = "Op")]
    pub op: ManageMutateOp,
```

```rust
impl ManageRequest {
    pub fn new(op: ManageMutateOp, auth_token: Vec<u8>) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            auth_token,
            op,
        }
    }
}
```

`ManageReply` = `PushSEArtifactsReply` (ack-only) renamed, with `success(id)`/`error(id, err)`. `ManageQueryRequest` = `ManageRequest` shape but `op: ManageQueryOp`. `ManageQueryReply` = the six fields + `result: Option<ManageQueryResult>` with:

```rust
impl ManageQueryReply {
    pub fn success(request_message_id: &str, result: ManageQueryResult) -> Self {
        Self { version: MESSAGE_VERSION.to_string(), message_id: request_message_id.to_string(),
            sender_id: String::new(), pubkey: Vec::new(), signature: None, err_message: None, result: Some(result) }
    }
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self { version: MESSAGE_VERSION.to_string(), message_id: request_message_id.to_string(),
            sender_id: String::new(), pubkey: Vec::new(), signature: None, err_message: Some(err.to_string()), result: None }
    }
}
```

Copy the `impl Message` block from `se.rs` for all four (verbatim, renamed). The `result` field uses `#[serde(rename = "Result", skip_serializing_if = "Option::is_none", default)]`. Export from `mod.rs`:

```rust
pub use manage::{ManageReply, ManageRequest, ManageQueryReply, ManageQueryRequest};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p message::manage`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/message/manage.rs crates/p2p/src/message/mod.rs
git commit -m "feat(p2p): manage request/reply envelopes"
```

---

## Phase 1: Protocol IDs & ALPNs

### Task 1.1: libp2p protocol IDs + iroh ALPNs + size cap

**Files:**
- Modify: `crates/p2p/src/protocol.rs`, `crates/p2p/src/iroh/protocols.rs`

- [ ] **Step 1: Write the failing test**

In `iroh/protocols.rs`:

```rust
#[cfg(test)]
mod manage_alpn_tests {
    use super::*;
    #[test]
    fn manage_alpns_registered() {
        for a in [ALPN_MANAGE_REQ, ALPN_MANAGE_RESP, ALPN_MANAGE_QUERY_REQ, ALPN_MANAGE_QUERY_RESP] {
            assert!(ALL_ALPNS.contains(&a));
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p manage_alpns_registered`
Expected: FAIL — consts not found.

- [ ] **Step 3: Write minimal implementation**

`iroh/protocols.rs` (mirror SE query ALPNs at :29-33, append all four to `ALL_ALPNS`):

```rust
pub const ALPN_MANAGE_REQ: &[u8] = b"/defra-iroh/manage/0.1/req";
pub const ALPN_MANAGE_RESP: &[u8] = b"/defra-iroh/manage/0.1/resp";
pub const ALPN_MANAGE_QUERY_REQ: &[u8] = b"/defra-iroh/manage-query/0.1/req";
pub const ALPN_MANAGE_QUERY_RESP: &[u8] = b"/defra-iroh/manage-query/0.1/resp";
pub const MAX_MANAGE_MSG_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
```

`protocol.rs` (mirror `SE_QUERY_REQUEST_PROTOCOL` at :55-59, plus any `StreamProtocol` helper fns):

```rust
pub const MANAGE_REQUEST_PROTOCOL: &str = "/defradb/manage_req/0.0.1";
pub const MANAGE_RESPONSE_PROTOCOL: &str = "/defradb/manage_resp/0.0.1";
pub const MANAGE_QUERY_REQUEST_PROTOCOL: &str = "/defradb/manage_query_req/0.0.1";
pub const MANAGE_QUERY_RESPONSE_PROTOCOL: &str = "/defradb/manage_query_resp/0.0.1";
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p manage_alpns_registered`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/protocol.rs crates/p2p/src/iroh/protocols.rs
git commit -m "feat(p2p): manage protocol IDs, ALPNs, size cap"
```

---

## Phase 2: Correlators, events, transport-trait send methods

### Task 2.1: Reply correlators

**Files:**
- Create: `crates/p2p/src/manage_correlator.rs`
- Modify: `crates/p2p/src/lib.rs`

Verbatim copy of `se_correlator.rs` for each reply type.

- [ ] **Step 1: Write the failing test** — copy `se_correlator.rs`'s test module, using `ManageReply::success("msg-1")` (no doc_ids):

```rust
#[tokio::test]
async fn register_then_deliver_routes_reply() {
    let c = ManageCorrelator::new();
    let mut pending = c.register("msg-1".into());
    assert!(c.deliver(ManageReply::success("msg-1")));
    assert_eq!(pending.recv().await.unwrap().message_id, "msg-1");
}
```

- [ ] **Step 2:** Run `cargo test -p p2p manage_correlator` → FAIL.

- [ ] **Step 3:** Copy `se_correlator.rs` → `manage_correlator.rs`. Replace `QuerySEArtifactsReply`→`ManageReply`, `SeQueryCorrelator`→`ManageCorrelator`, `PendingSeQuery`→`PendingManage`. Add a parallel `ManageQueryCorrelator`/`PendingManageQuery` over `ManageQueryReply` in the same file. In `lib.rs` (mirror the `se_correlator` decl/export):

```rust
mod manage_correlator;
pub use manage_correlator::{ManageCorrelator, ManageQueryCorrelator};
```

- [ ] **Step 4:** Run `cargo test -p p2p manage_correlator` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/manage_correlator.rs crates/p2p/src/lib.rs
git commit -m "feat(p2p): manage reply correlators"
```

### Task 2.2: `TransportEvent` + `TwoStreamEvent` manage variants

**Files:**
- Modify: `crates/p2p/src/transport.rs` (the `TransportEvent` enum), `crates/p2p/src/two_stream/event.rs`

- [ ] **Step 1:** Run `grep -rn "SEQueryRequest\|SEQueryReply" crates/p2p/src/transport.rs crates/p2p/src/two_stream/event.rs` to find both enums' SE variants.

- [ ] **Step 2: Write the failing test** in `two_stream/event.rs`:

```rust
#[test]
fn manage_two_stream_variants_exist() {
    fn _a(e: TwoStreamEvent) -> bool {
        matches!(e, TwoStreamEvent::ManageRequest { .. } | TwoStreamEvent::ManageReply { .. }
            | TwoStreamEvent::ManageQueryRequest { .. } | TwoStreamEvent::ManageQueryReply { .. })
    }
    let _ = _a;
}
```

Run: `cargo test -p p2p manage_two_stream_variants_exist` → FAIL.

- [ ] **Step 3: Write minimal implementation** — add the four variants to BOTH enums, mirroring `SEQueryRequest{peer_id, request}` / `SEQueryReply{peer_id, reply}` (use the exact `PeerId` type each enum uses — `TransportEvent` uses `crate::transport::PeerId`; `TwoStreamEvent` uses `libp2p::PeerId`):

```rust
// transport.rs TransportEvent:
ManageRequest { peer_id: PeerId, request: crate::message::ManageRequest },
ManageReply { peer_id: PeerId, reply: crate::message::ManageReply },
ManageQueryRequest { peer_id: PeerId, request: crate::message::ManageQueryRequest },
ManageQueryReply { peer_id: PeerId, reply: crate::message::ManageQueryReply },
```

(and the `libp2p::PeerId` equivalents in `two_stream/event.rs`).

- [ ] **Step 4:** Run `cargo test -p p2p manage_two_stream_variants_exist` → PASS, `cargo build -p p2p` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/transport.rs crates/p2p/src/two_stream/event.rs
git commit -m "feat(p2p): manage transport + two-stream event variants"
```

### Task 2.3: `P2PTransport` trait send methods (default unsupported)

**Files:**
- Modify: `crates/p2p/src/transport.rs`

- [ ] **Step 1: Write the failing test** — none needed beyond compile; the default impls are inert.

- [ ] **Step 2:** Run `grep -n "fn send_se_query_request\|fn send_se_query_response" crates/p2p/src/transport.rs`.

- [ ] **Step 3: Write minimal implementation** — add four trait methods mirroring `send_se_query_request`/`send_se_query_response` (default body returns `Err(Error::Transport("… not supported".into()))`):

```rust
async fn send_manage_request(&self, _peer_id: &PeerId, _req: crate::message::ManageRequest) -> Result<()> {
    Err(crate::error::Error::Transport("send_manage_request is not supported on this transport".into()))
}
async fn send_manage_response(&self, _peer_id: &PeerId, _reply: crate::message::ManageReply) -> Result<()> {
    Err(crate::error::Error::Transport("send_manage_response is not supported on this transport".into()))
}
async fn send_manage_query_request(&self, _peer_id: &PeerId, _req: crate::message::ManageQueryRequest) -> Result<()> {
    Err(crate::error::Error::Transport("send_manage_query_request is not supported on this transport".into()))
}
async fn send_manage_query_response(&self, _peer_id: &PeerId, _reply: crate::message::ManageQueryReply) -> Result<()> {
    Err(crate::error::Error::Transport("send_manage_query_response is not supported on this transport".into()))
}
```

(The `RecordingTransport` test mock in `subscriptions.rs` inherits these defaults — no change needed there unless a test exercises them.)

- [ ] **Step 4:** Run `cargo build -p p2p` → clean.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/transport.rs
git commit -m "feat(p2p): P2PTransport manage send methods (default unsupported)"
```

---

## Phase 3: Transport handlers (decode → verify → emit only)

### Task 3.1: libp2p two-stream handler methods

**Files:**
- Create: `crates/p2p/src/two_stream/handler/manage.rs`
- Modify: `crates/p2p/src/two_stream/handler/mod.rs`

Direct copy of `two_stream/handler/se_query.rs` (read it). For each channel provide `send_*_request_fire_and_forget`, `send_*_response`, `handle_*_request_stream`, `handle_*_response_stream`. The handlers call `read_cbor_message` (already bounded by `max_msg_size` + timed out — the #4718 fix), then `crate::verify_message(&msg)?` + `ensure_transport_sender(&peer_id, &msg)?`, then emit the event. No serve logic here.

- [ ] **Step 1: Write the failing test** (decode round-trip; full flow is Phase 7):

```rust
#[tokio::test]
async fn manage_request_decodes() {
    use crate::message::{ManageMutateOp, ManageRequest};
    let req = ManageRequest::new(ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] }, b"t".to_vec());
    let back: ManageRequest = serde_cbor::from_slice(&serde_cbor::to_vec(&req).unwrap()).unwrap();
    assert!(matches!(back.op, ManageMutateOp::CollectionAdd { .. }));
}
```

- [ ] **Step 2:** Run `cargo test -p p2p two_stream::handler::manage` → FAIL (module missing).

- [ ] **Step 3: Write minimal implementation** — copy `se_query.rs` → `manage.rs`, producing for the mutate channel (and the parallel query channel):

```rust
impl TwoStreamHandler {
    pub async fn send_manage_request_fire_and_forget(&mut self, peer_id: PeerId, request: crate::message::ManageRequest) -> Result<()> {
        let mut stream = self.control.open_stream(peer_id, Self::manage_request_protocol()).await
            .map_err(|e| Error::Transport(format!("failed to open manage stream: {e}")))?;
        write_message(&mut stream, &request).await
            .map_err(|e| Error::CborSerialization(format!("failed to write manage request: {e}")))?;
        Ok(())
    }
    pub async fn send_manage_response(&mut self, peer_id: PeerId, reply: crate::message::ManageReply) -> Result<()> {
        let mut stream = self.control.open_stream(peer_id, Self::manage_response_protocol()).await
            .map_err(|e| Error::Transport(format!("failed to open manage resp stream: {e}")))?;
        write_message(&mut stream, &reply).await
            .map_err(|e| Error::CborSerialization(format!("failed to write manage reply: {e}")))?;
        Ok(())
    }
    pub async fn handle_manage_request_stream(peer_id: PeerId, stream: Stream, max_msg_size: u64, t: std::time::Duration) -> Result<TwoStreamEvent> {
        let request = read_cbor_message::<crate::message::ManageRequest>(peer_id, stream, max_msg_size, t, "manage request").await?;
        crate::verify_message(&request)?;
        ensure_transport_sender(&peer_id, &request)?;
        Ok(TwoStreamEvent::ManageRequest { peer_id, request })
    }
    pub async fn handle_manage_response_stream(peer_id: PeerId, stream: Stream, max_msg_size: u64, t: std::time::Duration) -> Result<TwoStreamEvent> {
        let reply = read_cbor_message::<crate::message::ManageReply>(peer_id, stream, max_msg_size, t, "manage response").await?;
        crate::verify_message(&reply)?;
        ensure_transport_sender(&peer_id, &reply)?;
        Ok(TwoStreamEvent::ManageReply { peer_id, reply })
    }
}
```

Repeat for the query channel (`…manage_query…` over `ManageQueryRequest`/`ManageQueryReply`). Add the four `*_protocol()` helpers (return `StreamProtocol::new(crate::protocol::MANAGE_REQUEST_PROTOCOL)` etc., mirroring `se_query_request_protocol()`). Copy the private `read_cbor_message` helper into the file (or hoist it). Declare `mod manage;` beside `mod se_query;`.

- [ ] **Step 4:** Run `cargo test -p p2p two_stream::handler::manage` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/two_stream/handler/manage.rs crates/p2p/src/two_stream/handler/mod.rs
git commit -m "feat(p2p): libp2p two-stream manage handler methods"
```

### Task 3.2: libp2p inbound protocol registration/routing + `send_manage_*` impls

**Files:**
- Modify: the two-stream protocol registration/routing site and `crates/p2p/src/host/libp2p_transport.rs` (HostEvent→TransportEvent map at :441-445), `crates/p2p/src/host/p2p_host/two_stream.rs` (:279-296)

- [ ] **Step 1:** Run `grep -rn "SE_QUERY_REQUEST_PROTOCOL\|handle_se_query_request_stream\|SEQueryRequest" crates/p2p/src/host crates/p2p/src/two_stream` to list every SE query registration/routing/mapping site.

- [ ] **Step 2–3: Write minimal implementation** — at each site, add the four parallel manage protocols:
  - Register inbound `MANAGE_REQUEST_PROTOCOL`/`MANAGE_RESPONSE_PROTOCOL` (+ query) and route accepted streams to the Task 3.1 handlers, passing `protocols::MAX_MANAGE_MSG_SIZE as u64` and the existing stream-read timeout.
  - In `host/p2p_host/two_stream.rs`, map `TwoStreamEvent::Manage*` → `TransportEvent::Manage*` (mirror the SEQuery mapping at :279-296).
  - In `host/libp2p_transport.rs`, implement the trait `send_manage_request`/`send_manage_response`/query variants by signing with the host keypair and calling the Task 3.1 fire-and-forget senders (mirror how `send_se_query_request`/`send_se_query_response` are implemented here — find via `grep -n "send_se_query" crates/p2p/src/host/libp2p_transport.rs`).

> **No consumer yet:** the emitted `TransportEvent::Manage*` events have no handler until Phase 6, so this is side-effect-free per the sequencing invariant.

- [ ] **Step 4:** `cargo build -p p2p` → clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(p2p): register/route libp2p manage protocols + send impls"
```

### Task 3.3: iroh ALPN dispatch arms + `send_manage_*` impls

**Files:**
- Modify: `crates/p2p/src/iroh/endpoint_streams.rs`, `crates/p2p/src/iroh/transport.rs`, `crates/p2p/src/iroh/command.rs`

- [ ] **Step 1:** Run `grep -n "ALPN_SE_QUERY_REQ\|ALPN_SE_QUERY_RESP\|SEQueryRequest\|SEQueryReply" crates/p2p/src/iroh/endpoint_streams.rs crates/p2p/src/iroh/transport.rs crates/p2p/src/iroh/command.rs`.

- [ ] **Step 2–3: Write minimal implementation** — mirroring the SE query arms (`endpoint_streams.rs:401,423`; transport extract at `iroh/transport.rs:508,535`):
  - In `dispatch_stream`, add four arms: `ALPN_MANAGE_REQ` → `read_message::<ManageRequest>(&mut recv, MAX_MANAGE_MSG_SIZE)`, `verify_message`, **emit `TransportEvent::ManageRequest`** (do NOT call a correlator here — correlation is in the runtime handler, Phase 6). `ALPN_MANAGE_RESP` → read `ManageReply`, verify, **emit `TransportEvent::ManageReply`**. Same for the two query ALPNs.
  - Implement the iroh `send_manage_*` trait methods (sign + open ALPN stream + `write_message`), mirroring the iroh SE query send path in `iroh/transport.rs`/`command.rs`.

- [ ] **Step 4:** `cargo build -p p2p` → clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(p2p): iroh manage ALPN dispatch (emit events) + send impls"
```

---

## Phase 4: Error variant + actor-token verification

### Task 4.1: `Error::Unauthorized`

**Files:**
- Modify: `crates/p2p/src/error.rs`

- [ ] **Step 1: Write the failing test** in `error.rs`:

```rust
#[test]
fn unauthorized_displays() {
    assert_eq!(Error::Unauthorized("nope".into()).to_string(), "unauthorized: nope");
}
```

- [ ] **Step 2:** `cargo test -p p2p unauthorized_displays` → FAIL.

- [ ] **Step 3:** Add to the `Error` enum:

```rust
#[error("unauthorized: {0}")]
Unauthorized(String),
```

- [ ] **Step 4:** `cargo test -p p2p unauthorized_displays` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/error.rs
git commit -m "feat(p2p): Error::Unauthorized variant"
```

### Task 4.2: `verify_actor_token` (assembly layer)

**Files:**
- Create: `crates/db-merge/src/manage/auth.rs`
- Modify: `crates/db-merge/src/manage/mod.rs` (create), `crates/db-merge/src/lib.rs`

This wraps the identity crate: bytes → verified actor `Did`, with audience binding. Lives in db-merge (which depends on `identity`).

- [ ] **Step 1: Write the failing test** — first confirm the identity token-minting test helper: `grep -rn "new_token\|FullIdentity\|fn test_identity" crates/identity/src --include=*.rs`. Then:

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

(`mint_token_for(aud)` builds a `FullIdentity` and calls `identity::token::new_token(&id, Duration::from_secs(300), Some(aud.into()), None)`, returning `(Vec<u8>, Did)`. Base it on the identity crate's own token tests.)

- [ ] **Step 2:** `cargo test -p db-merge manage::auth` → FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
use identity::token::{from_token, verify_auth_token};
use identity::{Did, Identity};

/// Verify an actor JWT and return its DID, requiring `aud == expected_audience`.
pub fn verify_actor_token(token: &[u8], expected_audience: &str) -> Result<Did, String> {
    let ti = from_token(token).map_err(|e| format!("invalid actor token: {e}"))?;
    verify_auth_token(&ti, expected_audience).map_err(|e| format!("actor token rejected: {e}"))?;
    ti.did().map_err(|e| format!("token has no DID: {e}"))
}
```

Confirm the import paths (`grep -n "pub fn from_token\|pub fn verify_auth_token" crates/identity/src/token/mod.rs` and how they're re-exported from `identity` root). Register `pub mod manage;` with `pub mod auth;` inside it.

- [ ] **Step 4:** `cargo test -p db-merge manage::auth` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/db-merge/src/manage/ crates/db-merge/src/lib.rs
git commit -m "feat(db-merge): verify_actor_token with audience binding"
```

---

## Phase 5: Serve handler (auth → NAC → dispatch → sign → send)

### Task 5.1: NAC check seam (object-safe, assembly layer)

**Files:**
- Create: `crates/db-merge/src/manage/access.rs`

`NodeACP<S>` is generic; the serve handler takes an object-safe checker so it isn't generic over the store. Trait + blanket impl live in db-merge (which depends on `acp`).

- [ ] **Step 1: Write the failing test**

```rust
struct AllowAll;
#[async_trait::async_trait]
impl ManageAccessCheck for AllowAll {
    async fn check(&self, _: &identity::Did, _: acp::NodePermission) -> Result<bool, String> { Ok(true) }
}
#[tokio::test]
async fn allow_all_grants() {
    let c: std::sync::Arc<dyn ManageAccessCheck> = std::sync::Arc::new(AllowAll);
    assert!(c.check(&test_did(), acp::NodePermission::P2pReplicatorList).await.unwrap());
}
```

- [ ] **Step 2:** `cargo test -p db-merge manage::access` → FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
use async_trait::async_trait;
use identity::Did;

#[async_trait]
pub trait ManageAccessCheck: Send + Sync {
    async fn check(&self, actor: &Did, permission: acp::NodePermission) -> Result<bool, String>;
}

/// Adapter over the NAC engine. Generic over the Zanzibar store; erased behind
/// `Arc<dyn ManageAccessCheck>` at construction.
pub struct NacAccess<S>(pub std::sync::Arc<acp::NodeACP<S>>);

#[async_trait]
impl<S: zanzibar::ZanzibarStore + Send + Sync + 'static> ManageAccessCheck for NacAccess<S> {
    async fn check(&self, actor: &Did, permission: acp::NodePermission) -> Result<bool, String> {
        self.0.check_permission(actor, permission).await.map_err(|e| e.to_string())
    }
}
```

Confirm `acp::NodeACP` and the `ZanzibarStore` bound paths (`grep -n "pub use" crates/acp/src/lib.rs`; `grep -rn "ZanzibarStore" crates/zanzibar/src/lib.rs`). Add `zanzibar` to db-merge deps if needed for the bound (or relax to whatever bound `NodeACP`'s impl block uses — match `operations.rs:11`).

- [ ] **Step 4:** `cargo test -p db-merge manage::access` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/db-merge/src/manage/access.rs
git commit -m "feat(db-merge): ManageAccessCheck NAC seam + adapter"
```

### Task 5.2: Serve handlers

**Files:**
- Create: `crates/db-merge/src/manage/serve.rs`

Mirror `db-merge/src/se/serve.rs`. The audience the token must target is the **serving node's PeerID** (`transport.local_peer_id().to_string()`). Dispatch reuses the coordinator/transport ops; replicator-add dials the addresses first (mirroring the HTTP facade), then creates the replicator; peer-connect parses the multiaddr.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn unauthorized_actor_rejected_before_side_effects() {
    struct DenyAll;
    #[async_trait::async_trait]
    impl ManageAccessCheck for DenyAll {
        async fn check(&self, _: &identity::Did, _: acp::NodePermission) -> Result<bool, String> { Ok(false) }
    }
    let (coordinator, transport) = test_coordinator_and_transport().await; // mirror se serve tests / subscriptions.rs RecordingTransport
    let token = mint_token_for(transport.local_peer_id().as_str());
    let req = unsigned_manage_request(ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] }, token);
    let reply = handle_manage_request(&coordinator, &transport, &DenyAll, transport.local_peer_id().clone(), req).await;
    assert_eq!(reply.err_message(), Some("unauthorized"));
    assert!(coordinator.get_subscribed_collections().await.unwrap().is_empty());
}
```

- [ ] **Step 2:** `cargo test -p db-merge manage::serve` → FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
use identity::Did;
use p2p::message::{ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryRequest, ManageQueryResult, ManageReply, ManageRequest};
use p2p::message::traits::Message;
use p2p::transport::{P2PTransport, PeerAddr, PeerId};

use super::access::ManageAccessCheck;
use super::auth::verify_actor_token;

/// Serve a mutate request: authorize, dispatch, return a (still-unsigned) reply.
/// Signing + sending is done by the caller (runtime wiring), like se serve.
pub async fn build_manage_reply<C, T, A>(coordinator: &C, transport: &T, nac: &A, request: ManageRequest) -> ManageReply
where
    C: ManageOps, T: P2PTransport, A: ManageAccessCheck + ?Sized,
{
    let mid = request.message_id.clone();
    match authorize_and_apply(coordinator, transport, nac, &request).await {
        Ok(()) => ManageReply::success(&mid),
        Err(e) => ManageReply::error(&mid, &e),
    }
}

async fn authorize_and_apply<C, T, A>(coordinator: &C, transport: &T, nac: &A, request: &ManageRequest) -> Result<(), String>
where C: ManageOps, T: P2PTransport, A: ManageAccessCheck + ?Sized {
    let audience = transport.local_peer_id().to_string();
    let actor: Did = verify_actor_token(&request.auth_token, &audience)?;
    if !nac.check(&actor, request.op.permission()).await.map_err(|e| e)? {
        return Err("unauthorized".into());
    }
    coordinator.apply_mutate(transport, &request.op).await.map_err(|e| e.to_string())
}

pub async fn build_manage_query_reply<C, T, A>(coordinator: &C, transport: &T, nac: &A, request: ManageQueryRequest) -> ManageQueryReply
where C: ManageOps, T: P2PTransport, A: ManageAccessCheck + ?Sized {
    let mid = request.message_id.clone();
    let audience = transport.local_peer_id().to_string();
    let run = async {
        let actor = verify_actor_token(&request.auth_token, &audience)?;
        if !nac.check(&actor, request.op.permission()).await? { return Err("unauthorized".to_string()); }
        coordinator.apply_query(&request.op).await.map_err(|e| e.to_string())
    };
    match run.await {
        Ok(result) => ManageQueryReply::success(&mid, result),
        Err(e) => ManageQueryReply::error(&mid, &e),
    }
}
```

Define a small `ManageOps` trait in this file implemented for the concrete `SyncCoordinator` used by db-merge, so the serve fns stay store-generic-free:

```rust
#[async_trait::async_trait]
pub trait ManageOps {
    async fn apply_mutate<T: P2PTransport>(&self, transport: &T, op: &ManageMutateOp) -> p2p::error::Result<()>;
    async fn apply_query(&self, op: &ManageQueryOp) -> p2p::error::Result<ManageQueryResult>;
}
```

Implement `apply_mutate` by dispatching to the existing coordinator/transport methods:
- `ReplicatorAdd { addresses, collection_ids }`: for each multiaddr in `addresses`, parse → `(PeerId, PeerAddr)` using the **existing** address parser (find via `grep -rn "fn parse\|multiaddr" crates/p2p/src/address.rs`), `transport.dial(&peer_id, vec![addr]).await?`, then `coordinator.create_replicator(&peer_id, collection_ids.clone(), false).await?`.
- `ReplicatorDelete { peer_id, collection_ids }`: `let pid = PeerId::new(peer_id.clone());` then `delete_replicator(&pid)` (empty `collection_ids`) or `remove_replicator_collections(&pid, collection_ids.clone())`.
- `CollectionAdd/Remove`: loop `subscribe_collection`/`unsubscribe_collection`.
- `DocumentAdd/Remove`: loop `subscribe_document`/`unsubscribe_document`.
- `PeerConnect { address }`: parse multiaddr → `(PeerId, PeerAddr)`, `transport.dial(&peer_id, vec![addr]).await?`.

`apply_query`: `ReplicatorList` → `ManageQueryResult::Replicators { replicators: coordinator.list_replicators().await? }`; `CollectionList` → `ManageQueryResult::Strings { values: coordinator.get_subscribed_collections().await? }`.

> Use `p2p::error::Error::InvalidMultiaddress`/`InvalidPeerId` for parse failures — NOT `InvalidInput`/`Other`. `PeerId` is the string newtype: `PeerId::new(s.clone())`, never `s.parse()`.

- [ ] **Step 4:** `cargo test -p db-merge manage::serve` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/db-merge/src/manage/serve.rs
git commit -m "feat(db-merge): manage serve handlers (auth + NAC + dispatch)"
```

---

## Phase 6: Enable the channel (runtime wiring) + requester API

### Task 6.1: Construct + thread the manage correlators

**Files:**
- Modify: `crates/embedded/src/node_p2p.rs` (mirror `se_correlator` at :171-179), `crates/cli/src/commands/start/server_p2p.rs`

- [ ] **Step 1–3:** Where `SeQueryCorrelator::new()` is constructed and cloned into the transport + event loop, add `ManageCorrelator::new()` and `ManageQueryCorrelator::new()` the same way: one clone for the requester API (Task 6.3), one passed into the event handler (Task 6.2). Thread an `Arc<dyn ManageAccessCheck>` built from the node's `NodeACP` (`Arc::new(NacAccess(nac.clone()))`) into the event handler.
- [ ] **Step 4:** `cargo build -p embedded -p cli` → clean.
- [ ] **Step 5: Commit** `feat(embedded,cli): construct manage correlators + NAC access`

### Task 6.2: Route manage events to serve + correlator

**Files:**
- Modify: `crates/embedded/src/node_tasks.rs` (:88-101 libp2p, :172-184 iroh), `crates/cli/src/commands/start/server_p2p.rs` (:377-387, :794-804)

- [ ] **Step 1–3:** In every SE-query event match (libp2p + iroh, embedded + cli), add the four manage arms:
  - `TransportEvent::ManageRequest { peer_id, request }` →
    ```rust
    let mut reply = db_merge::manage::serve::build_manage_reply(coordinator.as_ref(), &transport, nac.as_ref(), request).await;
    if p2p::signing::sign_with_transport(&transport, &mut reply).is_ok() {
        let _ = transport.send_manage_response(&peer_id, reply).await;
    }
    ```
  - `TransportEvent::ManageQueryRequest { peer_id, request }` → `build_manage_query_reply` then `sign_with_transport` then `send_manage_query_response`.
  - `TransportEvent::ManageReply { reply, .. }` → `manage_correlator.deliver(reply);`
  - `TransportEvent::ManageQueryReply { reply, .. }` → `manage_query_correlator.deliver(reply);`

  This mirrors `se::serve::handle_query_request` + `se_correlator.deliver` exactly, with the sign-before-send step (the #1 review fix).

- [ ] **Step 4:** `cargo build -p embedded -p cli` → clean; `cargo test -p db-merge` → green.
- [ ] **Step 5: Commit** `feat: enable manage channel — serve + correlate wiring (libp2p + iroh)`

### Task 6.3: Requester API

**Files:**
- Modify: the host handle / a db-merge requester module (mirror the SE query requester — find via `grep -rn "SeQueryCorrelator\|register(" crates/db-merge/src crates/p2p/src/host/handle.rs`)

- [ ] **Step 1: Write the failing test** — covered by Phase 7 (needs two nodes). For this task, add a unit test that `register` + `deliver` round-trips through the requester wrapper if it is pure-async-testable; otherwise defer to Phase 7.

- [ ] **Step 2–3:** Add `manage(peer_id, op, auth_token) -> Result<ManageReply>` and `manage_query(peer_id, op, auth_token) -> Result<ManageQueryReply>` on the requester:
  - build `ManageRequest::new(op, auth_token)`, `signing::sign_message(keypair, &mut req)?` (or `sign_with_transport`) — this sets `message_id`;
  - `let mut pending = manage_correlator.register(req.message_id.clone());`
  - `transport.send_manage_request(&peer_id, req).await?;`
  - `tokio::time::timeout(REQUEST_TIMEOUT, pending.recv()).await` → map timeout to `Error::ResponseTimeout`.
  - mirror the exact SE query requester (`grep -rn "PendingSeQuery\|se_correlator" crates`).

- [ ] **Step 4:** `cargo build` workspace → clean.
- [ ] **Step 5: Commit** `feat: manage channel requester API`

---

## Phase 7: Integration tests (both transports)

### Task 7.1: `--test p2p` management module

**Files:**
- Create: `tools/integration-test/tests/p2p/management.rs`; Modify the `--test p2p` module root.

- [ ] **Step 1: Write the failing test** — model on `tools/integration-test/tests/p2p/replication.rs`. Node A drives B **over P2P only** (no HTTP to B). A mints actor tokens with `aud = B.peer_id()`:

```rust
#[tokio::test]
async fn manage_replicator_add_then_list_over_p2p() {
    let net = TwoNodeNet::start().await;
    let (a, b) = (net.node_a(), net.node_b());
    let token = a.actor_token_for(b.peer_id().as_str());
    let reply = a.manage_to(b.peer_id(), ManageMutateOp::ReplicatorAdd {
        addresses: a.listen_multiaddrs(), collection_ids: vec![b.collection_id("Users").await],
    }, token.clone()).await.expect("manage");
    assert!(reply.err_message().is_none());
    let listed = a.manage_query_to(b.peer_id(), ManageQueryOp::ReplicatorList, token).await.expect("query");
    assert!(matches!(listed.result, Some(ManageQueryResult::Replicators { .. })));
}

#[tokio::test]
async fn manage_denied_for_unauthorized_actor() {
    let net = TwoNodeNet::start_with_nac().await; // B NAC-enabled; A's actor not owner/admin
    let reply = net.node_a().manage_to(net.node_b().peer_id(),
        ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] },
        net.node_a().actor_token_for(net.node_b().peer_id().as_str()),
    ).await.expect("call completes");
    assert_eq!(reply.err_message(), Some("unauthorized"));
}
```

- [ ] **Step 2:** `cargo test -p integration-test --test p2p -- management::` → FAIL; iterate.
- [ ] **Step 3:** Add harness helpers (`manage_to`/`manage_query_to`/`actor_token_for`) wrapping the Task 6.3 API + the harness identity helpers.
- [ ] **Step 4:** `cargo test -p integration-test --test p2p -- management::` → PASS.
- [ ] **Step 5: Commit** `test(integration): p2p management channel over libp2p`

### Task 7.2: `--test p2p_iroh` mirror

**Files:**
- Create: `tools/integration-test/tests/p2p_iroh/management.rs`; Modify the `--test p2p_iroh` root.

- [ ] **Steps:** Copy Task 7.1 configured for iroh (primary target — defra-agent runs Iroh). Run `cargo test -p integration-test --test p2p_iroh -- management::` → PASS. Commit `test(integration): p2p management channel over iroh`.

---

## Phase 8: Final gate

### Task 8.1

- [ ] `cargo fmt --all`
- [ ] `cargo clippy --all -- -D warnings` — fix all.
- [ ] `cargo test -p p2p` and `cargo test -p db-merge` — green.
- [ ] `cargo test -p integration-test --test p2p` and `--test p2p_iroh` — green.
- [ ] `cargo test -p acp` — NAC suite unaffected.
- [ ] Commit `chore: fmt + clippy clean for management channel`.

---

## Self-review notes (for the implementer)

- **Replies MUST be signed** (review #1): every `send_manage_*` of a reply is preceded by `sign_with_transport(&transport, &mut reply)` in the runtime wiring (Task 6.2), exactly as `se/serve.rs:46`. The receive handler calls `verify_message`, so an unsigned reply is rejected by the requester.
- **No dependency cycle** (review #2): `crates/p2p` already depends on `acp`, so op→`acp::NodePermission` lives in p2p; the NAC adapter (`NacAccess`) lives in `crates/db-merge` (depends on acp + p2p + identity). Nothing in `acp` imports `p2p`.
- **Sequencing** (review #3): softened to "no state-mutating consumer before auth." Phases 1–3 only register protocols and emit unconsumed events; the serve handler is added in Phase 5 and routed in Phase 6.
- **DocumentList / PeerRemove deferred** (review #3/#4): not in v1. Open follow-up issues; PeerRemove needs a `P2PTransport::disconnect` primitive, DocumentList needs document-subscription storage (or the #1013 filtered-replication replacement).
- **PeerId is a string newtype** (review #4): `PeerId::new(s)` / `as_str()`, never `s.parse()`. Replicator/peer addresses are dialed via `transport.dial`, using the existing multiaddr parser in `crates/p2p/src/address.rs`.
- **iroh correlation in the runtime handler** (review #5/#6): `endpoint_streams` only emits `TransportEvent::Manage*`; `deliver` happens in `node_tasks.rs`/`server_p2p.rs`, like SE.
- **Symbols** (review #6): `Error::Unauthorized` is added (Task 4.1); use `InvalidMultiaddress`/`InvalidPeerId`/`Transport` elsewhere; the DID comes from `TokenIdentity::did()` (the `Identity` trait), not a field; no `Did::default()`.
- **Go-compat scope:** only the `MetaData` envelope is Go-byte-compatible; op enums are Rust-native (versioned). Tests assert CBOR self-round-trip.
- **No new `ReplicatorInfo` wire field:** the channel only reads it; Go `client.Replicator` compat is unaffected (that's #1013/B1, out of scope).
