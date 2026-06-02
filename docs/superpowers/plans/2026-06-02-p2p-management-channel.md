# P2P Management Channel Implementation Plan (#1012)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an authenticated P2P request/response management channel so a peer can add/remove/list replicators, P2P collections, P2P documents, and connect/remove peers over the P2P wire — gated by the existing NAC engine keyed on the remote actor's DID.

**Architecture:** Two request/response channels mirroring the existing SE store/query split — `manage` (mutations → ack reply) and `manage_query` (reads → typed reply). Each is implemented exactly like the SE query two-stream protocol (`two_stream/handler/se_query.rs`) plus the parallel iroh ALPN dispatch, reusing `signing::sign_message`/`verify_message`, the bounded+timed `read_cbor_message`, and a `SeQueryCorrelator`-shaped correlator. Authorization is enforced in the request handler: verify the per-message host-key signature, verify an embedded actor JWT, bind it to this node via `aud`, then call `nac.check_permission(actor_did, perm)` before dispatching to the existing coordinator method. HTTP `/p2p/*` is left untouched.

**Tech Stack:** Rust, libp2p (`Stream`/stream-control), iroh (ALPN/QUIC), `serde`/`serde_cbor` (Go-byte-compatible CBOR), `tokio`, the `identity` crate (JWT `TokenIdentity`/`verify_auth_token`), the `acp` crate (`NodeACP::check_permission`, `NodePermission`).

**Reference templates (read these first):**
- Message types: `crates/p2p/src/message/se.rs`
- Two-stream req/resp handler: `crates/p2p/src/two_stream/handler/se_query.rs`
- Reply correlation: `crates/p2p/src/se_correlator.rs`
- iroh ALPNs + wire helpers: `crates/p2p/src/iroh/protocols.rs`
- libp2p protocol IDs: `crates/p2p/src/protocol.rs`
- Signing/verify (already clears sig before signing): `crates/p2p/src/signing.rs`
- NAC check: `crates/acp/src/nac/node_acp/operations.rs:17`
- Coordinator methods: `crates/p2p/src/sync/coordinator/{replicators,subscriptions}.rs`

**Design spec:** `docs/superpowers/specs/2026-06-02-p2p-management-channel-design.md`

**Sequencing rule (critical):** The channel must never be registered/dialed-handled without the A2 auth check (Phase 4) in place. Phases 1–3 build inert message/transport plumbing that is not wired into the host event loop until Phase 4 adds the authorizing handler. Do not enable dispatch arms to mutate state before Phase 4.

---

## Phase 0: Scaffolding & constants

### Task 0.1: Add op enums and the message module skeleton

**Files:**
- Create: `crates/p2p/src/message/manage.rs`
- Modify: `crates/p2p/src/message/mod.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/p2p/src/message/manage.rs` with only the enums + a round-trip test:

```rust
//! P2P management channel message types.
//!
//! Two request/reply pairs mirroring `se.rs` (the SE store/query split):
//! `Manage*` (mutations, ack reply) and `ManageQuery*` (reads, typed reply).
//! The `MetaData` envelope fields are byte-identical to `se.rs` for the shared
//! `signing`/`verify_message` path.

use serde::{Deserialize, Serialize};

/// Mutating management operations (ack reply). One channel, request-side enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageMutateOp {
    ReplicatorAdd {
        #[serde(rename = "PeerID")]
        peer_id: String,
        #[serde(rename = "Addresses", default)]
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
    PeerConnect {
        #[serde(rename = "Addresses")]
        addresses: Vec<String>,
    },
    PeerRemove {
        #[serde(rename = "PeerID")]
        peer_id: String,
    },
}

/// Read-only management operations (typed reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryOp {
    ReplicatorList,
    CollectionList,
    DocumentList,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutate_op_cbor_round_trip() {
        let op = ManageMutateOp::CollectionAdd {
            collection_ids: vec!["bafy-col".to_string()],
        };
        let bytes = serde_cbor::to_vec(&op).unwrap();
        let back: ManageMutateOp = serde_cbor::from_slice(&bytes).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn query_result_strings_round_trip() {
        let r = ManageQueryResult::Strings {
            values: vec!["a".into(), "b".into()],
        };
        let bytes = serde_cbor::to_vec(&r).unwrap();
        let back: ManageQueryResult = serde_cbor::from_slice(&bytes).unwrap();
        assert_eq!(r, back);
    }
}
```

Add to `crates/p2p/src/message/mod.rs` (mirror the existing `mod se;` + `pub use` lines):

```rust
mod manage;
pub use manage::{ManageMutateOp, ManageQueryOp, ManageQueryResult};
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p message::manage`
Expected: COMPILE FAIL initially if `mod manage` not yet wired, then PASS once both files exist. (The enums have no dependencies beyond `ReplicatorInfo`, which already exists in `crate::replicator`.)

- [ ] **Step 3: (covered by Step 1 — files contain the implementation)**

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p message::manage`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/message/manage.rs crates/p2p/src/message/mod.rs
git commit -m "feat(p2p): manage channel op enums + query result type"
```

---

## Phase 1: Message envelopes

### Task 1.1: `ManageRequest` / `ManageReply` (mutate channel)

**Files:**
- Modify: `crates/p2p/src/message/manage.rs`

These four structs are mechanically identical to `QuerySEArtifactsRequest`/`QuerySEArtifactsReply` in `se.rs` — same `MetaData` fields (`Version`, `MessageID`, `SenderID`, `Pubkey`, `Signature`, `ErrMessage`), same serde attributes (`nullable_bytes`/`optional_bytes`), same `impl Message`. **Copy the `se.rs` template exactly; change only the type names and the payload fields.** Add an `auth_token` field (the actor JWT) and an `op` field.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `manage.rs`:

```rust
use super::super::traits::Message;

#[test]
fn manage_request_round_trip_and_message_trait() {
    let mut req = ManageRequest::new(
        ManageMutateOp::DocumentRemove { doc_ids: vec!["bae-1".into()] },
        b"jwt-bytes".to_vec(),
    );
    req.set_message_id("mid-1".to_string());
    let bytes = serde_cbor::to_vec(&req).unwrap();
    let back: ManageRequest = serde_cbor::from_slice(&bytes).unwrap();
    assert_eq!(back.message_id(), "mid-1");
    assert_eq!(back.auth_token, b"jwt-bytes");
    assert!(matches!(back.op, ManageMutateOp::DocumentRemove { .. }));
}

#[test]
fn manage_reply_success_and_error() {
    let ok = ManageReply::success("mid-1");
    assert_eq!(ok.message_id(), "mid-1");
    assert!(ok.err_message().is_none());
    let err = ManageReply::error("mid-1", "unauthorized");
    assert_eq!(err.err_message(), Some("unauthorized"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p message::manage::tests::manage_request_round_trip_and_message_trait`
Expected: FAIL — `ManageRequest` not found.

- [ ] **Step 3: Write minimal implementation**

Add `ManageRequest` and `ManageReply` to `manage.rs`, copying the `QuerySEArtifactsRequest`/`PushSEArtifactsReply` shape from `se.rs`. Use these imports at the top of the file (matching `se.rs`):

```rust
use super::cbor::{nullable_bytes, optional_bytes};
use super::traits::Message;
use crate::protocol::MESSAGE_VERSION;
```

`ManageRequest` = the six `QuerySEArtifactsRequest` MetaData fields, then:

```rust
    /// Signed actor auth token (JWT). Authenticates the actor DID for NAC.
    #[serde(rename = "AuthToken", with = "serde_bytes")]
    pub auth_token: Vec<u8>,

    /// The management operation to perform.
    #[serde(rename = "Op")]
    pub op: ManageMutateOp,
```

with:

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

`ManageReply` = exactly `PushSEArtifactsReply` (ack-only: the six MetaData fields, no payload), renamed, with the same `success(request_message_id)` / `error(request_message_id, err)` constructors. Copy both `impl Message` blocks verbatim from `se.rs`, changing only the type name.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p message::manage`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/message/manage.rs
git commit -m "feat(p2p): ManageRequest/ManageReply envelopes"
```

### Task 1.2: `ManageQueryRequest` / `ManageQueryReply` (query channel)

**Files:**
- Modify: `crates/p2p/src/message/manage.rs`, `crates/p2p/src/message/mod.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn manage_query_reply_carries_typed_result() {
    let reply = ManageQueryReply::success(
        "mid-q",
        ManageQueryResult::Strings { values: vec!["c1".into()] },
    );
    let bytes = serde_cbor::to_vec(&reply).unwrap();
    let back: ManageQueryReply = serde_cbor::from_slice(&bytes).unwrap();
    assert_eq!(back.message_id(), "mid-q");
    match back.result {
        Some(ManageQueryResult::Strings { values }) => assert_eq!(values, vec!["c1"]),
        other => panic!("unexpected result: {other:?}"),
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p message::manage::tests::manage_query_reply_carries_typed_result`
Expected: FAIL — `ManageQueryRequest`/`ManageQueryReply` not found.

- [ ] **Step 3: Write minimal implementation**

Add `ManageQueryRequest` (six MetaData fields + `auth_token` + `op: ManageQueryOp`, mirroring Task 1.1's request) and `ManageQueryReply` (six MetaData fields + an optional payload):

```rust
    /// Typed result payload (absent on error).
    #[serde(rename = "Result", skip_serializing_if = "Option::is_none", default)]
    pub result: Option<ManageQueryResult>,
```

Constructors:

```rust
impl ManageQueryReply {
    pub fn success(request_message_id: &str, result: ManageQueryResult) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            result: Some(result),
        }
    }
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
            result: None,
        }
    }
}
```

Copy the `impl Message` blocks for both (verbatim from `se.rs`, renamed). Export all four types from `mod.rs`:

```rust
pub use manage::{ManageReply, ManageRequest, ManageQueryReply, ManageQueryRequest};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p message::manage`
Expected: PASS (all manage tests).

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/message/manage.rs crates/p2p/src/message/mod.rs
git commit -m "feat(p2p): ManageQueryRequest/ManageQueryReply envelopes"
```

---

## Phase 2: Protocol IDs, ALPNs, correlators

### Task 2.1: libp2p protocol IDs + iroh ALPNs

**Files:**
- Modify: `crates/p2p/src/protocol.rs`
- Modify: `crates/p2p/src/iroh/protocols.rs`

- [ ] **Step 1: Write the failing test**

Add to `crates/p2p/src/iroh/protocols.rs` tests (or a new `#[cfg(test)]` block):

```rust
#[test]
fn manage_alpns_are_registered() {
    assert!(ALL_ALPNS.contains(&ALPN_MANAGE_REQ));
    assert!(ALL_ALPNS.contains(&ALPN_MANAGE_RESP));
    assert!(ALL_ALPNS.contains(&ALPN_MANAGE_QUERY_REQ));
    assert!(ALL_ALPNS.contains(&ALPN_MANAGE_QUERY_RESP));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p manage_alpns_are_registered`
Expected: FAIL — `ALPN_MANAGE_REQ` not found.

- [ ] **Step 3: Write minimal implementation**

In `iroh/protocols.rs`, add (mirroring the SE query ALPN pair at lines 29–33):

```rust
/// ALPN for management mutate requests.
pub const ALPN_MANAGE_REQ: &[u8] = b"/defra-iroh/manage/0.1/req";
/// ALPN for management mutate responses.
pub const ALPN_MANAGE_RESP: &[u8] = b"/defra-iroh/manage/0.1/resp";
/// ALPN for management query requests.
pub const ALPN_MANAGE_QUERY_REQ: &[u8] = b"/defra-iroh/manage-query/0.1/req";
/// ALPN for management query responses.
pub const ALPN_MANAGE_QUERY_RESP: &[u8] = b"/defra-iroh/manage-query/0.1/resp";

/// Maximum size for management messages.
pub const MAX_MANAGE_MSG_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
```

Append all four to the `ALL_ALPNS` array.

In `protocol.rs`, add (mirroring `SE_QUERY_REQUEST_PROTOCOL` at lines 55–59):

```rust
pub const MANAGE_REQUEST_PROTOCOL: &str = "/defradb/manage_req/0.0.1";
pub const MANAGE_RESPONSE_PROTOCOL: &str = "/defradb/manage_resp/0.0.1";
pub const MANAGE_QUERY_REQUEST_PROTOCOL: &str = "/defradb/manage_query_req/0.0.1";
pub const MANAGE_QUERY_RESPONSE_PROTOCOL: &str = "/defradb/manage_query_resp/0.0.1";
```

If `protocol.rs` has `StreamProtocol` helper fns for the SE query protocols, add the four parallel helpers the same way.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p manage_alpns_are_registered`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/protocol.rs crates/p2p/src/iroh/protocols.rs
git commit -m "feat(p2p): manage + manage_query protocol IDs and ALPNs"
```

### Task 2.2: Reply correlators

**Files:**
- Create: `crates/p2p/src/manage_correlator.rs`
- Modify: `crates/p2p/src/lib.rs`

The correlator is a verbatim copy of `se_correlator.rs` parameterized to the two manage reply types. Create two correlators (`ManageCorrelator` for `ManageReply`, `ManageQueryCorrelator` for `ManageQueryReply`) by copying `SeQueryCorrelator` and changing the reply type — or one generic correlator. Use the copy approach to match the existing per-type style.

- [ ] **Step 1: Write the failing test**

Copy the `se_correlator.rs` test module into `manage_correlator.rs`, swapping in `ManageReply::success(id)` (no doc_ids arg) as the reply constructor. Key test:

```rust
#[tokio::test]
async fn register_then_deliver_routes_reply() {
    let c = ManageCorrelator::new();
    let mut pending = c.register("msg-1".to_string());
    assert_eq!(c.in_flight(), 1);
    assert!(c.deliver(ManageReply::success("msg-1")));
    assert_eq!(c.in_flight(), 0);
    let got = pending.recv().await.expect("reply");
    assert_eq!(got.message_id, "msg-1");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p manage_correlator`
Expected: FAIL — module/type not found.

- [ ] **Step 3: Write minimal implementation**

Copy `se_correlator.rs` to `manage_correlator.rs`. Replace `QuerySEArtifactsReply` → `ManageReply`, `SeQueryCorrelator` → `ManageCorrelator`, `PendingSeQuery` → `PendingManage`, and the `deliver` keyed on `reply.message_id` (same field). Then add a second trio (`ManageQueryCorrelator`/`PendingManageQuery` over `ManageQueryReply`) in the same file. Register the module in `lib.rs`:

```rust
mod manage_correlator;
pub use manage_correlator::{ManageCorrelator, ManageQueryCorrelator};
```

(Match how `se_correlator` is declared/exported in `lib.rs`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p manage_correlator`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/manage_correlator.rs crates/p2p/src/lib.rs
git commit -m "feat(p2p): manage reply correlators"
```

---

## Phase 3: Transport handlers (libp2p two-stream + iroh)

### Task 3.1: `TwoStreamEvent` variants for manage

**Files:**
- Modify: `crates/p2p/src/two_stream/event.rs`

- [ ] **Step 1: Write the failing test**

Add a compile-asserting test in `two_stream/event.rs`:

```rust
#[test]
fn manage_event_variants_exist() {
    fn _assert(e: TwoStreamEvent) -> bool {
        matches!(
            e,
            TwoStreamEvent::ManageRequest { .. }
                | TwoStreamEvent::ManageReply { .. }
                | TwoStreamEvent::ManageQueryRequest { .. }
                | TwoStreamEvent::ManageQueryReply { .. }
        )
    }
    let _ = _assert;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p two_stream::event::`
Expected: FAIL — variants not found.

- [ ] **Step 3: Write minimal implementation**

Add to the `TwoStreamEvent` enum (mirror `SEQueryRequest`/`SEQueryReply`):

```rust
    ManageRequest { peer_id: libp2p::PeerId, request: crate::message::ManageRequest },
    ManageReply { peer_id: libp2p::PeerId, reply: crate::message::ManageReply },
    ManageQueryRequest { peer_id: libp2p::PeerId, request: crate::message::ManageQueryRequest },
    ManageQueryReply { peer_id: libp2p::PeerId, reply: crate::message::ManageQueryReply },
```

(Match the exact `PeerId` import path used by the existing variants.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p two_stream::event::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/two_stream/event.rs
git commit -m "feat(p2p): TwoStreamEvent manage variants"
```

### Task 3.2: libp2p two-stream handler methods

**Files:**
- Create: `crates/p2p/src/two_stream/handler/manage.rs`
- Modify: `crates/p2p/src/two_stream/handler/mod.rs` (or wherever `se_query` is declared)

This file is a direct copy of `two_stream/handler/se_query.rs` with the manage types. It provides, for **each** channel: `send_*_request_fire_and_forget`, `send_*_response`, `handle_*_request_stream`, `handle_*_response_stream`. The request must be signed before sending (the SE path signs upstream of this fire-and-forget call; for manage we sign in the host command path — Task 5.x). The handler verifies the per-message signature via `crate::verify_message` and bounds the read via the existing `read_cbor_message` helper.

- [ ] **Step 1: Write the failing test**

Add an integration-style unit test at the bottom of `manage.rs` that exercises just the read/verify helper round-trip is deferred to Phase 6 (needs two live nodes). For this task, write a decode test:

```rust
#[tokio::test]
async fn manage_request_decodes_from_cbor() {
    use crate::message::{ManageMutateOp, ManageRequest};
    let req = ManageRequest::new(
        ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] },
        b"tok".to_vec(),
    );
    let bytes = serde_cbor::to_vec(&req).unwrap();
    let back: ManageRequest = serde_cbor::from_slice(&bytes).unwrap();
    assert!(matches!(back.op, ManageMutateOp::CollectionAdd { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p two_stream::handler::manage`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

Copy `se_query.rs` → `manage.rs`. For the mutate channel, produce:

```rust
impl TwoStreamHandler {
    pub async fn send_manage_request_fire_and_forget(
        &mut self,
        peer_id: PeerId,
        request: crate::message::ManageRequest,
    ) -> Result<()> {
        let mut stream = self
            .control
            .open_stream(peer_id, Self::manage_request_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open manage stream: {e}")))?;
        write_message(&mut stream, &request).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write manage request: {e}"))
        })?;
        Ok(())
    }

    pub async fn send_manage_response(
        &mut self,
        peer_id: PeerId,
        reply: crate::message::ManageReply,
    ) -> Result<()> {
        let mut stream = self
            .control
            .open_stream(peer_id, Self::manage_response_protocol())
            .await
            .map_err(|e| Error::Transport(format!("failed to open manage resp stream: {e}")))?;
        write_message(&mut stream, &reply).await.map_err(|e| {
            Error::CborSerialization(format!("failed to write manage reply: {e}"))
        })?;
        Ok(())
    }

    pub async fn handle_manage_request_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let request = read_cbor_message::<crate::message::ManageRequest>(
            peer_id, stream, max_msg_size, stream_read_timeout, "manage request",
        ).await?;
        crate::verify_message(&request)?;
        ensure_transport_sender(&peer_id, &request)?;
        Ok(TwoStreamEvent::ManageRequest { peer_id, request })
    }

    pub async fn handle_manage_response_stream(
        peer_id: PeerId,
        stream: Stream,
        max_msg_size: u64,
        stream_read_timeout: std::time::Duration,
    ) -> Result<TwoStreamEvent> {
        let reply = read_cbor_message::<crate::message::ManageReply>(
            peer_id, stream, max_msg_size, stream_read_timeout, "manage response",
        ).await?;
        crate::verify_message(&reply)?;
        ensure_transport_sender(&peer_id, &reply)?;
        Ok(TwoStreamEvent::ManageReply { peer_id, reply })
    }
}
```

Repeat the four methods for the query channel (`send_manage_query_request_fire_and_forget`, `send_manage_query_response`, `handle_manage_query_request_stream`, `handle_manage_query_response_stream`) over `ManageQueryRequest`/`ManageQueryReply`. Add the `manage_request_protocol()`/`manage_response_protocol()`/`manage_query_request_protocol()`/`manage_query_response_protocol()` helper fns mirroring `se_query_request_protocol()` (return `StreamProtocol::new(crate::protocol::MANAGE_REQUEST_PROTOCOL)` etc.). Copy the private `read_cbor_message` helper into this file (it is file-private in `se_query.rs`) or hoist it to the parent module and reuse. Declare `mod manage;` next to `mod se_query;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p two_stream::handler::manage`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/two_stream/handler/manage.rs crates/p2p/src/two_stream/handler/mod.rs
git commit -m "feat(p2p): libp2p two-stream manage handler methods"
```

### Task 3.3: libp2p stream-protocol registration + inbound routing

**Files:**
- Modify: the two-stream behaviour/protocol-registration site (find via `grep -rn "se_query_request_protocol\|SE_QUERY_REQUEST_PROTOCOL" crates/p2p/src/two_stream crates/p2p/src/behaviour*`)

- [ ] **Step 1: Write the failing test** — N/A (wiring task; covered by Phase 6 integration). Instead, add an assertion test where the protocol list is built, if one exists, asserting the four manage protocols are advertised. If no such test seam exists, skip to Step 3 and rely on Phase 6.

- [ ] **Step 2:** Run: `grep -rn "SE_QUERY_REQUEST_PROTOCOL" crates/p2p/src` to find every site that registers the SE query inbound stream protocol and routes accepted streams to `handle_se_query_request_stream` / `handle_se_query_response_stream`.

- [ ] **Step 3: Write minimal implementation**

At each such site, add parallel registration + routing for the four manage protocols, calling the Task 3.2 handler methods. Route `MANAGE_REQUEST_PROTOCOL` → `handle_manage_request_stream`, `MANAGE_RESPONSE_PROTOCOL` → `handle_manage_response_stream`, and the two query equivalents. Pass `protocols::MAX_MANAGE_MSG_SIZE as u64` and the existing stream-read timeout.

- [ ] **Step 4: Run test to verify it builds**

Run: `cargo build -p p2p`
Expected: builds clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(p2p): register + route libp2p manage stream protocols"
```

### Task 3.4: iroh ALPN dispatch arms

**Files:**
- Modify: `crates/p2p/src/iroh/endpoint_streams.rs`
- Modify: `crates/p2p/src/iroh/command.rs` and/or `crates/p2p/src/iroh/transport.rs` (send side — mirror the SE query iroh send path)

- [ ] **Step 1: Write the failing test** — N/A directly (covered by Phase 6 iroh integration). 

- [ ] **Step 2:** Run: `grep -n "ALPN_SE_QUERY_REQ\|ALPN_SE_QUERY_RESP" crates/p2p/src/iroh/endpoint_streams.rs` to find the SE query arms in `dispatch_stream`.

- [ ] **Step 3: Write minimal implementation**

In `dispatch_stream` (the match at `endpoint_streams.rs:~195`, unknown-ALPN drop at `:434`), add four arms mirroring the `ALPN_SE_QUERY_REQ`/`ALPN_SE_QUERY_RESP` arms:

- `x if x == protocols::ALPN_MANAGE_REQ` → read `ManageRequest` via `protocols::read_message::<ManageRequest>(&mut recv, protocols::MAX_MANAGE_MSG_SIZE)`, `crate::verify_message(&request)?`, emit `TransportEvent::…ManageRequest { peer_id, request }`.
- `x if x == protocols::ALPN_MANAGE_RESP` → read `ManageReply`, verify, deliver to the `ManageCorrelator` (mirror how the SE query resp arm calls the correlator).
- The two `MANAGE_QUERY` arms likewise over `ManageQueryRequest`/`ManageQueryReply` + `ManageQueryCorrelator`.

Add the matching `TransportEvent` variants (mirror the SE query transport events) and the iroh send methods (mirror the SE query iroh send in `iroh/command.rs`/`transport.rs`).

- [ ] **Step 4: Run test to verify it builds**

Run: `cargo build -p p2p`
Expected: builds clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(p2p): iroh manage ALPN dispatch + send"
```

---

## Phase 4: Authorization (the gating correctness work)

### Task 4.1: `NodeAccessCheck` trait

**Files:**
- Create: `crates/p2p/src/node_access.rs`
- Modify: `crates/p2p/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/p2p/src/node_access.rs`:

```rust
//! Transport-agnostic node-access check seam for the management channel.
//!
//! Implemented by the NAC engine in the `acp` crate and injected as
//! `Arc<dyn NodeAccessCheck>`, mirroring how `MergeHandler` is injected into the
//! replication loop. Keeps `crates/p2p` free of a heavy `acp` dependency.

use async_trait::async_trait;
use identity::Did;

pub use crate::message::manage_permission::ManagePermission;

#[async_trait]
pub trait NodeAccessCheck: Send + Sync {
    /// Return `true` if `actor` may perform `permission`. Implementations return
    /// `true` when NAC is disabled (parity with current behaviour).
    async fn check(&self, actor: &Did, permission: ManagePermission) -> crate::error::Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct AllowAll;
    #[async_trait]
    impl NodeAccessCheck for AllowAll {
        async fn check(&self, _actor: &Did, _permission: ManagePermission) -> crate::error::Result<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn allow_all_grants() {
        let checker: Arc<dyn NodeAccessCheck> = Arc::new(AllowAll);
        let did = Did::try_from("did:key:z6Mkexample").unwrap_or_else(|_| Did::default());
        assert!(checker.check(&did, ManagePermission::ReplicatorList).await.unwrap());
    }
}
```

> NOTE: `ManagePermission` is a small p2p-local enum (Task 4.2) — `crates/p2p` must NOT depend on `acp`'s `NodePermission` directly (would invert the dependency). The `acp`-side impl maps `ManagePermission` → `NodePermission`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p node_access`
Expected: FAIL — `ManagePermission`/module not found.

- [ ] **Step 3: Write minimal implementation**

Add `mod node_access; pub use node_access::NodeAccessCheck;` to `lib.rs`. Ensure `async-trait` is a dependency of `crates/p2p` (it almost certainly already is — check `Cargo.toml`; if not, add it). `ManagePermission` lands in Task 4.2; do that task before re-running.

- [ ] **Step 4: Run test to verify it passes** (after Task 4.2)

Run: `cargo test -p p2p node_access`
Expected: PASS.

- [ ] **Step 5: Commit** (combined with 4.2)

### Task 4.2: `ManagePermission` enum + op→permission mapping

**Files:**
- Create: `crates/p2p/src/message/manage_permission.rs`
- Modify: `crates/p2p/src/message/mod.rs`, `crates/p2p/src/message/manage.rs`

- [ ] **Step 1: Write the failing test**

Create `manage_permission.rs`:

```rust
//! P2P-local permission enum for management ops. Mapped to `acp::NodePermission`
//! by the NAC-side `NodeAccessCheck` impl, so `crates/p2p` stays acp-free.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagePermission {
    ReplicatorAdd,
    ReplicatorDelete,
    ReplicatorList,
    CollectionAdd,
    CollectionDelete,
    CollectionList,
    DocumentAdd,
    DocumentDelete,
    DocumentList,
    PeerConnect,
}
```

Add `permission()` methods on the op enums in `manage.rs` and a test:

```rust
#[test]
fn mutate_ops_map_to_permissions() {
    use crate::message::manage_permission::ManagePermission as P;
    assert_eq!(
        ManageMutateOp::PeerRemove { peer_id: "p".into() }.permission(),
        P::PeerConnect, // PeerRemove reuses PeerConnect per design
    );
    assert_eq!(
        ManageMutateOp::DocumentAdd { doc_ids: vec![] }.permission(),
        P::DocumentAdd,
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p mutate_ops_map_to_permissions`
Expected: FAIL — `permission()`/`ManagePermission` not found.

- [ ] **Step 3: Write minimal implementation**

Register `pub mod manage_permission;` in `message/mod.rs` and `pub use manage_permission::ManagePermission;`. Add to `manage.rs`:

```rust
use crate::message::manage_permission::ManagePermission;

impl ManageMutateOp {
    pub fn permission(&self) -> ManagePermission {
        match self {
            ManageMutateOp::ReplicatorAdd { .. } => ManagePermission::ReplicatorAdd,
            ManageMutateOp::ReplicatorDelete { .. } => ManagePermission::ReplicatorDelete,
            ManageMutateOp::CollectionAdd { .. } => ManagePermission::CollectionAdd,
            ManageMutateOp::CollectionRemove { .. } => ManagePermission::CollectionDelete,
            ManageMutateOp::DocumentAdd { .. } => ManagePermission::DocumentAdd,
            ManageMutateOp::DocumentRemove { .. } => ManagePermission::DocumentDelete,
            ManageMutateOp::PeerConnect { .. } => ManagePermission::PeerConnect,
            ManageMutateOp::PeerRemove { .. } => ManagePermission::PeerConnect,
        }
    }
}

impl ManageQueryOp {
    pub fn permission(&self) -> ManagePermission {
        match self {
            ManageQueryOp::ReplicatorList => ManagePermission::ReplicatorList,
            ManageQueryOp::CollectionList => ManagePermission::CollectionList,
            ManageQueryOp::DocumentList => ManagePermission::DocumentList,
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p manage`
Expected: PASS, plus `node_access` tests from Task 4.1 now pass.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/node_access.rs crates/p2p/src/message/manage_permission.rs crates/p2p/src/message/mod.rs crates/p2p/src/message/manage.rs crates/p2p/src/lib.rs
git commit -m "feat(p2p): NodeAccessCheck trait + ManagePermission mapping"
```

### Task 4.3: Actor token verification + audience binding

**Files:**
- Create: `crates/p2p/src/manage_auth.rs`
- Modify: `crates/p2p/src/lib.rs`

This isolates the "bytes → verified actor `Did`" step so it is unit-testable without a live network. It wraps the `identity` crate's `verify_auth_token` and enforces `aud == this node's identity`.

- [ ] **Step 1: Write the failing test**

First inspect the identity API: `grep -rn "pub fn verify_auth_token\|pub fn from_token\|fn new_token" crates/identity/src`. Then create `manage_auth.rs` with a function `verify_actor_token(token: &[u8], expected_audience: &str) -> Result<Did>` and a test that mints a token for the wrong audience and asserts rejection, and one for the right audience and asserts the DID round-trips. Use the same minting helper the identity tests use (find via `grep -rn "new_token\|new_token_from" crates/identity/src --include=*.rs` and the identity test module).

```rust
#[tokio::test]
async fn rejects_wrong_audience() {
    let (token, _did) = mint_test_token("did:node:OTHER");
    assert!(verify_actor_token(&token, "did:node:THIS").is_err());
}

#[tokio::test]
async fn accepts_matching_audience_and_returns_did() {
    let (token, did) = mint_test_token("did:node:THIS");
    assert_eq!(verify_actor_token(&token, "did:node:THIS").unwrap(), did);
}
```

(`mint_test_token` is a test helper in this module built from the identity crate's token-creation API — fill in once the identity API is confirmed in Step 1.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p manage_auth`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

Implement `verify_actor_token`: deserialize the `TokenIdentity` from the token bytes (`from_token` or equivalent), call the identity crate's `verify_auth_token(&identity, expected_audience)`, and on success return `identity.did`. Map verification failure to `crate::error::Error::Unauthorized` (add that variant to `error.rs` if absent — `grep -n "Unauthorized" crates/p2p/src/error.rs`; if missing add `#[error("unauthorized: {0}")] Unauthorized(String)`). Register `mod manage_auth;` in `lib.rs`.

Add `crates/identity` to `crates/p2p/Cargo.toml` dependencies if not already present (`grep -n "identity" crates/p2p/Cargo.toml`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p manage_auth`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/manage_auth.rs crates/p2p/src/lib.rs crates/p2p/src/error.rs crates/p2p/Cargo.toml
git commit -m "feat(p2p): actor token verification with audience binding"
```

### Task 4.4: Request processors (auth → NAC → coordinator → reply)

**Files:**
- Create: `crates/p2p/src/sync/coordinator/manage.rs`
- Modify: `crates/p2p/src/sync/coordinator/mod.rs`

This is the heart: the function that consumes a decoded `ManageRequest`, authorizes it, dispatches to the existing coordinator method, and produces a `ManageReply`. It takes the injected `Arc<dyn NodeAccessCheck>` and this node's audience string.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn unauthorized_actor_is_rejected_before_side_effects() {
    // A checker that denies everything.
    struct DenyAll;
    #[async_trait::async_trait]
    impl crate::NodeAccessCheck for DenyAll {
        async fn check(&self, _: &identity::Did, _: crate::message::ManagePermission)
            -> crate::error::Result<bool> { Ok(false) }
    }
    let coordinator = test_coordinator().await; // existing test helper in this module's tests
    let (token, _did) = mint_valid_token_for(coordinator.node_audience());
    let req = signed_manage_request(
        ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] },
        token,
    );
    let reply = coordinator
        .process_manage_request(req, Arc::new(DenyAll))
        .await;
    assert_eq!(reply.err_message(), Some("unauthorized"));
    assert!(coordinator.get_subscribed_collections().await.unwrap().is_empty());
}
```

(`test_coordinator`, `mint_valid_token_for`, `signed_manage_request` are small helpers; base `test_coordinator` on existing coordinator tests — `grep -rn "fn test_coordinator\|async fn .*coordinator.*test\|SyncCoordinator::new" crates/p2p/src/sync/coordinator`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p coordinator::manage`
Expected: FAIL — `process_manage_request` not found.

- [ ] **Step 3: Write minimal implementation**

```rust
use std::sync::Arc;
use crate::message::{ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryResult, ManageReply, ManageRequest, ManageQueryRequest};
use crate::node_access::NodeAccessCheck;
use crate::manage_auth::verify_actor_token;

impl<B, T> SyncCoordinator<B, T>
where
    B: blockstore::Blockstore + 'static,
    T: crate::transport::P2PTransport,
{
    /// This node's audience string for actor-token `aud` binding.
    pub fn node_audience(&self) -> String {
        // The node's own identity (PeerID or node-DID). Mirror how the node's
        // identity string is produced elsewhere (grep `local_peer_id`/node DID).
        self.local_identity_string()
    }

    pub async fn process_manage_request(
        &self,
        request: ManageRequest,
        nac: Arc<dyn NodeAccessCheck>,
    ) -> ManageReply {
        let mid = request.message_id.clone();
        match self.authorize_and_apply_mutate(&request, nac).await {
            Ok(()) => ManageReply::success(&mid),
            Err(e) => ManageReply::error(&mid, &e.to_string()),
        }
    }

    async fn authorize_and_apply_mutate(
        &self,
        request: &ManageRequest,
        nac: Arc<dyn NodeAccessCheck>,
    ) -> crate::error::Result<()> {
        // 1. actor identity + audience binding
        let actor = verify_actor_token(&request.auth_token, &self.node_audience())?;
        // 2. NAC
        let perm = request.op.permission();
        if !nac.check(&actor, perm).await? {
            return Err(crate::error::Error::Unauthorized("unauthorized".into()));
        }
        // 3. dispatch to existing coordinator methods
        match &request.op {
            ManageMutateOp::ReplicatorAdd { peer_id, addresses, collection_ids } => {
                let pid = peer_id.parse().map_err(|_| crate::error::Error::InvalidInput("bad peer id".into()))?;
                // addresses are applied by create_replicator's existing path; pass through as the HTTP handler does
                let _ = addresses;
                self.create_replicator(&pid, collection_ids.clone(), false).await?;
            }
            ManageMutateOp::ReplicatorDelete { peer_id, collection_ids } => {
                let pid = peer_id.parse().map_err(|_| crate::error::Error::InvalidInput("bad peer id".into()))?;
                if collection_ids.is_empty() {
                    self.delete_replicator(&pid).await?;
                } else {
                    self.remove_replicator_collections(&pid, collection_ids.clone()).await?;
                }
            }
            ManageMutateOp::CollectionAdd { collection_ids } => {
                for id in collection_ids { self.subscribe_collection(id).await?; }
            }
            ManageMutateOp::CollectionRemove { collection_ids } => {
                for id in collection_ids { self.unsubscribe_collection(id).await?; }
            }
            ManageMutateOp::DocumentAdd { doc_ids } => {
                for id in doc_ids { self.subscribe_document(id).await?; }
            }
            ManageMutateOp::DocumentRemove { doc_ids } => {
                for id in doc_ids { self.unsubscribe_document(id).await?; }
            }
            ManageMutateOp::PeerConnect { addresses } => {
                self.connect_peer_addrs(addresses.clone()).await?;
            }
            ManageMutateOp::PeerRemove { peer_id } => {
                let pid = peer_id.parse().map_err(|_| crate::error::Error::InvalidInput("bad peer id".into()))?;
                self.disconnect_peer(&pid).await?;
            }
        }
        Ok(())
    }

    pub async fn process_manage_query_request(
        &self,
        request: ManageQueryRequest,
        nac: Arc<dyn NodeAccessCheck>,
    ) -> ManageQueryReply {
        let mid = request.message_id.clone();
        match self.authorize_and_apply_query(&request, nac).await {
            Ok(result) => ManageQueryReply::success(&mid, result),
            Err(e) => ManageQueryReply::error(&mid, &e.to_string()),
        }
    }

    async fn authorize_and_apply_query(
        &self,
        request: &ManageQueryRequest,
        nac: Arc<dyn NodeAccessCheck>,
    ) -> crate::error::Result<ManageQueryResult> {
        let actor = verify_actor_token(&request.auth_token, &self.node_audience())?;
        if !nac.check(&actor, request.op.permission()).await? {
            return Err(crate::error::Error::Unauthorized("unauthorized".into()));
        }
        Ok(match request.op {
            ManageQueryOp::ReplicatorList => {
                ManageQueryResult::Replicators { replicators: self.list_replicators().await? }
            }
            ManageQueryOp::CollectionList => {
                ManageQueryResult::Strings { values: self.get_subscribed_collections().await? }
            }
            ManageQueryOp::DocumentList => {
                ManageQueryResult::Strings { values: self.get_subscribed_documents().await? }
            }
        })
    }
}
```

> The methods `connect_peer_addrs`, `disconnect_peer`, `local_identity_string`, and `get_subscribed_documents` may not exist yet — Tasks 5.1–5.2 add the missing ones. If `connect_peer_addrs`/`disconnect_peer` already exist under different names, use those (grep `fn connect`/`fn disconnect` in `sync/coordinator` and `transport.rs`). `Error::InvalidInput` — confirm the variant name in `error.rs`; use the existing bad-input variant.

Register `mod manage;` in `sync/coordinator/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p coordinator::manage`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/sync/coordinator/manage.rs crates/p2p/src/sync/coordinator/mod.rs
git commit -m "feat(p2p): manage request processors (auth + NAC + dispatch)"
```

---

## Phase 5: Coordinator method gaps + host wiring

### Task 5.1: `get_subscribed_documents` getter

**Files:**
- Modify: `crates/p2p/src/sync/coordinator/subscriptions.rs`

- [ ] **Step 1: Write the failing test**

Add next to the existing subscriptions tests:

```rust
#[tokio::test]
async fn subscribe_document_then_list_returns_it() {
    let coordinator = test_coordinator().await;
    coordinator.subscribe_document("bae-doc-1").await.unwrap();
    let docs = coordinator.get_subscribed_documents().await.unwrap();
    assert!(docs.contains(&"bae-doc-1".to_string()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p subscribe_document_then_list_returns_it`
Expected: FAIL — `get_subscribed_documents` not found.

- [ ] **Step 3: Write minimal implementation**

Mirror `get_subscribed_collections` (`subscriptions.rs:100-103`) exactly, reading from the document subscription store/in-memory set instead of the collection one:

```rust
/// Return the set of currently subscribed document IDs.
pub async fn get_subscribed_documents(&self) -> Result<Vec<String>> {
    // Mirror get_subscribed_collections, reading the document subscription set.
    Ok(self.subscribed_documents().await)
}
```

(Use the same backing field/store that `subscribe_document`/`unsubscribe_document` mutate — grep those methods to find it.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p p2p subscribe_document_then_list_returns_it`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/p2p/src/sync/coordinator/subscriptions.rs
git commit -m "feat(p2p): get_subscribed_documents coordinator getter"
```

### Task 5.2: Peer connect/disconnect + node audience helpers (only if missing)

**Files:**
- Modify: `crates/p2p/src/sync/coordinator/*.rs` and/or `crates/p2p/src/transport.rs`

- [ ] **Step 1:** Run `grep -rn "fn connect\|fn disconnect\|fn dial\|local_peer_id\|node.*did" crates/p2p/src/sync/coordinator crates/p2p/src/transport.rs crates/p2p/src/host`. If coordinator-level connect/disconnect and a node-identity string already exist, **skip this task** and wire Task 4.4 to the existing names.

- [ ] **Step 2–4:** If missing, add thin `connect_peer_addrs(&self, Vec<String>)`, `disconnect_peer(&self, &PeerId)`, and `local_identity_string(&self) -> String` on `SyncCoordinator`, each delegating to the existing `P2PTransport` trait methods (the transport already exposes `local_peer_id()` at `transport.rs:246` and dial/connect primitives used by the HTTP peer handlers — reuse those). Add one unit test per added method using `test_coordinator`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(p2p): coordinator peer connect/disconnect + identity helpers"
```

### Task 5.3: Wire manage events into the host event loop + register channels

**Files:**
- Modify: the host/two-stream event consumer (find via `grep -rn "TwoStreamEvent::SEQueryRequest" crates/p2p/src`)
- Modify: the iroh transport-event consumer (find via `grep -rn "SEQueryRequest\|SEQueryReply" crates/p2p/src/iroh crates/p2p/src/host`)
- Modify: node/host construction to inject `Arc<dyn NodeAccessCheck>`

This is the step that **enables** the channel — do it only now (Phases 1–4 complete).

- [ ] **Step 1: Write the failing test** — N/A in unit scope; verified by Phase 6.

- [ ] **Step 2:** Locate every place SE query request/reply events are consumed.

- [ ] **Step 3: Write minimal implementation**

- Where `TwoStreamEvent::SEQueryRequest`/`SEQueryReply` (and the iroh `TransportEvent` equivalents) are handled, add arms for the four manage variants:
  - `ManageRequest { peer_id, request }` → `let reply = coordinator.process_manage_request(request, nac.clone()).await; two_stream.send_manage_response(peer_id, reply).await?;`
  - `ManageQueryRequest { peer_id, request }` → `process_manage_query_request` then `send_manage_query_response`.
  - `ManageReply { reply, .. }` → `manage_correlator.deliver(reply);`
  - `ManageQueryReply { reply, .. }` → `manage_query_correlator.deliver(reply);`
- Thread an `Arc<dyn NodeAccessCheck>` and the two correlators into the host/coordinator runtime struct (mirror how the `SeQueryCorrelator` is stored and how handlers are injected).
- Add a public requester API on the host handle: `manage(peer_id, ManageMutateOp, auth_token) -> Result<ManageReply>` and `manage_query(peer_id, ManageQueryOp, auth_token) -> Result<ManageQueryReply>`. Each: build the request, `signing::sign_message(self.keypair(), &mut request)?` (mirror `host/handle.rs:578`), `correlator.register(message_id)`, `send_*_request_fire_and_forget`, then `pending.recv()` with a timeout (mirror the SE query requester path).

- [ ] **Step 4: Run test to verify it builds + unit suite passes**

Run: `cargo build -p p2p && cargo test -p p2p`
Expected: builds clean, all unit tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(p2p): enable manage channel — event wiring, correlators, requester API"
```

### Task 5.4: NAC-side `NodeAccessCheck` impl

**Files:**
- Create: `crates/acp/src/nac/node_access_check.rs` (or the node-level crate that constructs the P2P host with a NAC handle)
- Modify: the node/db assembly that builds the P2P host (find via `grep -rn "SyncCoordinator::new\|build.*p2p\|P2PHost" crates/db crates/http`)

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn maps_manage_permission_to_node_permission_and_checks() {
    let nac = test_nac_enabled_with_owner(&owner_did).await;
    let check = NacNodeAccessCheck::new(nac.clone());
    // owner is granted, a stranger is denied
    assert!(check.check(&owner_did, ManagePermission::ReplicatorAdd).await.unwrap());
    assert!(!check.check(&stranger_did, ManagePermission::ReplicatorAdd).await.unwrap());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p acp node_access_check` (or the crate hosting the impl)
Expected: FAIL — `NacNodeAccessCheck` not found.

- [ ] **Step 3: Write minimal implementation**

```rust
use async_trait::async_trait;
use identity::Did;
use p2p::message::ManagePermission;
use p2p::NodeAccessCheck;

use crate::nac::permission::NodePermission;
use crate::nac::node_acp::NodeACP;

pub struct NacNodeAccessCheck<S> { nac: std::sync::Arc<NodeACP<S>> }

impl<S> NacNodeAccessCheck<S> {
    pub fn new(nac: std::sync::Arc<NodeACP<S>>) -> Self { Self { nac } }
}

fn to_node_permission(p: ManagePermission) -> NodePermission {
    match p {
        ManagePermission::ReplicatorAdd => NodePermission::P2pReplicatorAdd,
        ManagePermission::ReplicatorDelete => NodePermission::P2pReplicatorDelete,
        ManagePermission::ReplicatorList => NodePermission::P2pReplicatorList,
        ManagePermission::CollectionAdd => NodePermission::P2pCollectionAdd,
        ManagePermission::CollectionDelete => NodePermission::P2pCollectionDelete,
        ManagePermission::CollectionList => NodePermission::P2pCollectionList,
        ManagePermission::DocumentAdd => NodePermission::P2pDocumentAdd,
        ManagePermission::DocumentDelete => NodePermission::P2pDocumentDelete,
        ManagePermission::DocumentList => NodePermission::P2pDocumentList,
        ManagePermission::PeerConnect => NodePermission::P2pPeerConnect,
    }
}

#[async_trait]
impl<S: zanzibar::ZanzibarStore + Send + Sync> NodeAccessCheck for NacNodeAccessCheck<S> {
    async fn check(&self, actor: &Did, permission: ManagePermission) -> p2p::error::Result<bool> {
        self.nac
            .check_permission(actor, to_node_permission(permission))
            .await
            .map_err(|e| p2p::error::Error::Other(e.to_string()))
    }
}
```

> Confirm the exact `NodePermission` variant identifiers via `grep -n "P2p" crates/acp/src/nac/permission.rs`. Confirm `acp` may depend on `p2p` without a cycle (p2p must NOT depend on acp — that's why `ManagePermission` lives in p2p). If `acp`→`p2p` would create a cycle, place this impl in the higher-level node/db crate that already depends on both.

Wire it at host construction: pass `Arc::new(NacNodeAccessCheck::new(nac))` as the `Arc<dyn NodeAccessCheck>` into the P2P runtime (Task 5.3's injection point).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p acp node_access_check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(acp): NacNodeAccessCheck wiring manage perms into NAC"
```

---

## Phase 6: Integration tests (both transports)

### Task 6.1: P2P management integration module

**Files:**
- Create: `tools/integration-test/tests/p2p/management.rs`
- Modify: `tools/integration-test/tests/p2p.rs` (or the `--test p2p` module root) to add `mod management;`

- [ ] **Step 1: Write the failing test**

Model on an existing `--test p2p` submodule (read `tools/integration-test/tests/p2p/replication.rs` for the two-node harness pattern). Write a test that starts node A and node B, where **B is reachable only via P2P** (do not call B's HTTP for the management ops), then drives the management channel from A:

```rust
#[tokio::test]
async fn manage_replicator_add_list_remove_over_p2p() {
    let net = TwoNodeNet::start().await; // existing harness or equivalent
    let (a, b) = (net.node_a(), net.node_b());

    // A asks B (over P2P only) to add a replicator back to A for `Users`.
    let reply = a.manage_to(
        b.peer_id(),
        ManageMutateOp::ReplicatorAdd {
            peer_id: a.peer_id().to_string(),
            addresses: a.listen_addrs(),
            collection_ids: vec![b.collection_id("Users").await],
        },
        a.actor_token_for(b.node_audience()),
    ).await.expect("manage call");
    assert!(reply.err_message().is_none());

    // list over the query channel reflects it
    let listed = a.manage_query_to(
        b.peer_id(),
        ManageQueryOp::ReplicatorList,
        a.actor_token_for(b.node_audience()),
    ).await.expect("query");
    assert!(matches!(listed.result, Some(ManageQueryResult::Replicators { .. })));

    // remove
    let removed = a.manage_to(
        b.peer_id(),
        ManageMutateOp::ReplicatorDelete { peer_id: a.peer_id().to_string(), collection_ids: vec![] },
        a.actor_token_for(b.node_audience()),
    ).await.expect("manage remove");
    assert!(removed.err_message().is_none());
}

#[tokio::test]
async fn manage_denied_for_unauthorized_actor() {
    let net = TwoNodeNet::start_with_nac().await; // B has NAC enabled, A's actor is not owner/admin
    let reply = net.node_a().manage_to(
        net.node_b().peer_id(),
        ManageMutateOp::CollectionAdd { collection_ids: vec!["c1".into()] },
        net.node_a().actor_token_for(net.node_b().node_audience()),
    ).await.expect("call completes");
    assert_eq!(reply.err_message(), Some("unauthorized"));
}
```

(The `manage_to`/`manage_query_to`/`actor_token_for`/`node_audience` helpers are thin wrappers over the Task 5.3 host-handle API + the integration harness's identity helpers. Add them to the harness support module alongside the existing P2P helpers.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p integration-test --test p2p -- management::`
Expected: FAIL (helpers/wiring incomplete) — iterate until the two tests pass.

- [ ] **Step 3: Implement harness helpers** as needed (no production code should be missing after Phase 5; fill only test-support glue).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p integration-test --test p2p -- management::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/integration-test/tests/p2p/management.rs tools/integration-test/tests/p2p.rs
git commit -m "test(integration): p2p management channel over libp2p"
```

### Task 6.2: Mirror under `--test p2p_iroh`

**Files:**
- Create: `tools/integration-test/tests/p2p_iroh/management.rs`
- Modify: the `--test p2p_iroh` module root

- [ ] **Step 1–4:** Copy Task 6.1's tests into the iroh test binary, configured for the iroh transport (mirror how an existing `p2p_iroh` submodule selects the iroh transport). Iroh is the primary target (defra-agent runs Iroh in prod).

Run: `cargo test -p integration-test --test p2p_iroh -- management::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(integration): p2p management channel over iroh"
```

---

## Phase 7: Final verification

### Task 7.1: Full gate

- [ ] **Step 1:** `cargo fmt --all`
- [ ] **Step 2:** `cargo clippy --all -- -D warnings` — fix all warnings.
- [ ] **Step 3:** `cargo test -p p2p` — all unit tests pass.
- [ ] **Step 4:** `cargo test -p integration-test --test p2p` and `--test p2p_iroh` — pass.
- [ ] **Step 5:** `cargo test -p acp` — NAC suite still green (no regression from the new impl).
- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(p2p): fmt + clippy clean for management channel"
```

---

## Self-review notes (for the implementer)

- **Sequencing invariant:** Phase 5.3 is the only step that enables the channel. Confirm no earlier dispatch arm mutates state without the Phase 4 auth path. If the iroh `dispatch_stream` arms (Task 3.4) emit events that an existing consumer would auto-handle, gate them behind the authorizing handler from the start.
- **No new wire fork of `ReplicatorInfo`:** this plan only *reads* `ReplicatorInfo` in `ManageQueryResult` — it does not add fields, so Go `client.Replicator` wire compat is unaffected (that concern belongs to #1013/B1, out of scope).
- **Grep-confirm before coding** every symbol marked "confirm via grep": `Error` variants (`Unauthorized`, `InvalidInput`, `Other`), the identity token API (`verify_auth_token`/`from_token`/token minting), coordinator connect/disconnect/identity helpers, and the exact `NodePermission::P2p*` variant names.
- **Dependency direction:** `crates/p2p` must not depend on `crates/acp`. `ManagePermission` lives in `p2p`; the mapping to `NodePermission` lives on the `acp` (or node) side. If `acp`→`p2p` is a cycle, host the `NacNodeAccessCheck` impl in the node/db crate.
