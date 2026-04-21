//! libp2p pubsub request/response primitive compatible with Go's
//! [`sourcenetwork/go-libp2p-pubsub-rpc`](https://github.com/sourcenetwork/go-libp2p-pubsub-rpc).
//!
//! DefraDB's Go implementation layers this RPC protocol on top of gossipsub:
//! a publisher sends raw request bytes on a base topic, subscribers process
//! the message and publish a response envelope to a dynamic per-peer
//! `<base>/<caller>/_response` sub-topic. Requests are correlated by the
//! CID of the raw request bytes. This module implements the primitive in
//! Rust so DefraDB.rs nodes can interoperate with Go peers on `doc-sync` and
//! `sync-branchable` (issue #828).
//!
//! This module intentionally contains no libp2p `Swarm` code — it exposes a
//! transport-agnostic surface (publish → (topic, bytes); deliver → (topic,
//! from, bytes)). The gossipsub wiring lives in `crate::host::command_handler`
//! and `crate::host::p2p_host::protocols`.
//!
//! The protocol shape and wire-format constants:
//!
//! - Request ID: `CIDv1(raw, sha256(request_bytes))` — see [`id::derive_request_id`].
//! - Response sub-topic: `<base>/<caller-peer>/_response` — see
//!   [`topic::response_topic`].
//! - Response envelope: `{ID: string, From: bytes, Data: bytes, Err: string}`
//!   encoded as dag-cbor with a definite-length map — see
//!   [`envelope::InternalResponse`].

pub mod correlator;
pub mod envelope;
pub mod id;
pub mod topic;

pub use correlator::{Correlator, PreparedPublish, PublishOptions, PubsubResponse};
pub use envelope::InternalResponse;
pub use id::derive_request_id;
pub use topic::{response_topic, strip_response_topic};
