//! Two-stream protocol handler for Go compatibility.
//!
//! Go's DefraDB uses a two-stream pattern for request-response:
//! 1. Sender opens stream on `/defradb/rep_req/0.0.1`, sends request, closes stream
//! 2. Receiver processes request, opens NEW stream on `/defradb/rep_resp/0.0.1` to send response
//!
//! This is different from libp2p-rust's request-response which uses bidirectional streams.
//! This module implements Go's pattern for interoperability using libp2p-stream.

mod event;
mod handler;
mod runner;

pub use event::TwoStreamEvent;
pub use handler::TwoStreamHandler;
pub use runner::TwoStreamRunner;
