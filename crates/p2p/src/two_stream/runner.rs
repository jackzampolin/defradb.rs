//! Two-stream protocol runner.
//!
//! Runner that accepts incoming streams and emits events.
//! This should be spawned as a separate task.

use std::sync::Arc;

use futures::StreamExt;
use libp2p_stream as stream;
use tokio::sync::mpsc;

use super::event::TwoStreamEvent;
use super::handler::TwoStreamHandler;

/// Runner that accepts incoming streams and emits events.
///
/// This should be spawned as a separate task.
pub struct TwoStreamRunner {
    /// Handler for processing streams.
    handler: Arc<tokio::sync::Mutex<TwoStreamHandler>>,
    /// Incoming request streams.
    request_streams: stream::IncomingStreams,
    /// Incoming response streams.
    response_streams: stream::IncomingStreams,
    /// Incoming SE request streams.
    se_request_streams: stream::IncomingStreams,
    /// Incoming SE response streams.
    se_response_streams: stream::IncomingStreams,
    /// Channel to send events.
    event_tx: mpsc::Sender<TwoStreamEvent>,
}

impl TwoStreamRunner {
    /// Create a new runner.
    pub fn new(
        handler: Arc<tokio::sync::Mutex<TwoStreamHandler>>,
        request_streams: stream::IncomingStreams,
        response_streams: stream::IncomingStreams,
        se_request_streams: stream::IncomingStreams,
        se_response_streams: stream::IncomingStreams,
        event_tx: mpsc::Sender<TwoStreamEvent>,
    ) -> Self {
        Self {
            handler,
            request_streams,
            response_streams,
            se_request_streams,
            se_response_streams,
            event_tx,
        }
    }

    /// Run the stream handler loop.
    pub async fn run(mut self) {
        tracing::info!(
            "Two-stream runner started - listening for Go request/response streams on {} and {}",
            TwoStreamHandler::request_protocol(),
            TwoStreamHandler::response_protocol()
        );

        loop {
            tokio::select! {
                // Handle incoming request streams
                Some((peer_id, stream)) = self.request_streams.next() => {
                    tracing::info!(
                        peer_id = %peer_id,
                        "Received incoming stream on request protocol"
                    );
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        match TwoStreamHandler::handle_request_stream(peer_id, stream).await {
                            Ok(event) => {
                                tracing::info!(peer_id = %peer_id, "Sending TwoStreamEvent to host channel");
                                if event_tx.send(event).await.is_err() {
                                    tracing::warn!(
                                        peer_id = %peer_id,
                                        "Failed to send two-stream event - receiver dropped"
                                    );
                                } else {
                                    tracing::info!(peer_id = %peer_id, "Successfully sent TwoStreamEvent to host channel");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer_id = %peer_id,
                                    error = %e,
                                    "Failed to handle request stream"
                                );
                                let _ = event_tx.send(TwoStreamEvent::DecodeError {
                                    peer_id,
                                    error: e.to_string(),
                                }).await;
                            }
                        }
                    });
                }
                // Handle incoming response streams
                Some((peer_id, stream)) = self.response_streams.next() => {
                    tracing::info!(
                        peer_id = %peer_id,
                        "Received incoming stream on response protocol"
                    );
                    let handler = self.handler.clone();
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let h = handler.lock().await;
                        match h.handle_response_stream(peer_id, stream).await {
                            Ok(Some(event)) => {
                                // DocSyncReply events should be forwarded to the coordinator
                                tracing::debug!(peer_id = %peer_id, "Sending DocSyncReply event to host channel");
                                if event_tx.send(event).await.is_err() {
                                    tracing::warn!(
                                        peer_id = %peer_id,
                                        "Failed to send DocSyncReply event - receiver dropped"
                                    );
                                } else {
                                    tracing::debug!(peer_id = %peer_id, "Sent DocSyncReply event to host channel");
                                }
                            }
                            Ok(None) => {
                                tracing::trace!(peer_id = %peer_id, "PushLogReply handled internally via pending channels");
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer_id = %peer_id,
                                    error = %e,
                                    "Failed to handle response stream"
                                );
                            }
                        }
                    });
                }
                // Handle incoming SE request streams (Rust receiving SE artifacts - log for now)
                Some((peer_id, mut stream)) = self.se_request_streams.next() => {
                    tokio::spawn(async move {
                        use futures::AsyncReadExt;
                        let mut buf = Vec::new();
                        if let Err(e) = stream.read_to_end(&mut buf).await {
                            tracing::warn!(peer_id = %peer_id, error = %e, "Failed to read SE request stream");
                            return;
                        }
                        tracing::info!(
                            peer_id = %peer_id,
                            buf_len = buf.len(),
                            "Received SE request stream (Rust as receiver not yet implemented)"
                        );
                    });
                }
                // Handle incoming SE response streams (replies to our SE pushes)
                Some((peer_id, mut stream)) = self.se_response_streams.next() => {
                    tokio::spawn(async move {
                        use futures::AsyncReadExt;
                        let mut buf = Vec::new();
                        if let Err(e) = stream.read_to_end(&mut buf).await {
                            tracing::warn!(peer_id = %peer_id, error = %e, "Failed to read SE response stream");
                            return;
                        }
                        tracing::debug!(
                            peer_id = %peer_id,
                            buf_len = buf.len(),
                            "Received SE response (acknowledgement)"
                        );
                    });
                }
                else => {
                    tracing::info!("Two-stream runner shutting down");
                    break;
                }
            }
        }
    }
}
