//! Two-stream protocol runner.
//!
//! Runner that accepts incoming streams and emits events.
//! This should be spawned as a separate task.

use std::sync::Arc;

use futures::AsyncReadExt as _;
use futures::StreamExt;
use libp2p_stream as stream;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Semaphore};

use super::event::TwoStreamEvent;
use super::handler::{PendingResponses, TwoStreamHandler};
use crate::two_stream::{MAX_MSG_SIZE, STREAM_READ_TIMEOUT};

/// Maximum number of concurrently active stream handler tasks.
///
/// Caps the number of tokio tasks spawned from incoming streams.
/// Without this, a peer opening many streams rapidly can exhaust memory
/// and thread-pool capacity via unbounded task creation.
const MAX_CONCURRENT_STREAM_TASKS: usize = 64;

/// Runner that accepts incoming streams and emits events.
///
/// This should be spawned as a separate task.
pub struct TwoStreamRunner {
    /// Pending responses for lock-free response processing.
    /// Stored separately from the handler to avoid holding the handler's
    /// tokio::sync::Mutex during network I/O in response stream reads.
    pending: Arc<Mutex<PendingResponses>>,
    /// Incoming request streams.
    request_streams: stream::IncomingStreams,
    /// Incoming response streams.
    response_streams: stream::IncomingStreams,
    /// Incoming SE request streams.
    se_request_streams: stream::IncomingStreams,
    /// Incoming SE response streams.
    se_response_streams: stream::IncomingStreams,
    /// Incoming CAR request streams.
    car_request_streams: stream::IncomingStreams,
    /// Incoming CAR response streams.
    car_response_streams: stream::IncomingStreams,
    /// Channel to send events.
    event_tx: mpsc::Sender<TwoStreamEvent>,
    /// Bounds the number of concurrently active stream handler tasks.
    semaphore: Arc<Semaphore>,
}

impl TwoStreamRunner {
    /// Create a new runner.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pending: Arc<Mutex<PendingResponses>>,
        request_streams: stream::IncomingStreams,
        response_streams: stream::IncomingStreams,
        se_request_streams: stream::IncomingStreams,
        se_response_streams: stream::IncomingStreams,
        car_request_streams: stream::IncomingStreams,
        car_response_streams: stream::IncomingStreams,
        event_tx: mpsc::Sender<TwoStreamEvent>,
    ) -> Self {
        Self {
            pending,
            request_streams,
            response_streams,
            se_request_streams,
            se_response_streams,
            car_request_streams,
            car_response_streams,
            event_tx,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAM_TASKS)),
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
                    let sem = self.semaphore.clone();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
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
                    let pending = self.pending.clone();
                    let event_tx = self.event_tx.clone();
                    let sem = self.semaphore.clone();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
                        match TwoStreamHandler::handle_response_stream(&pending, peer_id, stream).await {
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
                // Handle incoming SE request streams (deserialize and forward for storage)
                Some((peer_id, mut stream)) = self.se_request_streams.next() => {
                    let sem = self.semaphore.clone();
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
                        let mut buf = Vec::new();
                        let read_result = tokio::time::timeout(
                            STREAM_READ_TIMEOUT,
                            (&mut stream).take(MAX_MSG_SIZE).read_to_end(&mut buf),
                        ).await;
                        match read_result {
                            Err(_) => {
                                tracing::warn!(peer_id = %peer_id, "SE request stream read timed out");
                            }
                            Ok(Err(e)) => {
                                tracing::warn!(peer_id = %peer_id, error = %e, "Failed to read SE request stream");
                            }
                            Ok(Ok(_)) => {
                                match serde_cbor::from_slice::<crate::message::PushSEArtifactsRequest>(&buf) {
                                    Ok(request) => {
                                        tracing::info!(
                                            peer_id = %peer_id,
                                            collection_id = %request.collection_id,
                                            artifact_count = request.artifacts.len(),
                                            "Received SE artifacts from peer"
                                        );
                                        let _ = event_tx.send(TwoStreamEvent::SEArtifactsReceived {
                                            peer_id,
                                            request,
                                        }).await;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            peer_id = %peer_id,
                                            error = %e,
                                            buf_len = buf.len(),
                                            "Failed to deserialize SE artifacts request"
                                        );
                                    }
                                }
                            }
                        }
                    });
                }
                // Handle incoming SE response streams (replies to our SE pushes)
                Some((peer_id, mut stream)) = self.se_response_streams.next() => {
                    let sem = self.semaphore.clone();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
                        let mut buf = Vec::new();
                        let read_result = tokio::time::timeout(
                            STREAM_READ_TIMEOUT,
                            (&mut stream).take(MAX_MSG_SIZE).read_to_end(&mut buf),
                        ).await;
                        match read_result {
                            Err(_) => {
                                tracing::warn!(peer_id = %peer_id, "SE response stream read timed out");
                            }
                            Ok(Err(e)) => {
                                tracing::warn!(peer_id = %peer_id, error = %e, "Failed to read SE response stream");
                            }
                            Ok(Ok(_)) => {
                                tracing::debug!(
                                    peer_id = %peer_id,
                                    buf_len = buf.len(),
                                    "Received SE response (acknowledgement)"
                                );
                            }
                        }
                    });
                }
                // Handle incoming CAR request streams
                Some((peer_id, stream)) = self.car_request_streams.next() => {
                    let event_tx = self.event_tx.clone();
                    let sem = self.semaphore.clone();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
                        match TwoStreamHandler::handle_car_request_stream(peer_id, stream).await {
                            Ok(event) => {
                                if event_tx.send(event).await.is_err() {
                                    tracing::warn!(peer_id = %peer_id, "Failed to send CAR request event");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(peer_id = %peer_id, error = %e, "Failed to handle CAR request stream");
                            }
                        }
                    });
                }
                // Handle incoming CAR response streams
                Some((peer_id, stream)) = self.car_response_streams.next() => {
                    let event_tx = self.event_tx.clone();
                    let sem = self.semaphore.clone();
                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
                        match TwoStreamHandler::handle_car_response_stream(peer_id, stream).await {
                            Ok(event) => {
                                if event_tx.send(event).await.is_err() {
                                    tracing::warn!(peer_id = %peer_id, "Failed to send CAR response event");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(peer_id = %peer_id, error = %e, "Failed to handle CAR response stream");
                            }
                        }
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
