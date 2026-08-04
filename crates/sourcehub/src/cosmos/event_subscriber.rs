use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::task::AbortHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::access_cache::AccessCache;
use crate::provider::ProviderError;

use super::event_decoder::{decode_event, CacheInvalidation, EventDecodeError};

const SUBSCRIPTION_QUERY: &str = "tm.event='Tx' AND message.module='acp'";
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub(super) struct CosmosEventSubscriber {
    abort_handle: AbortHandle,
}

impl CosmosEventSubscriber {
    pub(super) fn start(
        websocket_url: String,
        cache: Arc<AccessCache>,
    ) -> Result<Self, ProviderError> {
        websocket_url
            .as_str()
            .into_client_request()
            .map_err(|error| {
                ProviderError::Config(format!("invalid SourceHub events WebSocket URL: {error}"))
            })?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            ProviderError::Config(format!(
                "SourceHub event invalidation requires a Tokio runtime: {error}"
            ))
        })?;
        let task = runtime.spawn(run(websocket_url, cache));
        Ok(Self {
            abort_handle: task.abort_handle(),
        })
    }
}

impl Drop for CosmosEventSubscriber {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

async fn run(websocket_url: String, cache: Arc<AccessCache>) {
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;
    loop {
        match tokio_tungstenite::connect_async(&websocket_url).await {
            Ok((mut socket, _)) => {
                reconnect_delay = INITIAL_RECONNECT_DELAY;
                let subscription = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "subscribe",
                    "params": { "query": SUBSCRIPTION_QUERY }
                });
                if let Err(error) = socket
                    .send(Message::Text(subscription.to_string().into()))
                    .await
                {
                    tracing::warn!(%error, "failed to subscribe to SourceHub ACP events");
                } else {
                    tracing::info!(url = %websocket_url, "subscribed to SourceHub ACP events");
                    if let Err(error) = consume(&mut socket, &cache).await {
                        tracing::warn!(%error, "SourceHub ACP event stream disconnected");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    retry_in_seconds = reconnect_delay.as_secs(),
                    "failed to connect to SourceHub ACP event stream"
                );
            }
        }

        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = reconnect_delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
    }
}

async fn consume<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    cache: &AccessCache,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => {
                if !process_event(&text, cache) {
                    return Ok(());
                }
            }
            Message::Binary(bytes) => match std::str::from_utf8(&bytes) {
                Ok(text) => {
                    if !process_event(text, cache) {
                        return Ok(());
                    }
                }
                Err(error) => {
                    let invalidated_entries = cache.clear();
                    tracing::warn!(
                        %error,
                        invalidated_entries,
                        "invalid SourceHub ACP event payload; cleared access cache"
                    );
                }
            },
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => return Ok(()),
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    Ok(())
}

fn process_event(message: &str, cache: &AccessCache) -> bool {
    let invalidations = match decode_event(message) {
        Ok(invalidations) => invalidations,
        Err(error) => {
            let invalidated_entries = cache.clear();
            tracing::warn!(
                %error,
                invalidated_entries,
                "could not decode SourceHub ACP event; cleared access cache"
            );
            return !matches!(error, EventDecodeError::Subscription(_));
        }
    };

    for invalidation in invalidations {
        let invalidated_entries = match &invalidation {
            CacheInvalidation::Object {
                policy_id,
                resource,
                object_id,
            } => cache.invalidate_object(policy_id, resource, object_id),
            CacheInvalidation::Policy(policy_id) => cache.invalidate_policy(policy_id),
            CacheInvalidation::All => cache.clear(),
        };
        tracing::debug!(
            ?invalidation,
            invalidated_entries,
            "invalidated cached SourceHub access decisions"
        );
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;

    #[test]
    fn malformed_event_clears_cached_grants() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);

        assert!(process_event("not-json", &cache));

        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "read"),
            None
        );
    }

    #[test]
    fn subscription_error_clears_cache_and_requests_reconnect() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);

        assert!(!process_event(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603}}"#,
            &cache
        ));
        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "read"),
            None
        );
    }

    #[tokio::test]
    async fn subscriber_rejects_invalid_urls_before_spawning() {
        let cache = Arc::new(AccessCache::new(Duration::from_secs(300)));
        let result = CosmosEventSubscriber::start("not a websocket URL".into(), cache);

        assert!(matches!(result, Err(ProviderError::Config(_))));
    }

    #[tokio::test]
    async fn subscriber_processes_events_and_stops_on_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_url = format!("ws://{}/websocket", listener.local_addr().unwrap());
        let cache = Arc::new(AccessCache::new(Duration::from_secs(300)));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);

        let subscriber = CosmosEventSubscriber::start(websocket_url, Arc::clone(&cache)).unwrap();
        let (stream, _) = timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let request = timeout(Duration::from_secs(1), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        let request: serde_json::Value = serde_json::from_str(&request).unwrap();
        assert_eq!(request["method"], "subscribe");
        assert_eq!(request["params"]["query"], SUBSCRIPTION_QUERY);

        socket.send(Message::Text("not-json".into())).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if cache
                    .get("did:key:alice", "p1", "users", "doc1", "read")
                    .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let abort_handle = subscriber.abort_handle.clone();
        drop(subscriber);
        timeout(Duration::from_secs(1), async {
            while !abort_handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
