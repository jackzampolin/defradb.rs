use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::task::JoinHandle;

/// Open an SSE connection to `/api/v0/events?event=topic-peer-event`.
///
/// Waits for the HTTP connection to be established before returning,
/// ensuring the server-side subscription is active.
pub async fn open_peer_events_sse(api_url: &str) -> (JoinHandle<()>, Arc<Mutex<Vec<Value>>>) {
    open_events_sse(api_url, "topic-peer-event").await
}

/// Open an SSE connection to `/api/v0/events` with an event filter.
///
/// Waits for the HTTP connection to be established before returning,
/// ensuring the server-side event bus subscription is active and no
/// events will be missed.
pub async fn open_events_sse(
    api_url: &str,
    event_filter: &str,
) -> (JoinHandle<()>, Arc<Mutex<Vec<Value>>>) {
    let url = format!("{}/api/v0/events?event={}", api_url, event_filter);
    let events: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();

    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let resp = match client.get(&url).send().await {
            Ok(r) => {
                let _ = connected_tx.send(());
                r
            }
            Err(e) => {
                eprintln!("SSE events request failed: {}", e);
                let _ = connected_tx.send(());
                return;
            }
        };

        let mut buf = String::new();
        let mut stream = resp.bytes_stream();
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => break,
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buf.find("\n\n") {
                let block = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                let mut event_type = String::new();
                let mut data = String::new();
                for line in block.lines() {
                    if let Some(rest) = line.strip_prefix("event:") {
                        event_type = rest.trim().to_string();
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        data = rest.trim().to_string();
                    }
                }
                if event_type == "next" {
                    if let Ok(val) = serde_json::from_str::<Value>(&data) {
                        events_clone.lock().unwrap().push(val);
                    }
                }
            }
        }
    });

    // Wait for the HTTP connection to be established, ensuring the
    // server-side subscription is active before we return.
    let _ = connected_rx.await;

    (handle, events)
}

/// Wait until at least `expected_count` events have been collected, or timeout.
///
/// Returns the collected events. Panics if timeout is exceeded.
pub async fn wait_for_peer_events(
    events: &Arc<Mutex<Vec<Value>>>,
    expected_count: usize,
    timeout: Duration,
) -> Vec<Value> {
    let start = tokio::time::Instant::now();
    loop {
        let current = events.lock().unwrap().clone();
        if current.len() >= expected_count {
            return current;
        }
        if start.elapsed() >= timeout {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Open an SSE connection to `/api/v0/events?event=update`.
///
/// Subscribes to document update events emitted when P2P blocks are merged.
/// Waits for the connection to be established before returning.
pub async fn open_merge_events_sse(api_url: &str) -> (JoinHandle<()>, Arc<Mutex<Vec<Value>>>) {
    open_events_sse(api_url, "update").await
}

/// Wait until at least `count` update events have been collected, or panic.
pub async fn wait_for_merge_events(
    events: &Arc<Mutex<Vec<Value>>>,
    count: usize,
    timeout: Duration,
) {
    let start = tokio::time::Instant::now();
    loop {
        let current = events.lock().unwrap().clone();
        if current.len() >= count {
            return;
        }
        if start.elapsed() >= timeout {
            panic!(
                "timed out waiting for {} update events after {:?} (got {}): {:?}",
                count,
                timeout,
                current.len(),
                current
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
