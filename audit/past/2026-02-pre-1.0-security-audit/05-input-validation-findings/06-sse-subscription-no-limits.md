# Finding: SSE Subscription Has No Connection or Resource Limits

**Stream**: 05 - Input Validation
**Severity**: MEDIUM
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

The SSE (Server-Sent Events) subscription handler keeps connections open indefinitely, re-executing the subscription query on every database update event. There are no limits on connection count, connection duration, event rate, or per-event output size. An attacker can open many SSE connections with expensive subscription queries, then trigger update events to amplify the load.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/http/src/handlers/graphql/query.rs:273-342` | `graphql_sse()` | No connection timeout, no max connections |
| `crates/http/src/handlers/graphql/query.rs:199-225` | `graphql_transactional()` | SSE dispatched without connection limits |
| `crates/http/src/router/routes.rs:212` | Route definition | No per-route connection limits |

## Details

### Indefinite Connection Lifetime

The SSE handler loops indefinitely, waiting for update events from the event bus:

```rust
// query.rs:295-339
let stream = async_stream::stream! {
    while let Some(message) = subscription.recv().await {
        // Re-execute query on every update event
        if let Some(update) = message.as_update() {
            // ... docID filtering ...
            let response = execute_with_resolved_context(
                executor.clone(), req, signing_config.clone(), dac_bypass,
            ).await;
            // ... emit SSE event ...
            yield Ok(Event::default().event("next").data(json));
        }
    }
    yield Ok(Event::default().event("complete").data("{}"));
};
```

The stream only ends when the event bus channel closes (node shutdown). There is no:
- Connection timeout (e.g., max 1 hour)
- Idle timeout (e.g., no events for 5 minutes)
- Maximum event count per connection
- Keepalive/heartbeat mechanism

### Per-Event Query Re-execution

Every database update event triggers a full query re-execution. The subscription query is not cached — it's parsed, planned, and executed from scratch each time. If the subscription query is moderately expensive (joins, filters, aggregates), each event multiplies the cost.

### No Connection Counting

There is no limit on:
- Total concurrent SSE connections
- Per-IP SSE connections
- Per-identity SSE connections

An attacker can open hundreds of SSE connections from different source IPs, each with the same or different subscription queries.

### Amplification Attack

1. Open N SSE connections, each subscribing to updates on a large collection
2. Perform one document mutation (create/update/delete)
3. The mutation triggers one update event
4. All N subscriptions re-execute their queries = N * (query cost) CPU

With N=100 connections and a moderate query (scanning 10,000 documents), one mutation triggers 100 * 10,000 = 1,000,000 document reads.

### No Output Buffering or Backpressure

The SSE stream emits events as fast as they're generated. If events arrive faster than the client can consume them, they queue in memory (Axum/tokio channel buffers). A slow client with a fast event stream causes unbounded memory growth on the server side.

### Signing Config Resolved Once

One positive note: the signing config and DAC bypass are resolved once at subscription setup time (`resolve_signing_config`, `resolve_dac_bypass`), not per-event. This avoids re-authenticating per event.

### WebSocket: Not Applicable

The WebSocket handler returns 501 Not Implemented, so WebSocket-specific concerns don't apply:

```rust
pub async fn graphql_ws_handler() -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, "...")
}
```

## Impact

- **Connection exhaustion**: Hundreds of SSE connections exhaust server file descriptors
- **CPU amplification**: One mutation triggers N query executions across N subscriptions
- **Memory growth**: Slow clients cause unbounded SSE event buffering
- **No cleanup**: Connections persist until node restart or client disconnect

## Remediation

### Add Connection Limits

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

static ACTIVE_SSE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
const MAX_SSE_CONNECTIONS: usize = 50;

// In graphql_sse():
if ACTIVE_SSE_CONNECTIONS.fetch_add(1, Ordering::Relaxed) >= MAX_SSE_CONNECTIONS {
    ACTIVE_SSE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    return Err(HttpError::TooManyRequests("too many active subscriptions".into()));
}
// Decrement on stream completion using Drop guard
```

### Add Connection Timeout

```rust
use tokio::time::{timeout, Duration};

let stream = async_stream::stream! {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3600); // 1 hour
    loop {
        tokio::select! {
            msg = subscription.recv() => { /* process */ }
            _ = tokio::time::sleep_until(deadline) => {
                yield Ok(Event::default().event("complete").data("{}"));
                break;
            }
        }
    }
};
```

### Add Idle Timeout

Disconnect if no events are delivered for a configurable period (e.g., 5 minutes).

## Test Gap

No tests for:
- Multiple concurrent SSE connections
- SSE connection timeout/lifecycle
- SSE event delivery under load
- Slow client backpressure behavior
- Connection cleanup after client disconnect
