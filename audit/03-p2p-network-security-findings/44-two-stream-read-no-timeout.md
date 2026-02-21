# Finding 44: Two-Stream `read_to_end` Has No Timeout (Slowloris Vector)

**Severity**: HIGH
**Category**: Slowloris / Resource Exhaustion
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

All `read_to_end` calls in the two-stream handler run without any timeout. A malicious peer can open a stream, send data at 1 byte per minute, and the spawned task will wait indefinitely for EOF. This is a classic Slowloris attack vector — each slow stream ties up a tokio task and its memory allocation forever.

## Evidence

**5 unbounded `read_to_end` calls** (no `tokio::time::timeout` wrapper):

1. `two_stream/handler/inbound.rs:34` — PushLog/DocSync/BranchableSync request:
```rust
stream.read_to_end(&mut buf).await
```

2. `two_stream/handler/inbound.rs:100` — PushLog/DocSync/BranchableSync response:
```rust
stream.read_to_end(&mut buf).await
```

3. `two_stream/handler/car.rs:15` — CAR request/response (shared helper):
```rust
stream.read_to_end(&mut buf).await
```

4. `two_stream/runner.rs:149` — SE request stream:
```rust
stream.read_to_end(&mut buf).await
```

5. `two_stream/runner.rs:165` — SE response stream:
```rust
stream.read_to_end(&mut buf).await
```

**Contrast with protected path**: The PushLogCodec path (`codec.rs:46`) uses `reader.take(MAX_MESSAGE_SIZE).read_to_end()` — which at least has a size limit, though also no timeout.

## Attack Scenario

1. Attacker opens 1000 streams on `/defradb/rep_req/0.0.1`
2. For each stream, sends 1 byte then pauses
3. Each spawned task (`two_stream/runner.rs:82`) is now blocked on `read_to_end`
4. Tasks never complete — they wait for EOF indefinitely
5. tokio thread pool is consumed; legitimate requests cannot be processed
6. Memory grows unboundedly as `Vec::new()` buffers accumulate

## Additional Context

The yamux idle stream timeout (default 30s) *might* eventually close stalled streams, but this depends on yamux detecting the stream as idle vs. in-progress. A stream that receives 1 byte every 29 seconds would never trigger the yamux idle timeout.

## Recommendation

Wrap every `read_to_end` in `tokio::time::timeout(Duration::from_secs(30), ...)`. Apply the existing `MAX_MESSAGE_SIZE` limit via `.take(MAX_MESSAGE_SIZE)` to all paths.
