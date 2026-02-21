# Finding 51: Yamux Default Max Concurrent Streams = 256

**Severity**: LOW (informational, extends Finding 05)
**Category**: Configuration Audit
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

Finding 05 noted that yamux uses default stream limits. This finding documents the actual default values from the `yamux` crate and analyzes their adequacy.

## Evidence

**Yamux configuration** (`host/p2p_host/mod.rs:195,209`):
```rust
.with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
```

**libp2p-yamux defaults** (from `yamux::Config::default()` in the `libp2p-yamux` crate):

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_num_streams` | 256 | Maximum concurrent streams per connection |
| `max_buffer_size` | 16MB (16 × 1024 × 1024) | Maximum buffer size per stream |
| `receive_window` | 256KB | Flow control window |
| `read_after_close` | true | Continue reading after stream close |

## Analysis

**256 concurrent streams per connection**: This is the effective limit on how many simultaneous two-stream protocol messages a single peer can send. Each PushLog request opens a stream, sends data, then closes it. Since Finding 44 shows `read_to_end` has no timeout, a slow peer could hold 256 streams open simultaneously — but cannot exceed 256 per connection.

**16MB max buffer size**: Aligns with the `MAX_MESSAGE_SIZE` constant in `codec.rs:27`. This means yamux will not buffer more than 16MB per stream, providing an implicit size limit even for the two-stream path that lacks explicit size limits.

**Combined with Finding 43 (no per-peer connection limits)**: A peer could open multiple connections (each getting 256 streams). Without connection limits, the effective stream limit is 256 × number_of_connections — which is unbounded.

## Assessment

256 streams per connection is reasonable. The 16MB buffer limit provides implicit protection against oversized messages on individual streams. The real issue is the combination with unlimited connections (Finding 43).

## Recommendation

Set yamux `max_num_streams` to 128 (lower is safer) and combine with connection limits (Finding 43) to cap total streams per peer.
