# Finding: PushLogCodec Signing/Verification Is Dead Code in Production

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Misleading Security Architecture
**Status**: CONFIRMED

## Summary

The `PushLogCodec` in `codec.rs` implements message signing in `write_request`/`write_response` and verification in `read_request`/`read_response`. However, the codec is registered with the request-response behaviour using **zero protocols** (`std::iter::empty()`), meaning it is never invoked for actual message exchange. All production traffic flows through the two-stream handler, which has no signing or verification (Finding 12). The existence of signing code in `PushLogCodec` creates a false sense of security.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/behaviour.rs` | 183-188 | `PushLogCodec::with_keypair(keypair)` registered with `std::iter::empty()` — no protocols |
| `crates/p2p/src/codec.rs` | 153-242 | Signing/verification code that never executes in production |
| `crates/p2p/tests/codec_tests.rs` | 14-135 | All codec tests use `PushLogCodec::new()` (no keypair), so signing paths are also untested in codec tests |

## Details

### Dead Registration

```rust
// behaviour.rs:183-188
let codec = PushLogCodec::with_keypair(keypair.clone());
let pushlog = request_response::Behaviour::with_codec(
    codec,
    std::iter::empty::<(StreamProtocol, ProtocolSupport)>(),  // NO PROTOCOLS
    request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
);
```

The comment explains this is intentional: "We do NOT register rep_request or rep_response protocols here because stream::Behaviour handles those for Go two-stream compatibility. Request-response is kept for potential future Rust-only protocols."

### Test Coverage Gap

All 8 codec tests in `codec_tests.rs` use `PushLogCodec::new()` (no keypair), which disables signing:

```rust
// All tests use:
let mut codec = PushLogCodec::new();  // keypair = None, signing disabled
```

No codec test uses `PushLogCodec::with_keypair()`. The signing/verification paths in `read_request`, `read_response`, `write_request`, and `write_response` have zero test coverage.

### Confusion Risk

A code reviewer seeing `PushLogCodec::with_keypair(keypair)` in `behaviour.rs` might reasonably conclude that all PushLog messages are signed and verified. The dead-code nature of this registration is non-obvious without understanding the `std::iter::empty()` detail.

## Impact

- No production messages are signed or verified through this code path
- Creates a false security signal for auditors and reviewers
- Test suite provides false confidence (signing tests pass but never exercise the production code path)

## Remediation

Either:
1. Add signing/verification to the two-stream handler (see Finding 12), or
2. Remove the dead PushLogCodec signing code and its keypair to reduce confusion, or
3. If Rust-only protocols are planned, document explicitly that this is future code not currently active

## Test Gap

No test verifies that `PushLogCodec::with_keypair()` actually signs/verifies messages through the codec. The signing tests in `signing_tests.rs` test the functions directly but not through the codec integration.
