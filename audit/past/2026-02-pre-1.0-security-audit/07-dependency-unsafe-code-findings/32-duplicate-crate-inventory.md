# Duplicate Crate Version Inventory

**Severity:** Low (mostly), Medium (crypto)
**Category:** Dependency hygiene — Binary bloat and confusion risk
**Status:** ~50 duplicate crates, majority caused by iroh-bitswap

## Summary

`cargo tree --duplicates` reports approximately 50 crate names with multiple versions in the dependency tree. The vast majority are caused by the `iroh-bitswap` git dependency pulling in old versions of the HTTP/gRPC stack. A few duplicates are in security-sensitive categories.

## Security-Relevant Duplicates

| Crate | Versions | Concern |
|-------|----------|---------|
| sha2 | 0.9.9, 0.10.9 | Crypto — different digest trait versions |
| digest | 0.9.0, 0.10.7 | Crypto — trait incompatibility |
| block-buffer | 0.9.0, 0.10.4 | Crypto primitive |
| rand | 0.8.5, 0.9.2 | RNG — different API surfaces |
| rand_core | 0.6.4, 0.9.5 | RNG traits |
| getrandom | 0.2.17, 0.3.4, 0.4.1 | Three versions of OS RNG |

## HTTP/Networking Duplicates (via iroh-bitswap)

| Crate | Versions | Root Cause |
|-------|----------|------------|
| hyper | 0.14.32, 1.8.1 | iroh-bitswap uses reqwest 0.11 |
| reqwest | 0.11.27, 0.12.28 | iroh-bitswap + tendermint-rpc |
| axum | 0.6.20, 0.7.9 | iroh-bitswap uses tonic 0.8 |
| tonic | 0.8.3, 0.12.3 | iroh-bitswap's opentelemetry |
| prost | 0.11.9, 0.13.5 | tonic version mismatch |
| h2 | 0.3.27, 0.4.13 | hyper version mismatch |
| tower | 0.4.13, 0.5.3 | axum/tonic version mismatch |
| tower-http | 0.5.2, 0.6.8 | tower version mismatch |
| http | 0.2.12, 1.4.0 | hyper version mismatch |
| http-body | 0.4.6, 1.0.1 | hyper version mismatch |

## Other Notable Duplicates

| Crate | Versions | Notes |
|-------|----------|-------|
| yamux | 0.12.1, 0.13.8 | P2P multiplexer |
| parking_lot | 0.11.2, 0.12.5 | Concurrency primitive |
| thiserror | 1.0.69, 2.0.18 | Error handling |
| syn | 1.0.109, 2.0.116 | Proc-macro (compile-time only) |
| base64 | 0.13.1, 0.21.7, 0.22.1 | Three versions |

## Impact

1. **Binary size:** Duplicate crates increase binary size (each version is compiled separately)
2. **Compile time:** More crates = longer compilation
3. **Confusion risk:** If two versions of a crypto type exist, runtime type confusion is possible (though Rust's type system generally prevents this at compile time)
4. **Attack surface:** More code linked = more potential vulnerabilities

## Remediation

Resolving iroh-bitswap's outdated dependencies (finding 25) would eliminate ~80% of the duplicates. The remaining crypto duplicates (sha2, digest) are caused by the cosmrs/tendermint dependency chain and will resolve as that ecosystem updates.
