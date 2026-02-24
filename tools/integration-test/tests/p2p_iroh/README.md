# Iroh P2P Integration Tests

Tests for the iroh QUIC transport alternative to libp2p. Run with:

```bash
cargo test -p integration-test --test p2p_iroh
```

The CLI binary must be built with `--features iroh` for P2P transport tests.
Single-node tests (signature verification, schema) work with the default build.

## Test Suites

| Suite | Tests | Ignored | Description |
|-------|-------|---------|-------------|
| connection/ | 18 | 0 | Peer connectivity, smoke tests, signature verification |
| sync/ | 25 | 0 | Document sync, branchable sync, version sync |
| replication/ | 40 | 0 | Replicator lifecycle, persistence, filtering |
| acp/ | 65 | 0 | Access control: local ACP, NAC, DAC |
| peer/ | 43 | 16 | Peer events, subscriptions, create/update/delete |
| schema/ | 32 | 27 | Encryption, schema migration |
| **Total** | **223** | **43** | |

## Running Individual Suites

```bash
cargo test -p integration-test --test p2p_iroh -- connection::
cargo test -p integration-test --test p2p_iroh -- sync::
cargo test -p integration-test --test p2p_iroh -- replication::
cargo test -p integration-test --test p2p_iroh -- acp::
cargo test -p integration-test --test p2p_iroh -- peer::
cargo test -p integration-test --test p2p_iroh -- schema::
```
