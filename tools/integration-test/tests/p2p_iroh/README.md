# Iroh P2P Integration Tests

Tests for the iroh QUIC transport alternative to libp2p. Run with:

```bash
cargo test -p integration-test --test p2p_iroh
```

The CLI binary must be built with `--features iroh` for P2P transport tests.

## Test Suites

| Suite | Tests | Pass | Fail | Ignored | Description |
|-------|-------|------|------|---------|-------------|
| connection/ | 18 | 18 | 0 | 0 | Peer connectivity, smoke tests, signature verification |
| sync/ | 25 | 25 | 0 | 0 | Document sync, branchable sync, version sync with views |
| replication/ | 40 | 40 | 0 | 0 | Replicator lifecycle, persistence, filtering |
| peer/ | 43 | 42 | 0 | 1 | Peer events, subscriptions, create/update/delete |
| schema/ | 38 | 31 | 0 | 7 | Encryption, schema migration with lens transforms |
| acp/ | 65 | 53 | 12 | 0 | Access control: local ACP, NAC, DAC |
| **Total** | **229** | **209** | **12** | **8** | |

## Known Failures

### ACP replication tests (12 tests)

All 12 failures are in `acp::acp`, `acp::dac`, and `acp::trust_boundary`. They time
out waiting for ACP-protected documents to replicate between iroh peers. The
controlled-mode access checks and PeerState tracking are working (#501 fixed),
but the ACP-protected replication path has a remaining issue in the pushlog/DAG
fetch flow for permissioned documents.

## Running Individual Suites

```bash
cargo test -p integration-test --test p2p_iroh -- connection::
cargo test -p integration-test --test p2p_iroh -- sync::
cargo test -p integration-test --test p2p_iroh -- replication::
cargo test -p integration-test --test p2p_iroh -- acp::
cargo test -p integration-test --test p2p_iroh -- peer::
cargo test -p integration-test --test p2p_iroh -- schema::
```
