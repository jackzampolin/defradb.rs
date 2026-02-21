# Finding 55: Node Identity Integration Test Is Minimal

**Severity**: LOW
**Category**: Test Coverage / Node Identity
**Status**: Confirmed

## Summary

The `node_identity.rs` integration test only verifies that the node identity endpoint returns a non-empty JSON response. It does not verify: (a) the identity is a valid DID or PeerId, (b) the identity persists across restarts, (c) the identity matches the node's libp2p keypair, or (d) the identity is used for any security-relevant purpose.

## Affected Files

- `tools/integration-test/tests/node_identity.rs` — 24 lines, single assertion

## Details

### Current test

```rust
async fn node_identity_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let identity = node.node_identity().expect("node-identity");
    let id_str = serde_json::to_string(&identity).unwrap();
    assert!(!id_str.is_empty(), "non-empty response");
    assert!(identity.is_object() || identity.is_string(), "type check");
}
```

### What's not tested

1. **Identity format validation**: No check that the response contains a valid PeerId or DID
2. **Persistence across restarts**: No test that stops and restarts a node and verifies the same identity
3. **Cryptographic binding**: No test that the node identity is derived from the node's actual keypair
4. **Key rotation**: No test for what happens if the node's keyring is regenerated — does the identity change?
5. **P2P identity consistency**: No test that the node identity matches what peers see via libp2p identify protocol

### Risk

If the node identity endpoint returns stale, hardcoded, or incorrectly derived values, this test wouldn't catch it. Node identity is used for P2P peer authentication and potentially for ACP trust decisions.

## Remediation

Enhance the integration test to:
1. Parse the returned identity as a PeerId or DID
2. Verify it matches between two calls (consistency)
3. If feasible, verify it matches the P2P peer info endpoint

## Test Gap

- No identity persistence test across node restart
- No identity-to-keypair binding verification
- No key rotation test
