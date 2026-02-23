# identity/ — Identity and keyring management tests

```
cargo test -p integration-test --test identity
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `keyring_lifecycle.rs` | 33 | CLI keyring generate, import, export, delete, list (Go + Rust) |
| `lifecycle.rs` | 12 | Identity new (ed25519, secp256k1, secp256r1), JWK import/export, delete/reimport |
| `negative.rs` | 8 | Expired tokens, malformed tokens, identity isolation, unauthenticated ACP |
| `node_identity.rs` | 2 | Node-level identity configuration |
| `types.rs` | 2 | Identity type handling |

**57 tests, 0 ignored.** All pass on both Go and Rust nodes.
