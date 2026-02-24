# Schema Tests

5 passing, 27 ignored.

## Files

- `encryption.rs` — Encrypted document P2P sync (18 ignored)
- `schema_migration.rs` — Schema version migration during replication (9 ignored)

## Ignored Tests

### encryption.rs (18 tests)
Encrypted document tests require:
- KMS (Key Management Service) activation for encrypted DAG verification
- GraphQL `decrypt`, `encrypt`, `encryptFields` parameter support
- Field-level encryption support
- Encrypted CRDT delta merge
- Encrypted index queries
- Encryption + ACP combined policy enforcement

### schema_migration.rs (9 tests)
Schema migration tests require:
- Schema branch/version history support
- Cross-version replication (older/newer schema versions)
- Schema evolution with new fields across versions
- Version gap handling during replication
