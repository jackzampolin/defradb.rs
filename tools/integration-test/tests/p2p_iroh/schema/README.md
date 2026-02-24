# Schema Tests

31 passing, 7 ignored.

## Files

- `encryption.rs` — Encrypted document P2P sync (7 ignored)
- `schema_migration.rs` — Schema version migration during replication (all pass)
- `index.rs` — Index sync across peers (all pass)
- `one_to_many.rs` — Relational replication (all pass)

## Ignored Tests

### encryption.rs (7 tests)

- `all_fields_of_public_doc_individually_encrypted` — requires Orbis KMS for encryption key distribution
- `encrypted_public_doc_encrypted_field` — requires Orbis KMS for encryption key distribution
- `encryption_acp_node_partial_access` — requires ACP + encryption combined support
- `encryption_acp_server_not_available` — requires ACP + encryption combined support
- `encryption_acp_user_access_not_node` — requires ACP + encryption combined support
- `encryption_acp_user_and_node_access` — requires ACP + encryption combined support
- `peer_no_key_should_not_fetch` — requires KMS key distribution enforcement
