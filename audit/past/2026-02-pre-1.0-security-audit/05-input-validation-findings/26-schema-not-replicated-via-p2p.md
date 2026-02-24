# Schema Not Replicated via P2P — No Injection Vector

**Severity**: INFO (GREEN)
**Category**: Input Validation — P2P Schema Safety
**Status**: Confirmed Safe

## Summary

Schemas are NOT replicated between peers via the P2P protocol. The P2P sync layer replicates documents (blocks, DAGs, pushlogs) but not SDL schemas. Each node must have its schema independently configured. This eliminates the "malicious peer sends dangerous schema" attack vector entirely.

## Affected Files

- `crates/p2p/src/sync/replication/handlers.rs` — handles document blocks, not schemas
- `crates/p2p/src/sync/coordinator/` — DAG fetching and synchronization, schema-agnostic
- `crates/http/src/handlers/schema.rs` — schema only added via HTTP API

## Details

### P2P Protocol Scope

The P2P sync protocol handles:
- Document blocks (CRDT deltas)
- DAG links and heads
- Pushlogs (document update notifications)
- Schema version IDs (as metadata on blocks, not schema definitions)

The P2P protocol does **not** handle:
- SDL schema definitions
- Collection creation/modification
- Index definitions
- Policy definitions

### Schema Version IDs in P2P

The `schema_version_id` field appears in P2P block metadata, but this is a reference (content hash) used for routing blocks to the correct collection — not the schema definition itself. If a node receives a block with an unknown `schema_version_id`, it cannot process it (the collection doesn't exist locally).

### Security Implication

A malicious peer cannot:
- Create new collections on a target node
- Modify existing schema definitions
- Inject directives into the type system
- Add or remove fields from collections

Schema management is strictly an admin operation via the HTTP API or CLI, protected by NAC permissions.

## Test Gap

No test explicitly verifies that schema definitions cannot arrive via P2P. This is implicitly tested by the fact that the P2P protocol has no schema-carrying message type.
