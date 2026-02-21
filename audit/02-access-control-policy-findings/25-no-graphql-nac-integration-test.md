# Finding: No Integration Test for GraphQL NAC Bypass

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM (test gap for HIGH vulnerability)
**Category**: Test Gap
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation
**Related Finding**: 08 (GraphQL endpoint bypasses NAC permission checks — HIGH)

## Summary

Finding 08 identified a HIGH vulnerability: the GraphQL endpoint (`/api/v0/graphql`) does not call `require_permission()` to enforce NAC. While `nac_core_operations.rs` and `nac_operations.rs` thoroughly test NAC enforcement on REST API endpoints (collection create/update/delete, index create/list/drop, etc.), **no integration test verifies NAC enforcement on GraphQL queries or mutations.**

## Evidence

### NAC Tests Only Cover REST API

`nac_core_operations.rs` tests 8 REST operations with the anonymous/outsider/admin pattern:

| Operation | Anonymous | Outsider | Admin |
|-----------|-----------|----------|-------|
| collection list | tested | tested | tested |
| collection describe | tested | tested | tested |
| collection truncate | tested | tested | tested |
| index create | tested | tested | tested |
| index list | tested | tested | tested |
| index drop | tested | tested | tested |
| collection create (REST) | tested | tested | tested |
| collection update (REST) | tested | tested | tested |
| collection delete (REST) | tested | tested | tested |
| **GraphQL query** | **NOT tested** | **NOT tested** | **NOT tested** |
| **GraphQL mutation** | **NOT tested** | **NOT tested** | **NOT tested** |

### The Only GraphQL+NAC Reference

```rust
// nac_core_operations.rs:38
// Note: Go doesn't NAC-gate GraphQL introspection; Rust does.
```

This comment acknowledges a GraphQL/NAC interaction but only in the context of collection listing (introspection), not query/mutation enforcement.

### ACP Integration Tests Use GraphQL Without NAC

All ACP tests (`acp_basic.rs`, `acp_multi_identity.rs`, etc.) use `query_with_identity()` which goes through the GraphQL endpoint. However, these tests run with `.with_acp_local()` (document-level ACP only), NOT `.with_nac()` (node-level access control). They test document-level filtering but never test node-level operation gating on GraphQL.

### nac_document_acp.rs — Close But Not Enough

`nac_document_acp.rs` tests NAC + document ACP together and uses `query_with_identity()` (GraphQL). However, the NAC checks it relies on are at the **document ACP layer** (the query returns 0 results because document ACP denies access), not the **NAC layer** (which should reject the request before it reaches the query executor). The test would pass identically whether NAC gates GraphQL or not — it tests document-level filtering, not node-level rejection.

## Missing Test

```
1. Start node with .with_acp_local().with_nac()
2. Admin deploys schema and creates a document via GraphQL
3. Outsider attempts GraphQL query → should be REJECTED by NAC (currently: returns data)
4. Outsider attempts GraphQL mutation → should be REJECTED by NAC (currently: succeeds)
5. Anonymous attempts GraphQL query → should be REJECTED by NAC (currently: returns data)
```

The key distinction: NAC rejection should return a 401/403 HTTP error, NOT an empty result set. The REST tests correctly assert `is_err()` for unauthorized attempts.

## Severity Rationale

MEDIUM because:
- Finding 08 (GraphQL NAC bypass) is HIGH and confirmed
- The comprehensive REST NAC tests create a false sense of security — NAC appears well-tested but has a complete bypass via the primary query interface
- GraphQL is the main query path (used by all ACP tests), making this the widest NAC bypass
