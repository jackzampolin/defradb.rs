# Session 5 Summary: SourceHub Provider Integration

**Stream**: 02 - Access Control Policy
**Session**: 5 of 5 (FINAL)
**Focus**: SourceHub ACP provider — on-chain integration, cache behavior, provider equivalence

## Architecture Overview

The SourceHub ACP integration consists of three layers:

1. **SourceHubDocumentACP** (`crates/sourcehub/src/dac.rs`) — implements `DocumentACP` trait, delegates all operations to an on-chain SourceHub provider via HTTP. Permission checks (`check_doc_access`) query SourceHub on-demand. Write operations (`register_doc_object`, `add_actor_relationship`) require bearer tokens signed by the requestor's private key.

2. **SourceHubAcpAdapter** (`crates/cli/src/sourcehub_acp_adapter.rs`) — bridges HTTP policy CRUD operations. `add_policy()` validates locally, submits on-chain, then re-caches under the on-chain ID. `list_policies()` and `get_policy()` read from local cache only.

3. **CosmosProvider** (`crates/sourcehub/src/cosmos.rs`) — implements the `SourceHubProvider` trait for Cosmos SDK. Uses REST/LCD for queries and CometBFT JSON-RPC for transaction broadcast.

## Session 5 Findings

| # | Severity | Title | Key Risk |
|---|----------|-------|----------|
| 30 | HIGH | verify_access fails open on ABCI error codes | Error masqueraded as denial; brittle protobuf parsing |
| 31 | MEDIUM | Policy add non-atomic | On-chain success + local cache failure → orphaned policy |
| 32 | MEDIUM | Cache staleness — no refresh mechanism | Policies added by other nodes never visible locally |
| 33 | MEDIUM | Network partition — no explicit fail-closed | Emergent fail-closed via error handling, no circuit breaker |
| 34 | MEDIUM | Bearer token requires global signing config | Operations fail for unknown DIDs; `unregister` needs owner's key |
| 35 | LOW | Managing relations parameter ignored | Correct architecturally but redundant local validation |
| 36 | HIGH | Recovery mode bypasses on-chain permissions | Amplified version of finding 00 for SourceHub |
| 37 | INFO | All Session 1-4 findings apply to SourceHub | Systematic provider equivalence verification |
| 38 | MEDIUM | Integration test coverage gaps | Happy-path only; no failure mode or edge case testing |

## Provider Equivalence Assessment

### Where Equivalence Holds

The `DocumentACP` trait abstraction ensures that the **core enforcement path** works identically for both providers:

- **Query-level permission filtering** (`PermissionFilterNode`) — same code, same fail-closed behavior
- **Write-path permission checks** (`check_doc_permission`) — same dispatch
- **P2P merge ACP checks** (`AcpMergeHandler`) — same trait-based dispatch
- **Owner registration** — same flow, different storage backend

### Where Equivalence Breaks

| Aspect | Local ACP | SourceHub ACP |
|--------|-----------|---------------|
| Permission check latency | Sub-microsecond (HashMap) | HTTP round-trip per document |
| Network dependency | None | SourceHub must be reachable |
| Policy reads | Authoritative (local IS source of truth) | Stale cache (on-chain is truth) |
| Bearer tokens | Not needed | Required for all writes |
| Recovery bypass impact | Self-consistent (ACP + data recover together) | Bypasses external authority (on-chain) |
| Error handling | In-process errors only | HTTP, ABCI, protobuf errors |
| Multi-node policy visibility | N/A (single-node) | Requires cache sync |

### Design Asymmetry

Go's implementation queries SourceHub on-demand for all operations (comment in adapter: "Go doesn't cache locally at all"). The Rust implementation introduced local caching for performance, creating cache staleness issues (finding 32) that don't exist in Go. This is a significant divergence from Go behavior and should be considered during 1.0 parity validation.

## Cross-Stream Patterns

Across all 5 sessions, three systemic patterns emerge:

1. **Bypass Surface**: Multiple code paths reach document data without ACP checks — `_commits`, dump, encrypted search, CID time-travel, recovery mode. These are provider-agnostic and affect SourceHub equally.

2. **Test Gap**: Of 20+ security findings across the stream, zero have regression tests. The SourceHub integration tests are happy-path only, and the local ACP tests don't cover the critical bypass vectors.

3. **P2P Trust Boundary**: The P2P merge path has fundamental authentication gaps — no signature verification, metadata-based identity (spoofable), and recovery bypass. These are more severe in SourceHub mode where the on-chain authorization model is the intended security boundary.
