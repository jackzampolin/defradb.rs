# Finding: Merge Handler Does Not Generate SE Artifacts for Replicated Documents

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: MEDIUM (SE artifacts only generated on push, not on merge — replicator chain broken)
**Category**: Searchable Encryption / Replication Integration
**Status**: NEW

## Summary

When a Rust node merges a document received from a peer (via PushLog), the merge handler does not generate SE artifacts for the merged document. SE artifacts are only generated in the `push_existing_docs` path (when a node proactively pushes to a replicator). This means:
1. A node that receives a document via replication does not generate SE artifacts locally
2. If that node is also a replicator for other peers, it cannot serve SE queries for replicated documents
3. SE artifact chain breaks at the first hop

## Evidence

### Merge Handler Has No SE References

Grep for `artifact|search_tag|se_artifact|SECoordinator` in `crates/db/src/merge_handler/` returned zero matches. The merge handler processes incoming blocks but does not generate SE artifacts.

### SE Artifacts Only Generated in push_existing_docs

`crates/db/src/push_docs.rs:210-318` — SE artifacts are generated when proactively pushing documents to a replicator. This is a one-time push, not triggered by merge events.

### Go's Architecture

In Go, the SE artifact flow is:
1. Document created/updated → artifacts generated and pushed to replicators
2. Document replicated from peer → NO artifacts generated (replicators serve artifacts, not replicate them)

The Rust implementation matches this model — but the implications should be documented:
- SE artifacts live only on the designated replicator(s)
- If a replicator goes down, SE queries fail until it recovers
- There's no SE artifact re-replication

## Impact

### SE Query Availability Depends on Single Replicator

If the original replicator node is unavailable, there's no fallback for SE queries. The artifacts are not re-generated on other nodes.

### Multi-Hop Replication Doesn't Propagate SE Artifacts

In a chain A → B → C, where A creates documents and B/C are replicators:
- A pushes artifacts to B (works)
- B does NOT generate/push artifacts to C when merging A's documents
- C cannot serve SE queries

## Affected Code

- `crates/db/src/merge_handler/` — no SE artifact generation on merge
- `crates/db/src/push_docs.rs:210-318` — only push path generates artifacts

## Remediation

This is consistent with Go's design. For 1.0, document this behavior:
- SE artifacts are pushed from producer to designated replicator(s)
- SE artifacts are NOT propagated through replication chains
- SE query availability depends on the original replicator being online

For post-1.0, consider:
- SE artifact re-push mechanism on replicator recovery
- Optional SE artifact propagation through replication chains
