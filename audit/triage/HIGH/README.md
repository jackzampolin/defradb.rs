# HIGH Findings

26 findings across 7 streams.

| # | Stream | Finding File | Title | Status |
|---|--------|-------------|-------|--------|
| 01-10 | 01-Cryptographic Inventory | `audit/01-cryptographic-inventory-findings/10-se-tag-utf8-lossy-go-divergence.md` | SE Tag UTF-8 Lossy Go Divergence | NEW |
| 02-00 | 02-Access Control Policy | `audit/02-access-control-policy-findings/00-recovery-mode-acp-bypass.md` | Recovery mode bypasses ACP on P2P merge | CONFIRMED |
| 02-01 | 02-Access Control Policy | `audit/02-access-control-policy-findings/01-dump-bypasses-acp.md` | Database dump bypasses ACP and NAC | CONFIRMED |
| 02-08 | 02-Access Control Policy | `audit/02-access-control-policy-findings/08-graphql-bypasses-nac-permission-checks.md` | GraphQL endpoint bypasses NAC permission checks | CONFIRMED |
| 02-18 | 02-Access Control Policy | `audit/02-access-control-policy-findings/18-p2p-merge-no-signature-verification.md` | P2P merge path does not verify block signatures | CONFIRMED |
| 02-19 | 02-Access Control Policy | `audit/02-access-control-policy-findings/19-p2p-creator-identity-from-metadata-not-signature.md` | P2P block creator identity from peer-reported metadata | CONFIRMED |
| 02-22 | 02-Access Control Policy | `audit/02-access-control-policy-findings/22-no-commits-acp-integration-test.md` | No integration test for _commits ACP bypass | CONFIRMED |
| 02-24 | 02-Access Control Policy | `audit/02-access-control-policy-findings/24-acp-p2p-never-tests-merge-denial.md` | P2P ACP tests never verify merge denial | CONFIRMED |
| 02-30 | 02-Access Control Policy | `audit/02-access-control-policy-findings/30-sourcehub-verify-access-fail-open-on-abci-error.md` | SourceHub verify_access fails open on ABCI error | CONFIRMED |
| 02-36 | 02-Access Control Policy | `audit/02-access-control-policy-findings/36-sourcehub-recovery-bypass-on-chain-permissions.md` | Recovery mode bypasses on-chain SourceHub permissions | CONFIRMED |
| 03-00 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/00-two-stream-no-message-size-limit.md` | Two-stream protocol has no message size limit | CONFIRMED |
| 03-01 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/01-no-swarm-connection-limits.md` | No swarm-level connection limits | CONFIRMED |
| 03-21 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/21-docsync-branchable-car-no-access-checks.md` | DocSync, BranchableSync, and CAR fetch have no access checks | CONFIRMED |
| 03-30 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/30-unbounded-task-spawning-per-peer.md` | Unbounded tokio task spawning per peer | CONFIRMED |
| 03-31 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/31-docsync-doc-ids-unbounded-array.md` | DocSyncRequest.doc_ids is an unbounded array | CONFIRMED |
| 03-42 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/42-no-per-peer-rate-limiting.md` | No per-peer rate limiting | CONFIRMED |
| 03-43 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/43-no-per-peer-connection-limits.md` | No per-peer connection limits | CONFIRMED |
| 03-44 | 03-P2P Network Security | `audit/03-p2p-network-security-findings/44-two-stream-read-no-timeout.md` | Two-stream read_to_end has no timeout (Slowloris) | CONFIRMED |
| 04-37 | 04-Identity & Key Management | `audit/04-identity-key-management-findings/37-debug-dump-no-identity-check.md` | Debug dump endpoint has no identity or NAC check | CONFIRMED |
| 05-00 | 05-Input Validation | `audit/05-input-validation-findings/00-graphql-no-depth-complexity-limits.md` | GraphQL Parser Has No Depth or Complexity Limits | CONFIRMED |
| 05-01 | 05-Input Validation | `audit/05-input-validation-findings/01-no-http-body-size-limit.md` | No Explicit HTTP Request Body Size Limit | CONFIRMED |
| 05-15 | 05-Input Validation | `audit/05-input-validation-findings/15-lens-path-reachable-via-http-api.md` | Lens WASM Path Traversal Reachable via HTTP API | CONFIRMED |
| 05-31 | 05-Input Validation | `audit/05-input-validation-findings/31-wasm-sandbox-no-memory-limits.md` | WASM Sandbox Has No Memory, CPU, or Syscall Restrictions | CONFIRMED |
| 06-11 | 06-Data Integrity & CRDT | `audit/06-data-integrity-crdt-findings/11-recursive-dag-traversal-no-depth-limit.md` | Recursive DAG traversal no depth limit | CONFIRMED |
| 06-34 | 06-Data Integrity & CRDT | `audit/06-data-integrity-crdt-findings/34-se-receiver-not-implemented-artifacts-discarded.md` | SE receiver not implemented -- artifacts discarded | CONFIRMED |
| 06-37 | 06-Data Integrity & CRDT | `audit/06-data-integrity-crdt-findings/37-se-no-query-evaluation-in-rust-planner.md` | SE query evaluation not in Rust planner/runner | CONFIRMED |
| 07-51 | 07-Dependency & Unsafe Code | `audit/07-dependency-unsafe-code-findings/51-no-negative-ffi-boundary-testing.md` | No Negative FFI Boundary Testing | CONFIRMED |
