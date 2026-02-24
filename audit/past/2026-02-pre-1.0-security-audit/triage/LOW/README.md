# LOW Findings

82 findings across 7 streams. Includes findings classified as LOW, LOW-MEDIUM, and borderline LOW by their stream triage reports.

## Stream 01 -- Cryptographic Inventory (5)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 01-03 | `audit/01-cryptographic-inventory-findings/03-key-raw-returns-unprotected-vec.md` | Key::raw() Returns Unprotected Vec (LOW-MEDIUM) | NEW |
| 01-07 | `audit/01-cryptographic-inventory-findings/07-ecies-x25519-low-order-key-acceptance.md` | ECIES X25519 Low-Order Key Acceptance (LOW-MEDIUM) | NEW |
| 01-15 | `audit/01-cryptographic-inventory-findings/15-se-domain-separator-delimiter-collision.md` | SE Domain Separator Delimiter Collision (LOW-MEDIUM) | NEW |
| 01-19 | `audit/01-cryptographic-inventory-findings/19-se-hmac-key-no-length-validation.md` | SE HMAC Key No Length Validation (LOW-MEDIUM) | NEW |
| 01-18 | `audit/01-cryptographic-inventory-findings/18-se-artifact-metadata-leakage-to-replicators.md` | SE Artifact Metadata Leakage to Replicators (MEDIUM, Accept Risk) | NEW |
| 01-01 | `audit/01-cryptographic-inventory-findings/01-ecies-shared-secret-not-zeroed.md` | ECIES Shared Secret Not Zeroed | CONFIRMED |
| 01-05 | `audit/01-cryptographic-inventory-findings/05-jwt-algorithm-dispatch-from-header.md` | JWT Algorithm Dispatch from Header | NEW |
| 01-06 | `audit/01-cryptographic-inventory-findings/06-batch-signing-missing-secp256r1.md` | Batch Signing Missing secp256r1 | NEW |
| 01-08 | `audit/01-cryptographic-inventory-findings/08-ecies-ciphertext-validation-gaps.md` | ECIES Ciphertext Validation Gaps | NEW |
| 01-20 | `audit/01-cryptographic-inventory-findings/20-merkle-proof-verification-sound-trust-boundary.md` | Merkle Proof Verification Sound; Trust Boundary | NEW |

## Stream 02 -- Access Control Policy (8)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 02-05 | `audit/02-access-control-policy-findings/05-dac-bypass-thread-local-safety.md` | DAC bypass thread-local flag safety concerns | CONFIRMED |
| 02-06 | `audit/02-access-control-policy-findings/06-view-plan-skips-own-acp.md` | View plans skip view-collection ACP policy | CONFIRMED |
| 02-11 | `audit/02-access-control-policy-findings/11-policy-expressions-support-intersection-difference.md` | Policy expressions support intersection and difference | CONFIRMED |
| 02-12 | `audit/02-access-control-policy-findings/12-zanzibar-storage-key-delimiter-injection.md` | Zanzibar storage key lacks delimiter sanitization | CONFIRMED |
| 02-14 | `audit/02-access-control-policy-findings/14-policy-yaml-no-size-limits.md` | Policy YAML parsing has no size limits | CONFIRMED |
| 02-26 | `audit/02-access-control-policy-findings/26-weak-mutation-denial-assertions.md` | Weak mutation denial assertions in tests | CONFIRMED |
| 02-28 | `audit/02-access-control-policy-findings/28-no-policy-transition-test.md` | No integration test for policy transitions or DAC bypass | CONFIRMED |
| 02-35 | `audit/02-access-control-policy-findings/35-sourcehub-managing-relations-not-validated-locally.md` | SourceHub ignores managing relations parameter | CONFIRMED |

## Stream 03 -- P2P Network Security (11)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 03-04 | `audit/03-p2p-network-security-findings/04-gossipsub-flood-publish-amplification.md` | GossipSub flood_publish amplifies to all subscribed peers | CONFIRMED |
| 03-07 | `audit/03-p2p-network-security-findings/07-identify-version-leakage.md` | Identify protocol leaks exact build version to all peers | CONFIRMED |
| 03-15 | `audit/03-p2p-network-security-findings/15-no-message-replay-protection.md` | No message replay protection | CONFIRMED |
| 03-17 | `audit/03-p2p-network-security-findings/17-gossipsub-no-application-signature-check.md` | GossipSub messages skip application-level signature verification | CONFIRMED |
| 03-19 | `audit/03-p2p-network-security-findings/19-two-stream-response-signing-continues-after-failure.md` | Two-stream response signing failure sends unsigned reply | CONFIRMED |
| 03-24 | `audit/03-p2p-network-security-findings/24-gossipsub-topic-names-leak-collection-ids.md` | GossipSub topic names leak collection IDs to mesh peers | CONFIRMED |
| 03-34 | `audit/03-p2p-network-security-findings/34-cbor-triple-try-deserialization.md` | CBOR triple-try deserialization amplifies large message cost | CONFIRMED |
| 03-36 | `audit/03-p2p-network-security-findings/36-channel-backpressure-memory-accumulation.md` | Bounded channels create backpressure-induced memory accumulation | CONFIRMED |
| 03-45 | `audit/03-p2p-network-security-findings/45-gossipsub-default-mesh-parameters.md` | GossipSub uses default mesh parameters | CONFIRMED |
| 03-51 | `audit/03-p2p-network-security-findings/51-yamux-default-stream-limit-analysis.md` | Yamux default max concurrent streams = 256 | CONFIRMED |
| 03-54 | `audit/03-p2p-network-security-findings/54-dag-sync-config-unlimited-depth.md` | DagSyncConfig default has unlimited depth | CONFIRMED |

## Stream 04 -- Identity & Key Management (17)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 04-01 | `audit/04-identity-key-management-findings/01-did-validation-prefix-only.md` | DID validation only checks prefix, not structure | CONFIRMED |
| 04-02 | `audit/04-identity-key-management-findings/02-zanzibar-did-new-unchecked-pub.md` | zanzibar::Did::new_unchecked() is pub not pub(crate) | CONFIRMED |
| 04-03 | `audit/04-identity-key-management-findings/03-wildcard-did-serde-asymmetry.md` | Wildcard DID cannot survive serde roundtrip | CONFIRMED |
| 04-10 | `audit/04-identity-key-management-findings/10-der-parser-lax-non-canonical.md` | DER parser accepts non-canonical encodings | CONFIRMED |
| 04-12 | `audit/04-identity-key-management-findings/12-jwt-token-test-coverage-gaps.md` | JWT token test coverage gaps | CONFIRMED |
| 04-22 | `audit/04-identity-key-management-findings/22-file-delete-no-fsync-before-unlink.md` | File delete: no fsync before unlink | CONFIRMED |
| 04-25 | `audit/04-identity-key-management-findings/25-systemd-creds-path-lookup-no-timeout.md` | SystemdCreds PATH-based lookup, no subprocess timeout | CONFIRMED |
| 04-26 | `audit/04-identity-key-management-findings/26-systemd-creds-no-secure-delete.md` | SystemdCreds: no secure deletion of .cred files | CONFIRMED |
| 04-28 | `audit/04-identity-key-management-findings/28-directory-permission-toctou-on-create.md` | Directory permission TOCTOU on create | CONFIRMED |
| 04-29 | `audit/04-identity-key-management-findings/29-no-file-locking-concurrent-access.md` | No file locking for concurrent access | CONFIRMED |
| 04-33 | `audit/04-identity-key-management-findings/33-file-keyring-set-no-fsync.md` | FileKeyring set() missing fsync | CONFIRMED |
| 04-35 | `audit/04-identity-key-management-findings/35-bearer-prefix-incomplete-case-insensitivity.md` | Bearer prefix incomplete case-insensitivity | CONFIRMED |
| 04-38 | `audit/04-identity-key-management-findings/38-403-error-leaks-failure-reason.md` | 403 error response leaks failure reason | CONFIRMED |
| 04-44 | `audit/04-identity-key-management-findings/44-websocket-endpoint-no-auth.md` | WebSocket endpoint registered without auth | CONFIRMED |
| 04-46 | `audit/04-identity-key-management-findings/46-host-header-audience-exact-match-no-port-normalization.md` | Host header audience exact match, no port normalization | CONFIRMED |
| 04-48 | `audit/04-identity-key-management-findings/48-keyring-export-prints-raw-key-hex.md` | keyring export prints raw key hex to stdout | CONFIRMED |
| 04-50 | `audit/04-identity-key-management-findings/50-multiple-authorization-headers-first-wins.md` | Multiple Authorization headers: first wins | CONFIRMED |
| 04-54 | `audit/04-identity-key-management-findings/54-no-anonymous-access-acp-integration-test.md` | No anonymous access test in acp_basic.rs | CONFIRMED |
| 04-55 | `audit/04-identity-key-management-findings/55-node-identity-test-minimal.md` | Node identity integration test is minimal | CONFIRMED |
| 04-61 | `audit/04-identity-key-management-findings/61-no-wrong-key-type-token-integration-test.md` | No wrong-key-type token integration test | CONFIRMED |

## Stream 05 -- Input Validation (11)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 05-04 | `audit/05-input-validation-findings/04-fragment-width-amplification.md` | Fragment Width Amplification (Non-Cyclic) | CONFIRMED |
| 05-10 | `audit/05-input-validation-findings/10-cli-file-read-no-size-limit.md` | CLI File Reading Operations Have No Size Limit | CONFIRMED |
| 05-12 | `audit/05-input-validation-findings/12-no-canonicalize-or-symlink-resolution.md` | No `canonicalize()` or Symlink Resolution on User-Controlled Paths | CONFIRMED |
| 05-13 | `audit/05-input-validation-findings/13-data-directory-no-permission-hardening.md` | Data Directory Created Without Permission Hardening | CONFIRMED |
| 05-18 | `audit/05-input-validation-findings/18-unknown-directives-silently-accepted.md` | Unknown SDL Directives Silently Accepted | CONFIRMED |
| 05-20 | `audit/05-input-validation-findings/20-error-messages-echo-user-input.md` | Error Messages Echo User Input Unsanitized | CONFIRMED |
| 05-22 | `audit/05-input-validation-findings/22-schema-no-field-drop-migration-guard.md` | Schema Migration -- No Field Drop or Type Change Guard | CONFIRMED |
| 05-23 | `audit/05-input-validation-findings/23-content-type-not-enforced-on-schema-endpoint.md` | Content-Type Not Enforced on Schema Endpoint | CONFIRMED |
| 05-24 | `audit/05-input-validation-findings/24-identifier-no-length-limit.md` | Identifiers Accept Unbounded Length | CONFIRMED |
| 05-34 | `audit/05-input-validation-findings/34-wasm-no-module-size-limit.md` | No Size Limit on WASM Module Binaries | CONFIRMED |
| 05-35 | `audit/05-input-validation-findings/35-string-key-separator-injection-in-headstore-peerstore.md` | String-Based Keys Use `/` Separator Without Escaping | CONFIRMED |

## Stream 06 -- Data Integrity & CRDT (15)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 06-03 | `audit/06-data-integrity-crdt-findings/03-float64-counter-non-associative-divergence.md` | Float64 counter non-associative divergence | CONFIRMED |
| 06-04 | `audit/06-data-integrity-crdt-findings/04-property-test-coverage-gaps.md` | Property test coverage gaps | CONFIRMED |
| 06-06 | `audit/06-data-integrity-crdt-findings/06-counter-nonce-storage-unbounded-growth.md` | Counter nonce storage unbounded growth | CONFIRMED |
| 06-13 | `audit/06-data-integrity-crdt-findings/13-parent-block-missing-silently-skipped.md` | Parent block missing silently skipped | CONFIRMED |
| 06-15 | `audit/06-data-integrity-crdt-findings/15-decryption-failure-falls-back-to-ciphertext.md` | Decryption failure falls back to ciphertext | CONFIRMED |
| 06-16 | `audit/06-data-integrity-crdt-findings/16-collection-delta-no-dedup-guard.md` | Collection delta no dedup guard | CONFIRMED |
| 06-17 | `audit/06-data-integrity-crdt-findings/17-composite-dedup-toctou-race.md` | Composite dedup TOCTOU race | CONFIRMED |
| 06-27 | `audit/06-data-integrity-crdt-findings/27-backup-no-block-level-integrity.md` | Backup no block-level integrity | CONFIRMED |
| 06-42 | `audit/06-data-integrity-crdt-findings/42-memory-backend-committed-before-apply.md` | Memory backend committed before apply | CONFIRMED |
| 06-43 | `audit/06-data-integrity-crdt-findings/43-conflict-check-not-atomic-with-storage-write.md` | Conflict check not atomic with storage write | CONFIRMED |
| 06-45 | `audit/06-data-integrity-crdt-findings/45-drop-does-not-execute-discard-callbacks.md` | Drop does not execute discard callbacks | CONFIRMED |
| 06-55 | `audit/06-data-integrity-crdt-findings/55-float64-running-sum-divergence-confirmed.md` | Float64 running-sum divergence confirmed | CONFIRMED |
| 06-57 | `audit/06-data-integrity-crdt-findings/57-schema-evolution-unknown-fields-silently-discarded.md` | Schema evolution unknown fields silently discarded | CONFIRMED |
| 06-59 | `audit/06-data-integrity-crdt-findings/59-no-document-size-limit.md` | No document size limit | CONFIRMED |
| 06-63 | `audit/06-data-integrity-crdt-findings/63-float-equality-epsilon-comparison-in-queries.md` | Float equality epsilon comparison in queries | CONFIRMED |

## Stream 07 -- Dependency & Unsafe Code (15)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 07-02 | `audit/07-dependency-unsafe-code-findings/02-handle-counter-wrapping-no-protection.md` | Handle Counter Wraps to Zero on Overflow | CONFIRMED |
| 07-03 | `audit/07-dependency-unsafe-code-findings/03-defra-free-string-double-free-no-guard.md` | defra_free_string Double-Free No Guard | CONFIRMED |
| 07-05 | `audit/07-dependency-unsafe-code-findings/05-new-node-not-marked-unsafe.md` | FFI Functions Not Consistently Marked unsafe | CONFIRMED |
| 07-17 | `audit/07-dependency-unsafe-code-findings/17-rocksdb-crate-version-audit.md` | RocksDB Crate Version Audit | INFORMATIONAL |
| 07-27 | `audit/07-dependency-unsafe-code-findings/27-sha2-duplicate-versions.md` | sha2 Duplicate Versions (0.9.9 + 0.10.9) | INFORMATIONAL |
| 07-28 | `audit/07-dependency-unsafe-code-findings/28-blst-c-library-audit.md` | blst C Library Audit | INFORMATIONAL |
| 07-31 | `audit/07-dependency-unsafe-code-findings/31-build-scripts-audit.md` | Build Scripts Audit | GREEN |
| 07-32 | `audit/07-dependency-unsafe-code-findings/32-duplicate-crate-inventory.md` | Duplicate Crate Inventory (~50 duplicates) | INFORMATIONAL |
| 07-33 | `audit/07-dependency-unsafe-code-findings/33-josekit-jwt-library-audit.md` | josekit 0.8.7 JWT Library Outdated | INFORMATIONAL |
| 07-34 | `audit/07-dependency-unsafe-code-findings/34-cosmrs-tendermint-dependency-chain.md` | cosmrs/tendermint Dependency Chain | INFORMATIONAL |
| 07-38 | `audit/07-dependency-unsafe-code-findings/38-no-rust-toolchain-pinning.md` | No Rust Toolchain Pinning | INFORMATIONAL |
| 07-39 | `audit/07-dependency-unsafe-code-findings/39-defra-version-git-path-dependency.md` | defra-version build.rs Uses PATH-Relative git | INFORMATIONAL |
| 07-44 | `audit/07-dependency-unsafe-code-findings/44-docker-base-image-not-pinned.md` | Docker Base Images Not Digest-Pinned | INFORMATIONAL |
| 07-54 | `audit/07-dependency-unsafe-code-findings/54-no-memory-leak-detection.md` | No Memory Leak Detection in CI | CONFIRMED |
