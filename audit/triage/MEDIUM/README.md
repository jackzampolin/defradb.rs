# MEDIUM Findings

100 findings across 7 streams. Some findings have borderline severities (MEDIUM-HIGH or LOW-MEDIUM) and are listed in the category assigned by their stream triage report.

## Stream 01 -- Cryptographic Inventory (7)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 01-11 | `audit/01-cryptographic-inventory-findings/11-se-tags-no-go-test-vectors.md` | SE Tags No Go Test Vectors | NEW |
| 01-00 | `audit/01-cryptographic-inventory-findings/00-private-key-zeroization.md` | Private Key Zeroization (Ed25519 MEDIUM-HIGH) | CONFIRMED |
| 01-02 | `audit/01-cryptographic-inventory-findings/02-ed25519-keygen-seed-not-zeroed.md` | Ed25519 Keygen Seed Not Zeroed | NEW |
| 01-04 | `audit/01-cryptographic-inventory-findings/04-secp256r1-go-signature-s-normalization-gap.md` | secp256r1 Go Signature S-Normalization Gap | NEW |
| 01-12 | `audit/01-cryptographic-inventory-findings/12-jwt-no-go-compat-tests.md` | JWT No Go Compat Tests | NEW |
| 01-13 | `audit/01-cryptographic-inventory-findings/13-secp256r1-systematic-compat-gaps.md` | secp256r1 Systematic Compat Gaps | NEW |
| 01-16 | `audit/01-cryptographic-inventory-findings/16-se-enc-key-not-zeroized-and-default-zeros.md` | SE Enc Key Not Zeroized and Default Zeros | NEW |

## Stream 02 -- Access Control Policy (15)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 02-03 | `audit/02-access-control-policy-findings/03-cid-time-travel-bypasses-acp.md` | CID time-travel queries bypass ACP | CONFIRMED |
| 02-04 | `audit/02-access-control-policy-findings/04-encrypted-search-bypasses-acp.md` | Encrypted search queries bypass ACP | CONFIRMED |
| 02-09 | `audit/02-access-control-policy-findings/09-nac-enable-no-authentication.md` | NAC enable endpoint has no authentication | CONFIRMED |
| 02-10 | `audit/02-access-control-policy-findings/10-policy-transition-guards-dead-code.md` | Policy transition safety guards are dead code | CONFIRMED |
| 02-15 | `audit/02-access-control-policy-findings/15-zanzibar-read-check-error-suppression.md` | Zanzibar read check silently suppresses errors | CONFIRMED |
| 02-16 | `audit/02-access-control-policy-findings/16-debug-dump-no-nac-check.md` | Debug dump endpoint has no NAC check | CONFIRMED |
| 02-20 | `audit/02-access-control-policy-findings/20-block-verify-not-in-merge-path.md` | Block verification function disconnected from merge path | CONFIRMED |
| 02-23 | `audit/02-access-control-policy-findings/23-no-dump-backup-acp-test.md` | No integration test for dump or backup with ACP | CONFIRMED |
| 02-25 | `audit/02-access-control-policy-findings/25-no-graphql-nac-integration-test.md` | No integration test for GraphQL NAC bypass | CONFIRMED |
| 02-27 | `audit/02-access-control-policy-findings/27-no-unauthorized-create-test.md` | No test for unauthorized document creation | CONFIRMED |
| 02-31 | `audit/02-access-control-policy-findings/31-sourcehub-policy-add-non-atomic.md` | SourceHub policy add is non-atomic | CONFIRMED |
| 02-32 | `audit/02-access-control-policy-findings/32-sourcehub-cache-staleness-no-refresh.md` | SourceHub policy cache has no refresh mechanism | CONFIRMED |
| 02-33 | `audit/02-access-control-policy-findings/33-sourcehub-network-partition-no-fail-closed.md` | SourceHub network partition: no explicit fail-closed policy | CONFIRMED |
| 02-34 | `audit/02-access-control-policy-findings/34-sourcehub-bearer-token-signing-config-dependency.md` | SourceHub bearer token requires global signing config | CONFIRMED |
| 02-38 | `audit/02-access-control-policy-findings/38-sourcehub-integration-test-coverage-gaps.md` | SourceHub integration tests cover happy path only | CONFIRMED |

## Stream 03 -- P2P Network Security (16)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 03-02 | `audit/03-p2p-network-security-findings/02-kademlia-mode-server-no-auto.md` | Kademlia hardcoded to Mode::Server instead of ModeAuto | CONFIRMED |
| 03-03 | `audit/03-p2p-network-security-findings/03-kademlia-memorystore-eclipse-surface.md` | Kademlia MemoryStore loses DHT state on restart | CONFIRMED |
| 03-05 | `audit/03-p2p-network-security-findings/05-yamux-default-streams-no-limit.md` | Yamux uses all defaults -- no max concurrent streams limit | CONFIRMED |
| 03-09 | `audit/03-p2p-network-security-findings/09-identify-address-flooding-to-kademlia.md` | Identify address flooding to Kademlia without limit | CONFIRMED |
| 03-13 | `audit/03-p2p-network-security-findings/13-broadcast-signing-failure-silent-drop.md` | Broadcast signing failure silently drops field blocks | CONFIRMED |
| 03-14 | `audit/03-p2p-network-security-findings/14-pushlog-codec-signing-is-dead-code.md` | PushLogCodec signing/verification is dead code in production | CONFIRMED |
| 03-16 | `audit/03-p2p-network-security-findings/16-serde-cbor-flatten-indefinite-map-divergence.md` | serde_cbor `#[serde(flatten)]` produces indefinite-length CBOR maps | CONFIRMED |
| 03-23 | `audit/03-p2p-network-security-findings/23-gossipsub-checks-relay-not-originator.md` | GossipSub access check uses relay peer, not message originator | CONFIRMED |
| 03-32 | `audit/03-p2p-network-security-findings/32-pending-dags-unbounded-growth.md` | Pending DAGs HashMap has unbounded growth | CONFIRMED |
| 03-33 | `audit/03-p2p-network-security-findings/33-dag-fetcher-unbounded-task-fan-out.md` | DAG fetcher spawns unbounded concurrent tasks per reply | CONFIRMED |
| 03-35 | `audit/03-p2p-network-security-findings/35-car-response-no-origin-verification.md` | CAR response blocks stored without origin verification | CONFIRMED |
| 03-46 | `audit/03-p2p-network-security-findings/46-channel-bounds-audit.md` | Channel bounds audit -- one unbounded channel found | CONFIRMED |
| 03-47 | `audit/03-p2p-network-security-findings/47-timeout-map-completeness-audit.md` | Timeout map -- two-stream reads have no timeout | CONFIRMED |
| 03-49 | `audit/03-p2p-network-security-findings/49-pending-responses-no-eviction.md` | PendingResponses HashMap has no eviction | CONFIRMED |
| 03-50 | `audit/03-p2p-network-security-findings/50-car-response-unbounded-dag-collection.md` | CAR response collects unbounded DAG from blockstore | CONFIRMED |
| 03-52 | `audit/03-p2p-network-security-findings/52-no-global-memory-budget.md` | No global memory budget or per-peer memory tracking | CONFIRMED |
| 03-22 | `audit/03-p2p-network-security-findings/22-bitswap-no-collection-access-checks.md` | Bitswap serves blocks without collection-level access checks | CONFIRMED |

## Stream 04 -- Identity & Key Management (14)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 04-00 | `audit/04-identity-key-management-findings/00-wildcard-did-key-portion-panic.md` | Wildcard DID key_portion() panics on out-of-bounds slice | CONFIRMED |
| 04-21 | `audit/04-identity-key-management-findings/21-pbkdf2-iteration-count-weak.md` | PBKDF2 iteration count weak (10,000) | CONFIRMED |
| 04-23 | `audit/04-identity-key-management-findings/23-load-secret-from-env-not-zeroized.md` | Keyring secret from environment not zeroized | CONFIRMED |
| 04-24 | `audit/04-identity-key-management-findings/24-get-returns-plain-vec-not-zeroizing.md` | Keyring get() returns plain Vec, not Zeroizing | CONFIRMED |
| 04-27 | `audit/04-identity-key-management-findings/27-private-key-printed-to-stdout.md` | Private key material printed to stdout | CONFIRMED |
| 04-36 | `audit/04-identity-key-management-findings/36-empty-bearer-treated-as-anonymous.md` | Empty Bearer token treated as anonymous | CONFIRMED |
| 04-40 | `audit/04-identity-key-management-findings/40-cors-wildcard-with-auth-header.md` | CORS allows wildcard origin with auth header | CONFIRMED |
| 04-41 | `audit/04-identity-key-management-findings/41-no-x-forwarded-host-support.md` | No X-Forwarded-Host support for audience validation | CONFIRMED |
| 04-42 | `audit/04-identity-key-management-findings/42-private-key-on-cli-argument.md` | Private key passed as CLI argument visible in process table | CONFIRMED |
| 04-43 | `audit/04-identity-key-management-findings/43-identity-new-prints-private-key-to-stdout.md` | identity new prints private key to stdout | CONFIRMED |
| 04-45 | `audit/04-identity-key-management-findings/45-identity-extractor-per-handler-not-middleware.md` | Identity extraction is per-handler, not global middleware | CONFIRMED |
| 04-47 | `audit/04-identity-key-management-findings/47-keyring-import-accepts-key-on-cli-argument.md` | keyring import accepts key on CLI argument | CONFIRMED |
| 04-51 | `audit/04-identity-key-management-findings/51-key-type-ambiguity-32-byte-keys.md` | Key type ambiguity for 32-byte keys | CONFIRMED |
| 04-53 | `audit/04-identity-key-management-findings/53-no-expired-token-integration-test.md` | No expired token integration test | CONFIRMED |
| 04-58 | `audit/04-identity-key-management-findings/58-no-identity-confusion-test.md` | No identity confusion/substitution integration test | CONFIRMED |

## Stream 05 -- Input Validation (11)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 05-02 | `audit/05-input-validation-findings/02-filter-recursion-unbounded.md` | Filter Logical Operators Allow Unbounded Recursion | CONFIRMED |
| 05-03 | `audit/05-input-validation-findings/03-sdl-schema-no-size-limits.md` | SDL Schema Endpoint Accepts Unbounded Input | CONFIRMED |
| 05-05 | `audit/05-input-validation-findings/05-no-query-timeout-or-cost-budget.md` | No Query Timeout or Cost Budget | CONFIRMED |
| 05-06 | `audit/05-input-validation-findings/06-sse-subscription-no-limits.md` | SSE Subscription Has No Connection or Resource Limits | CONFIRMED |
| 05-08 | `audit/05-input-validation-findings/08-wasm-lens-path-traversal.md` | WASM Lens Module Path Traversal via `file://` Prefix | CONFIRMED |
| 05-09 | `audit/05-input-validation-findings/09-ffi-backup-arbitrary-path-write.md` | FFI Backup Export Writes to Arbitrary Filesystem Path | CONFIRMED |
| 05-19 | `audit/05-input-validation-findings/19-multiaddr-ssrf-no-ip-blocklist.md` | Multiaddr SSRF -- No Private IP Blocklist | CONFIRMED |
| 05-21 | `audit/05-input-validation-findings/21-graphql-introspection-always-enabled.md` | GraphQL Introspection Always Enabled | CONFIRMED |
| 05-32 | `audit/05-input-validation-findings/32-no-http-rate-limiting.md` | No HTTP Rate Limiting, Request Timeout, or Connection Limits | CONFIRMED |
| 05-33 | `audit/05-input-validation-findings/33-lens-transform-output-no-validation.md` | Lens Transform Output Not Validated Against Schema | CONFIRMED |
| 05-36 | `audit/05-input-validation-findings/36-wasm-transform-runs-on-tokio-thread.md` | WASM Transform Execution Blocks Tokio Worker Thread | CONFIRMED |

## Stream 06 -- Data Integrity & CRDT (18)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 06-00 | `audit/06-data-integrity-crdt-findings/00-composite-counter-nonce-ordering-unsafe.md` | Composite counter nonce ordering unsafe | CONFIRMED |
| 06-01 | `audit/06-data-integrity-crdt-findings/01-composite-counter-missing-allow-decrement.md` | Composite counter missing allow_decrement | CONFIRMED |
| 06-02 | `audit/06-data-integrity-crdt-findings/02-composite-counter-missing-float64-support.md` | Composite counter missing Float64 support | CONFIRMED |
| 06-12 | `audit/06-data-integrity-crdt-findings/12-merged-composites-unbounded-growth.md` | merged_composites unbounded growth | CONFIRMED |
| 06-14 | `audit/06-data-integrity-crdt-findings/14-no-per-document-merge-locking.md` | No per-document merge locking | CONFIRMED |
| 06-18 | `audit/06-data-integrity-crdt-findings/18-block-cid-not-verified-before-merge.md` | Block CID not verified before merge | CONFIRMED |
| 06-23 | `audit/06-data-integrity-crdt-findings/23-no-cid-verification-on-put.md` | No CID verification on put() | CONFIRMED |
| 06-24 | `audit/06-data-integrity-crdt-findings/24-unsupported-hash-algorithm-bypass.md` | Unsupported hash algorithm bypass | CONFIRMED |
| 06-29 | `audit/06-data-integrity-crdt-findings/29-p2p-pushlog-no-cid-verification-before-storage.md` | P2P PushLog no CID verification before storage | CONFIRMED |
| 06-32 | `audit/06-data-integrity-crdt-findings/32-se-push-docs-no-identity-isolation.md` | SE push docs no identity isolation | CONFIRMED |
| 06-33 | `audit/06-data-integrity-crdt-findings/33-se-artifact-storage-key-reveals-document-tag-association.md` | SE artifact storage key reveals document-tag association | CONFIRMED |
| 06-35 | `audit/06-data-integrity-crdt-findings/35-se-no-artifact-validation-on-receive.md` | No SE artifact validation on receive | CONFIRMED |
| 06-36 | `audit/06-data-integrity-crdt-findings/36-se-enc-key-not-zeroized-vec-u8.md` | SE enc_key not zeroized Vec<u8> | CONFIRMED |
| 06-39 | `audit/06-data-integrity-crdt-findings/39-se-merge-handler-no-artifact-generation.md` | SE merge handler no artifact generation | CONFIRMED |
| 06-41 | `audit/06-data-integrity-crdt-findings/41-conflict-tracker-gc-misses-long-running-txns.md` | ConflictTracker GC misses long-running transactions | CONFIRMED |
| 06-44 | `audit/06-data-integrity-crdt-findings/44-no-transaction-timeout-or-limit.md` | No transaction timeout or concurrent limit | CONFIRMED |
| 06-56 | `audit/06-data-integrity-crdt-findings/56-index-update-failure-non-blocking-stale-indexes.md` | Index update failure non-blocking -- stale indexes | CONFIRMED |
| 06-61 | `audit/06-data-integrity-crdt-findings/61-nonce-storage-cost-quantified.md` | Nonce storage cost quantified -- P2P amplification | CONFIRMED |

## Stream 07 -- Dependency & Unsafe Code (19)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 07-01 | `audit/07-dependency-unsafe-code-findings/01-raw-slice-no-length-cap.md` | `from_raw_parts` with Uncapped Length | CONFIRMED |
| 07-04 | `audit/07-dependency-unsafe-code-findings/04-race-node-close-vs-operations.md` | Race Between `node_close` and Concurrent Operations | CONFIRMED |
| 07-12 | `audit/07-dependency-unsafe-code-findings/12-rocksdb-owned-snapshot-transmute.md` | RocksDB OwnedSnapshot Lifetime Transmute | SUSPECTED |
| 07-13 | `audit/07-dependency-unsafe-code-findings/13-fetcher-wrapper-fat-pointer-transmute.md` | FetcherWrapper Fat Pointer Transmute | SUSPECTED |
| 07-15 | `audit/07-dependency-unsafe-code-findings/15-unsafe-send-sync-impls-inventory.md` | Manual `unsafe impl Send/Sync` Inventory | SUSPECTED |
| 07-21 | `audit/07-dependency-unsafe-code-findings/21-ring-0-16-aes-panic-cve.md` | ring 0.16.20 AES Panic CVE (RUSTSEC-2025-0009) | CONFIRMED |
| 07-22 | `audit/07-dependency-unsafe-code-findings/22-wasmtime-27-multiple-cves.md` | wasmtime 27.0.0 Multiple CVEs | CONFIRMED |
| 07-23 | `audit/07-dependency-unsafe-code-findings/23-lru-unsound-itermut.md` | lru 0.12.5 Unsound IterMut (RUSTSEC-2026-0002) | CONFIRMED |
| 07-24 | `audit/07-dependency-unsafe-code-findings/24-serde-cbor-unmaintained.md` | serde_cbor 0.11.2 Unmaintained Since 2021 | CONFIRMED |
| 07-25 | `audit/07-dependency-unsafe-code-findings/25-iroh-bitswap-git-dependency.md` | iroh-bitswap Git Dependency with Stale Deps | CONFIRMED |
| 07-26 | `audit/07-dependency-unsafe-code-findings/26-libp2p-0-53-version-lag.md` | libp2p 0.53.2 Version Lag | CONFIRMED |
| 07-29 | `audit/07-dependency-unsafe-code-findings/29-no-cargo-deny-config.md` | No cargo-deny Configuration | CONFIRMED |
| 07-40 | `audit/07-dependency-unsafe-code-findings/40-cbindgen-header-no-ci-verification.md` | cbindgen Header Not Verified in CI | CONFIRMED |
| 07-41 | `audit/07-dependency-unsafe-code-findings/41-no-overflow-checks-in-release.md` | No Integer Overflow Checks in Release | CONFIRMED |
| 07-42 | `audit/07-dependency-unsafe-code-findings/42-ci-wasm-pack-curl-pipe-sh.md` | CI WASM Build Uses curl-pipe-sh | CONFIRMED |
| 07-43 | `audit/07-dependency-unsafe-code-findings/43-ci-no-cargo-audit-step.md` | CI Missing cargo audit / cargo deny Steps | CONFIRMED |
| 07-50 | `audit/07-dependency-unsafe-code-findings/50-ffi-test-suite-non-functional.md` | FFI Test Suite on Feature Branch Only | CONFIRMED |
| 07-52 | `audit/07-dependency-unsafe-code-findings/52-no-handle-lifecycle-stress-testing.md` | No Handle Lifecycle Stress Testing | CONFIRMED |
| 07-56 | `audit/07-dependency-unsafe-code-findings/56-cross-stream-integration-gaps.md` | Cross-Stream Integration Gaps | CONFIRMED |
