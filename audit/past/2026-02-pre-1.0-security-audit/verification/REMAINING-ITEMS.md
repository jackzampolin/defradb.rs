# Audit Verification: Final Status

**Audit**: Pre-1.0 Security Audit
**Completed**: 2026-02-24
**Streams**: 7 parallel audit streams, 354 total findings
**Reports**: `audit/verification/stream-{01..07}-verification.md`

---

## Status: COMPLETE

All 1.0 blockers resolved. All partially-fixed items either remediated or tracked for ongoing work.

---

## RESOLVED ITEMS

### Previously partially fixed — now resolved

- ~~**02-32/33/34**~~: Circuit breaker and fail-closed implemented. Ongoing cache TTL and performance work tracked in #516 (SourceHub ACP Performance epic).
- ~~**04-45**~~: Global deny-by-default HTTP auth middleware implemented (commit `6a6c475f`).
- ~~**07-51**~~: FFI negative tests — no longer applicable; integration test framework is the primary validation path going forward.

### Block signature verification in merge path (commit 96d3c835)
- ~~02-19~~: `verify_block_signature()` now returns verified signer identity as `did:key:` DID. `BlockMetadata.verified_creator` populated after verification. `effective_creator()` prefers verified over self-reported `creator`.
- ~~02-20~~: `verify_block_signature()` is now blocking — invalid/tampered signatures reject the block with `SignatureVerificationFailed`. Both single-block and batch merge paths verify. 10 new unit tests (6 for verification, 4 for `effective_creator`).

### wasmtime CVE fix
- ~~07-22~~: Upgraded wasmtime from 27.0.0 to 41.0.3. All three CVEs (RUSTSEC-2025-0046, RUSTSEC-2025-0118, RUSTSEC-2026-0006) resolved. Zero API breakage; all lens unit + integration tests pass.

### Block-level signature verification completes 02-18
- ~~02-18~~: Block-level signature verification in merge path now implemented (see 02-19/02-20 above). P2P message-level + block-level both verified.

### Audit hardening batch
- ~~01-05~~: `generate_ed25519()` seed and key_bytes now zeroized after keypair derivation via `zeroize::Zeroize`.
- ~~01-09~~: Binary identity SE artifact round-trip test added — creates tag with invalid-UTF-8 identity bytes, serializes to JSON, deserializes, verifies equality.
- ~~03-20~~: FFI path `crates/ffi/src/p2p/node.rs:221` now uses `AccessMode::Controlled` (parity with CLI).

### Quick wins (commits a9454c38, 61879bb3)
1. ~~06-29~~: `verify_block_cid()` added to PushLog handler + existing unit test
2. ~~05-31~~: `WasmSandboxConfig::restrictive()` enabled by default + existing unit test
3. ~~03-21~~: Access checks added to DocSync/BranchableSync + 9 new unit tests
4. ~~06-36~~: Merge handler `OnceLock` wrapped with `Zeroizing`
5. ~~06-18~~: Mitigated by 06-29 fix (all P2P ingestion paths now verify CIDs)
6. ~~04-45~~: Global deny-by-default HTTP auth middleware (commit `6a6c475f`)

---

## 1.0 BLOCKERS

_None — all resolved._

---

## ONGOING WORK (tracked in issues)

| Area | Tracking Issue | Description |
|------|---------------|-------------|
| SourceHub ACP caching | #516 | Cache TTL, identity-aware caching, event-driven invalidation |
| SourceHub configurability | #509 | Wire hardcoded constants through CLI flags |
