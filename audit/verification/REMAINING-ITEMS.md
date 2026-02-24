# Audit Verification: Remaining Items

**Date**: 2026-02-24
**Pass**: First verification re-audit (7 parallel streams)
**Reports**: `audit/verification/stream-{01..07}-verification.md`

---

## NOT FIXED (0 items)

_None — all 1.0 blockers resolved._

---

## PARTIALLY FIXED (3 items)

### 02-XX: ACP (1 item)
- **02-32/33/34**: Circuit breaker implemented for SourceHub fail-closed on partition. Cache TTL refresh and bearer token handling need further work.

### 04-XX: Identity (1 item)
- **04-45**: No global deny-by-default auth middleware. Each handler must explicitly include `ExtractIdentity`. The dump endpoint was fixed individually but the structural gap remains.

### 07-XX: Deps/Unsafe (1 item)
- **07-51**: Go FFI wrapper negative tests remain on feature branch. Need merge to main before 1.0.

---

## FIXED — DONE

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

---

## 1.0 BLOCKERS

_None — all 1.0 blockers resolved._

---

## POST-1.0 HARDENING

| Item | Description |
|------|-------------|
| 04-45 | Global deny-by-default HTTP middleware |
| 02-32/33/34 | SourceHub cache TTL refresh + bearer token |
| 07-51 | FFI negative tests merge to main |
