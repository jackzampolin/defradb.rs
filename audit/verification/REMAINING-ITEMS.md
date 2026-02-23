# Audit Verification: Remaining Items

**Date**: 2026-02-23
**Pass**: First verification re-audit (7 parallel streams)
**Reports**: `audit/verification/stream-{01..07}-verification.md`

---

## NOT FIXED (2 items)

### 02-19: P2P Creator Identity from Metadata Not Signature
- **Severity**: HIGH
- **Location**: `crates/db/src/acp_merge_handler.rs:211-220`
- **Issue**: AcpMergeHandler extracts `creator` from PushLog metadata (`metadata.creator`) -- a self-reported value. A compromised peer can sign the PushLog message with their own key while setting `Creator` to someone else's identity.
- **Fix**: Derive creator from the block's embedded CRDT signature instead of PushLog metadata.
- **Coupled with**: 02-20

### 02-20: Block Verify Disconnected from Merge Path
- **Severity**: HIGH
- **Location**: `crates/db/src/block_verify.rs:15-112`
- **Issue**: `verify_block_signature()` exists and works correctly but is only called from the HTTP `/api/v0/block/verify` endpoint. Not integrated into `AcpMergeHandler::handle_block()` or any P2P merge pipeline.
- **Fix**: Call `verify_block_signature()` from the merge handler before processing blocks received via P2P.
- **Coupled with**: 02-19

### 07-22: wasmtime 27.0.0 CVEs
- **Severity**: HIGH
- **Location**: `Cargo.lock` (wasmtime 27.0.0)
- **Issue**: Three unpatched CVEs in wasmtime 27.0.0. Sandbox config is now enabled (05-31 fixed) but version still has known vulns.
- **Fix**: Upgrade wasmtime to latest stable.

---

## PARTIALLY FIXED (7 items)

### 01-XX: Crypto (2 items)
- **01-05**: `generate_ed25519()` seed not zeroized after keypair derivation. The seed `Vec<u8>` lives on the heap until dropped but is not explicitly zeroed.
- **01-09**: No binary identity SE test vector. Text-based test exists but no binary round-trip test for identity-tagged SE artifacts.

### 02-XX: ACP (2 items)
- **02-18**: P2P message-level signature verification is done. Block-level signature verification in merge path is not. (See 02-19/02-20.)
- **02-32/33/34**: Circuit breaker implemented for SourceHub fail-closed on partition. Cache TTL refresh and bearer token handling need further work.

### 03-XX: P2P (1 item)
- **03-20**: CLI path activates `AccessMode::Controlled` when ACP is configured. FFI path in `crates/ffi/src/p2p/node.rs:221` hardcodes `AccessMode::Open`.

### 04-XX: Identity (1 item)
- **04-45**: No global deny-by-default auth middleware. Each handler must explicitly include `ExtractIdentity`. The dump endpoint was fixed individually but the structural gap remains.

### 07-XX: Deps/Unsafe (1 item)
- **07-51**: Go FFI wrapper negative tests remain on feature branch. Need merge to main before 1.0.

---

## QUICK WINS — DONE (commit a9454c38, 61879bb3)

All 4 quick wins implemented and tested:
1. ~~06-29~~: `verify_block_cid()` added to PushLog handler + existing unit test
2. ~~05-31~~: `WasmSandboxConfig::restrictive()` enabled by default + existing unit test
3. ~~03-21~~: Access checks added to DocSync/BranchableSync + 9 new unit tests
4. ~~06-36~~: Merge handler `OnceLock` wrapped with `Zeroizing`
5. ~~06-18~~: Mitigated by 06-29 fix (all P2P ingestion paths now verify CIDs)

---

## 1.0 BLOCKERS

| Item | Fix Complexity | Notes |
|------|---------------|-------|
| 07-22 (wasmtime CVEs) | Medium (dependency upgrade) | May have breaking API changes |
| 02-19 + 02-20 (block verify in merge) | Medium-High | Architectural — coupled pair |

---

## POST-1.0 HARDENING

| Item | Description |
|------|-------------|
| 03-20 | FFI AccessMode parity with CLI |
| 04-45 | Global deny-by-default HTTP middleware |
| 01-05 | Ed25519 seed zeroization |
| 01-09 | Binary identity SE test vector |
| 02-32/33/34 | SourceHub cache TTL refresh + bearer token |
| 07-51 | FFI negative tests merge to main |
