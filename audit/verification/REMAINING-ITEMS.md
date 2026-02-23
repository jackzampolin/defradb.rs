# Audit Verification: Remaining Items

**Date**: 2026-02-23
**Pass**: First verification re-audit (7 parallel streams)
**Reports**: `audit/verification/stream-{01..07}-verification.md`

---

## NOT FIXED (5 items)

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

### 05-31: WASM Sandbox Never Activated
- **Severity**: HIGH
- **Location**: `crates/db/src/database.rs:341`
- **Issue**: `WasmSandboxConfig` with memory limits, fuel metering, and epoch deadline exists in `crates/lens/src/wasm.rs`. But `WasmTransformStore::new()` passes `sandbox: None`. Malicious WASM can OOM or infinite-loop the node.
- **Fix**: Enable `WasmSandboxConfig::restrictive()` by default.

### 06-29: PushLog No CID Verification
- **Severity**: CRITICAL
- **Location**: `crates/p2p/src/sync/manager/process/pushlog.rs:155`
- **Issue**: `process_block_inner()` stores blocks via `self.blockstore.put(cid, &msg.block)` without calling `verify_block_cid()`. Bitswap and CAR paths verify CIDs, but PushLog (highest volume P2P path) does not.
- **Fix**: Add `verify_block_cid(&cid, &msg.block)?` before `blockstore.put()`. ~3 lines.

### 07-22: wasmtime 27.0.0 CVEs
- **Severity**: HIGH
- **Location**: `Cargo.lock` (wasmtime 27.0.0)
- **Issue**: Three unpatched CVEs in wasmtime 27.0.0. Sandbox config exists but is not enabled (see 05-31).
- **Fix**: Upgrade wasmtime to latest stable. Immediate mitigation: enable sandbox (05-31).

---

## PARTIALLY FIXED (10 items)

### 01-XX: Crypto (2 items)
- **01-05**: `generate_ed25519()` seed not zeroized after keypair derivation. The seed `Vec<u8>` lives on the heap until dropped but is not explicitly zeroed.
- **01-09**: No binary identity SE test vector. Text-based test exists but no binary round-trip test for identity-tagged SE artifacts.

### 02-XX: ACP (2 items)
- **02-18**: P2P message-level signature verification is done. Block-level signature verification in merge path is not. (See 02-19/02-20.)
- **02-32/33/34**: Circuit breaker implemented for SourceHub fail-closed on partition. Cache TTL refresh and bearer token handling need further work.

### 03-XX: P2P (2 items)
- **03-20**: CLI path activates `AccessMode::Controlled` when ACP is configured. FFI path in `crates/ffi/src/p2p/node.rs:221` hardcodes `AccessMode::Open`.
- **03-21**: CAR handler has `check_peer_is_replicator()`. DocSync and BranchableSync handlers lack access checks -- unauthorized peers can enumerate document/collection heads.

### 04-XX: Identity (1 item)
- **04-45**: No global deny-by-default auth middleware. Each handler must explicitly include `ExtractIdentity`. The dump endpoint was fixed individually but the structural gap remains.

### 06-XX: CRDT/Data (2 items)
- **06-18**: `hash_on_read` exists but not enabled for P2P blockstores. Mitigated by ingestion-time `verify_block_cid()` where implemented (Bitswap, CAR -- but not PushLog, see 06-29).
- **06-36**: SE `enc_key` uses `Zeroizing<Vec<u8>>` in `SECoordinatorConfig`. But merge handler's copy at `crates/db/src/merge_handler/mod.rs:144` uses plain `OnceLock<Vec<u8>>`.

### 07-XX: Deps/Unsafe (1 item)
- **07-51**: Go FFI wrapper negative tests remain on feature branch. Need merge to main before 1.0.

---

## QUICK WINS (fix now)

1. **06-29**: Add `verify_block_cid(&cid, &msg.block)?` before `blockstore.put()` in PushLog handler (~3 lines)
2. **05-31 + 07-22**: Enable `WasmSandboxConfig::restrictive()` by default in `database.rs:341`
3. **03-21**: Add `check_peer_is_replicator()` to DocSync/BranchableSync handlers (~4 lines each)
4. **06-36**: Wrap merge handler `OnceLock<Vec<u8>>` with `Zeroizing`

---

## 1.0 BLOCKERS

| Item | Fix Complexity | Notes |
|------|---------------|-------|
| 06-29 (PushLog CID) | Trivial (~3 lines) | Highest priority |
| 05-31 (WASM sandbox) | Small (enable existing config) | |
| 07-22 (wasmtime CVEs) | Medium (dependency upgrade) | May have breaking API changes |

---

## POST-1.0 HARDENING

| Item | Description |
|------|-------------|
| 02-19 + 02-20 | Block-level identity verification in merge path (architectural) |
| 03-20 | FFI AccessMode parity with CLI |
| 04-45 | Global deny-by-default HTTP middleware |
| 01-05 | Ed25519 seed zeroization |
