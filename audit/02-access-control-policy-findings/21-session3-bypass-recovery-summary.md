# Session 3 Summary: Bypass Surface & Recovery Mode

**Stream**: 02 - Access Control Policy
**Session**: 3 of 5 (CRITICAL)

## Critical Questions Answered

### 3a: Recovery Mode Bypass

| Question | Answer |
|----------|--------|
| Recovery only at startup? | **NO** — version sync uses recovery metadata during normal P2P operation |
| Can a P2P peer cause recovery path? | **YES** — version sync fetches blocks from peers and merges with recovery flag |
| Is `BlockMetadata::recovery()` only from `recover_unmerged()`? | **NO** — also from `version_syncer.rs:308` and `ffi/version_sync.rs:264` |
| Could version sync inject schemas? | **YES** — `CollectionDefinition` blocks bypass ACP via recovery mode |
| Silent ACP bypass on metadata extraction failure? | **NO** — decode failures return `MergeError::BlockDecode`, not silent bypass |

### 3b: Dump/Export Bypass

| Question | Answer |
|----------|--------|
| HTTP endpoint for dump? | **YES** — `GET /api/v0/debug/dump` with NO auth |
| CLI-only risk lower? | **YES** — CLI requires local filesystem access |
| Does export use query executor with ACP? | **YES** — `export_database()` uses `runner.execute()` (correct) |
| What does Acpstore contain? | Policy YAML text and Zanzibar relation tuples (who → what → permission) |

### 3c: Block Verification & Signature

| Question | Answer |
|----------|--------|
| Signature checks before ACP in merge? | **NO** — signature checks are NOT in merge path at all |
| Block creator from signature? | **NO** — creator from peer-reported `BlockMetadata.creator` |
| `verify_block_signature()` in merge flow? | **NO** — only in on-demand `/api/v0/block/verify` endpoint |

## Findings Summary

| # | Severity | Title | Status |
|---|----------|-------|--------|
| 00 | **HIGH** ↑ | Recovery mode bypasses ACP — version sync exploitable mid-operation | UPGRADED from MEDIUM |
| 01 | **HIGH** ↑ | Dump bypasses ACP and NAC — HTTP-exposed, no auth | UPGRADED from MEDIUM |
| 18 | **HIGH** (new) | P2P merge path does not verify block signatures | NEW |
| 19 | **HIGH** (new) | P2P block creator identity from metadata, not signature | NEW |
| 20 | **MEDIUM** (new) | Block verification function disconnected from merge path | NEW (structural) |

## Systemic Issue: P2P Merge Authentication Gap

Findings 00, 18, and 19 together reveal a **systemic authentication gap** in the P2P merge path:

```
P2P peer sends block with metadata
    ↓
metadata.creator = self-reported string         ← Finding 19: identity spoofing
    ↓
AcpMergeHandler: checks ACP using creator       ← uses unverified identity
    ↓
DbMergeHandler: decodes + merges                ← Finding 18: no signature check
    ↓
CRDT delta applied to database                  ← arbitrary data accepted

Version sync additionally:
    ↓
Uses BlockMetadata::recovery()                  ← Finding 00: skips ACP entirely
    ↓
Schema definitions merged without any auth      ← schema injection
```

The root cause is that the P2P merge path trusts metadata from remote peers without cryptographic verification. The `verify_block_signature()` function has the correct verification logic but is not integrated into this path.

### Remediation Priority

1. **Integrate signature verification into merge path** (fixes 18 + 19)
2. **Derive creator identity from signature, not metadata** (fixes 19)
3. **Remove recovery mode from version sync** (fixes 00)
4. **Add auth to dump endpoint** (fixes 01)
