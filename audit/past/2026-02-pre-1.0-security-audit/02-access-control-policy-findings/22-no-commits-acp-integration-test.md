# Finding: No Integration Test for _commits ACP Bypass

**Stream**: 02 - Access Control Policy
**Severity**: HIGH (test gap for CRITICAL vulnerability)
**Category**: Test Gap
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation
**Related Finding**: 02 (_commits queries bypass ACP entirely — CRITICAL)

## Summary

Finding 02 identified a CRITICAL vulnerability: `_commits` queries bypass ACP entirely, allowing any user to query the full commit history of any ACP-protected document. **No integration test exists that queries `_commits` on an ACP-protected document with an unauthorized identity.** The only `_commits` usage in the test suite is in `block_verify.rs` (without ACP) and `subscription_docid.rs` (without ACP), neither of which exercises the bypass.

## Evidence

### Search for `_commits` in integration tests

| File | Context | ACP? |
|------|---------|------|
| `block_verify.rs:40` | `_commits(docID: ...)` query to get CIDs | **NO** — `.with_signing()`, no ACP |
| `subscription_docid.rs:177` | `_commits` subscription filtered by docID | **NO** — no ACP policy deployed |

### What's missing

Every ACP integration test uses only `User` or `Document` collection queries:

```graphql
query { User { _docID name age } }
```

No test ever runs:

```graphql
query { _commits(docID: "bae-protected-doc") { cid docID fieldName height } }
```

### Impact

The _commits bypass is the single most severe ACP vulnerability found in this audit (CRITICAL). Without a regression test, any fix could silently regress, and the bypass has been present since the initial implementation. The `block_verify.rs` test at line 40 actually **demonstrates** the bypass: it queries `_commits` without any identity on a document created with an identity, and succeeds.

## Missing Test

```
1. Deploy ACP policy + schema with @policy directive
2. Create document as Alice (owner)
3. Query `_commits(docID: ...)` as Alice → ALLOW, returns commits
4. Query `_commits(docID: ...)` as Bob (no relation) → should return EMPTY (currently returns all commits)
5. Query `_commits(docID: ...)` with no identity (anonymous) → should return EMPTY (currently returns all commits)
```

## Severity Rationale

A CRITICAL vulnerability without any test coverage is HIGH priority as a test gap because:
1. No regression detection exists if the code changes
2. The existing `block_verify.rs` test silently relies on the bypass without documenting it
3. Any future fix needs a test to verify correctness
