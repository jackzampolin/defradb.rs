# Finding: All Session 1-4 ACP Findings Apply Equally to SourceHub Mode

**Stream**: 02 - Access Control Policy
**Severity**: INFO (meta-finding)
**Category**: Provider Equivalence Analysis
**Status**: CONFIRMED

## Summary

A systematic review of all 29 findings from Sessions 1-4 confirms that **every finding applies to SourceHub mode** — either directly (same code path) or with amplified impact. The `DocumentACP` trait abstraction means that bypass vectors, missing checks, and test gaps are provider-agnostic.

## Finding-by-Finding Analysis

### Session 1: DAC Implementation Review

| # | Finding | SourceHub Impact |
|---|---------|-----------------|
| 03 | CID time-travel bypasses ACP | **SAME** — `_caller_identity` is unused regardless of provider |
| 04 | Encrypted search bypasses ACP | **SAME** — no identity in SE query path regardless of provider |
| 05 | DAC bypass thread-local safety | **SAME** — thread-local is provider-agnostic |
| 06 | View plans skip view-collection ACP | **SAME** — planner logic is provider-agnostic |
| 07 | DAC checklist (INFO) | **SAME** — `PermissionFilterNode` works with any `DocumentACP` |

### Session 2: NAC and Zanzibar

| # | Finding | SourceHub Impact |
|---|---------|-----------------|
| 08 | GraphQL bypasses NAC | **SAME** — NAC layer is orthogonal to DAC provider |
| 09 | NAC enable no authentication | **SAME** — NAC bootstrap is provider-agnostic |
| 10 | Policy transition guards dead code | **SAME** — transition checks in `collection_acp.rs` are provider-agnostic |
| 11 | Policy expressions (INFO) | **SAME** — policy language is the same |
| 12 | Zanzibar key delimiter injection | **AMPLIFIED** — local ZanzibarStore is used for SourceHub policy caching |
| 13 | NAC disabled state (INFO) | **SAME** — NAC is provider-agnostic |
| 14 | Policy YAML no size limits | **AMPLIFIED** — large policies submitted to SourceHub also hit on-chain limits |
| 15 | Zanzibar read check error suppression | **LESS RELEVANT** — SourceHub uses on-chain checks, not local Zanzibar for permissions |
| 16 | Debug dump no NAC check | **SAME** — dump endpoint is provider-agnostic |
| 17 | Policy ID not content hash (INFO) | **DIVERGENT** — SourceHub uses on-chain ID, local uses counter-SHA256 |

### Session 3: Bypass Surface & Recovery

| # | Finding | SourceHub Impact |
|---|---------|-----------------|
| 00 | Recovery mode bypass | **AMPLIFIED** — see finding 36 (on-chain permissions bypassed) |
| 01 | Dump bypasses ACP | **SAME** — dump reads from local store regardless of provider |
| 18 | P2P merge no signature verification | **SAME** — merge handler is provider-agnostic |
| 19 | P2P creator identity from metadata | **SAME** — metadata spoofing is provider-agnostic |
| 20 | Block verify not in merge path | **SAME** — block_verify.rs is not called regardless of provider |

### Session 4: Integration Test Gaps

| # | Finding | SourceHub Impact |
|---|---------|-----------------|
| 22 | No _commits ACP test | **SAME** — no SourceHub-specific _commits test either |
| 23 | No dump/backup ACP test | **SAME** — no SourceHub-specific dump test either |
| 24 | P2P ACP never tests merge denial | **SAME** — SourceHub P2P test doesn't verify denial |
| 25 | No GraphQL NAC test | **SAME** — no SourceHub-specific NAC test either |
| 26 | Weak mutation denial assertions | **SAME** — assertion patterns are provider-agnostic |
| 27 | No unauthorized create test | **SAME** — no SourceHub-specific create denial test |
| 28 | No policy transition test | **SAME** — no SourceHub-specific transition test |

## Key Insight

The `DocumentACP` trait abstraction is both a strength and a weakness:
- **Strength**: A single fix for a bypass vector fixes it for all providers
- **Weakness**: A single bypass vector affects all providers, including the on-chain SourceHub path where bypasses have broader implications (blockchain authorization bypassed without audit trail)

## Provider-Specific Gaps Not Covered by Sessions 1-4

The following gaps are unique to SourceHub and not covered by previous findings:
- **Cache staleness** (finding 32) — doesn't exist in local mode
- **Bearer token dependency** (finding 34) — doesn't exist in local mode
- **Network partition behavior** (finding 33) — doesn't exist in local mode
- **Policy add atomicity** (finding 31) — doesn't exist in local mode
- **ABCI error masking** (finding 30) — doesn't exist in local mode

## Remediation

All remediations from Sessions 1-4 apply. Priority should be given to fixes that benefit both providers equally via the trait abstraction.
