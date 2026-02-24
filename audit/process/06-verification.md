# Phase 6: Verification Re-Audit

Confirm fixes are correct, complete, and haven't introduced regressions.

## When to Verify

- After all CRITICAL and HIGH items are remediated
- Before a release
- After large merges that touch audited code

## Execution

Launch N parallel verification agents (one per stream):

```
Task(subagent_type="general-purpose", prompt="
You are performing a verification re-audit of stream XX: {stream_name}
in the project at {project_root}.

Read:
1. All findings in audit/XX-stream-findings/
2. TRIAGE.md for this stream
3. Git log since audit start: git log --oneline --since='YYYY-MM-DD'

For each finding marked as needing remediation:
1. Check if a fix commit exists (grep git log for finding ID)
2. Read the fix — is it correct and complete?
3. Check for regressions — did the fix break anything else?
4. Verify test exists — is there a test that would catch regression?

Write verification report to audit/verification/stream-XX-verification.md

For each finding, report:
- FIXED: Fix is correct, complete, and tested
- PARTIALLY FIXED: Fix is incomplete or missing tests
- NOT FIXED: No fix found
- REGRESSED: Fix was reverted or broken by later change

Also check: are there NEW vulnerabilities introduced by the fixes?
")
```

## Verification Report Format

```markdown
# Stream XX Verification Report

## Summary
| Status | Count |
|--------|-------|
| FIXED | N |
| PARTIALLY FIXED | N |
| NOT FIXED | N |
| REGRESSED | N |
| NEW FINDINGS | N |

## Details
### XX-00: Finding Title
**Status**: FIXED
**Commit**: abc1234
**Verification**: [How it was verified]

### XX-01: Finding Title
**Status**: PARTIALLY FIXED
**Commit**: def5678
**Gap**: [What's still missing]
```

## Collating Results

After all verification agents complete, create `audit/verification/REMAINING-ITEMS.md`:

```markdown
# Remaining Items

## NOT FIXED (N items)
- XX-NN: Description

## PARTIALLY FIXED (N items)
- XX-NN: Description — what's missing

## REGRESSED (N items)
- XX-NN: Description — what happened

## NEW FINDINGS (N items)
- Description
```

## Squash Merge Hazard

Squash-merged PRs from branches based on older main can silently revert files they didn't touch. After any squash merge during remediation:

1. Check that all previously-fixed files still contain the fix
2. Run `git diff HEAD~1 -- <fixed-files>` to verify
3. If a fix was reverted, re-apply and note the regression

## Time Budget

~1 hour. All verification agents run in parallel. Review REMAINING-ITEMS.md and decide if another remediation pass is needed.
