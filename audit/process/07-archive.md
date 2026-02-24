# Phase 7: Archive

Close out the audit and preserve it for future reference.

## Steps

### 1. Write SUMMARY.md

Create a top-level summary in the audit directory:

```markdown
# {Audit Name} Summary

**Period**: {date range}
**Scope**: {what was audited}
**Status**: Complete — all {severity} blockers resolved

## Audit Streams
| # | Stream | Findings |
|---|--------|----------|

## Key Remediations
- Bullet list of the most important fixes

## Ongoing Work
Items tracked in issues for future work.
```

### 2. Create SESSION-LOG-INDEX.md

Map session log files back to audit streams for traceability. Use haiku agents to scan session logs for audit-related content:

```
Task(subagent_type="Explore", model="haiku", prompt="
Search Claude Code session logs in ~/.claude/projects/{project}/
for sessions related to security audit streams {N-M}.
Report: session ID, date, stream, session number.
")
```

### 3. Update REMAINING-ITEMS.md

Mark all items as either:
- Resolved (with commit hash or issue link)
- Tracked in issue #NNN
- No longer applicable (with reason)

### 4. Move to Archive

```bash
mkdir -p audit/past
mv audit/{findings,plans,triage,verification,*.md} audit/past/{YYYY-MM-audit-name}/
```

Keep only `audit/process/` and `audit/past/` in the audit directory.

### 5. Create Re-Audit Issue

Create a GitHub issue for the next audit, targeting ~1 month out:

```
Title: security: Post-{milestone} re-audit (target: {date})
Body:
- Link to archived audit
- Scope: changed code since last audit + new subsystems
- All 7 original streams + any new streams for new subsystems
```

### 6. Commit and Push

```bash
git add audit/
git commit -m "docs(audit): archive {audit name} as complete"
git push
```

## Directory Structure After Archive

```
audit/
├── process/          ← Reusable process docs (this directory)
└── past/
    └── YYYY-MM-audit-name/
        ├── SUMMARY.md
        ├── SESSION-LOG-INDEX.md
        ├── AUDIT-TRIAGE.md
        ├── REMEDIATION_ROADMAP.md
        ├── verification/
        │   ├── REMAINING-ITEMS.md
        │   └── stream-{01..NN}-verification.md
        ├── 01-stream-name-findings/
        ├── 02-stream-name-findings/
        └── ...
```

## Reuse for Next Audit

When it's time for the next audit, start at Phase 0 again. Don't assume the same streams apply — the codebase has changed. But reference the previous audit's findings to check for regressions and to inform scope.
