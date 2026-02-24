# Security Audit Process

Repeatable process for running a Claude Code security audit against any codebase. Designed for AI-human pair programming — the human defines scope and reviews findings, Claude executes the audit in parallel waves.

## Process Overview

```
Phase 0: Scope Definition        (human + Claude, ~30 min)
Phase 1: Reconnaissance          (parallel agents, ~10 min)
Phase 2: Session Planning        (parallel agents, ~10 min)
Phase 3: Deep-Dive Execution     (parallel waves, ~2-4 hours)
Phase 4: Triage & Prioritization (parallel agents, ~30 min)
Phase 5: Remediation             (iterative, varies)
Phase 6: Verification Re-Audit   (parallel agents, ~1 hour)
Phase 7: Archive                 (human + Claude, ~15 min)
```

Total clock time for a ~180k LOC codebase: roughly one working day for phases 0-4, then remediation work, then verification.

## Files in This Directory

| File | Purpose |
|------|---------|
| `README.md` | This file — process overview |
| `00-scope-definition.md` | How to identify audit areas for a codebase |
| `01-reconnaissance.md` | Recon agent template and instructions |
| `02-session-planning.md` | Planning agent template and session breakdown |
| `03-execution.md` | Deep-dive agent template and wave execution |
| `04-triage.md` | Severity classification and cross-stream analysis |
| `05-remediation.md` | Fix workflow and commit conventions |
| `06-verification.md` | Re-audit methodology |
| `07-archive.md` | How to close out and archive an audit |

## Key Principles

**1. Areas are NOT predetermined.** Every audit starts with scope definition. The areas depend on what the codebase does — a database has different audit areas than a game engine or a web app.

**2. Parallel streams with isolated output.** Each stream writes to its own directory. No file collisions. This enables maximum parallelism without coordination overhead.

**3. Findings go to disk, not issues.** Write findings as markdown files in the audit directory. Git history is the audit trail. GitHub issues are for tracking remediation work, not findings.

**4. Fail-closed mentality.** When uncertain, flag it. A false positive finding is cheap; a missed vulnerability is expensive.

**5. Session-based execution.** Each stream is broken into 3-7 sessions, each a coherent audit unit with specific files, line ranges, and a security checklist. Sessions are the unit of parallelism.

## Lessons Learned (Pre-1.0 Audit)

From the February 2026 defradb.rs audit (354 findings, 7 streams, ~50 sessions):

- **Parallel agent waves work.** Running one session per stream simultaneously was efficient and produced no file conflicts.
- **Recon is essential.** Agents that skip recon produce shallow findings. The 10-minute recon phase pays for itself many times over.
- **Triage is a separate phase.** Don't try to prioritize during deep-dive. Write everything down, then triage as a batch operation.
- **Verification catches regressions.** Squash merges during remediation can silently revert fixes. Always re-audit after remediation.
- **Cross-stream themes emerge in triage.** Individual stream findings often connect — a missing validation in input handling compounds with a missing access check in the query layer.
