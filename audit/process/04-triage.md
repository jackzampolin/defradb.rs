# Phase 4: Triage & Prioritization

Classify findings by severity, identify cross-stream themes, and build the remediation roadmap.

## Step 1: Characterization (Parallel Agents)

Launch N parallel agents (one per stream) to characterize their stream's findings:

```
Task(subagent_type="general-purpose", model="haiku", prompt="
Read all findings in audit/XX-stream-findings/.
For each finding, verify:
1. Is the severity rating accurate? (check code against claim)
2. Is the finding a duplicate of another finding in this stream?
3. What is the effort to fix? (1-session / 2-3 sessions / 4+ sessions)
4. What is the blast radius if exploited?

Write a triage summary to audit/XX-stream-findings/TRIAGE.md with a table:
| Finding | Severity | Effort | Blast Radius | Notes |
")
```

## Step 2: Cross-Stream Theme Analysis

After characterization, run a single agent across all TRIAGE.md files:

```
Read all audit/XX-stream-findings/TRIAGE.md files.
Identify cross-stream themes:
- Same root cause appearing in multiple streams
- Compounding vulnerabilities (A in stream X + B in stream Y = worse)
- Systemic patterns (e.g., "error suppression is pervasive")

Write to audit/AUDIT-TRIAGE.md.
```

## Step 3: Severity Sorting

Organize findings into severity buckets:

| Severity | Criteria | Action |
|----------|----------|--------|
| **CRITICAL** | Exploitable now, data loss or auth bypass | Fix before release |
| **HIGH** | Exploitable with some effort, security degradation | Fix before release |
| **MEDIUM** | Defense-in-depth gap, requires specific conditions | Fix if time allows |
| **LOW** | Hardening opportunity, unlikely to be exploited | Post-release backlog |
| **INFO** | Observation, documentation gap, or design note | Track but no fix needed |
| **GREEN** | Area confirmed secure | No action, provides confidence |

## Step 4: Remediation Roadmap

Create `audit/REMEDIATION_ROADMAP.md`:

```markdown
# Remediation Roadmap

## Immediate (This Sprint)
CRITICAL and HIGH findings. Estimated sessions per fix.

## Short-Term (Next 2-4 Weeks)
MEDIUM findings with exploit potential.

## Post-Release Backlog
LOW/INFO findings, hardening, test gaps.

## Ongoing Work (Tracked in Issues)
Findings that overlap with existing feature work.
```

## Step 5: Create Tracking Issues

For CRITICAL and HIGH findings, create GitHub issues. Link to the finding file. Don't create issues for MEDIUM/LOW — those live in the roadmap until prioritized.

## Time Budget

~30 minutes. Characterization agents run in parallel. Theme analysis and roadmap are sequential.
