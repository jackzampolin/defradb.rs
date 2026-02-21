# Workflow Details & Templates

## Plan File Template

Each stream's plan file (`audit/XX-stream-name.md`):

```markdown
# Audit Stream XX: Stream Name

## Scope
What this stream covers and why it matters.

## Key Questions
Security questions this stream must answer.

## Crates of Interest
Which crates/modules are in scope.

## Recon Findings
(Filled in Phase 2 by recon agent)

### Surface Area
- LOC counts, file counts, test coverage

### Implementation Details
- Algorithms, libraries, patterns used

### Red Flags
- Immediate concerns found during recon

### Areas for Deep Dive
- What needs dedicated sessions

## Estimated Scope
**SIZE: N sessions**

### Session 1: Title (PRIORITY)
| File | Lines | Focus |
|------|-------|-------|
| `path/to/file.rs` | line range | what to look for |

**Checklist**: Key security checks
**Status**: PENDING / COMPLETE — N findings. See `XX-findings/NN-summary.md`.
```

## Agent Prompt Templates

### Recon Agent

```
You are performing security reconnaissance on audit stream XX: {stream_name}
in the project at {project_root}.

Scope: {scope_description}
Crates to examine: {crate_list}

Tasks:
1. Map each crate: LOC (src vs test), public API, unsafe blocks
2. Identify red flags: hardcoded secrets, missing validation, error suppression
3. Assess attack surface: untrusted input paths, trust boundaries
4. Categorize: RED FLAG / DEEP DIVE / GREEN

Return structured summary with: Surface Area, Implementation Details,
Red Flags, Areas for Deep Dive, Green Areas. Do NOT edit files.
```

### Planning Agent

```
You are planning audit sessions for stream XX: {stream_name}
in the project at {project_root}.

Read the plan file at: audit/XX-stream-name.md

Using recon findings, create a session breakdown:
1. T-shirt size (SMALL 2-3 / MEDIUM 3-5 / LARGE 5-7 sessions)
2. Numbered sessions, each a coherent audit unit
3. Per session: title, priority, exact files/lines, security checklist (5-10 checks)
4. Order by priority (CRITICAL first)

Write session breakdown into the plan file. Do NOT create findings yet.
```

### Execution Agent (Session)

```
You are conducting a security audit of {component} in the project
at {project_root}. Session {N} of {total} in {stream_name}.

Objective: {session_objective}

Files to audit (read ALL in full):
{file_table}

Cross-reference files:
{cross_reference_list}

Security checklist:
{numbered_checklist}

Output: Write findings to `audit/XX-stream-findings/##-short-name.md`.
Use the finding template format (severity, category, status, summary,
affected files, details with code, remediation, test gap).

If FINAL session: also create `audit/XX-stream-findings/STREAM-SUMMARY.md`.

Grep commands to run:
{grep_commands}
```

## Finding Template

```markdown
# Finding: Short Descriptive Title

| Field | Value |
|-------|-------|
| **Severity** | CRITICAL / HIGH / MEDIUM / LOW / INFO |
| **Category** | e.g., Authentication Bypass, Memory Safety, DoS |
| **Status** | CONFIRMED / SUSPECTED / INFORMATIONAL / GREEN |
| **Stream** | XX — Stream Name |
| **Session** | N |

## Summary

One paragraph: issue, impact, exploitability.

## Affected Files

| File | Lines | Issue |
|------|-------|-------|
| `path/to/file.rs` | 42-58 | Description |

## Details

Technical analysis with code snippets.

## Remediation

Specific, actionable fix.

## Test Gap

What test should exist to catch this.
```

## Stream Summary Template

```markdown
# Stream XX: Stream Name — Summary

## Sessions Completed

| Session | Title | Findings |
|---------|-------|----------|
| 1 | ... | N findings (X HIGH, Y MEDIUM, ...) |

## Findings by Severity

| Severity | Count | Key Issues |
|----------|-------|------------|
| CRITICAL | N | Brief list |
| HIGH | N | ... |
| MEDIUM | N | ... |
| LOW | N | ... |
| INFO | N | ... |
| GREEN | N | Areas confirmed safe |

## Overall Assessment

Security posture summary for this subsystem.

## Top Recommendations

1. Most important fix
2. Second most important
```

## Master Summary Template

```markdown
# Security Audit Summary

## Scope
Codebase, date, streams, sessions, total findings.

## Findings by Severity (All Streams)
| Severity | Count |
|----------|-------|

## Top 10 Critical Findings
| # | Finding | Severity | Stream | File |
|---|---------|----------|--------|------|

## Cross-Cutting Themes
Patterns across multiple streams.

## Prioritized Remediation Roadmap

### Immediate (This Sprint)
CRITICAL and HIGH findings.

### Short-Term (Next 2-4 Weeks)
MEDIUM findings with exploit potential.

### Ongoing
LOW/INFO, hardening, test gaps.

## Stream Summaries
| Stream | Sessions | Findings | Top Issue |
|--------|----------|----------|-----------|
```
