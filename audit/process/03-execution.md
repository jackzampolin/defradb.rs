# Phase 3: Deep-Dive Execution

The main event. Execute audit sessions in parallel waves, writing findings to disk.

## Wave Execution Pattern

Run one session from every stream simultaneously per wave:

```
Wave 1: Stream1.Session1 | Stream2.Session1 | Stream3.Session1 | ...
Wave 2: Stream1.Session2 | Stream2.Session2 | Stream3.Session2 | ...
Wave 3: Stream1.Session3 | Stream2.Session3 | Stream3.Session3 | ...
...until all sessions exhausted (streams with fewer sessions skip later waves)
```

Launch each wave as a single message with N parallel Task calls:

```
Task(subagent_type="general-purpose", prompt="<session prompt>")  // x N
```

Wait for all agents in a wave to complete before launching the next wave. This keeps output manageable and lets you spot-check findings between waves.

## Execution Agent Prompt Template

```
You are conducting a security audit of {component} in the project
at {project_root}. This is Session {N} of {total} in the
{stream_name} audit stream.

Objective: {session_objective}

Files to audit (read ALL of these in full before writing findings):
| File | Lines | Focus |
|------|-------|-------|
{file_table}

Cross-reference files:
{cross_reference_list}

Security checklist:
{numbered_checklist}

Grep commands to run:
{grep_commands}

IMPORTANT:
- Read every file listed above before making judgments
- For each finding, include the specific code that is problematic
- GREEN findings (areas confirmed secure) are valuable — write them too
- Write each finding to: audit/XX-stream-findings/##-short-name.md
- Use sequential numbering (00, 01, 02, ...) for finding files
- If this is the FINAL session for this stream, also create:
  audit/XX-stream-findings/STREAM-SUMMARY.md

Finding file format:
# Finding: Short Descriptive Title

| Field | Value |
|-------|-------|
| **Severity** | CRITICAL / HIGH / MEDIUM / LOW / INFO / GREEN |
| **Category** | e.g., Authentication Bypass, Memory Safety, DoS |
| **Status** | CONFIRMED / SUSPECTED / INFORMATIONAL / GREEN |
| **Stream** | XX — Stream Name |
| **Session** | N |

## Summary
One paragraph: what, impact, exploitability.

## Affected Files
| File | Lines | Issue |
|------|-------|-------|
| path | range | description |

## Details
Technical analysis with code snippets.

## Remediation
Specific, actionable fix.

## Test Gap
What test should exist to catch this.
```

## Between Waves

After each wave completes:

1. **Spot-check**: Read 2-3 findings from different streams. Are they substantive or shallow?
2. **Adjust**: If a stream is producing shallow findings, refine the session prompt for the next wave.
3. **Commit**: `git add audit/ && git commit -m "audit: wave N complete"` — preserves the audit trail.
4. **Update plan files**: Mark completed sessions with finding counts.

## Handling Agent Issues

| Problem | Fix |
|---------|-----|
| Agent produces no findings | Session scope may be too narrow. Expand file list or merge with next session. |
| Agent produces only GREEN | The subsystem may genuinely be secure. Verify one GREEN finding manually. |
| Agent hallucinates code | Always verify findings reference real file paths and line numbers. |
| Agent runs out of context | Session is too large. Split it and re-run. |
| Agent duplicates another stream's finding | Expected for cross-cutting issues. Triage phase deduplicates. |

## Time Budget

2-4 hours wall clock depending on stream count and session depth. Each wave takes 15-30 minutes.
