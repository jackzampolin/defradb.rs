# Phase 2: Session Planning

Break each stream into concrete audit sessions with specific files, line ranges, and security checklists.

## Execution

Launch N parallel Plan agents (one per stream):

```
Task(subagent_type="Plan", model="haiku", prompt="<planning prompt>")  // x N
```

## Planning Agent Prompt Template

```
You are planning audit sessions for stream XX: {stream_name}
in the project at {project_root}.

Read the plan file at: audit/XX-stream-name.md (includes recon findings).

Using recon findings, create a session breakdown:

1. T-shirt size the stream:
   - SMALL: 2-3 sessions
   - MEDIUM: 3-5 sessions
   - LARGE: 5-7 sessions
   (Never exceed 7 sessions per stream)

2. For each session:
   - Title and priority (CRITICAL / HIGH / MEDIUM)
   - Specific files and line ranges to audit
   - Security checklist (5-10 concrete checks)
   - Cross-reference files (related code in other crates)
   - Grep commands to run for pattern detection

3. Order sessions by priority (CRITICAL first)

4. Final session always includes:
   - Integration test coverage assessment
   - Cross-cutting concerns check
   - STREAM-SUMMARY.md creation

Write the session breakdown into the plan file. Do NOT create findings yet.
```

## Session Plan Format

Each session in the plan file should look like:

```markdown
### Session N: Title (PRIORITY)

**Objective**: One sentence describing what this session investigates.

| File | Lines | Focus |
|------|-------|-------|
| `crates/foo/src/bar.rs` | 1-200 | Input validation on untrusted data |
| `crates/foo/src/baz.rs` | 45-120 | Error handling in auth path |

**Cross-references**:
- `crates/other/src/related.rs` — uses the same trust boundary

**Checklist**:
- [ ] Are inputs validated before processing?
- [ ] Are errors propagated, not swallowed?
- [ ] Is authentication checked before authorization?
- [ ] Are cryptographic operations using constant-time comparisons?
- [ ] Are resources bounded (no unbounded allocations from untrusted input)?

**Grep patterns**:
```bash
rg "unwrap\(\)" crates/foo/src/
rg "todo!\|unimplemented!" crates/foo/src/
rg "unsafe" crates/foo/src/
```

**Status**: PENDING
```

## Session Sizing Guidelines

| Session Size | Lines of Security-Relevant Code | Typical Duration |
|-------------|--------------------------------|-----------------|
| Light | <500 lines | 5-10 min agent time |
| Normal | 500-1500 lines | 10-20 min agent time |
| Heavy | 1500-3000 lines | 20-30 min agent time |

Sessions over 3000 lines of security-relevant code should be split. Agent quality degrades with context overload.

## Time Budget

~10 minutes wall clock. All agents run in parallel. Review session plans before proceeding to Phase 3.
