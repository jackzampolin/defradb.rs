# Phase 1: Reconnaissance

Parallel recon across all streams. Each agent maps one stream's attack surface without writing findings yet.

## Execution

Launch N parallel Explore agents (one per stream) in a single message:

```
Task(subagent_type="Explore", model="haiku", prompt="<recon prompt>")  // x N
```

Use haiku for recon — it's fast and this is mapping, not deep analysis.

## Recon Agent Prompt Template

```
You are performing security reconnaissance on audit stream XX: {stream_name}
in the project at {project_root}.

Scope: {scope_description}
Modules to examine: {module_list}

Tasks:
1. Map each module: LOC (src vs test), public API surface, unsafe blocks
2. Identify red flags: hardcoded secrets, missing validation, error suppression,
   unwrap() on untrusted input, silent error swallowing
3. Assess attack surface: untrusted input paths, trust boundaries, network-facing code
4. Note: algorithms used, crypto libraries, external dependencies
5. Categorize each area: RED FLAG / NEEDS DEEP DIVE / GREEN

Return structured summary:
- Surface Area (LOC, file counts, test coverage ratio)
- Implementation Details (algorithms, libraries, patterns)
- Red Flags (immediate concerns)
- Areas for Deep Dive (what needs dedicated sessions)
- Green Areas (areas that look sound and why)

Do NOT edit files. Research only.
```

## What Recon Produces

Each recon agent returns a structured summary. Use these to update the stream plan files:

```markdown
## Recon Findings

### Surface Area
- Total LOC: X (Y src, Z test)
- Files: N
- Test coverage ratio: X:Y (src:test)

### Red Flags
- [file:line] Description of concern

### Areas for Deep Dive
1. Component A — reason for concern
2. Component B — reason for concern

### Green Areas
- Component C — sound because [reason]
```

## Time Budget

~10 minutes wall clock. All agents run in parallel. Review results before proceeding to Phase 2.

## Common Pitfalls

- **Agent finds nothing**: Scope may be too narrow, or the module is genuinely clean. Verify manually before skipping.
- **Agent finds everything**: Scope may be too broad. Consider splitting the stream.
- **False red flags on intentional patterns**: Some "red flags" are intentional design choices. Note but don't dismiss — verify in deep-dive.
