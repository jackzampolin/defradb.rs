---
name: defra-security-audit
description: |
  Security audit of the defradb.rs Rust codebase using parallel agent waves. Performs reconnaissance, builds session-level audit plans, and executes deep-dive sessions writing findings to disk. Each of the 7 audit streams writes to its own directory — no file collisions — enabling maximum parallelism. Use when starting a security audit, continuing an in-progress audit, re-auditing after remediation, or checking audit status. Triggers on "security audit", "audit the codebase", "re-audit", "start audit", "run security audit", or "/defra-security-audit".
---

# defradb.rs Security Audit

Comprehensive security audit executed in waves of parallel agents. Seven streams, ~35 sessions, all findings committed to git.

## Project

- **Rust repo**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs`
- **Audit output**: `audit/` directory in repo root
- **Each stream**: plan file (`audit/XX-name.md`) + findings dir (`audit/XX-name-findings/`)

## Audit Streams

| # | Stream | Key Crates | Sessions |
|---|--------|------------|----------|
| 01 | Cryptographic Inventory | crypto, identity/token, db/se | 5 |
| 02 | Access Control Policy | acp, db (DAC/NAC), query (PermissionFilter) | 5 |
| 03 | P2P Network Security | p2p, db/merge_handler | 5 |
| 04 | Identity & Key Management | identity, keyring, http (auth), cli | 5 |
| 05 | Input Validation | query (parsers), http (handlers), lens, cli | 4 |
| 06 | Data Integrity & CRDT | crdt, db/merge_handler, blockstore, document, storage | 6 |
| 07 | Dependency & Unsafe Code | ffi, storage (rocksdb), Cargo.toml, build.rs | 5 |

## Workflow

Four phases, each maximizing parallelism. See [references/workflow.md](references/workflow.md) for detailed agent prompts and templates.

### Phase 1: Stream Definition (sequential)

Create 7 plan files in `audit/`. Use the plan template from [references/workflow.md](references/workflow.md#plan-file-template).

### Phase 2: Reconnaissance (7 parallel agents)

Launch 7 Explore agents simultaneously. Each maps one stream's LOC, red flags, and attack surface. Update plan files with recon findings.

### Phase 3: Session Planning (7 parallel agents)

Launch 7 Plan agents simultaneously. Each t-shirt sizes one stream and breaks it into sessions with specific files, line ranges, and security checklists. Update plan files.

### Phase 4: Execution (parallel waves)

Execute in waves — each wave runs one session from every stream in parallel:

```
Wave 1: S1.1 | S2.1 | S3.1 | S4.1 | S5.1 | S6.1 | S7.1
Wave 2: S1.2 | S2.2 | S3.2 | S4.2 | S5.2 | S6.2 | S7.2
...until all sessions exhausted (streams with fewer sessions skip later waves)
```

Launch with 7 parallel Task calls per wave:

```
Task(subagent_type="general-purpose", prompt="<session prompt>")  // x7 in one message
```

Each agent writes findings directly to `audit/XX-findings/##-name.md`. No collisions.

Final session per stream creates `STREAM-SUMMARY.md`. After all waves: create `audit/AUDIT-SUMMARY.md`.

## Re-Audit After Remediation

1. Read `AUDIT-SUMMARY.md` and `STREAM-SUMMARY.md` files
2. Check git history for remediated findings
3. Re-run only affected sessions (same parallel wave pattern)
4. Update finding statuses: REMEDIATED / PARTIALLY FIXED / STILL OPEN
5. Regenerate summaries

## Status Check

Read each plan file's session statuses. Plan files are the source of truth for audit progress.
