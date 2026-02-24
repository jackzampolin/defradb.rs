# Phase 5: Remediation

Fix findings, commit with audit references, and track progress.

## Workflow

1. **Pick the highest-severity unfixed finding**
2. **Read the finding file** — it has affected files, code snippets, and suggested remediation
3. **Fix it** — implement the remediation (or a better approach if the suggestion is wrong)
4. **Write tests** — the finding's "Test Gap" section tells you what test should exist
5. **Commit** — reference the finding ID in the commit message
6. **Update the finding file** — mark status as FIXED with commit hash

## Commit Convention

```
fix(security): short description (XX-NN)

Detailed explanation of what was fixed and why.

Addresses audit finding XX-NN (stream-name/finding-slug).
```

Where `XX-NN` is the stream number and finding number (e.g., `02-19` for stream 02, finding 19).

## Batch Remediation

Some findings cluster — fixing one root cause resolves several findings. When this happens:

1. Fix the root cause in one commit
2. Reference all affected finding IDs in the commit message
3. Update all affected finding files

## What NOT to Do

- **Don't fix and verify in the same session.** Remediation and verification are separate phases. You'll miss regressions if you self-verify.
- **Don't squash-merge remediation PRs carelessly.** Squash merges from stale branches can silently revert files they didn't touch. Always verify file state after squash merges.
- **Don't close findings without a test.** A fix without a test is a regression waiting to happen.

## Progress Tracking

Update `REMEDIATION_ROADMAP.md` as fixes land:

```markdown
## Immediate (This Sprint)
- [x] 02-19: Block signature verification (commit 96d3c835)
- [x] 02-20: Block verify in merge path (commit 96d3c835)
- [ ] 03-21: DocSync access checks
```

## Time Budget

Varies. CRITICAL fixes are often 1-2 sessions each. The pre-1.0 audit remediation took roughly a week for 15-20 critical/high items.
