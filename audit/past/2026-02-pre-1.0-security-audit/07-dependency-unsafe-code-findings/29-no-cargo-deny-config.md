# No cargo-deny Configuration

**Severity:** Medium
**Category:** Supply chain — Missing policy enforcement
**Status:** No `deny.toml` found in repository

## Summary

The project has no `deny.toml` file, meaning `cargo deny` runs with default settings. This means there is no automated enforcement of:
- License allowlists/denylists
- Banned crates or versions
- Advisory severity thresholds
- Source restrictions (e.g., only crates.io)
- Duplicate crate policies

## Details

Without a `deny.toml`:
1. **License compliance** is not checked in CI — GPL dependencies could be inadvertently introduced
2. **Known advisories** are not blocked from entering the dependency tree
3. **Banned crates** cannot be specified (e.g., banning serde_cbor after migration)
4. **Source restrictions** aren't enforced — any new git dependency can be added without policy review

## Current State

Running `cargo deny check advisories` with defaults found 14+ issues (3 vulnerabilities, 11+ warnings for unmaintained crates). These would be caught and could block CI if configured.

## Remediation

Create a `deny.toml` at the workspace root with:

```toml
[advisories]
vulnerability = "deny"
unmaintained = "warn"
unsound = "deny"
yanked = "deny"

[licenses]
allow = ["MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Unicode-3.0"]
confidence-threshold = 0.8

[bans]
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-git = ["https://github.com/sourcenetwork/beetle"]
```

Add `cargo deny check` to CI pipeline.
