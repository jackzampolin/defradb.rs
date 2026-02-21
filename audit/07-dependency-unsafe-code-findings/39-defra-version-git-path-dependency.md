# defra-version build.rs Uses PATH-Relative git Command

**Severity:** Low
**Category:** Build script — External command invocation
**Status:** Yellow — standard pattern but worth noting

## Summary

The `crates/defra-version/build.rs` script invokes `Command::new("git")` twice to embed the commit hash and build date at compile time. The `git` binary is resolved via PATH, which means a malicious binary named `git` placed earlier in PATH could be executed during `cargo build` with the build user's full privileges.

## Affected Files

- `crates/defra-version/build.rs:5` — `Command::new("git").args(["rev-parse", "HEAD"])`
- `crates/defra-version/build.rs:14` — `Command::new("git").args(["show", "-s", "--date=short", "--format=%cd", "HEAD"])`

## Details

### What the Script Does

1. Runs `git rev-parse HEAD` to get the current commit hash
2. Runs `git show -s --date=short --format=%cd HEAD` to get the commit date
3. Sets `GIT_COMMIT` and `BUILD_DATE` as compile-time environment variables
4. On failure, falls back to `"unknown"` (graceful degradation)

### Risk Assessment

**PATH hijacking is a theoretical supply chain risk.** If an attacker can place a malicious `git` binary in a directory that appears before `/usr/bin` in PATH, they could:
- Exfiltrate environment variables (including secrets)
- Inject arbitrary code into the build output
- Modify the build environment

**Mitigating factors:**
1. This is a ubiquitous pattern in Rust projects (thousands of crates do this)
2. The `git` command is read-only — it doesn't modify the repository
3. The output is only used for version display strings, not security-critical logic
4. Build scripts already run with full user privileges regardless
5. The fallback to "unknown" means builds succeed even without git

### The Output is Not Security-Critical

The `GIT_COMMIT` and `BUILD_DATE` values are used in `crates/defra-version/src/lib.rs` for version display only. They do not affect cryptographic operations, authentication, or access control decisions.

## Remediation

No action needed. This is standard practice and the risk is theoretical. If hardening is desired, the script could use an absolute path to git, but this reduces portability.

## Exploitability

Not practically exploitable without prior system compromise (PATH manipulation requires write access to system directories or user profile).
