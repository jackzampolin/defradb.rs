# CI Pipeline Missing cargo audit / cargo deny Steps

**Severity:** Medium
**Category:** Supply chain — Automated vulnerability detection
**Status:** Yellow — no automated dependency scanning in CI

## Summary

Neither the CI workflow (`.github/workflows/ci.yml`) nor the release workflow (`.github/workflows/release.yml`) runs `cargo audit` or `cargo deny check`. Known CVEs in dependencies are not detected until a human manually runs these tools. This was partially noted in finding 29 (no deny.toml), but the CI gap deserves its own finding.

## Affected Files

- `.github/workflows/ci.yml` — 3 jobs: fmt, clippy, test. No audit step.
- `.github/workflows/release.yml` — build, package, release. No audit step.

## Details

### Current CI Pipeline

```
CI Pipeline:
  fmt      → cargo fmt --all -- --check
  clippy   → cargo clippy --all -- -D warnings
  test     → cargo test --workspace

Release Pipeline:
  build    → cargo build --release for each target matrix
  package  → tar + upload artifacts
  docker   → docker build + push
  release  → GitHub Release with artifacts
```

**Missing from both:**
- `cargo audit` — checks Cargo.lock against the RustSec advisory database
- `cargo deny check` — comprehensive policy enforcement (advisories, licenses, sources, bans)
- `cargo vet` — supply chain audit of first-party proc macros and dependencies

### Impact

The dependency tree currently has (as of Session 3):
- 3 CVEs in wasmtime
- 1 CVE in ring
- 1 unsoundness advisory in lru
- 7 unmaintained crate warnings

None of these would be caught by CI. New advisories published after Session 3 would also go undetected.

### What a Good CI Pipeline Includes

```yaml
  audit:
    name: Security Audit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo install cargo-deny
      - run: cargo deny check advisories
      - run: cargo deny check licenses
      - run: cargo deny check sources
```

### Rust Cache Integrity

The CI uses `Swatinem/rust-cache@v2` for dependency caching. This caches the `target/` directory and Cargo registry. The cache key is based on `Cargo.lock` hash, which is good. However, without `cargo deny check sources`, a compromised cache could contain modified dependency sources without detection.

## Remediation

1. Create `deny.toml` (per finding 29's recommendation)
2. Add an audit job to CI:

```yaml
  audit:
    name: Audit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: rustsec/audit-check@v2
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
```

3. Consider running `cargo deny check` as a separate job or combining with the audit step.

## Exploitability

Not directly exploitable. This is a detection gap — the absence of scanning does not create a vulnerability, but it means existing vulnerabilities are not surfaced automatically.
