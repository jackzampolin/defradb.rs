# WASM Release Build Uses curl-pipe-sh for wasm-pack

**Severity:** Medium
**Category:** Supply chain — CI pipeline integrity
**Status:** Yellow — standard pattern but avoidable risk

## Summary

The release CI workflow installs wasm-pack by piping a remote shell script directly into `sh`: `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`. This is a classic supply chain attack vector. If the rustwasm.github.io domain is compromised, or DNS is poisoned, or the GitHub Pages deployment is hijacked, arbitrary code runs in the release build environment with access to the GITHUB_TOKEN and release artifacts.

## Affected Files

- `.github/workflows/release.yml:146` — `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`

## Details

### The Attack Surface

1. **DNS spoofing**: An attacker who can poison DNS for GitHub Actions runners could redirect the curl request to a malicious script.
2. **GitHub Pages compromise**: rustwasm.github.io is hosted on GitHub Pages. If the rustwasm/wasm-pack repository or its Pages deployment is compromised, the installer script is modified.
3. **TLS interception**: While curl uses HTTPS, corporate or cloud provider MITM proxies could theoretically intercept the request.
4. **Content modification**: The script is not hash-verified. A modified script that appears to install wasm-pack but also exfiltrates secrets would go undetected.

### What the CI Runner Has Access To

At this point in the release workflow, the runner has:
- `GITHUB_TOKEN` with `contents: write` and `packages: write` permissions
- Access to the checked-out source code
- Access to compiled artifacts
- Network access

A compromised wasm-pack installer could:
- Exfiltrate the GITHUB_TOKEN
- Modify compiled artifacts before packaging
- Inject malicious code into the WASM build output

### Mitigating Factors

- This only runs on tag pushes (`on: push: tags: ["v*"]`), not on every PR
- The wasm-pack project is maintained by the Rust WASM working group
- The installer URL uses HTTPS
- The WASM build is a separate job from the native binary builds

## Remediation

Replace curl-pipe-sh with a pinned GitHub Action or cargo install:

**Option A: Use cargo install with version pinning**
```yaml
- name: Install wasm-pack
  run: cargo install wasm-pack@0.13.1
```

**Option B: Use the official GitHub Action**
```yaml
- uses: nicolo-ribaudo/setup-wasm-pack@v1
  with:
    version: 'v0.13.1'
```

**Option C: Pin with hash verification**
```yaml
- name: Install wasm-pack
  run: |
    curl -L https://github.com/nicolo-ribaudo/setup-wasm-pack/releases/download/v0.13.1/wasm-pack-v0.13.1-x86_64-unknown-linux-musl.tar.gz -o wasm-pack.tar.gz
    echo "EXPECTED_HASH  wasm-pack.tar.gz" | sha256sum -c
    tar xzf wasm-pack.tar.gz
```

## Exploitability

Requires compromising the wasm-pack installer infrastructure or performing a network-level attack against GitHub Actions runners. Low probability but high impact — this is a known supply chain attack pattern (e.g., Codecov bash uploader compromise in 2021).
