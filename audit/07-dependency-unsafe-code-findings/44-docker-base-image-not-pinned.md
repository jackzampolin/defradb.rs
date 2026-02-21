# Docker Base Images Not Digest-Pinned

**Severity:** Low
**Category:** Supply chain — Container image integrity
**Status:** Yellow — standard pattern but imprecise

## Summary

Both Dockerfiles use tag-based image references (`rust:1.93-bookworm`, `debian:bookworm-slim`) without pinning to specific image digests. Tags are mutable — they can be updated to point to different image layers. A compromised Docker Hub account or registry could push a modified image under an existing tag.

## Affected Files

- `Dockerfile:1` — `FROM rust:1.93-bookworm AS builder`
- `Dockerfile:7` — `FROM debian:bookworm-slim`
- `Dockerfile.release:1` — `FROM debian:bookworm-slim`

## Details

### Current State

| Dockerfile | Base Image | Pinning |
|---|---|---|
| `Dockerfile` (dev) | `rust:1.93-bookworm` | Tag only |
| `Dockerfile` (runtime) | `debian:bookworm-slim` | Tag only |
| `Dockerfile.release` | `debian:bookworm-slim` | Tag only |

### Risk

Docker image tags are mutable pointers. The `debian:bookworm-slim` tag is updated regularly with security patches (which is good), but this also means:
1. Builds at different times produce different images (non-reproducible)
2. A compromised tag could inject malicious packages

### Mitigating Factors

1. `Dockerfile.release` is used in CI only — it receives pre-built binaries from the build matrix, so the Rust compilation is not affected by the base image.
2. The runtime images install only `ca-certificates` and `libssl3` — minimal attack surface.
3. Docker Hub has content trust (Notary) for official images.
4. The APT packages are also not version-pinned (`apt-get install -y libssl3`), but this is standard practice for Debian-based images.

### Positive: Minimal Runtime Image

The runtime stage uses `debian:bookworm-slim` with only two packages installed (`ca-certificates libssl3`), then runs `rm -rf /var/lib/apt/lists/*` to clean up. This is a good minimal base.

## Remediation

Pin base images to digest for reproducible builds:

```dockerfile
FROM rust:1.93-bookworm@sha256:<digest> AS builder
FROM debian:bookworm-slim@sha256:<digest>
```

Use Dependabot or Renovate to automatically update digest pins when new images are published.

## Exploitability

Requires compromising Docker Hub official image repositories. Very low probability given Docker's content trust infrastructure. The practical risk is build non-determinism rather than active exploitation.
