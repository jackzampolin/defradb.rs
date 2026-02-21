# Finding: NAC Enable Endpoint Has No Authentication Gate

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Privilege Escalation
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The `POST /api/v0/acp/node/enable` endpoint accepts any request without identity verification. An attacker who reaches the endpoint before the legitimate administrator can set themselves as the NAC owner, gaining permanent control over all node operations. The owner identity cannot be changed without purging NAC entirely.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/http/src/handlers/nac.rs` | 55-74 | `enable()` — no identity extraction or verification |
| `crates/acp/src/nac/node_acp/lifecycle.rs` | 18-57 | `NodeACP::enable()` — only checks if already enabled |

## Details

### The Vulnerability

```rust
// crates/http/src/handlers/nac.rs:55-74
pub async fn enable(
    State(state): State<AppState>,
    Json(body): Json<EnableNacRequest>,  // No ExtractIdentity parameter
) -> Result<impl IntoResponse, HttpError> {
    let nac = state.require_nac()?;
    let owner = identity::Did::new(&body.owner_did)
        .map_err(|e| HttpError::BadRequest(format!("invalid OwnerDID: {}", e)))?;
    nac.enable(&owner).await.map_err(|e| { ... })?;
    Ok(axum::http::StatusCode::OK.into_response())
}
```

Key observations:
1. No `ExtractIdentity` parameter — no JWT/identity verification
2. No `require_permission()` call
3. The `OwnerDID` in the request body is accepted as-is — the caller chooses who becomes owner
4. Once enabled, the owner is stored permanently and cannot be changed without `purge()`

### Why This Is By Design (But Still Risky)

NAC can't check permissions before it's enabled — there's no owner to check against. This is a bootstrap problem. However, the current design allows any network-reachable client to:

1. Enable NAC with an attacker-controlled DID
2. The attacker becomes the permanent owner
3. All subsequent operations require the attacker's permission
4. The legitimate admin is locked out

### Attack Scenario

```bash
# Node starts with --node-acp-enable but before admin configures it:
curl -X POST http://target:9181/api/v0/acp/node/enable \
  -H 'Content-Type: application/json' \
  -d '{"OwnerDID": "did:key:z6MkATTACKER..."}'
# → 200 OK
# Attacker is now the permanent NAC owner
```

### The Owner Is Permanent

```rust
// crates/acp/src/nac/node_acp/lifecycle.rs:18-21
pub async fn enable(&self, owner: &Did) -> Result<()> {
    let status = *self.status.read().await;
    if status == NacStatus::Enabled {
        return Err(Error::InvalidPolicy("NAC is already enabled".into()));
    }
```

Once enabled, re-calling enable fails. The owner can only be changed by calling `purge()`, which requires admin permission — creating a deadlock if an attacker is the owner.

### Severity Rationale

MEDIUM because:
1. Requires network access to the node during the bootstrap window
2. The window is typically brief (between node start and admin configuration)
3. But in automated deployments, this window may be predictable
4. Impact is total node takeover — all operations gated by attacker's identity

## Remediation

### Option A: Require CLI-only enable

Only allow enabling NAC via the CLI (local access), not via HTTP API. This ensures physical/SSH access is required.

### Option B: Pre-shared secret

Require a pre-configured secret (from config file or environment variable) in the enable request to prove the caller is the intended administrator.

### Option C: Startup-only enable

Only allow NAC enable during the first N seconds after node startup, or require a special startup flag that enables NAC with a specified owner DID before the HTTP server starts.
