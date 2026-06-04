//! Actor-token verification for the P2P management channel.

use identity::{from_token, verify_auth_token, Did, Identity};

/// Verify an actor JWT and return its DID, requiring `aud == expected_audience`.
///
/// `expected_audience` is the serving node's own peer-id string, so a token
/// minted for node X cannot be replayed against node Y.
pub fn verify_actor_token(token: &[u8], expected_audience: &str) -> Result<Did, String> {
    let ti = from_token(token).map_err(|e| format!("invalid actor token: {e}"))?;
    verify_auth_token(&ti, expected_audience).map_err(|e| format!("actor token rejected: {e}"))?;
    ti.did().map_err(|e| format!("token has no DID: {e}"))
}

#[cfg(test)]
pub(crate) fn mint_token_for(audience: &str) -> (Vec<u8>, identity::Did) {
    use identity::RawIdentity;

    let private_key = crypto::generate_ed25519().unwrap();
    let id = RawIdentity::from_private_key(private_key).unwrap();
    let token = identity::new_token(
        &id,
        std::time::Duration::from_secs(300),
        Some(audience.to_string()),
        None,
    )
    .unwrap();
    let did = id.did().unwrap();
    (token, did)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_wrong_audience() {
        let (token, _did) = mint_token_for("12D3KooW-OTHER");
        assert!(verify_actor_token(&token, "12D3KooW-THIS").is_err());
    }

    #[test]
    fn accepts_matching_audience_returns_did() {
        let (token, did) = mint_token_for("12D3KooW-THIS");
        assert_eq!(verify_actor_token(&token, "12D3KooW-THIS").unwrap(), did);
    }
}
