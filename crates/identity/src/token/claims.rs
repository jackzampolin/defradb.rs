//! JWT claims structure for identity tokens.

use serde::{Deserialize, Serialize};

/// JWT claims structure for identity tokens.
///
/// Contains standard JWT claims plus custom claims for identity-specific data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityClaims {
    /// Subject: hex-encoded public key
    pub sub: String,

    /// Issuer: DID of the identity
    pub iss: String,

    /// Expiration time (Unix timestamp)
    pub exp: u64,

    /// Not before time (Unix timestamp)
    pub nbf: u64,

    /// Issued at time (Unix timestamp)
    pub iat: u64,

    /// Audience (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<Vec<String>>,

    /// Authorized account (optional)
    #[serde(rename = "authorized_account", skip_serializing_if = "Option::is_none")]
    pub authorized_account: Option<String>,

    /// Key type (ed25519 or secp256k1)
    pub key_type: String,
}
