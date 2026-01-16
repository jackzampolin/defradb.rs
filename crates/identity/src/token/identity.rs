//! Token identity type extracted from JWT tokens.

use crypto::keys::PublicKey;

use crate::did::Did;
use crate::key_type::IdentityKeyType;
use crate::{Identity, Result};

use super::claims::IdentityClaims;

/// An identity extracted from a JWT token.
///
/// TokenIdentity only has access to the public key and cannot sign new tokens
/// since it doesn't have the private key.
pub struct TokenIdentity {
    pub(crate) public_key: Box<dyn PublicKey>,
    pub(crate) did: Did,
    pub(crate) bearer_token: String,
    pub(crate) authorized_account: Option<String>,
    pub(crate) key_type: IdentityKeyType,
    pub(crate) claims: IdentityClaims,
}

impl TokenIdentity {
    /// Returns the bearer token string.
    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }

    /// Returns the authorized account if present.
    pub fn authorized_account(&self) -> Option<&str> {
        self.authorized_account.as_deref()
    }

    /// Returns the key type of this identity.
    pub fn key_type(&self) -> IdentityKeyType {
        self.key_type
    }

    /// Returns the claims from the token.
    pub fn claims(&self) -> &IdentityClaims {
        &self.claims
    }
}

impl Identity for TokenIdentity {
    fn pub_key(&self) -> &dyn PublicKey {
        self.public_key.as_ref()
    }

    fn did(&self) -> Result<Did> {
        Ok(self.did.clone())
    }
}

impl std::fmt::Debug for TokenIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenIdentity")
            .field("did", &self.did)
            .field("key_type", &self.key_type)
            .field("authorized_account", &self.authorized_account)
            .finish_non_exhaustive()
    }
}
