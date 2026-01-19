//! Identity types for ACP.
//!
//! This module provides the `Identity` enum which represents the identity
//! context for ACP operations. Using an explicit enum is clearer than
//! `Option<Did>` and prevents confusion about semantics.

use identity::Did;

/// Identity context for ACP operations.
///
/// This enum explicitly represents the two possible identity states:
/// - Anonymous: No identity provided (requests without authentication)
/// - Authenticated: A verified DID identity
///
/// Using this enum instead of `Option<Did>` provides clearer semantics:
/// - `Identity::Anonymous` clearly indicates no identity (not "missing" identity)
/// - `Identity::Authenticated(did)` clearly indicates a verified identity
///
/// # Access Control Behavior
///
/// - `Anonymous`: Can access unregistered (public) documents only
/// - `Authenticated`: Can access owned documents and documents with granted relations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    /// No identity provided (anonymous/unauthenticated request).
    Anonymous,

    /// An authenticated identity with a verified DID.
    Authenticated(Did),
}

impl Identity {
    /// Create an anonymous identity.
    pub fn anonymous() -> Self {
        Self::Anonymous
    }

    /// Create an authenticated identity.
    pub fn authenticated(did: Did) -> Self {
        Self::Authenticated(did)
    }

    /// Check if this is an anonymous identity.
    pub fn is_anonymous(&self) -> bool {
        matches!(self, Self::Anonymous)
    }

    /// Check if this is an authenticated identity.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated(_))
    }

    /// Get the DID if authenticated, None if anonymous.
    pub fn did(&self) -> Option<&Did> {
        match self {
            Self::Anonymous => None,
            Self::Authenticated(did) => Some(did),
        }
    }

    /// Convert to Option<&Did> for compatibility with existing APIs.
    pub fn as_option(&self) -> Option<&Did> {
        self.did()
    }
}

impl From<Did> for Identity {
    fn from(did: Did) -> Self {
        Self::Authenticated(did)
    }
}

impl From<Option<Did>> for Identity {
    fn from(opt: Option<Did>) -> Self {
        match opt {
            Some(did) => Self::Authenticated(did),
            None => Self::Anonymous,
        }
    }
}

impl<'a> From<Option<&'a Did>> for Identity {
    fn from(opt: Option<&'a Did>) -> Self {
        match opt {
            Some(did) => Self::Authenticated(did.clone()),
            None => Self::Anonymous,
        }
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => write!(f, "anonymous"),
            Self::Authenticated(did) => write!(f, "{}", did),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    #[test]
    fn test_identity_anonymous() {
        let id = Identity::anonymous();
        assert!(id.is_anonymous());
        assert!(!id.is_authenticated());
        assert!(id.did().is_none());
        assert_eq!(format!("{}", id), "anonymous");
    }

    #[test]
    fn test_identity_authenticated() {
        let did = test_did();
        let id = Identity::authenticated(did.clone());
        assert!(!id.is_anonymous());
        assert!(id.is_authenticated());
        assert_eq!(id.did(), Some(&did));
        assert!(format!("{}", id).contains("did:key:"));
    }

    #[test]
    fn test_identity_from_did() {
        let did = test_did();
        let id: Identity = did.clone().into();
        assert!(id.is_authenticated());
        assert_eq!(id.did(), Some(&did));
    }

    #[test]
    fn test_identity_from_option_did() {
        let did = test_did();

        let id1: Identity = Some(did.clone()).into();
        assert!(id1.is_authenticated());

        let none_did: Option<Did> = None;
        let id2: Identity = none_did.into();
        assert!(id2.is_anonymous());
    }

    #[test]
    fn test_identity_from_option_ref_did() {
        let did = test_did();

        let id1: Identity = Some(&did).into();
        assert!(id1.is_authenticated());

        let id2: Identity = (None as Option<&Did>).into();
        assert!(id2.is_anonymous());
    }

    #[test]
    fn test_identity_as_option() {
        let did = test_did();

        let anon = Identity::anonymous();
        assert_eq!(anon.as_option(), None);

        let auth = Identity::authenticated(did.clone());
        assert_eq!(auth.as_option(), Some(&did));
    }

    #[test]
    fn test_identity_equality() {
        let did1 = test_did();
        let did2 = test_did();

        assert_eq!(Identity::Anonymous, Identity::Anonymous);
        assert_eq!(
            Identity::Authenticated(did1.clone()),
            Identity::Authenticated(did2)
        );
        assert_ne!(Identity::Anonymous, Identity::Authenticated(did1));
    }
}
