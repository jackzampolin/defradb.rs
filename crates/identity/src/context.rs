//! Identity context for propagating identity through request handling.
//!
//! This module provides types and functions for carrying identity information
//! through the request handling pipeline, similar to Go's context-based approach.

use std::sync::Arc;

use crate::{FullIdentity, Identity, RawIdentity};

/// A container for identity that can be passed through request contexts.
///
/// `IdentityContext` holds an optional identity that can be:
/// - A full identity (with signing capability) for authenticated operations
/// - A public identity (read-only) for verification
/// - Empty for unauthenticated requests
///
/// # Example
///
/// ```rust
/// use identity::{Identity, IdentityContext, RawIdentity};
///
/// // Generate a new Ed25519 key for the example
/// let private_key = crypto::generate_ed25519().unwrap();
/// let identity = RawIdentity::from_private_key(private_key).unwrap();
///
/// // Create a context with the identity
/// let ctx = IdentityContext::with_full_identity(identity);
///
/// // Access the identity
/// if let Some(id) = ctx.identity() {
///     println!("Request from: {:?}", id.did());
/// }
/// ```
#[derive(Clone)]
pub struct IdentityContext {
    inner: Option<IdentityHolder>,
}

/// Internal holder for different identity types.
#[derive(Clone)]
enum IdentityHolder {
    /// Full identity with signing capability.
    Full(Arc<RawIdentity>),
}

impl IdentityContext {
    /// Creates an empty identity context (unauthenticated).
    pub fn empty() -> Self {
        Self { inner: None }
    }

    /// Creates a context with a full identity (has signing capability).
    pub fn with_full_identity(identity: RawIdentity) -> Self {
        Self {
            inner: Some(IdentityHolder::Full(Arc::new(identity))),
        }
    }

    /// Creates a context with a full identity from an Arc.
    pub fn with_full_identity_arc(identity: Arc<RawIdentity>) -> Self {
        Self {
            inner: Some(IdentityHolder::Full(identity)),
        }
    }

    /// Returns true if this context has an identity.
    pub fn has_identity(&self) -> bool {
        self.inner.is_some()
    }

    /// Returns true if this context has a full identity (with signing capability).
    pub fn has_full_identity(&self) -> bool {
        matches!(&self.inner, Some(IdentityHolder::Full(_)))
    }

    /// Returns the identity as a trait object, if present.
    pub fn identity(&self) -> Option<&dyn Identity> {
        match &self.inner {
            Some(IdentityHolder::Full(id)) => Some(id.as_ref()),
            None => None,
        }
    }

    /// Returns the full identity if present.
    ///
    /// Returns `None` if there is no identity or if the identity
    /// does not have signing capability.
    pub fn full_identity(&self) -> Option<&dyn FullIdentity> {
        match &self.inner {
            Some(IdentityHolder::Full(id)) => Some(id.as_ref()),
            None => None,
        }
    }

    /// Returns the raw identity if present.
    pub fn raw_identity(&self) -> Option<&RawIdentity> {
        match &self.inner {
            Some(IdentityHolder::Full(id)) => Some(id.as_ref()),
            None => None,
        }
    }

    /// Returns a shared reference to the raw identity if present.
    pub fn raw_identity_arc(&self) -> Option<Arc<RawIdentity>> {
        self.inner
            .as_ref()
            .map(|IdentityHolder::Full(id)| id.clone())
    }
}

impl Default for IdentityContext {
    fn default() -> Self {
        Self::empty()
    }
}

impl std::fmt::Debug for IdentityContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Some(IdentityHolder::Full(id)) => {
                let did_str = id
                    .did()
                    .map(|d| d.to_string())
                    .unwrap_or_else(|e| format!("<DID error: {}>", e));
                f.debug_struct("IdentityContext")
                    .field("type", &"full")
                    .field("did", &did_str)
                    .finish()
            }
            None => f
                .debug_struct("IdentityContext")
                .field("type", &"empty")
                .finish(),
        }
    }
}
