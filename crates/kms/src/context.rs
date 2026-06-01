//! Request context for threading caller identity through KMS calls.
//!
//! Rust defradb does not use Go-style `context.Context`; identity is
//! explicit parameter at every call site (see `crates/db/src/block_verify.rs`
//! and `crates/query/src/runner/version.rs`). `RequestContext` exists
//! only so the KMS surface has room to grow (tracing IDs, future
//! attestation metadata) without re-threading every call site.

use identity::Did;

#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    user_identity: Option<Did>,
}

impl RequestContext {
    /// Construct an anonymous request context (no user identity attached).
    /// The KMS will fall back to node identity at the wire boundary.
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// Construct a request context carrying a verified user DID.
    pub fn with_user(did: Did) -> Self {
        Self {
            user_identity: Some(did),
        }
    }

    /// Return the verified user DID, if one is attached. `None` ⇒ anonymous.
    pub fn user_identity(&self) -> Option<&Did> {
        self.user_identity.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_has_no_identity() {
        assert!(RequestContext::anonymous().user_identity().is_none());
    }

    #[test]
    fn with_user_carries_did() {
        let did: identity::Did = "did:key:zalice".parse().unwrap();
        let ctx = RequestContext::with_user(did.clone());
        assert_eq!(ctx.user_identity(), Some(&did));
    }
}
