//! Core types used across the KMS service.

use cid::Cid;

/// CID of an `Encryption` metadata block. One CID == one DEK.
pub type EncryptionCid = Cid;

/// Scope of a DEK, derived from the on-disk `Encryption` block.
///
/// Drives which `AccessPolicy` gate applies. Never serialized on the wire —
/// re-derived on the responder from the block contents (mirrors Go's
/// `internal/kms/pubsub.go::getEncryptionKeysLocally`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyScope {
    /// Document-scoped DEK. `field: None` means a whole-document key shared
    /// by all encrypted fields; `field: Some(name)` means a per-field key.
    Document {
        doc_id: String,
        field: Option<String>,
    },
    /// Per-collection DEK (e.g. @branchable collection-head blocks). Gated
    /// by node-level NAC on release, not by per-doc DAC.
    Collection { collection_id: String },
}

/// Outcome of an `AccessPolicy` decision. M1 ships only `Allow` / `Deny`.
/// `AllowAttested` lands with M4 SourceHub.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    /// AccessPolicy granted release.
    Allow,
    /// AccessPolicy refused release.
    Deny,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_scope_equality() {
        let a = KeyScope::Document {
            doc_id: "d".into(),
            field: None,
        };
        let b = KeyScope::Document {
            doc_id: "d".into(),
            field: None,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn policy_decision_equality() {
        assert_eq!(PolicyDecision::Allow, PolicyDecision::Allow);
        assert_ne!(PolicyDecision::Allow, PolicyDecision::Deny);
    }
}
