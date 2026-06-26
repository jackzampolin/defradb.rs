//! Collection-level ACP helpers.
//!
//! These helpers provide document-level access control integration for
//! collection mutations (create/update/delete).
//!
//! # Policy Transition Behavior
//!
//! When a collection's policy changes, existing documents are affected:
//!
//! ## No Policy → Has Policy (open → protected)
//!
//! - Existing documents remain **public** (unregistered with ACP)
//! - New documents created with identity will be protected
//! - To protect existing documents, they must be explicitly registered
//!
//! ## Has Policy → No Policy (protected → open)
//!
//! - **SECURITY WARNING**: Previously protected documents become public
//! - ACP registrations become orphaned (data remains but unused)
//! - Consider this a destructive operation that removes access control
//!
//! ## Policy Resource Name Change
//!
//! - Existing ACP registrations use the old resource name
//! - Documents become effectively unregistered under the new policy
//! - Same effect as removing policy - previously protected documents become public

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity};
use identity::Did;
use schema::CollectionVersion;

/// Check if identity has permission for a document operation.
///
/// Returns true if any of the following holds:
/// 1. Collection has no policy (ACP not enforced)
/// 2. The thread-local DAC bypass flag is set (NAC admin with `bypass-dac`,
///    resolved at the HTTP/FFI entry point via `should_bypass_dac()`)
/// 3. The request identity equals the configured `node_identity` (the
///    process owner gets full access — matches Go DefraDB's
///    `internal/db/collection_acp.go:60-62`)
/// 4. Document is unregistered (public) — handled inside the backend
/// 5. Identity has the required relation/permission — handled by the backend
pub async fn check_doc_permission(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    collection: &CollectionVersion,
    doc_id: &str,
    node_identity: Option<&Did>,
) -> acp::Result<bool> {
    // If collection has no policy, ACP is not enforced
    let policy = match &collection.policy {
        Some(p) => p,
        None => return Ok(true),
    };

    // DAC bypass via thread-local flag (set by HTTP/FFI entry points after
    // resolving NAC `bypass-dac` permission via `acp::nac::should_bypass_dac`).
    // Mirrors how the query path honors the bypass via `permission_filter`.
    if defra_core::dac_bypass::get_dac_bypass() {
        return Ok(true);
    }

    // Node-identity full-access shortcut (matches Go's
    // `internal/db/collection_acp.go:60-62`). The process owner is granted
    // full access to all documents regardless of DAC, which is required for
    // background tasks, backup, GC, and admin operations.
    if let (Some(node_did), Identity::Authenticated(req_did)) = (node_identity, identity) {
        if node_did == req_did {
            return Ok(true);
        }
    }

    acp.check_doc_access(
        identity,
        permission,
        &policy.id,
        &policy.resource_name,
        doc_id,
    )
    .await
}

/// Register a document with ACP after creation.
///
/// Only registers if:
/// 1. Collection has a policy
/// 2. Identity is provided
///
/// If collection has no policy or no identity is provided, the document
/// remains unregistered (public).
pub async fn register_doc_if_needed(
    acp: &dyn DocumentACP,
    identity: Option<&Did>,
    collection: &CollectionVersion,
    doc_id: &str,
) -> acp::Result<()> {
    // Only register if collection has policy AND identity is provided
    let (policy, did) = match (&collection.policy, identity) {
        (Some(p), Some(id)) => (p, id),
        _ => return Ok(()), // No policy or no identity = public document
    };

    acp.register_doc_object(did, &policy.id, &policy.resource_name, doc_id)
        .await
}

/// Register a branchable collection as an ACP object after creation.
///
/// Only registers if:
/// 1. Collection is branchable
/// 2. Collection has a policy
/// 3. Identity is provided
///
/// The ACP object id is the stable collection ID. Re-registering an existing
/// collection object is a no-op: the same collection can be defined on multiple
/// nodes backed by a shared ACP store.
pub async fn register_collection_if_needed(
    acp: &dyn DocumentACP,
    identity: Option<&Did>,
    collection: &CollectionVersion,
) -> acp::Result<()> {
    if !collection.is_branchable {
        return Ok(());
    }

    let (policy, did) = match (&collection.policy, identity) {
        (Some(p), Some(id)) => (p, id),
        _ => return Ok(()),
    };

    if acp
        .is_doc_registered(&policy.id, &policy.resource_name, &collection.collection_id)
        .await?
    {
        return Ok(());
    }

    acp.register_object(
        did,
        &policy.id,
        &policy.resource_name,
        &collection.collection_id,
    )
    .await
}

/// Clean up ACP relations when deleting a document.
///
/// This should be called when deleting a document to remove all
/// associated relation tuples from the ACP store (owner, reader, updater, deleter).
pub async fn unregister_doc_if_needed(
    acp: &dyn DocumentACP,
    collection: &CollectionVersion,
    doc_id: &str,
) -> acp::Result<()> {
    // Only need to clean up if collection has policy
    let policy = match &collection.policy {
        Some(p) => p,
        None => return Ok(()), // No policy = no ACP tuples to clean up
    };

    // Check if document is registered
    if !acp
        .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
        .await?
    {
        return Ok(()); // Not registered, nothing to clean up
    }

    // Delete all ACP tuples for this document
    acp.unregister_doc_object(&policy.id, &policy.resource_name, doc_id)
        .await
}

/// ACP context for mutation operations.
///
/// This wraps the DocumentACP, request identity, and the node identity for
/// convenient access during collection mutations. The node identity is used
/// for the full-access shortcut (matches Go DefraDB's behavior — the process
/// owner gets full access to all documents).
#[derive(Clone)]
pub struct AcpContext {
    /// Document ACP for permission checks
    pub acp: Arc<dyn DocumentACP>,
    /// Identity making the request
    pub identity: Identity,
    /// Node identity (process owner) — granted full access if equal to `identity`
    pub node_identity: Option<Did>,
}

impl AcpContext {
    /// Create a new ACP context.
    pub fn new(acp: Arc<dyn DocumentACP>, identity: Identity, node_identity: Option<Did>) -> Self {
        Self {
            acp,
            identity,
            node_identity,
        }
    }

    /// Create from an optional DID for backward compatibility.
    pub fn from_optional_did(
        acp: Arc<dyn DocumentACP>,
        did: Option<Did>,
        node_identity: Option<Did>,
    ) -> Self {
        Self {
            acp,
            identity: Identity::from(did),
            node_identity,
        }
    }

    /// Check if identity has permission for a document operation.
    pub async fn check_permission(
        &self,
        permission: DocumentPermission,
        collection: &CollectionVersion,
        doc_id: &str,
    ) -> acp::Result<bool> {
        check_doc_permission(
            self.acp.as_ref(),
            &self.identity,
            permission,
            collection,
            doc_id,
            self.node_identity.as_ref(),
        )
        .await
    }

    /// Register a document after creation.
    pub async fn register_doc(
        &self,
        collection: &CollectionVersion,
        doc_id: &str,
    ) -> acp::Result<()> {
        register_doc_if_needed(self.acp.as_ref(), self.identity.did(), collection, doc_id).await
    }

    /// Register a branchable collection after creation.
    pub async fn register_collection(&self, collection: &CollectionVersion) -> acp::Result<()> {
        register_collection_if_needed(self.acp.as_ref(), self.identity.did(), collection).await
    }
}

// ============================================================================
// Policy Transition Safety
// ============================================================================

/// Result of checking a policy transition.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PolicyTransitionCheck {
    /// Transition is safe (no documents at risk).
    Safe,
    /// Warning: Documents may lose protection.
    Warning {
        /// Human-readable warning message.
        message: String,
        /// Number of registered documents that may be affected.
        affected_doc_count: Option<usize>,
    },
}

impl PolicyTransitionCheck {
    /// Returns true if this is a safe transition.
    pub fn is_safe(&self) -> bool {
        matches!(self, Self::Safe)
    }

    /// Returns true if this transition has warnings.
    pub fn has_warning(&self) -> bool {
        matches!(self, Self::Warning { .. })
    }

    /// Get the warning message if present.
    pub fn warning_message(&self) -> Option<&str> {
        match self {
            Self::Warning { message, .. } => Some(message),
            Self::Safe => None,
        }
    }
}

/// Check if a policy transition is safe.
///
/// This function should be called before changing a collection's policy
/// to warn about potentially dangerous transitions that could expose
/// previously protected documents.
///
/// # Arguments
///
/// * `old_policy` - The current policy (None = no ACP)
/// * `new_policy` - The new policy (None = no ACP)
///
/// # Returns
///
/// A `PolicyTransitionCheck` indicating whether the transition is safe
/// or has warnings.
///
/// # Warning Cases
///
/// 1. **Protected → Open**: Removing a policy makes all documents public.
/// 2. **Resource Name Change**: Changing the resource name orphans existing
///    ACP registrations, making documents effectively public.
pub fn check_policy_transition(
    old_policy: Option<&schema::PolicyDescription>,
    new_policy: Option<&schema::PolicyDescription>,
) -> PolicyTransitionCheck {
    match (old_policy, new_policy) {
        // No change
        (None, None) => PolicyTransitionCheck::Safe,

        // Open → Protected: Safe (documents stay public until explicitly registered)
        (None, Some(_)) => PolicyTransitionCheck::Safe,

        // Protected → Open: DANGEROUS
        (Some(old), None) => PolicyTransitionCheck::Warning {
            message: format!(
                "Removing policy '{}' will make all documents in this collection public. \
                 Previously protected documents will become accessible to anyone.",
                old.resource_name
            ),
            affected_doc_count: None, // Would need ACP query to determine
        },

        // Both have policies - check if resource name changed
        (Some(old), Some(new)) => {
            if old.resource_name != new.resource_name {
                PolicyTransitionCheck::Warning {
                    message: format!(
                        "Changing resource name from '{}' to '{}' will orphan existing ACP registrations. \
                         Previously protected documents will become public under the new policy.",
                        old.resource_name, new.resource_name
                    ),
                    affected_doc_count: None,
                }
            } else if old.id != new.id {
                PolicyTransitionCheck::Warning {
                    message: format!(
                        "Changing policy ID from '{}' to '{}' with same resource name. \
                         This may affect how permissions are evaluated.",
                        old.id, new.id
                    ),
                    affected_doc_count: None,
                }
            } else {
                PolicyTransitionCheck::Safe
            }
        }
    }
}

/// Log a warning if a policy transition is unsafe.
///
/// This is a convenience function that calls `check_policy_transition`
/// and logs a warning if the transition is not safe.
///
/// # Arguments
///
/// * `collection_name` - Name of the collection being modified
/// * `old_policy` - The current policy
/// * `new_policy` - The new policy
///
/// # Returns
///
/// The `PolicyTransitionCheck` result for further handling if needed.
pub fn warn_on_unsafe_policy_transition(
    collection_name: &str,
    old_policy: Option<&schema::PolicyDescription>,
    new_policy: Option<&schema::PolicyDescription>,
) -> PolicyTransitionCheck {
    let check = check_policy_transition(old_policy, new_policy);

    if let PolicyTransitionCheck::Warning { ref message, .. } = check {
        tracing::warn!(
            collection = %collection_name,
            "SECURITY WARNING - Unsafe policy transition: {}",
            message
        );
    }

    check
}

/// Block unsafe policy transitions by returning an error.
///
/// This function should be called before changing a collection's policy
/// to prevent dangerous transitions that could expose previously protected
/// documents.
///
/// # Arguments
///
/// * `collection_name` - Name of the collection being modified
/// * `old_policy` - The current policy
/// * `new_policy` - The new policy
/// * `force` - If true, allows unsafe transitions (with warning logged)
///
/// # Returns
///
/// `Ok(())` if the transition is safe or forced.
/// `Err(UnsafePolicyTransition)` if the transition is unsafe and not forced.
///
/// # Examples
///
/// ```ignore
/// // This will fail if collection has a policy that would be removed
/// block_unsafe_policy_transition("users", Some(&old_policy), None, false)?;
///
/// // Force allows the transition with a warning
/// block_unsafe_policy_transition("users", Some(&old_policy), None, true)?;
/// ```
pub fn block_unsafe_policy_transition(
    collection_name: &str,
    old_policy: Option<&schema::PolicyDescription>,
    new_policy: Option<&schema::PolicyDescription>,
    force: bool,
) -> crate::Result<()> {
    let check = check_policy_transition(old_policy, new_policy);

    match check {
        PolicyTransitionCheck::Safe => Ok(()),
        PolicyTransitionCheck::Warning { ref message, .. } => {
            if force {
                tracing::warn!(
                    target: "acp::audit",
                    event = "unsafe_policy_transition_forced",
                    collection = %collection_name,
                    "SECURITY WARNING - Unsafe policy transition forced: {}",
                    message
                );
                Ok(())
            } else {
                tracing::error!(
                    target: "acp::audit",
                    event = "unsafe_policy_transition_blocked",
                    collection = %collection_name,
                    "Unsafe policy transition blocked: {}",
                    message
                );
                Err(crate::Error::UnsafePolicyTransition(format!(
                    "Collection '{}': {}. Use force=true to override.",
                    collection_name, message
                )))
            }
        }
    }
}

// Tests extracted to crates/db/tests/collection_acp_tests.rs
