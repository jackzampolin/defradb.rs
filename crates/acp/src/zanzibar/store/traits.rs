//! ZanzibarStore trait definition.

use async_trait::async_trait;
use identity::Did;

use storage::corekv::MaybeSendSync;

use super::StorePolicyOptions;
use crate::error::Result;
use crate::zanzibar::types::{ObjectRef, Policy, Relationship, Subject};

/// Trait for Zanzibar policy and relationship storage.
///
/// Provides operations for:
/// - Policy storage and retrieval
/// - Relationship tuple storage
/// - Relationship queries (for permission evaluation)
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ZanzibarStore: MaybeSendSync {
    /// Store a policy.
    async fn store_policy(&self, policy: &Policy) -> Result<()>;

    /// Store a policy with validation options.
    ///
    /// If `options.validate` is true, validates the policy structure before storing.
    /// If `options.enforce_dpi` is true, also validates DPI compliance.
    async fn store_policy_with_options(
        &self,
        policy: &Policy,
        options: &StorePolicyOptions,
    ) -> Result<()> {
        if options.validate {
            policy.validate()?;
        }
        if options.enforce_dpi {
            policy.validate_dpi()?;
        }
        self.store_policy(policy).await
    }

    /// Get a policy by ID.
    async fn get_policy(&self, policy_id: &str) -> Result<Option<Policy>>;

    /// List all stored policies.
    async fn list_policies(&self) -> Result<Vec<Policy>>;

    /// Delete a policy.
    async fn delete_policy(&self, policy_id: &str) -> Result<bool>;

    /// Store a relationship tuple.
    async fn store_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<()>;

    /// Delete a relationship tuple.
    async fn delete_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<bool>;

    /// Check if a specific relationship exists.
    ///
    /// For direct entity subjects, checks for exact match.
    /// For wildcard subjects, this checks for the wildcard tuple.
    async fn has_relationship(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Subject,
    ) -> Result<bool>;

    /// Check if subject has the relation either directly or via wildcard.
    ///
    /// Returns true if:
    /// - Direct tuple exists: (resource, object_id, relation, subject)
    /// - Wildcard tuple exists: (resource, object_id, relation, *)
    async fn check_permission_direct(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool>;

    /// Get all subjects with a specific relation to an object.
    ///
    /// Returns entity subjects and entity set subjects.
    async fn get_relation_subjects(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<Subject>>;

    /// Get objects that the subject has a specific relation to.
    ///
    /// Used for tuple-to-userset: find objects where we have the tuple relation.
    async fn get_relation_targets(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<ObjectRef>>;

    /// Delete all relationships for an object.
    async fn delete_object_relationships(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<()>;
}
