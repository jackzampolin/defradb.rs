//! Access control policy types.
//!
//! Matches Go's client/acp.go

use serde::{Deserialize, Serialize};

use crate::error::{Result, SchemaError};

/// Describes an access control policy on a collection.
/// Matches Go's PolicyDescription.
///
/// Collections without a policy have no access control.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyDescription {
    /// The policy ID.
    /// - For local ACP: local policy ID
    /// - For remote ACP (SourceHub): global policy ID
    #[serde(rename = "ID")]
    pub id: String,

    /// Name of the corresponding resource within the policy.
    #[serde(rename = "ResourceName")]
    pub resource_name: String,
}

impl PolicyDescription {
    /// Create a new policy description.
    pub fn new(id: impl Into<String>, resource_name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            resource_name: resource_name.into(),
        }
    }

    /// Validate the policy description.
    ///
    /// Ensures:
    /// - `id` is non-empty and doesn't contain path separators
    /// - `resource_name` is non-empty and doesn't contain path separators
    ///
    /// Path separators are rejected to prevent path traversal attacks in ACP storage keys.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() {
            return Err(SchemaError::InvalidPolicy(
                "policy id cannot be empty".into(),
            ));
        }
        if self.resource_name.is_empty() {
            return Err(SchemaError::InvalidPolicy(
                "policy resource_name cannot be empty".into(),
            ));
        }
        // Prevent path traversal in ACP storage keys
        if self.id.contains('/') || self.id.contains('\\') {
            return Err(SchemaError::InvalidPolicy(
                "policy id cannot contain path separators (/ or \\)".into(),
            ));
        }
        if self.resource_name.contains('/') || self.resource_name.contains('\\') {
            return Err(SchemaError::InvalidPolicy(
                "policy resource_name cannot contain path separators (/ or \\)".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_serialization() {
        let policy = PolicyDescription::new("policy-123", "users");
        let json = serde_json::to_string(&policy).unwrap();

        assert!(json.contains("\"ID\""));
        assert!(json.contains("\"ResourceName\""));

        let parsed: PolicyDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(policy, parsed);
    }

    #[test]
    fn test_policy_validate_valid() {
        let policy = PolicyDescription::new("policy-123", "users");
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_policy_validate_empty_id() {
        let policy = PolicyDescription::new("", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("id cannot be empty"));
    }

    #[test]
    fn test_policy_validate_empty_resource_name() {
        let policy = PolicyDescription::new("policy-123", "");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("resource_name cannot be empty"));
    }

    #[test]
    fn test_policy_validate_id_with_forward_slash() {
        let policy = PolicyDescription::new("policy/123", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path separators"));
    }

    #[test]
    fn test_policy_validate_id_with_backslash() {
        let policy = PolicyDescription::new("policy\\123", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path separators"));
    }

    #[test]
    fn test_policy_validate_resource_name_with_forward_slash() {
        let policy = PolicyDescription::new("policy-123", "users/admin");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path separators"));
    }

    #[test]
    fn test_policy_validate_resource_name_with_backslash() {
        let policy = PolicyDescription::new("policy-123", "users\\admin");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path separators"));
    }
}
