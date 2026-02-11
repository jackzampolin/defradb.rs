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
    #[serde(rename = "ID", default)]
    pub id: String,

    /// Name of the corresponding resource within the policy.
    #[serde(rename = "ResourceName", default)]
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
    /// - `id` is non-empty (not empty or whitespace-only) and doesn't contain dangerous characters
    /// - `resource_name` is non-empty and doesn't contain dangerous characters
    ///
    /// Dangerous characters rejected for defense-in-depth against path traversal:
    /// - Path separators: `/` and `\`
    /// - Parent directory sequences: `..`
    /// - Null bytes: `\0`
    pub fn validate(&self) -> Result<()> {
        Self::validate_field(&self.id, "policy id")?;
        Self::validate_field(&self.resource_name, "policy resource_name")?;
        Ok(())
    }

    /// Validate a single field for dangerous characters.
    fn validate_field(value: &str, field_name: &str) -> Result<()> {
        // Check empty or whitespace-only
        if value.is_empty() || value.trim().is_empty() {
            return Err(SchemaError::InvalidPolicy(format!(
                "{} cannot be empty or whitespace-only",
                field_name
            )));
        }
        // Check path separators
        if value.contains('/') || value.contains('\\') {
            return Err(SchemaError::InvalidPolicy(format!(
                "{} cannot contain path separators (/ or \\)",
                field_name
            )));
        }
        // Check parent directory sequences
        if value.contains("..") {
            return Err(SchemaError::InvalidPolicy(format!(
                "{} cannot contain '..' sequences",
                field_name
            )));
        }
        // Check null bytes
        if value.contains('\0') {
            return Err(SchemaError::InvalidPolicy(format!(
                "{} cannot contain null bytes",
                field_name
            )));
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

    // === Empty string tests ===

    #[test]
    fn test_policy_validate_empty_id() {
        let policy = PolicyDescription::new("", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_policy_validate_empty_resource_name() {
        let policy = PolicyDescription::new("policy-123", "");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    // === Whitespace-only tests ===

    #[test]
    fn test_policy_validate_whitespace_only_id() {
        let policy = PolicyDescription::new("   ", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot be empty or whitespace-only"));
    }

    #[test]
    fn test_policy_validate_whitespace_only_resource_name() {
        let policy = PolicyDescription::new("policy-123", "\t\n  ");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot be empty or whitespace-only"));
    }

    // === Path separator tests ===

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

    // === Path traversal sequence tests ===

    #[test]
    fn test_policy_validate_id_with_path_traversal() {
        let policy = PolicyDescription::new("../admin", "users");
        let result = policy.validate();
        assert!(result.is_err());
        // Should be caught by path separator check first
        assert!(result.unwrap_err().to_string().contains("path separators"));
    }

    #[test]
    fn test_policy_validate_id_with_dotdot_sequence() {
        let policy = PolicyDescription::new("policy..secret", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("'..'"));
    }

    #[test]
    fn test_policy_validate_resource_name_with_dotdot_sequence() {
        let policy = PolicyDescription::new("policy-123", "users..admin");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("'..'"));
    }

    // === Null byte tests ===

    #[test]
    fn test_policy_validate_id_with_null_byte() {
        let policy = PolicyDescription::new("policy\x00123", "users");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null bytes"));
    }

    #[test]
    fn test_policy_validate_resource_name_with_null_byte() {
        let policy = PolicyDescription::new("policy-123", "users\0admin");
        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null bytes"));
    }

    // === Valid edge cases ===

    #[test]
    fn test_policy_validate_single_dot_allowed() {
        // Single dots are allowed (not path traversal)
        let policy = PolicyDescription::new("policy.v1", "users.table");
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_policy_validate_with_dashes_and_underscores() {
        let policy = PolicyDescription::new("my-policy_v1", "user_table-v2");
        assert!(policy.validate().is_ok());
    }
}
