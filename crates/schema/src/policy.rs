//! Access control policy types.
//!
//! Matches Go's client/acp.go

use serde::{Deserialize, Serialize};

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
}
