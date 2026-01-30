//! YAML policy parser for validating policy structure.
//!
//! Parses the YAML structure of a policy to inspect resources and permissions.

use serde::Deserialize;

#[derive(Deserialize)]
pub struct ParsedPolicy {
    #[serde(default)]
    pub resources: Vec<PolicyResource>,
}

#[derive(Deserialize)]
pub struct PolicyResource {
    pub name: String,
    #[serde(default)]
    pub permissions: Vec<PolicyPermission>,
}

#[derive(Deserialize)]
pub struct PolicyPermission {
    pub name: String,
}

impl ParsedPolicy {
    pub fn find_resource(&self, name: &str) -> Option<&PolicyResource> {
        self.resources.iter().find(|r| r.name == name)
    }
}

impl PolicyResource {
    pub fn has_permission(&self, name: &str) -> bool {
        self.permissions.iter().any(|p| p.name == name)
    }
}

pub fn parse_policy_yaml(yaml: &str) -> Result<ParsedPolicy, String> {
    serde_yaml::from_str(yaml).map_err(|e| format!("invalid policy YAML: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_POLICY: &str = r#"
name: test
description: a test policy
resources:
  - name: users
    permissions:
      - name: read
        expr: owner + reader
      - name: update
        expr: owner
      - name: delete
        expr: owner
    relations:
      - name: owner
        types:
          - actor
      - name: reader
        types:
          - actor
"#;

    #[test]
    fn test_parse_valid_policy() {
        let policy = parse_policy_yaml(TEST_POLICY).unwrap();
        assert_eq!(policy.resources.len(), 1);
        let resource = policy.find_resource("users").unwrap();
        assert!(resource.has_permission("read"));
        assert!(resource.has_permission("update"));
        assert!(resource.has_permission("delete"));
        assert!(!resource.has_permission("nonexistent"));
    }

    #[test]
    fn test_find_missing_resource() {
        let policy = parse_policy_yaml(TEST_POLICY).unwrap();
        assert!(policy.find_resource("nonexistent").is_none());
    }

    #[test]
    fn test_parse_invalid_yaml() {
        let result = parse_policy_yaml("{{invalid");
        assert!(result.is_err());
    }
}
