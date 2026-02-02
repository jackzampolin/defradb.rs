//! YAML policy parser for validating policy structure.
//!
//! Parses the YAML structure of a policy to inspect resources and permissions.

use serde::Deserialize;

#[derive(Deserialize)]
pub struct ParsedPolicy {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub resources: Vec<PolicyResource>,
}

#[derive(Deserialize)]
pub struct PolicyResource {
    pub name: String,
    #[serde(default)]
    pub permissions: Vec<PolicyPermission>,
    #[serde(default)]
    pub relations: Vec<PolicyRelation>,
}

#[derive(Deserialize)]
pub struct PolicyPermission {
    pub name: String,
    #[serde(default)]
    pub expr: String,
}

#[derive(Deserialize)]
pub struct PolicyRelation {
    pub name: String,
    #[serde(default)]
    pub manages: Vec<String>,
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

    pub fn has_relation(&self, name: &str) -> bool {
        self.relations.iter().any(|r| r.name == name)
    }

    /// Get relation names that manage the given relation.
    ///
    /// For example, if "admin" has `manages: [reader]`, then
    /// `get_managers_for_relation("reader")` returns `["admin"]`.
    pub fn get_managers_for_relation(&self, relation: &str) -> Vec<&str> {
        self.relations
            .iter()
            .filter(|r| r.manages.iter().any(|m| m == relation))
            .map(|r| r.name.as_str())
            .collect()
    }
}

pub fn parse_policy_yaml(yaml: &str) -> Result<ParsedPolicy, String> {
    serde_yaml::from_str(yaml).map_err(|e| format!("invalid policy YAML: {}", e))
}

/// Check for duplicate map keys in raw YAML text.
///
/// Go's `yaml.YAMLToJSONStrict` rejects duplicate keys with
/// `key "<name>" already set in map`. `serde_yaml` also detects
/// duplicates but with a different error format, so we re-format
/// the error to match Go.
pub fn check_duplicate_yaml_keys(yaml_text: &str) -> Result<(), String> {
    let result: Result<serde_yaml::Value, _> = serde_yaml::from_str(yaml_text);
    match result {
        Ok(_) => {
            // serde_yaml didn't find duplicates; also run raw text scan as fallback
            scan_raw_yaml_for_duplicates(yaml_text)
        }
        Err(e) => {
            let err_msg = e.to_string();
            // serde_yaml error format:
            //   "path.to.field: duplicate entry with key \"<key>\" at line N column M"
            if let Some(dup_pos) = err_msg.find("duplicate entry with key") {
                // Extract the key name between quotes after "duplicate entry with key"
                let after = &err_msg[dup_pos..];
                if let Some(q1) = after.find('"') {
                    let after_q1 = &after[q1 + 1..];
                    if let Some(q2) = after_q1.find('"') {
                        let key_name = &after_q1[..q2];
                        return Err(format!("key \"{}\" already set in map", key_name));
                    }
                }
                Err(err_msg)
            } else {
                // Not a duplicate key error — let the real parser handle it
                Ok(())
            }
        }
    }
}

/// Scan raw YAML text for duplicate keys within the same indentation block.
///
/// Tracks map keys at each indentation level. List items (`- ...`) reset
/// the key set for deeper levels, so keys can repeat across list items
/// but not within the same map.
fn scan_raw_yaml_for_duplicates(yaml_text: &str) -> Result<(), String> {
    // Each entry: (indent_level, set_of_keys_seen)
    let mut key_stack: Vec<(usize, Vec<String>)> = Vec::new();

    for line in yaml_text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let indent = line.len() - line.trim_start().len();

        // List items (`- key: val` or `- name`) clear deeper key sets
        // since each list item starts a fresh mapping context
        if trimmed.starts_with('-') {
            // Remove all entries at indent levels >= the list item's indent
            // (the list item resets the context for its children)
            while let Some(last) = key_stack.last() {
                if last.0 >= indent {
                    key_stack.pop();
                } else {
                    break;
                }
            }
            continue;
        }

        if let Some(key) = extract_yaml_map_key(trimmed) {
            // Pop entries strictly deeper than current indent
            while let Some(last) = key_stack.last() {
                if last.0 > indent {
                    key_stack.pop();
                } else {
                    break;
                }
            }

            // If the top of stack is at the same indent level, check for duplicates
            if let Some(last) = key_stack.last_mut() {
                if last.0 == indent {
                    if last.1.contains(&key) {
                        return Err(format!("key \"{}\" already set in map", key));
                    }
                    last.1.push(key);
                    continue;
                }
            }

            // New deeper indent level
            key_stack.push((indent, vec![key]));
        }
    }

    Ok(())
}

/// Extract a YAML map key from a non-list line.
fn extract_yaml_map_key(trimmed: &str) -> Option<String> {
    if let Some(colon_pos) = trimmed.find(':') {
        let key = trimmed[..colon_pos].trim();
        if !key.is_empty() && !key.contains(' ') {
            return Some(key.to_string());
        }
    }
    None
}

/// Validate permission expressions in a parsed policy.
///
/// Checks:
/// 1. Expressions cannot reference the reserved `owner` relation
/// 2. Expression operators must be valid (`+` and `-` only)
/// 3. Expressions can only reference relations declared in the same resource
pub fn validate_policy_expressions(policy: &ParsedPolicy) -> Result<(), String> {
    for resource in &policy.resources {
        for permission in &resource.permissions {
            if permission.expr.is_empty() {
                continue;
            }

            let tokens = tokenize_expression(&permission.expr)?;

            for token in &tokens {
                match token {
                    ExprToken::Identifier(name) => {
                        // Check for owner reference
                        if name == "owner" {
                            return Err("permission cannot reference `owner` relation".to_string());
                        }

                        // Check that the relation exists in this resource
                        if !resource.has_relation(name) {
                            // Check if it exists in another resource (cross-resource error)
                            let exists_elsewhere = policy
                                .resources
                                .iter()
                                .any(|r| r.name != resource.name && r.has_relation(name));
                            if exists_elsewhere {
                                return Err("resource does not have relation".to_string());
                            }
                            return Err("BAD_INPUT".to_string());
                        }
                    }
                    ExprToken::Operator(_) | ExprToken::Paren(_) => {}
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
#[allow(dead_code)]
enum ExprToken {
    Identifier(String),
    Operator(char),
    Paren(char),
}

/// Tokenize a permission expression like "reader + writer - admin".
/// Valid operators: +, -
/// Valid tokens: identifiers (alphanumeric + underscore), operators, parentheses
fn tokenize_expression(expr: &str) -> Result<Vec<ExprToken>, String> {
    let mut tokens = Vec::new();
    let mut chars = expr.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '+' | '-' => {
                // Check for -> (TTU operator)
                if ch == '-' {
                    let mut lookahead = chars.clone();
                    lookahead.next(); // skip '-'
                    if lookahead.peek() == Some(&'>') {
                        // This is a TTU operator "->", consume both
                        chars.next();
                        chars.next();
                        tokens.push(ExprToken::Operator('>'));
                        continue;
                    }
                }
                tokens.push(ExprToken::Operator(ch));
                chars.next();
            }
            '&' => {
                tokens.push(ExprToken::Operator(ch));
                chars.next();
            }
            '(' | ')' => {
                tokens.push(ExprToken::Paren(ch));
                chars.next();
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut ident = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        ident.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(ExprToken::Identifier(ident));
            }
            _ => {
                return Err("token recognition error".to_string());
            }
        }
    }

    Ok(tokens)
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
        assert_eq!(policy.name, "test");
        assert_eq!(policy.description, "a test policy");
        assert_eq!(policy.resources.len(), 1);
        let resource = policy.find_resource("users").unwrap();
        assert!(resource.has_permission("read"));
        assert!(resource.has_permission("update"));
        assert!(resource.has_permission("delete"));
        assert!(!resource.has_permission("nonexistent"));
        assert!(resource.has_relation("owner"));
        assert!(resource.has_relation("reader"));
        assert!(!resource.has_relation("nonexistent"));
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

    #[test]
    fn test_parse_policy_name() {
        let policy = parse_policy_yaml(TEST_POLICY).unwrap();
        assert_eq!(policy.name, "test");
    }

    #[test]
    fn test_parse_permission_expr() {
        let policy = parse_policy_yaml(TEST_POLICY).unwrap();
        let resource = policy.find_resource("users").unwrap();
        let read_perm = resource
            .permissions
            .iter()
            .find(|p| p.name == "read")
            .unwrap();
        assert_eq!(read_perm.expr, "owner + reader");
    }

    #[test]
    fn test_duplicate_keys_permissions() {
        let yaml = r#"
                    name: a policy
                    description: a policy

                    resources:
                      users:
                        permissions:
                          read:
                            expr: owner
                          update:
                            expr: owner
                          delete:
                            expr: owner
                          read:
                            expr: owner

                        relations:
                          owner:
                            types:
                              - actor

                    actor:
                      name: actor
                "#;
        let result = check_duplicate_yaml_keys(yaml);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("key \"read\" already set in map"));
    }

    #[test]
    fn test_duplicate_keys_relations() {
        let yaml = r#"
                    name: a policy
                    description: a policy

                    actor:
                      name: actor

                    resources:
                      users:
                        permissions:
                          update:
                            expr: owner
                          delete:
                            expr: owner
                          read:
                            expr: owner + reader

                        relations:
                          owner:
                            types:
                              - actor
                          reader:
                            types:
                              - actor
                          joker:
                            types:
                              - actor

                          joker:
                            types:
                              - actor
                "#;
        let result = check_duplicate_yaml_keys(yaml);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("key \"joker\" already set in map"));
    }

    #[test]
    fn test_no_duplicate_keys() {
        let result = check_duplicate_yaml_keys(TEST_POLICY);
        assert!(result.is_ok());
    }

    #[test]
    fn test_owner_reference_error() {
        let yaml = r#"
name: test
description: a policy
resources:
- name: users
  permissions:
  - expr: reader + owner
    name: read
  relations:
  - name: reader
    types:
    - actor
"#;
        let policy = parse_policy_yaml(yaml).unwrap();
        let result = validate_policy_expressions(&policy);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("permission cannot reference `owner`"));
    }

    #[test]
    fn test_bad_token_error() {
        let yaml = r#"
name: test
description: a policy
resources:
- name: users
  permissions:
  - name: read
    expr: reader ^ asf
  relations:
  - name: reader
    types:
    - actor
"#;
        let policy = parse_policy_yaml(yaml).unwrap();
        let result = validate_policy_expressions(&policy);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("token recognition error"));
    }

    #[test]
    fn test_undeclared_relation_error() {
        let yaml = r#"
description: a policy
name: a policy
resources:
- name: users
  permissions:
  - name: delete
  - expr: reader
    name: read
  - name: update
  relations:
"#;
        let policy = parse_policy_yaml(yaml).unwrap();
        let result = validate_policy_expressions(&policy);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("BAD_INPUT"));
    }
}
