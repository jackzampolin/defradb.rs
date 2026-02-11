use super::ParsedPolicy;

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
}
