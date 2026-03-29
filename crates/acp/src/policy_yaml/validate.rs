use super::ParsedPolicy;

/// Validate permission expressions in a parsed policy.
///
/// Checks:
/// 1. Expressions cannot reference the reserved `owner` relation
/// 2. Expressions can use Zanzibar operators, including TTU (`->`)
/// 3. Direct relation references and TTU tuple relations must exist in the same resource
pub fn validate_policy_expressions(policy: &ParsedPolicy) -> Result<(), String> {
    for resource in &policy.resources {
        // 'owner' is a reserved relation name, auto-injected by the system
        for relation in &resource.relations {
            if relation.name == "owner" {
                return Err(format!(
                    "invalid resource: 'owner` is a reserved relation name: \
                     rename 'owner' to a different name; attrs={{\"resource\":\"{}\",\
                     \"transformer\":\"Discretionary transformer\"}}; kind=BAD_INPUT",
                    resource.name
                ));
            }
        }

        for permission in &resource.permissions {
            if permission.expr.is_empty() {
                continue;
            }

            let tokens = tokenize_expression(&permission.expr)?;
            let mut skip_local_relation_check = false;

            for token in &tokens {
                match token {
                    ExprToken::Identifier(name) => {
                        // Check for owner reference
                        if name == "owner" {
                            return Err("permission cannot reference `owner` relation".to_string());
                        }

                        if skip_local_relation_check {
                            skip_local_relation_check = false;
                            continue;
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
                    ExprToken::TupleToUserset => {
                        skip_local_relation_check = true;
                    }
                    ExprToken::Operator | ExprToken::Paren => {}
                }
            }
        }
    }

    Ok(())
}

enum ExprToken {
    Identifier(String),
    Operator,
    TupleToUserset,
    Paren,
}

/// Tokenize a permission expression like "reader + writer - admin".
/// Valid operators: +, -, ->, &
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
                        tokens.push(ExprToken::TupleToUserset);
                        continue;
                    }
                }
                tokens.push(ExprToken::Operator);
                chars.next();
            }
            '&' => {
                tokens.push(ExprToken::Operator);
                chars.next();
            }
            '(' | ')' => {
                tokens.push(ExprToken::Paren);
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
    use crate::policy_yaml::{build_policy, parse_policy_yaml};

    fn assert_policy_loads(yaml: &str) {
        let parsed = parse_policy_yaml(yaml).unwrap();
        validate_policy_expressions(&parsed).unwrap();

        let policy = build_policy(&parsed, 1).unwrap();
        assert!(policy.validate().is_ok());
        assert!(policy.validate_dpi().is_ok());
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
    fn test_reserved_owner_relation_error() {
        let yaml = r#"
name: test
description: a policy
resources:
- name: users
  permissions:
  - name: read
    expr: reader
  relations:
  - name: owner
    types:
    - actor
  - name: reader
    types:
    - actor
"#;
        let policy = parse_policy_yaml(yaml).unwrap();
        let result = validate_policy_expressions(&policy);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("reserved relation name"));
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

    #[test]
    fn test_difference_expression_loads() {
        let yaml = r#"
name: public_except_blocked
resources:
- name: document
  permissions:
  - name: read
    expr: reader - blocked
  - name: update
    expr: writer - blocked
  - name: delete
    expr: admin
  relations:
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
  - name: blocked
    types: [actor]
  - name: admin
    manages: [reader, writer, blocked]
    types: [actor]
"#;

        assert_policy_loads(yaml);
    }

    #[test]
    fn test_ttu_expression_loads() {
        let yaml = r#"
name: filesystem
resources:
- name: file
  permissions:
  - name: read
    expr: parent->read
  - name: update
    expr: writer
  - name: delete
    expr: writer
  relations:
  - name: parent
    types: [directory]
  - name: writer
    types: [actor]
- name: directory
  permissions:
  - name: read
    expr: reader + writer
  - name: update
    expr: writer
  - name: delete
    expr: writer
  relations:
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
"#;

        assert_policy_loads(yaml);
    }

    #[test]
    fn test_nested_difference_expression_loads() {
        let yaml = r#"
name: nested_difference
resources:
- name: document
  permissions:
  - name: read
    expr: (reader + writer) - blocked
  - name: update
    expr: writer
  - name: delete
    expr: admin
  relations:
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
  - name: blocked
    types: [actor]
  - name: admin
    types: [actor]
"#;

        assert_policy_loads(yaml);
    }

    #[test]
    fn test_nested_ttu_expression_loads() {
        let yaml = r#"
name: filesystem
resources:
- name: file
  permissions:
  - name: read
    expr: reader + parent->read
  - name: update
    expr: writer + parent->update
  - name: delete
    expr: writer
  relations:
  - name: parent
    types: [directory]
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
- name: directory
  permissions:
  - name: read
    expr: reader + writer
  - name: update
    expr: writer
  - name: delete
    expr: writer
  relations:
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
"#;

        assert_policy_loads(yaml);
    }
}
