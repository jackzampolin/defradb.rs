/// A minimal ACP policy for user document access control.
///
/// The `owner` relation is auto-injected by the system (reserved name in Go DefraDB).
/// Only declare non-owner relations here.
pub const USER_ACP_POLICY: &str = r#"name: test-user-policy
description: A test policy for user document access control

resources:
  - name: users
    permissions:
      - name: read
        expr: writer + reader
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor"#;

/// Build a User schema that references an ACP policy.
pub fn users_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type User @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    )
}
