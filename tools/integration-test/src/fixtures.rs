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

/// ACP policy with admin/writer/reader role hierarchy and tiered permissions.
pub const MULTI_ROLE_ACP_POLICY: &str = r#"name: test-multi-role-policy
description: A test policy with admin, writer, and reader role hierarchy

resources:
  - name: documents
    permissions:
      - name: read
        expr: admin + writer + reader
      - name: update
        expr: admin + writer
      - name: delete
        expr: admin
    relations:
      - name: admin
        types:
          - actor
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor"#;

/// Build a Document schema with title/content/classification that references an ACP policy.
pub fn documents_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Document @policy(id: "{}", resource: "documents") {{ title: String  content: String  classification: String }}"#,
        policy_id
    )
}

/// Simple Product schema without ACP, for encrypted index tests.
pub const PRODUCT_SCHEMA: &str = "type Product { name: String  sku: String  price: Int }";
