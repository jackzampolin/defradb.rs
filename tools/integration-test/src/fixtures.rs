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

/// Standard fields used across test schemas for consistent access matrix testing.
pub const STANDARD_FIELDS: &str = "title: String  body: String  score: Int";

/// Generate an ACP policy YAML with admin/writer/reader role hierarchy for multiple resources.
///
/// Each resource gets identical permission structure:
/// - read: admin + writer + reader
/// - update: admin + writer
/// - delete: admin (owner always has implicit access; nobody gets admin, so delete = owner-only)
pub fn multi_resource_policy(name: &str, description: &str, resources: &[&str]) -> String {
    let mut yaml = format!("name: {}\ndescription: {}\n\nresources:", name, description);
    for resource in resources {
        yaml.push_str(&format!(
            r#"
  - name: {}
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
          - actor"#,
            resource
        ));
    }
    yaml
}

/// Build a schema type with @policy directive referencing a specific resource.
pub fn typed_schema(type_name: &str, policy_id: &str, resource: &str, fields: &str) -> String {
    format!(
        r#"type {} @policy(id: "{}", resource: "{}") {{ {} }}"#,
        type_name, policy_id, resource, fields
    )
}
