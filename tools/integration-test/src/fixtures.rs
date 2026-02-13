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

/// ACP policy for x-archive compartment (tweets, interactions).
/// Note: `owner` is a reserved relation auto-injected by the system.
/// The implicit owner always has all permissions. `admin` is declared but
/// never granted, making delete effectively owner-only.
pub const XARCHIVE_ACP_POLICY: &str = r#"name: xarchive-policy
description: X-archive compartment policy for tweets and interactions

resources:
  - name: tweets
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
          - actor
  - name: interactions
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

/// ACP policy for hiking compartment (workouts, peaks).
/// Note: `owner` is a reserved relation auto-injected by the system.
pub const HIKING_ACP_POLICY: &str = r#"name: hiking-policy
description: Hiking compartment policy for workouts and peaks

resources:
  - name: workouts
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
          - actor
  - name: peaks
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

/// Strict owner-only ACP policy for secrets.
/// Uses an `admin` relation that nobody is ever granted, so only the
/// implicit owner has access to anything.
pub const SECRET_ACP_POLICY: &str = r#"name: secret-policy
description: Owner-only policy for secret collections

resources:
  - name: secrets
    permissions:
      - name: read
        expr: admin
      - name: update
        expr: admin
      - name: delete
        expr: admin
    relations:
      - name: admin
        types:
          - actor"#;

pub fn tweet_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Tweet @policy(id: "{}", resource: "tweets") {{ text: String  likes: Int  archived: Boolean }}"#,
        policy_id
    )
}

pub fn interaction_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Interaction @policy(id: "{}", resource: "interactions") {{ kind: String  target_id: String }}"#,
        policy_id
    )
}

pub fn workout_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Workout @policy(id: "{}", resource: "workouts") {{ activity: String  duration_min: Int }}"#,
        policy_id
    )
}

pub fn peak_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Peak @policy(id: "{}", resource: "peaks") {{ name: String  elevation_m: Int }}"#,
        policy_id
    )
}

pub fn secret_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Secret @policy(id: "{}", resource: "secrets") {{ content: String  classification: String }}"#,
        policy_id
    )
}
