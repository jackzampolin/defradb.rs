//! Cross-implementation conformance tests.
//!
//! These tests use YAML fixtures that can be shared between Rust and Go
//! implementations to ensure behavioral parity.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zanzibar::Did;

use acp::{
    MemoryZanzibarStore, PermissionEngine, Policy, Relation, RelationExpression, Relationship,
    Resource, Subject, SubjectRestriction, ZanzibarStore,
};

/// Schema for cross-implementation test fixtures.
///
/// This format is designed to be portable between Rust and Go implementations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestFixture {
    /// Name of this test fixture
    pub name: String,

    /// Description of what this test validates
    #[serde(default)]
    pub description: String,

    /// Policy definition
    pub policy: PolicyDef,

    /// Relationships to create
    #[serde(default)]
    pub relationships: Vec<RelationshipDef>,

    /// Permission checks to perform
    pub checks: Vec<PermissionCheck>,
}

/// Policy definition in portable format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDef {
    pub id: String,
    pub name: String,
    pub resources: Vec<ResourceDef>,
}

/// Resource definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDef {
    pub name: String,
    pub relations: Vec<RelationDef>,
}

/// Relation definition with expression string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationDef {
    pub name: String,
    /// Expression string (e.g., "_this", "owner", "parent->owner", "_this+owner")
    #[serde(default = "default_this")]
    pub expression: String,
    /// Subject restriction type
    #[serde(default)]
    pub subject_restriction: Option<SubjectRestrictionDef>,
}

fn default_this() -> String {
    "_this".to_string()
}

/// Subject restriction definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SubjectRestrictionDef {
    Entity,
    EntitySet { resource: String, relation: String },
    TypedWildcard { resource: String },
    Any,
}

/// Relationship tuple definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipDef {
    pub resource: String,
    pub object_id: String,
    pub relation: String,
    pub subject: SubjectDef,
}

/// Subject definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubjectDef {
    /// Entity subject: "did:key:..."
    Entity { did: String },
    /// EntitySet subject: "resource:object#relation"
    EntitySet {
        resource: String,
        object_id: String,
        relation: String,
    },
    /// Typed wildcard: "resource:*"
    TypedWildcard { resource_wildcard: String },
    /// Untyped wildcard: "*"
    Wildcard { wildcard: bool },
}

/// Permission check definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionCheck {
    pub resource: String,
    pub object_id: String,
    pub relation: String,
    pub subject_did: String,
    pub expected: bool,
    #[serde(default)]
    pub description: String,
}

impl TestFixture {
    /// Convert policy definition to actual Policy.
    pub fn to_policy(&self) -> Policy {
        let mut policy = Policy::new(&self.policy.id, &self.policy.name);

        for resource_def in &self.policy.resources {
            let mut resource = Resource::new(&resource_def.name);

            for relation_def in &resource_def.relations {
                let expression = RelationExpression::parse(&relation_def.expression)
                    .unwrap_or_else(|_| {
                        panic!("Failed to parse expression: {}", relation_def.expression)
                    });

                let mut relation = Relation::computed(&relation_def.name, expression);

                if let Some(restriction_def) = &relation_def.subject_restriction {
                    let restriction = match restriction_def {
                        SubjectRestrictionDef::Entity => SubjectRestriction::Entity,
                        SubjectRestrictionDef::EntitySet { resource, relation } => {
                            SubjectRestriction::EntitySet {
                                resource: resource.clone(),
                                relation: relation.clone(),
                            }
                        }
                        SubjectRestrictionDef::TypedWildcard { resource } => {
                            SubjectRestriction::TypedWildcard {
                                resource: resource.clone(),
                            }
                        }
                        SubjectRestrictionDef::Any => SubjectRestriction::Any,
                    };
                    relation = relation.with_restriction(restriction);
                }

                resource = resource.with_relation(relation);
            }

            policy = policy.with_resource(resource);
        }

        policy
    }

    /// Convert relationship definitions to actual Relationships.
    pub fn to_relationships(&self) -> Vec<Relationship> {
        self.relationships
            .iter()
            .map(|r| {
                let subject = match &r.subject {
                    SubjectDef::Entity { did } => {
                        Subject::Entity(Did::new(did).expect("Invalid DID"))
                    }
                    SubjectDef::EntitySet {
                        resource,
                        object_id,
                        relation,
                    } => Subject::EntitySet {
                        resource: resource.clone(),
                        object_id: object_id.clone(),
                        relation: relation.clone(),
                    },
                    SubjectDef::TypedWildcard { resource_wildcard } => Subject::TypedWildcard {
                        resource: resource_wildcard.clone(),
                    },
                    SubjectDef::Wildcard { .. } => Subject::Wildcard,
                };

                Relationship::new(&r.resource, &r.object_id, &r.relation, subject)
            })
            .collect()
    }
}

/// Run a test fixture and return results.
pub async fn run_fixture(fixture: &TestFixture) -> Vec<(PermissionCheck, bool, bool)> {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Set up policy
    let policy = fixture.to_policy();
    engine.add_policy(&policy);

    // Store relationships
    for rel in fixture.to_relationships() {
        store
            .store_relationship(&fixture.policy.id, &rel)
            .await
            .expect("Failed to store relationship");
    }

    // Run checks
    let mut results = Vec::new();
    for check in &fixture.checks {
        let did = Did::new(&check.subject_did).expect("Invalid DID in check");
        let result = engine
            .check(
                &fixture.policy.id,
                &check.resource,
                &check.object_id,
                &check.relation,
                &did,
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Permission check failed for {}.{}#{}",
                    check.resource, check.object_id, check.relation
                )
            });

        results.push((check.clone(), check.expected, result));
    }

    results
}

// =============================================================================
// Embedded Test Fixtures
// =============================================================================

const BASIC_OWNERSHIP_YAML: &str = r#"
name: basic_ownership
description: Basic owner permission check

policy:
  id: policy1
  name: Basic Ownership
  resources:
    - name: document
      relations:
        - name: owner
          expression: "_this"

relationships:
  - resource: document
    object_id: doc1
    relation: owner
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

checks:
  - resource: document
    object_id: doc1
    relation: owner
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "Owner should have owner relation"
  - resource: document
    object_id: doc1
    relation: owner
    subject_did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    expected: false
    description: "Non-owner should not have owner relation"
"#;

const COMPUTED_USERSET_YAML: &str = r#"
name: computed_userset
description: Owner implies reader through computed userset

policy:
  id: policy1
  name: Computed Userset
  resources:
    - name: document
      relations:
        - name: owner
          expression: "_this"
        - name: reader
          expression: "_this+owner"

relationships:
  - resource: document
    object_id: doc1
    relation: owner
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
  - resource: document
    object_id: doc1
    relation: reader
    subject:
      did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"

checks:
  - resource: document
    object_id: doc1
    relation: reader
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "Owner should be reader via computed userset"
  - resource: document
    object_id: doc1
    relation: reader
    subject_did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    expected: true
    description: "Direct reader should be reader"
"#;

const TUPLE_TO_USERSET_YAML: &str = r#"
name: tuple_to_userset
description: Folder owner can read file via parent relation

policy:
  id: policy1
  name: TTU Test
  resources:
    - name: folder
      relations:
        - name: owner
          expression: "_this"
    - name: file
      relations:
        - name: parent
          expression: "_this"
        - name: reader
          expression: "_this+parent->owner"

relationships:
  - resource: folder
    object_id: folder1
    relation: owner
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
  - resource: file
    object_id: file1
    relation: parent
    subject:
      resource: folder
      object_id: folder1
      relation: owner

checks:
  - resource: file
    object_id: file1
    relation: reader
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "Folder owner should read file via TTU"
  - resource: file
    object_id: file1
    relation: reader
    subject_did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    expected: false
    description: "Non-owner should not read file"
"#;

const INTERSECTION_YAML: &str = r#"
name: intersection
description: Must be both member AND approved

policy:
  id: policy1
  name: Intersection
  resources:
    - name: document
      relations:
        - name: member
          expression: "_this"
        - name: approved
          expression: "_this"
        - name: editor
          expression: "member&approved"

relationships:
  - resource: document
    object_id: doc1
    relation: member
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
  - resource: document
    object_id: doc1
    relation: approved
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
  - resource: document
    object_id: doc1
    relation: member
    subject:
      did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"

checks:
  - resource: document
    object_id: doc1
    relation: editor
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "User with both member AND approved should be editor"
  - resource: document
    object_id: doc1
    relation: editor
    subject_did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    expected: false
    description: "User with only member (not approved) should not be editor"
"#;

const DIFFERENCE_YAML: &str = r#"
name: difference
description: Members minus banned

policy:
  id: policy1
  name: Difference
  resources:
    - name: document
      relations:
        - name: member
          expression: "_this"
        - name: banned
          expression: "_this"
        - name: viewer
          expression: "member-banned"

relationships:
  - resource: document
    object_id: doc1
    relation: member
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
  - resource: document
    object_id: doc1
    relation: member
    subject:
      did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
  - resource: document
    object_id: doc1
    relation: banned
    subject:
      did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"

checks:
  - resource: document
    object_id: doc1
    relation: viewer
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "Non-banned member should be viewer"
  - resource: document
    object_id: doc1
    relation: viewer
    subject_did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    expected: false
    description: "Banned member should not be viewer"
"#;

const WILDCARD_YAML: &str = r#"
name: wildcard_access
description: Public access via wildcard

policy:
  id: policy1
  name: Wildcard
  resources:
    - name: document
      relations:
        - name: viewer
          expression: "_this"

relationships:
  - resource: document
    object_id: doc1
    relation: viewer
    subject:
      wildcard: true

checks:
  - resource: document
    object_id: doc1
    relation: viewer
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "Any user should access public document"
  - resource: document
    object_id: doc1
    relation: viewer
    subject_did: "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    expected: true
    description: "Another user should also access public document"
"#;

const EXPRESSION_PRECEDENCE_YAML: &str = r#"
name: expression_precedence
description: Left-to-right precedence for operators

policy:
  id: policy1
  name: Precedence
  resources:
    - name: document
      relations:
        - name: a
          expression: "_this"
        - name: b
          expression: "_this"
        - name: c
          expression: "_this"
        - name: result
          expression: "a+b&c"

relationships:
  - resource: document
    object_id: doc1
    relation: a
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
  - resource: document
    object_id: doc1
    relation: c
    subject:
      did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

checks:
  - resource: document
    object_id: doc1
    relation: result
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: true
    description: "a+b&c with left-to-right: (a+b)&c. User has a and c, so (true+false)&true = true&true = true"
"#;

const CYCLE_DETECTION_YAML: &str = r#"
name: cycle_detection
description: Cycles return false, not error

policy:
  id: policy1
  name: Cycle
  resources:
    - name: document
      relations:
        - name: relation_a
          expression: "relation_b"
        - name: relation_b
          expression: "relation_a"

relationships: []

checks:
  - resource: document
    object_id: doc1
    relation: relation_a
    subject_did: "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    expected: false
    description: "Cycle should return false (unauthorized), not error"
"#;

// =============================================================================
// Tests
// =============================================================================

fn parse_and_run_fixture(yaml: &str) -> TestFixture {
    serde_yaml::from_str(yaml).expect("Failed to parse YAML fixture")
}

#[tokio::test]
async fn test_fixture_basic_ownership() {
    let fixture = parse_and_run_fixture(BASIC_OWNERSHIP_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_computed_userset() {
    let fixture = parse_and_run_fixture(COMPUTED_USERSET_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_tuple_to_userset() {
    let fixture = parse_and_run_fixture(TUPLE_TO_USERSET_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_intersection() {
    let fixture = parse_and_run_fixture(INTERSECTION_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_difference() {
    let fixture = parse_and_run_fixture(DIFFERENCE_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_wildcard() {
    let fixture = parse_and_run_fixture(WILDCARD_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_expression_precedence() {
    let fixture = parse_and_run_fixture(EXPRESSION_PRECEDENCE_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

#[tokio::test]
async fn test_fixture_cycle_detection() {
    let fixture = parse_and_run_fixture(CYCLE_DETECTION_YAML);
    let results = run_fixture(&fixture).await;

    for (check, expected, actual) in results {
        assert_eq!(
            expected, actual,
            "Check failed: {} - expected {}, got {}",
            check.description, expected, actual
        );
    }
}

/// Test that YAML fixtures can be serialized and deserialized correctly.
#[test]
fn test_fixture_roundtrip() {
    let fixture = parse_and_run_fixture(BASIC_OWNERSHIP_YAML);
    let serialized = serde_yaml::to_string(&fixture).expect("Failed to serialize");
    let deserialized: TestFixture =
        serde_yaml::from_str(&serialized).expect("Failed to deserialize");

    assert_eq!(fixture.name, deserialized.name);
    assert_eq!(fixture.policy.id, deserialized.policy.id);
    assert_eq!(
        fixture.relationships.len(),
        deserialized.relationships.len()
    );
    assert_eq!(fixture.checks.len(), deserialized.checks.len());
}
