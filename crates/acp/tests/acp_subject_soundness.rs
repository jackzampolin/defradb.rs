//! Soundness floor for cross-object / userset relationship subjects.
//!
//! Before the public document-ACP API is widened to accept arbitrary subjects,
//! the validator must (1) recognise the empty-relation object-edge form that a
//! cross-object (TTU) target uses, and (2) actually enforce the `types:`
//! declared on a relation, so a relation never accepts a subject whose type the
//! policy did not authorise.

use acp::policy_yaml::{build_policy, parse_policy_yaml};
use acp::{Policy, Relation, Relationship, Resource, Subject};
use zanzibar::error::Error;
use zanzibar::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

// --- 1a: validate the empty-relation object-edge -------------------------

#[test]
fn validate_accepts_object_edge_to_existing_resource() {
    let policy = Policy::new("p", "Test")
        .with_resource(Resource::new("directory").with_relation(Relation::direct("reader")))
        .with_resource(Resource::new("file").with_relation(Relation::direct("parent")));

    // file:report#parent@directory:teamdir — a cross-object edge whose subject
    // carries NO relation (an object reference, not a userset).
    let edge = Relationship::new(
        "file",
        "report",
        "parent",
        Subject::entity_set("directory", "teamdir", ""),
    );

    assert!(
        edge.validate(&policy).is_ok(),
        "an object-edge to a declared resource must validate"
    );
}

#[test]
fn validate_rejects_object_edge_to_unknown_resource() {
    let policy = Policy::new("p", "Test")
        .with_resource(Resource::new("file").with_relation(Relation::direct("parent")));

    let edge = Relationship::new(
        "file",
        "report",
        "parent",
        Subject::entity_set("directory", "teamdir", ""),
    );

    assert!(
        matches!(
            edge.validate(&policy).unwrap_err(),
            Error::InvalidEntitySetReference { .. }
        ),
        "an object-edge to a resource the policy never declared must be rejected"
    );
}

#[test]
fn validate_still_rejects_userset_to_unknown_relation() {
    // The non-empty-relation (userset) branch must keep its existing behaviour:
    // a userset pointing at a relation the policy does not declare is invalid.
    let policy = Policy::new("p", "Test")
        .with_resource(Resource::new("group"))
        .with_resource(Resource::new("file").with_relation(Relation::direct("reader")));

    let userset = Relationship::new(
        "file",
        "report",
        "reader",
        Subject::entity_set("group", "hr", "nonexistent"),
    );

    assert!(
        matches!(
            userset.validate(&policy).unwrap_err(),
            Error::InvalidEntitySetReference { .. }
        ),
        "a userset to an undeclared relation must still be rejected"
    );
}

// --- 1b: thread `types:` into an enforced subject restriction ------------

const FS_POLICY: &str = r#"
name: filesystem
resources:
- name: directory
  permissions:
  - name: read
    expr: reader
  relations:
  - name: reader
    types: [actor]
- name: file
  permissions:
  - name: read
    expr: reader + parent->read
  relations:
  - name: reader
    types: [actor]
  - name: parent
    types: [directory]
"#;

fn fs_policy() -> Policy {
    let parsed = parse_policy_yaml(FS_POLICY).unwrap();
    build_policy(&parsed, 1).unwrap()
}

#[test]
fn actor_typed_relation_accepts_a_did() {
    let policy = fs_policy();
    let rel = Relationship::with_entity("file", "r1", "reader", test_did());
    assert!(rel.validate(&policy).is_ok());
}

#[test]
fn actor_typed_relation_still_accepts_all_actors_wildcard() {
    // Regression guard: `*` (all actors) must remain valid on an actor relation.
    let policy = fs_policy();
    let rel = Relationship::new("file", "r1", "reader", Subject::wildcard());
    assert!(
        rel.validate(&policy).is_ok(),
        "threading types: [actor] must not break existing '*' grants"
    );
}

#[test]
fn actor_typed_relation_rejects_an_object_subject() {
    let policy = fs_policy();
    let rel = Relationship::new(
        "file",
        "r1",
        "reader",
        Subject::entity_set("directory", "d1", ""),
    );
    assert!(
        matches!(
            rel.validate(&policy).unwrap_err(),
            Error::SubjectRestrictionViolation { .. }
        ),
        "an actor-typed relation must reject a cross-object subject"
    );
}

#[test]
fn directory_typed_relation_accepts_a_directory_object_edge() {
    let policy = fs_policy();
    let rel = Relationship::new(
        "file",
        "r1",
        "parent",
        Subject::entity_set("directory", "teamdir", ""),
    );
    assert!(rel.validate(&policy).is_ok());
}

#[test]
fn directory_typed_relation_rejects_an_actor_did() {
    let policy = fs_policy();
    let rel = Relationship::with_entity("file", "r1", "parent", test_did());
    assert!(
        matches!(
            rel.validate(&policy).unwrap_err(),
            Error::SubjectRestrictionViolation { .. }
        ),
        "a directory-typed relation must reject an actor DID"
    );
}

// A union relation declaring `types: [actor, group->participant]`. The userset
// type separator is acp_core's TTU operator `->`, not `#` (which is the
// tuple-subject grammar). Both a DID and a `group#participant` userset must
// validate; any other shape is rejected via the `AnyOf` union.
const UNION_POLICY: &str = r#"
name: union
resources:
- name: group
  relations:
  - name: participant
    types: [actor]
- name: doc
  permissions:
  - name: read
    expr: reader
  relations:
  - name: reader
    types: [actor, group->participant]
"#;

fn union_policy() -> Policy {
    let parsed = parse_policy_yaml(UNION_POLICY).unwrap();
    build_policy(&parsed, 1).unwrap()
}

#[test]
fn union_type_accepts_a_userset_via_arrow_separator() {
    let policy = union_policy();
    // Subject grammar uses `#`: group:hr#participant.
    let rel = Relationship::new(
        "doc",
        "d1",
        "reader",
        Subject::entity_set("group", "hr", "participant"),
    );
    assert!(
        rel.validate(&policy).is_ok(),
        "types: [.., group->participant] must accept a group#participant userset"
    );
}

#[test]
fn union_type_accepts_an_actor_did() {
    let policy = union_policy();
    let rel = Relationship::with_entity("doc", "d1", "reader", test_did());
    assert!(rel.validate(&policy).is_ok());
}

#[test]
fn union_type_rejects_an_unauthorized_shape() {
    let policy = union_policy();
    // A userset from a relation the union never authorised (`group#owner`).
    let rel = Relationship::new(
        "doc",
        "d1",
        "reader",
        Subject::entity_set("group", "hr", "owner"),
    );
    assert!(
        matches!(
            rel.validate(&policy).unwrap_err(),
            Error::SubjectRestrictionViolation { .. }
        ),
        "a userset outside the declared union must be rejected"
    );
}
