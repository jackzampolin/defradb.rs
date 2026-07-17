use super::super::helpers::{generate_collection_id, generate_field_id};
use super::super::warnings::DirectiveLocation;
use super::*;
use schema::{CType, FieldKind, ScalarKind};

#[test]
fn test_parse_simple_type() {
    let sdl = r#"
        type User {
            name: String
            age: Int
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    assert_eq!(collections.len(), 1);

    let user = &collections[0];
    assert_eq!(user.name, "User");
    // _docID + name + age = 3 fields
    assert_eq!(user.fields.len(), 3);

    let name_field = user.field_by_name("name").unwrap();
    assert_eq!(name_field.kind, FieldKind::string());

    let age_field = user.field_by_name("age").unwrap();
    assert_eq!(age_field.kind, FieldKind::int());
}

#[test]
fn test_parse_non_null_type_returns_error() {
    let sdl = r#"
        type Post {
            title: String!
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("NonNull fields are not currently supported"),
        "error should reject NonNull: {}",
        err
    );
}

#[test]
fn test_parse_array_type() {
    let sdl = r#"
        type User {
            tags: [String!]
            scores: [Int]
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    // [String!] -> non-nillable elements
    let tags = user.field_by_name("tags").unwrap();
    assert_eq!(tags.kind, FieldKind::string_array());

    // [Int] -> nillable elements
    let scores = user.field_by_name("scores").unwrap();
    assert_eq!(scores.kind, FieldKind::nillable_int_array());
}

#[test]
fn test_parse_crdt_directive() {
    let sdl = r#"
        type Counter {
            value: Int @crdt(type: "pncounter")
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let counter = &collections[0];

    let value = counter.field_by_name("value").unwrap();
    assert_eq!(value.crdt_type, CType::PnCounter);
}

#[test]
fn test_parse_immutable_directive() {
    let sdl = r#"
        type AgentDoc {
            agent_did: String @immutable
            body: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let agent_doc = &collections[0];

    let agent_did = agent_doc.field_by_name("agent_did").unwrap();
    assert!(agent_did.immutable);

    let body = agent_doc.field_by_name("body").unwrap();
    assert!(!body.immutable);
}

#[test]
fn test_parse_primary_directive() {
    let sdl = r#"
        type Post {
            author: User @primary
        }
        type User {
            name: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let post = collections.iter().find(|c| c.name == "Post").unwrap();

    let author = post.field_by_name("author").unwrap();
    assert!(author.is_primary);
}

#[test]
fn test_parse_relation() {
    let sdl = r#"
        type User {
            name: String
            posts: [Post]
        }
        type Post {
            title: String
            author: User
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    assert_eq!(collections.len(), 2);

    let user = collections.iter().find(|c| c.name == "User").unwrap();
    let posts_field = user.field_by_name("posts").unwrap();
    assert!(posts_field.kind.is_relation());
    assert!(posts_field.kind.is_array());

    let post = collections.iter().find(|c| c.name == "Post").unwrap();
    let author_field = post.field_by_name("author").unwrap();
    assert!(author_field.kind.is_relation());
    assert!(!author_field.kind.is_array());

    assert!(
        post.indexes.iter().all(|idx| {
            idx.fields
                .first()
                .is_none_or(|field| field.name != "_authorID")
        }),
        "one-to-many relation FK indexes require an explicit @index"
    );
}

#[test]
fn test_parse_self_reference() {
    let sdl = r#"
        type Category {
            name: String
            parent: Category
            children: [Category]
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let category = &collections[0];

    let parent = category.field_by_name("parent").unwrap();
    assert!(matches!(
        parent.kind,
        FieldKind::SelfRef {
            is_array: false,
            ..
        }
    ));

    let children = category.field_by_name("children").unwrap();
    assert!(matches!(
        children.kind,
        FieldKind::SelfRef { is_array: true, .. }
    ));
}

#[test]
fn test_parse_index_directive() {
    let sdl = r#"
        type User {
            email: String @index(unique: true)
            name: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    assert_eq!(user.indexes.len(), 1);
    let idx = &user.indexes[0];
    assert!(idx.unique);
    assert_eq!(idx.fields.len(), 1);
    assert_eq!(idx.fields[0].name, "email");
}

#[test]
fn test_parse_all_scalar_types() {
    let sdl = r#"
        type AllTypes {
            s: String
            i: Int
            f: Float
            f32: Float32
            f64: Float64
            b: Boolean
            id: ID
            dt: DateTime
            j: JSON
            blob: Blob
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let all = &collections[0];

    assert_eq!(all.field_by_name("s").unwrap().kind, FieldKind::string());
    assert_eq!(all.field_by_name("i").unwrap().kind, FieldKind::int());
    assert_eq!(all.field_by_name("f").unwrap().kind, FieldKind::float64());
    assert_eq!(all.field_by_name("f32").unwrap().kind, FieldKind::float32());
    assert_eq!(all.field_by_name("f64").unwrap().kind, FieldKind::float64());
    assert_eq!(all.field_by_name("b").unwrap().kind, FieldKind::bool());
    assert_eq!(all.field_by_name("id").unwrap().kind, FieldKind::doc_id());
    assert_eq!(all.field_by_name("dt").unwrap().kind, FieldKind::datetime());
    assert_eq!(all.field_by_name("j").unwrap().kind, FieldKind::json());
    assert_eq!(all.field_by_name("blob").unwrap().kind, FieldKind::blob());
}

// NOTE: test_parse_issue_example removed - the @primary directive behavior
// is validated through Go interop tests which are the source of truth for
// behavioral compatibility.

#[test]
fn test_parse_empty_sdl() {
    let sdl = "";
    let collections = parse_sdl(sdl).unwrap();
    assert!(collections.is_empty());
}

#[test]
fn test_parse_invalid_sdl() {
    let sdl = "not valid graphql { {";
    let result = parse_sdl(sdl);
    assert!(result.is_err());
}

#[test]
fn test_doc_id_always_present() {
    let sdl = r#"
        type Simple {
            name: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let simple = &collections[0];

    let doc_id = simple.field_by_name("_docID");
    assert!(doc_id.is_some());
    assert_eq!(doc_id.unwrap().kind, FieldKind::doc_id());
}

#[test]
fn test_collection_and_field_ids_generated() {
    let sdl = r#"
        type User {
            name: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    // collection_id and version_id should be non-empty
    assert!(!user.collection_id.is_empty());
    assert!(!user.version_id.is_empty());

    // field IDs should be non-empty
    for field in &user.fields {
        assert!(!field.id.is_empty());
    }
}

#[test]
fn test_relation_names_generated() {
    let sdl = r#"
        type Author {
            books: [Book]
        }
        type Book {
            author: Author
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();

    for coll in &collections {
        for field in coll.relation_fields() {
            // Relation fields should have relation names
            assert!(field.relation_name.is_some());
        }
    }
}

// =========================================================================
// Go Compatibility Tests
// =========================================================================

#[test]
fn test_relation_directive_explicit_name() {
    let sdl = r#"
        type User {
            posts: [Post] @relation(name: "user_authored_posts")
        }
        type Post {
            author: User @relation(name: "user_authored_posts") @primary
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();

    let user = collections.iter().find(|c| c.name == "User").unwrap();
    let posts = user.field_by_name("posts").unwrap();
    assert_eq!(
        posts.relation_name.as_deref(),
        Some("user_authored_posts"),
        "explicit @relation name should be used"
    );

    let post = collections.iter().find(|c| c.name == "Post").unwrap();
    let author = post.field_by_name("author").unwrap();
    assert_eq!(
        author.relation_name.as_deref(),
        Some("user_authored_posts"),
        "explicit @relation name should be used"
    );
    assert!(author.is_primary, "@primary should mark the primary side");
}

// NOTE: test_relation_name_auto_generation removed - relation naming conventions
// are validated through Go interop tests which are the source of truth for
// behavioral compatibility.

#[test]
fn test_default_directive_string() {
    let sdl = r#"
        type User {
            role: String @default(value: "member")
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    let role = user.field_by_name("role").unwrap();
    assert_eq!(
        role.default_value,
        Some(serde_json::Value::String("member".to_string()))
    );
}

#[test]
fn test_default_directive_int() {
    let sdl = r#"
        type Counter {
            count: Int @default(value: 0)
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let counter = &collections[0];

    let count = counter.field_by_name("count").unwrap();
    assert_eq!(
        count.default_value,
        Some(serde_json::Value::Number(0.into()))
    );
}

#[test]
fn test_default_directive_bool() {
    let sdl = r#"
        type Settings {
            enabled: Boolean @default(value: true)
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let settings = &collections[0];

    let enabled = settings.field_by_name("enabled").unwrap();
    assert_eq!(enabled.default_value, Some(serde_json::Value::Bool(true)));
}

#[test]
fn test_constraints_directive_array_size() {
    let sdl = r#"
        type Article {
            tags: [String!] @constraints(size: 10)
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let article = &collections[0];

    let tags = article.field_by_name("tags").unwrap();
    assert_eq!(tags.size, 10, "@constraints(size:) should set field.size");
}

#[test]
fn test_materialized_directive() {
    let sdl = r#"
        type CachedView @materialized {
            data: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let view = &collections[0];

    assert!(
        view.is_materialized,
        "@materialized should set is_materialized = true"
    );
}

#[test]
fn test_downsample_directive() {
    let sdl = r#"
        type CpuRollup @downsample(interval: "60s", timeField: "ts", retention: "168h") {
            ts: DateTime
            avg: Float
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let view = &collections[0];

    assert!(
        view.is_materialized,
        "@downsample should materialize the view"
    );
    assert_eq!(view.downsample_interval.as_deref(), Some("60s"));
    assert_eq!(view.downsample_time_field.as_deref(), Some("ts"));
    assert_eq!(view.downsample_retention.as_deref(), Some("168h"));
}

#[test]
fn test_downsample_requires_interval() {
    let sdl = r#"
        type CpuRollup @downsample(timeField: "ts") {
            ts: DateTime
            avg: Float
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("@downsample directive requires an 'interval' argument"),
        "expected missing interval validation error, got: {}",
        err
    );
}

#[test]
fn test_downsample_requires_time_field() {
    let sdl = r#"
        type CpuRollup @downsample(interval: "60s") {
            avg: Float
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("@downsample directive requires a string 'timeField' argument"),
        "expected missing timeField validation error, got: {}",
        err
    );
}

#[test]
fn test_downsample_rejects_empty_retention() {
    let sdl = r#"
        type CpuRollup @downsample(interval: "60s", timeField: "ts", retention: "") {
            ts: DateTime
            avg: Float
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("@downsample retention must not be empty"),
        "expected empty retention validation error, got: {}",
        err
    );
}

#[test]
fn test_branchable_directive() {
    let sdl = r#"
        type VersionedDoc @branchable {
            content: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let doc = &collections[0];

    assert!(
        doc.is_branchable,
        "@branchable should set is_branchable = true"
    );
}

#[test]
fn test_materialized_directive_with_if_false() {
    let sdl = r#"
        type NotCached @materialized(if: false) {
            data: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let view = &collections[0];

    assert!(
        !view.is_materialized,
        "@materialized(if: false) should set is_materialized = false"
    );
}

#[test]
fn test_float32_scalar_type() {
    let sdl = r#"
        type Sensor {
            temperature: Float32
            values: [Float32!]
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let sensor = &collections[0];

    let temp = sensor.field_by_name("temperature").unwrap();
    assert_eq!(temp.kind, FieldKind::float32());

    let values = sensor.field_by_name("values").unwrap();
    assert_eq!(values.kind, FieldKind::float32_array());
}

#[test]
fn test_multiple_field_indexes() {
    let sdl = r#"
        type User {
            email: String @index(unique: true)
            username: String @index(unique: true)
            age: Int @index
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    assert_eq!(user.indexes.len(), 3, "should have 3 separate indexes");

    let email_idx = user.indexes.iter().find(|i| i.fields[0].name == "email");
    assert!(email_idx.is_some());
    assert!(email_idx.unwrap().unique);

    let age_idx = user.indexes.iter().find(|i| i.fields[0].name == "age");
    assert!(age_idx.is_some());
    assert!(!age_idx.unwrap().unique);
}

#[test]
fn test_type_level_composite_index() {
    let sdl = r#"
        type User @index(fields: ["firstName", "lastName"], name: "full_name_idx") {
            firstName: String
            lastName: String
            email: String
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    assert_eq!(user.indexes.len(), 1);
    let idx = &user.indexes[0];
    assert_eq!(idx.name, "full_name_idx");
    assert_eq!(idx.fields.len(), 2);
    assert_eq!(idx.fields[0].name, "firstName");
    assert_eq!(idx.fields[1].name, "lastName");
}

#[test]
fn test_crdt_directive_variations() {
    let sdl = r#"
        type Counters {
            lww: Int @crdt(type: "lww")
            lwwRegister: Int @crdt(type: "LWW_REGISTER")
            pn: Int @crdt(type: "pncounter")
            pnCounter: Int @crdt(type: "PN_COUNTER")
            p: Int @crdt(type: "pcounter")
            pCounter: Int @crdt(type: "P_COUNTER")
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let counters = &collections[0];

    assert_eq!(
        counters.field_by_name("lww").unwrap().crdt_type,
        CType::LwwRegister
    );
    assert_eq!(
        counters.field_by_name("lwwRegister").unwrap().crdt_type,
        CType::LwwRegister
    );
    assert_eq!(
        counters.field_by_name("pn").unwrap().crdt_type,
        CType::PnCounter
    );
    assert_eq!(
        counters.field_by_name("pnCounter").unwrap().crdt_type,
        CType::PnCounter
    );
    assert_eq!(
        counters.field_by_name("p").unwrap().crdt_type,
        CType::PCounter
    );
    assert_eq!(
        counters.field_by_name("pCounter").unwrap().crdt_type,
        CType::PCounter
    );
}

#[test]
fn test_crdt_validation_fails_for_non_numeric() {
    let sdl = r#"
        type Article {
            title: String @crdt(type: "pncounter")
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let article = &collections[0];

    // Parsing succeeds, but validation should fail
    let result = article.validate();
    assert!(
        result.is_err(),
        "PnCounter on String should fail validation"
    );
}

#[test]
fn test_collection_ids_are_deterministic() {
    // With empty fields (matches Go's behavior for field-less collections)
    let id1 = generate_collection_id("User", &[], &HashMap::new());
    let id2 = generate_collection_id("User", &[], &HashMap::new());
    assert_eq!(id1, id2, "same type name should produce same collection ID");

    let id3 = generate_collection_id("Post", &[], &HashMap::new());
    assert_ne!(
        id1, id3,
        "different type names should produce different IDs"
    );
}

#[test]
fn test_collection_id_matches_go_when_link_string_order_differs_from_cid_order() {
    let collections = parse_sdl(
        r#"
        type Block {
            data: String
            idx: Int
        }
    "#,
    )
    .unwrap();

    let block = &collections[0];
    assert_eq!(
        block.collection_id,
        "bafyreiajfzj23wjpiiborrteeh3h6fmazttq2svb6vzmhxu3n3o2jyk4me",
    );
}

#[test]
fn test_field_ids_are_deterministic() {
    let string_kind = FieldKind::Scalar(ScalarKind::String);
    let id1 = generate_field_id("name", &string_kind, CType::LwwRegister);
    let id2 = generate_field_id("name", &string_kind, CType::LwwRegister);
    assert_eq!(id1, id2, "same field should produce same field ID");

    let id3 = generate_field_id("email", &string_kind, CType::LwwRegister);
    assert_ne!(
        id1, id3,
        "different field names should produce different IDs"
    );

    // Different types should produce different IDs
    let int_kind = FieldKind::Scalar(ScalarKind::Int);
    let id4 = generate_field_id("count", &string_kind, CType::LwwRegister);
    let id5 = generate_field_id("count", &int_kind, CType::LwwRegister);
    assert_ne!(
        id4, id5,
        "different field types should produce different IDs"
    );
}

#[test]
fn test_self_ref_collection_id_matches_go() {
    // Go's TestSchemaSelfReferenceSimple expects this CID for `type User { boss: User }`
    let sdl = r#"
        type User {
            boss: User
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    assert_eq!(
        collections[0].collection_id,
        "bafyreicuxpdrri4wwdknhbchhdii6tu4myqlhspv3s2c3pci7jt7qc3zua",
    );
}

#[test]
fn test_self_ref_complex_collection_id_matches_go() {
    // Self-ref schema with multiple relation fields and @primary
    let sdl = r#"
        type User {
            name: String
            age: Int
            boss: User @primary @relation(name: "boss_minion")
            minion: User @relation(name: "boss_minion")
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    assert_eq!(
        collections[0].collection_id,
        "bafyreibgdepgcg4y4odgoju4ac6bu5u2jejta6jg6pvzxblm5fnovsa3gi",
    );
}

#[test]
fn test_index_descending_direction() {
    let sdl = r#"
        type Event {
            timestamp: DateTime @index(direction: "DESC")
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let event = &collections[0];

    assert_eq!(event.indexes.len(), 1);
    assert!(event.indexes[0].fields[0].descending);
}

#[test]
fn test_composite_index_with_default_direction() {
    // Type-level @index with direction: DESC should apply to all fields
    let sdl = r#"
        type User @index(direction: DESC, includes: [{field: "name"}, {field: "age"}]) {
            name: String
            age: Int
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    assert_eq!(user.indexes.len(), 1);
    let idx = &user.indexes[0];
    assert_eq!(idx.fields.len(), 2);
    // Both fields should inherit DESC from the top-level direction
    assert!(idx.fields[0].descending, "name should be descending");
    assert!(idx.fields[1].descending, "age should be descending");
}

#[test]
fn test_composite_index_override_default_direction() {
    // Per-field direction should override top-level direction
    let sdl = r#"
        type User @index(direction: DESC, includes: [{field: "name"}, {field: "age", direction: ASC}]) {
            name: String
            age: Int
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let user = &collections[0];

    assert_eq!(user.indexes.len(), 1);
    let idx = &user.indexes[0];
    assert_eq!(idx.fields.len(), 2);
    // name inherits DESC, age overrides to ASC
    assert!(idx.fields[0].descending, "name should be descending");
    assert!(
        !idx.fields[1].descending,
        "age should be ascending (override)"
    );
}

// =========================================================================
// Error Path Tests
// =========================================================================

#[test]
fn test_crdt_directive_unknown_type_returns_error() {
    let sdl = r#"
        type Counter {
            value: Int @crdt(type: "invalid_crdt")
        }
    "#;
    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Argument \"type\" has invalid value"),
        "error should mention invalid CRDT type argument: {}",
        err
    );
}

#[test]
fn test_crdt_directive_missing_type_argument_returns_error() {
    let sdl = r#"
        type Counter {
            value: Int @crdt
        }
    "#;
    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("requires 'type' argument"),
        "error should mention missing type argument: {}",
        err
    );
}

#[test]
fn test_default_directive_missing_value_returns_error() {
    let sdl = r#"
        type User {
            role: String @default
        }
    "#;
    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("requires a value argument"),
        "error should mention missing value: {}",
        err
    );
}

#[test]
fn test_default_directive_legacy_argument_returns_error() {
    let sdl = r#"
        type User {
            role: String @default(string: "test")
        }
    "#;
    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("unknown @default argument"),
        "error should mention unknown argument: {}",
        err
    );
}

#[test]
fn test_default_directive_invalid_json_returns_error() {
    let sdl = r#"
        type Config {
            settings: JSON @default(value: "{ invalid json }")
        }
    "#;
    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("invalid JSON"),
        "error should mention invalid JSON: {}",
        err
    );
}

#[test]
fn test_constraints_directive_negative_size_returns_error() {
    let sdl = r#"
        type Article {
            tags: [String!] @constraints(size: -1)
        }
    "#;
    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("non-negative"),
        "error should mention non-negative requirement: {}",
        err
    );
}

#[test]
fn test_default_directive_float() {
    let sdl = r#"
        type Measurement {
            value: Float @default(value: 3.15)
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    let m = &collections[0];
    let value = m.field_by_name("value").unwrap();
    assert!(value.default_value.is_some());
    if let Some(serde_json::Value::Number(n)) = &value.default_value {
        assert!((n.as_f64().unwrap() - 3.15).abs() < 0.001);
    } else {
        panic!("expected number default value");
    }
}

#[test]
fn test_default_directive_json() {
    let sdl = r#"
        type Config {
            settings: JSON @default(value: "{\"key\": \"value\"}")
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    let config = &collections[0];
    let settings = config.field_by_name("settings").unwrap();
    assert!(settings.default_value.is_some());
    // Go stores JSON defaults as string literals, so the value is a JSON string
    if let Some(serde_json::Value::String(s)) = &settings.default_value {
        assert_eq!(s, r#"{"key": "value"}"#);
    } else {
        panic!("expected string default value");
    }
}

#[test]
fn test_default_directive_float32() {
    let sdl = r#"
        type Sensor {
            temp: Float32 @default(value: 25.5)
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    let sensor = &collections[0];
    let temp = sensor.field_by_name("temp").unwrap();
    assert!(temp.default_value.is_some());
    if let Some(serde_json::Value::Number(n)) = &temp.default_value {
        assert!((n.as_f64().unwrap() - 25.5).abs() < 0.001);
    } else {
        panic!("expected number default value");
    }
}

#[test]
fn test_default_directive_datetime() {
    let sdl = r#"
        type Event {
            created: DateTime @default(value: "2024-01-15T10:30:00Z")
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    let event = &collections[0];
    let created = event.field_by_name("created").unwrap();
    assert_eq!(
        created.default_value,
        Some(serde_json::Value::String(
            "2024-01-15T10:30:00Z".to_string()
        ))
    );
}

#[test]
fn test_default_directive_blob() {
    let sdl = r#"
        type Document {
            data: Blob @default(value: "SGVsbG8gV29ybGQ=")
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    let doc = &collections[0];
    let data = doc.field_by_name("data").unwrap();
    assert_eq!(
        data.default_value,
        Some(serde_json::Value::String("SGVsbG8gV29ybGQ=".to_string()))
    );
}

#[test]
fn test_whitespace_only_sdl() {
    let sdl = "   \n\t\n   ";
    let collections = parse_sdl(sdl).unwrap();
    assert!(collections.is_empty());
}

#[test]
fn test_branchable_directive_with_if_false() {
    let sdl = r#"
        type Doc @branchable(if: false) {
            content: String
        }
    "#;
    let collections = parse_sdl(sdl).unwrap();
    let doc = &collections[0];
    assert!(!doc.is_branchable);
}

// =========================================================================
// Issue #28 & #29: Warnings and Validation Tests
// =========================================================================

#[test]
fn test_unknown_field_directive_emits_warning() {
    let sdl = r#"
        type User {
            name: String @unknownDirective
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirective {
            directive_name,
            location,
            type_name,
            field_name,
        } => {
            assert_eq!(directive_name, "unknownDirective");
            assert_eq!(*location, DirectiveLocation::Field);
            assert_eq!(type_name, "User");
            assert_eq!(field_name.as_deref(), Some("name"));
        }
        other => panic!("expected UnknownDirective warning, got {:?}", other),
    }
}

#[test]
fn test_unknown_type_directive_emits_warning() {
    let sdl = r#"
        type User @futureFeature {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirective {
            directive_name,
            location,
            type_name,
            field_name,
        } => {
            assert_eq!(directive_name, "futureFeature");
            assert_eq!(*location, DirectiveLocation::Type);
            assert_eq!(type_name, "User");
            assert!(field_name.is_none());
        }
        other => panic!("expected UnknownDirective warning, got {:?}", other),
    }
}

#[test]
fn test_unknown_directive_argument_emits_warning() {
    let sdl = r#"
        type User {
            email: String @index(unique: true, unknownArg: "value")
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            type_name,
            field_name,
        } => {
            assert_eq!(directive_name, "index");
            assert_eq!(argument_name, "unknownArg");
            assert_eq!(type_name, "User");
            assert_eq!(field_name.as_deref(), Some("email"));
        }
        other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
    }
}

#[test]
fn test_multiple_unknown_directives_emit_multiple_warnings() {
    let sdl = r#"
        type User @futureTypeDirective @anotherUnknown {
            name: String @customDirective
            age: Int @anotherCustom
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    // 2 unknown type directives + 2 unknown field directives
    assert_eq!(output.warnings.len(), 4);
}

#[test]
fn test_known_directives_no_warnings() {
    let sdl = r#"
        type User @materialized @branchable {
            name: String @index(unique: true)
            age: Int @crdt(type: "pncounter")
            role: String @default(value: "user")
            agent_did: String @immutable
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert!(
        output.warnings.is_empty(),
        "known directives should not emit warnings: {:?}",
        output.warnings
    );
}

#[test]
fn test_policy_directive_requires_id() {
    let sdl = r#"
        type User @policy(resource: "users") {
            name: String
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("policyID must not be empty"),
        "error should mention missing id argument: {}",
        err
    );
}

#[test]
fn test_policy_directive_requires_resource() {
    let sdl = r#"
        type User @policy(id: "policy123") {
            name: String
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("resource name must not be empty"),
        "error should mention missing resource argument: {}",
        err
    );
}

#[test]
fn test_policy_directive_valid() {
    let sdl = r#"
        type User @policy(id: "policy123", resource: "users") {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert!(output.warnings.is_empty());
}

#[test]
fn test_composite_index_unknown_field_returns_error() {
    let sdl = r#"
        type User @index(fields: ["name", "nonexistent"]) {
            name: String
            age: Int
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("unknown field 'nonexistent'"),
        "error should mention unknown field: {}",
        err
    );
}

#[test]
fn test_composite_index_valid_fields() {
    let sdl = r#"
        type User @index(fields: ["name", "age"]) {
            name: String
            age: Int
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert!(output.warnings.is_empty());

    let user = &output.collections[0];
    assert_eq!(user.indexes.len(), 1);
    assert_eq!(user.indexes[0].fields.len(), 2);
}

#[test]
fn test_warning_display_format() {
    let warning = ParseWarning::UnknownDirective {
        directive_name: "custom".to_string(),
        location: DirectiveLocation::Field,
        type_name: "User".to_string(),
        field_name: Some("name".to_string()),
    };

    let display = warning.to_string();
    assert!(display.contains("@custom"));
    assert!(display.contains("User.name"));
    assert!(display.contains("forward compatibility"));
}

#[test]
fn test_warning_display_format_type_level() {
    let warning = ParseWarning::UnknownDirective {
        directive_name: "future".to_string(),
        location: DirectiveLocation::Type,
        type_name: "User".to_string(),
        field_name: None,
    };

    let display = warning.to_string();
    assert!(display.contains("@future"));
    assert!(display.contains("type User"));
}

#[test]
fn test_unknown_argument_warning_display() {
    let warning = ParseWarning::UnknownDirectiveArgument {
        directive_name: "index".to_string(),
        argument_name: "badArg".to_string(),
        type_name: "User".to_string(),
        field_name: Some("email".to_string()),
    };

    let display = warning.to_string();
    assert!(display.contains("badArg"));
    assert!(display.contains("@index"));
    assert!(display.contains("User.email"));
}

// =========================================================================
// Additional Test Coverage (PR Review Gaps)
// =========================================================================

#[test]
fn test_unknown_argument_on_type_directive_emits_warning() {
    let sdl = r#"
        type User @materialized(unknownArg: true) {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            field_name,
            ..
        } => {
            assert_eq!(directive_name, "materialized");
            assert_eq!(argument_name, "unknownArg");
            assert!(field_name.is_none()); // type-level, not field-level
        }
        other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
    }
}

#[test]
fn test_unknown_argument_on_downsample_directive_emits_warning() {
    let sdl = r#"
        type CpuRollup @downsample(interval: "60s", timeField: "ts", extraArg: "value") {
            ts: DateTime
            avg: Float
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            ..
        } => {
            assert_eq!(directive_name, "downsample");
            assert_eq!(argument_name, "extraArg");
        }
        other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
    }
}

#[test]
fn test_unknown_argument_on_branchable_directive() {
    let sdl = r#"
        type User @branchable(if: true, extraArg: "value") {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            ..
        } => {
            assert_eq!(directive_name, "branchable");
            assert_eq!(argument_name, "extraArg");
        }
        other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
    }
}

#[test]
fn test_policy_directive_unknown_argument_emits_warning() {
    let sdl = r#"
        type User @policy(id: "p1", resource: "users", unknownArg: "value") {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            ..
        } => {
            assert_eq!(directive_name, "policy");
            assert_eq!(argument_name, "unknownArg");
        }
        other => panic!("expected UnknownDirectiveArgument warning, got {:?}", other),
    }
}

#[test]
fn test_embedding_directive_parses_config() {
    let sdl = r#"
        type Document {
            content: String
            content_v: [Float32!] @embedding(provider: "openai", model: "ada", url: "http://localhost:8080", fields: ["content"])
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 0);

    let col = &output.collections[0];
    assert_eq!(col.vector_embeddings.len(), 1);
    assert_eq!(col.vector_embeddings[0].field_name, "content_v");
    assert_eq!(col.vector_embeddings[0].provider, "openai");
    assert_eq!(col.vector_embeddings[0].model, "ada");
    assert_eq!(col.vector_embeddings[0].url, "http://localhost:8080");
    assert_eq!(col.vector_embeddings[0].fields, vec!["content"]);
}

#[test]
fn test_embedding_directive_unknown_argument_emits_warning() {
    let sdl = r#"
        type Document {
            content: String @embedding(provider: "openai", unknownArg: "x")
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    // Should have UnknownDirectiveArgument warning only
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            ..
        } => {
            assert_eq!(directive_name, "embedding");
            assert_eq!(argument_name, "unknownArg");
        }
        other => panic!("expected UnknownDirectiveArgument, got {:?}", other),
    }
}

#[test]
fn test_encrypted_index_directive_emits_unimplemented_warning() {
    let sdl = r#"
        type Secret {
            data: String @encryptedIndex(type: "match")
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    // @encryptedIndex is recognized and sets the encrypted_index flag (no warning)
    assert_eq!(output.warnings.len(), 0);
    assert_eq!(output.collections[0].encrypted_indexes.len(), 1);
}

#[test]
fn test_encrypted_index_unknown_argument_emits_warning() {
    let sdl = r#"
        type Secret {
            data: String @encryptedIndex(type: "match", badArg: true)
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    // Should have UnknownDirectiveArgument only (encryptedIndex is implemented, not unimplemented)
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            ..
        } => {
            assert_eq!(directive_name, "encryptedIndex");
            assert_eq!(argument_name, "badArg");
        }
        other => panic!("expected UnknownDirectiveArgument, got {:?}", other),
    }
}

#[test]
fn test_default_float32_wrong_type_returns_error() {
    let sdl = r#"
        type Sensor {
            temp: Float32 @default(value: "not a float")
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("has invalid value"),
        "error should mention invalid value: {}",
        err
    );
}

#[test]
fn test_default_datetime_wrong_type_returns_error() {
    let sdl = r#"
        type Event {
            created: DateTime @default(value: 12345)
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("has invalid value"),
        "error should mention invalid value: {}",
        err
    );
}

#[test]
fn test_default_blob_wrong_type_returns_error() {
    let sdl = r#"
        type Document {
            data: Blob @default(value: 12345)
        }
    "#;

    let result = parse_sdl(sdl);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("has invalid value"),
        "error should mention invalid value: {}",
        err
    );
}

#[test]
fn test_default_directive_value_for_all_scalar_types() {
    let sdl = r#"
        type Defaults {
            active: Boolean @default(value: true)
            age: Int @default(value: 40)
            points: Float @default(value: 10)
            points32: Float32 @default(value: 11.5)
            points64: Float64 @default(value: 12)
            name: String @default(value: "Bob")
            created: DateTime @default(value: "2000-07-23T03:00:00-00:00")
            metadata: JSON @default(value: "{\"one\":1}")
            image: Blob @default(value: "ff0099")
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let defaults = &collections[0];
    for (field, expected) in [
        ("active", serde_json::json!(true)),
        ("age", serde_json::json!(40)),
        ("points", serde_json::json!(10)),
        ("points32", serde_json::json!(11.5)),
        ("points64", serde_json::json!(12)),
        ("name", serde_json::json!("Bob")),
        ("created", serde_json::json!("2000-07-23T03:00:00Z")),
        ("metadata", serde_json::json!("{\"one\":1}")),
        ("image", serde_json::json!("ff0099")),
    ] {
        assert_eq!(
            defaults.field_by_name(field).unwrap().default_value,
            Some(expected),
            "unexpected default for {field}"
        );
    }
}

#[test]
fn test_default_directive_value_uses_field_type_coercion() {
    let result = parse_sdl(
        r#"
            type Defaults {
                age: Int @default(value: "forty")
            }
        "#,
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains(r#"Argument "value" has invalid value "forty""#),
        "unexpected error: {err}"
    );
}

#[test]
fn test_default_directive_value_rejects_unsupported_field_type() {
    let result = parse_sdl(
        r#"
            type Defaults {
                externalID: ID @default(value: "bae-example")
            }
        "#,
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("default value is not allowed for this field type"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_type_level_index_unknown_argument_emits_warning() {
    let sdl = r#"
        type User @index(fields: ["name"], unknownArg: "value") {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnknownDirectiveArgument {
            directive_name,
            argument_name,
            field_name,
            ..
        } => {
            assert_eq!(directive_name, "index");
            assert_eq!(argument_name, "unknownArg");
            assert!(field_name.is_none()); // type-level
        }
        other => panic!("expected UnknownDirectiveArgument, got {:?}", other),
    }
}

// =========================================================================
// InvalidArgumentType Warning Tests
// =========================================================================

#[test]
fn test_invalid_bool_argument_type_emits_warning() {
    let sdl = r#"
        type User @materialized(if: "yes") {
            name: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::InvalidArgumentType {
            directive_name,
            argument_name,
            expected_type,
            ..
        } => {
            assert_eq!(directive_name, "materialized");
            assert_eq!(argument_name, "if");
            assert_eq!(expected_type, "boolean");
        }
        other => panic!("expected InvalidArgumentType, got {:?}", other),
    }

    // Should still work with default value (true)
    assert!(output.collections[0].is_materialized);
}

#[test]
fn test_invalid_int_argument_type_emits_warning() {
    let sdl = r#"
        type User {
            name: String @constraints(size: "ten")
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::InvalidArgumentType {
            directive_name,
            argument_name,
            expected_type,
            ..
        } => {
            assert_eq!(directive_name, "constraints");
            assert_eq!(argument_name, "size");
            assert_eq!(expected_type, "integer");
        }
        other => panic!("expected InvalidArgumentType, got {:?}", other),
    }
}

#[test]
fn test_invalid_argument_type_warning_display() {
    let warning = ParseWarning::InvalidArgumentType {
        directive_name: "index".to_string(),
        argument_name: "unique".to_string(),
        expected_type: "boolean".to_string(),
        type_name: "User".to_string(),
        field_name: Some("email".to_string()),
    };

    let display = warning.to_string();
    assert!(display.contains("unique"));
    assert!(display.contains("@index"));
    assert!(display.contains("User.email"));
    assert!(display.contains("boolean"));
}

#[test]
fn test_unimplemented_directive_warning_display() {
    let warning = ParseWarning::UnimplementedDirective {
        directive_name: "embedding".to_string(),
        type_name: "Document".to_string(),
        field_name: Some("content".to_string()),
    };

    let display = warning.to_string();
    assert!(display.contains("@embedding"));
    assert!(display.contains("Document.content"));
    assert!(display.contains("not yet implemented"));
}

#[test]
fn test_field_policy_directive_emits_unimplemented_warning() {
    let sdl = r#"
        type User {
            name: String @policy(id: "p1", resource: "r1")
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    assert_eq!(output.warnings.len(), 1);

    match &output.warnings[0] {
        ParseWarning::UnimplementedDirective {
            directive_name,
            type_name,
            field_name,
        } => {
            assert_eq!(directive_name, "policy");
            assert_eq!(type_name, "User");
            assert_eq!(field_name.as_deref(), Some("name"));
        }
        other => panic!("expected UnimplementedDirective, got {:?}", other),
    }
}

#[test]
fn test_index_with_includes_argument_no_warning() {
    let sdl = r#"
        type User @index(fields: ["name"], includes: ["email"]) {
            name: String
            email: String
        }
    "#;

    let output = parse_sdl_with_warnings(sdl).unwrap();
    assert_eq!(output.collections.len(), 1);
    // includes is a known argument, should not trigger warning
    assert!(
        output.warnings.is_empty(),
        "includes is a known argument but got warnings: {:?}",
        output.warnings
    );
}

// =========================================================================
// Go Interoperability - Field Ordering Tests
// =========================================================================

#[test]
fn test_fields_sorted_alphabetically_after_docid() {
    // Go sorts fields alphabetically after _docID
    // For "name, age" input order, Go outputs [_docID, age, name]
    let sdl = r#"
        type Users {
            name: String
            age: Int
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let users = &collections[0];

    // Verify field order: _docID first, then alphabetical
    let field_names: Vec<&str> = users.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        field_names,
        vec!["_docID", "age", "name"],
        "Fields should be sorted alphabetically after _docID"
    );
}

#[test]
fn test_collection_cid_matches_go_with_sorted_fields() {
    // This CID was generated by Go DefraDB for:
    // type Users { name: String, age: Int }
    // With fields sorted as [_docID, age, name]
    //
    // Go debug output (from running TestDebugUsersCIDGeneration):
    // Collection 'Users' (p=4, 3 field links): bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq
    //
    // Note: This CID comes from Go's actual AddSchema behavior, not the debug test
    // which manually specifies field order. The actual AddSchema sorts fields.
    const GO_EXPECTED_CID: &str = "bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq";

    let sdl = r#"
        type Users {
            name: String
            age: Int
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let users = &collections[0];

    // Print debug info for diagnosing CID mismatches
    println!(
        "Field order: {:?}",
        users.fields.iter().map(|f| &f.name).collect::<Vec<_>>()
    );
    println!("Collection CID: {}", users.collection_id);
    println!("Expected (Go): {}", GO_EXPECTED_CID);

    assert_eq!(
        users.collection_id, GO_EXPECTED_CID,
        "Collection CID should match Go DefraDB"
    );
}

#[test]
fn test_secondary_relation_is_primary_false() {
    // This tests the exact schema from the failing FFI test
    // TestQueryOneToOne_WithRelationIDFromSecondarySide
    let sdl = r#"
        type Book {
            name: String
            author: Author
        }
        type Author {
            name: String
            published: Book @primary
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();

    let book = collections.iter().find(|c| c.name == "Book").unwrap();
    let author_field = book.field_by_name("author").unwrap();

    // Book.author should be SECONDARY (is_primary = false) because
    // Author.published has @primary directive
    assert!(
        !author_field.is_primary,
        "Book.author should be secondary (is_primary=false) because Author.published has @primary"
    );

    // Verify _authorID field exists on Book (created for all single-object relations)
    let author_id_field = book.field_by_name("_authorID");
    assert!(
        author_id_field.is_some(),
        "Book should have implicit _authorID field"
    );

    // _authorID should also be secondary (empty field_id)
    let author_id_field = author_id_field.unwrap();
    assert!(
        author_id_field.id.is_empty(),
        "_authorID should have empty field_id (secondary)"
    );
    assert!(
        !author_id_field.is_primary,
        "_authorID should be secondary (is_primary=false)"
    );

    // Verify Author.published is primary
    let author = collections.iter().find(|c| c.name == "Author").unwrap();
    let published_field = author.field_by_name("published").unwrap();
    assert!(
        published_field.is_primary,
        "Author.published should be primary (has @primary directive)"
    );

    let published_idx = author
        .indexes
        .iter()
        .find(|idx| {
            idx.fields
                .first()
                .is_some_and(|field| field.name == "_publishedID")
        })
        .expect("expected auto-created unique FK index on _publishedID");
    assert!(
        published_idx.unique,
        "one-to-one primary relation should auto-create a unique FK index"
    );
}

#[test]
fn test_one_to_many_collection_ids_match_go() {
    let sdl = r#"
        type Book {
            name: String
            rating: Float
            author: Author
        }
        type Author {
            name: String
            age: Int
            verified: Boolean
            published: [Book]
        }
    "#;

    let collections = parse_sdl(sdl).unwrap();
    let book = collections.iter().find(|c| c.name == "Book").unwrap();
    let author = collections.iter().find(|c| c.name == "Author").unwrap();

    assert_eq!(
        book.collection_id,
        "bafyreihpq2q7a7bgpmp54uwzpwomrmzar77qu4ncjrukumbj66pxomrlsq",
    );
    assert_eq!(
        author.collection_id,
        "bafyreibsjnlzaqfu6lq2njqjfgot2p4lwjhoxp63karkxzfu7flft4fohy",
    );
    assert_eq!(
        book.field_by_name("author")
            .unwrap()
            .kind
            .relation_collection_id(),
        Some(author.collection_id.as_str()),
    );
    assert_eq!(
        author
            .field_by_name("published")
            .unwrap()
            .kind
            .relation_collection_id(),
        Some(book.collection_id.as_str()),
    );
}
