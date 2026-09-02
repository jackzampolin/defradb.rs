use std::fmt::Display;

use integration_test::{DefraClient, TestCluster};

#[path = "cross_runtime_schema_validation/stateful.rs"]
mod stateful;

struct AddCase {
    validator: &'static str,
    sdl: &'static str,
    accepted: bool,
}

struct PatchCase {
    validator: &'static str,
    sdl: &'static str,
    patch: &'static str,
    accepted: bool,
}

fn assert_outcome<RT, RE, GT, GE>(
    case: &str,
    expected: bool,
    rust: &Result<RT, RE>,
    go: &Result<GT, GE>,
) where
    RE: Display,
    GE: Display,
{
    let rust_accepted = rust.is_ok();
    let go_accepted = go.is_ok();
    assert_eq!(
        rust_accepted,
        go_accepted,
        "{case}: Rust and Go outcomes differ; Rust={:?}, Go={:?}",
        rust.as_ref().err().map(ToString::to_string),
        go.as_ref().err().map(ToString::to_string),
    );
    assert_eq!(
        rust_accepted,
        expected,
        "{case}: unexpected shared outcome; Rust={:?}, Go={:?}",
        rust.as_ref().err().map(ToString::to_string),
        go.as_ref().err().map(ToString::to_string),
    );
}

fn purge_both(rust: &DefraClient, go: &DefraClient) {
    rust.purge().expect("purge Rust node");
    go.purge().expect("purge Go node");
}

#[tokio::test]
async fn go_schema_creation_validation_parity() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_development()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);

    let cases = [
        AddCase {
            validator: "valid schema",
            sdl: "type ValidSchema { name: String score: Int @default(value: 1) }",
            accepted: true,
        },
        AddCase {
            validator: "valid relation",
            sdl: r#"
                type ValidAuthor {
                    books: [ValidBook] @relation(name: "valid_author_books")
                }
                type ValidBook {
                    author: ValidAuthor @primary @relation(name: "valid_author_books")
                }
            "#,
            accepted: true,
        },
        AddCase {
            validator: "validateRelationPointsToValidKind",
            sdl: "type InvalidRelationTarget { missing: MissingType @primary }",
            accepted: false,
        },
        AddCase {
            validator: "validateSecondaryFieldsPairUp",
            sdl: r#"
                type MissingPrimary {
                    boss: MissingPrimary @relation(name: "missing_primary")
                    report: MissingPrimary @relation(name: "missing_primary")
                }
            "#,
            accepted: false,
        },
        AddCase {
            validator: "validateSingleSidePrimary",
            sdl: r#"
                type MultiplePrimaries {
                    boss: MultiplePrimaries @primary @relation(name: "multiple_primaries")
                    report: MultiplePrimaries @primary @relation(name: "multiple_primaries")
                }
            "#,
            accepted: false,
        },
        AddCase {
            validator: "validateCollectionDefinitionPolicyDesc",
            sdl: r#"type PolicyWithoutACP @policy(id: "missing", resource: "records") { name: String }"#,
            accepted: false,
        },
        AddCase {
            validator: "validateTypeAndKindCompatible",
            sdl: r#"type IncompatibleCRDT { enabled: Boolean @crdt(type: "pncounter") }"#,
            accepted: false,
        },
        AddCase {
            validator: "validateFieldNotDuplicated",
            sdl: "type DuplicateField { name: String name: Int }",
            accepted: false,
        },
        AddCase {
            validator: "validateCollectionNameUnique",
            sdl: "type DuplicateCollection { first: String } type DuplicateCollection { second: Int }",
            accepted: false,
        },
        AddCase {
            validator: "validateRelationNameUnique",
            sdl: r#"
                type DuplicateRelationName {
                    boss: Self @primary @relation(name: "duplicate_relation")
                    report: Self @relation(name: "duplicate_relation")
                    mentor: Self @primary @relation(name: "duplicate_relation")
                    student: Self @relation(name: "duplicate_relation")
                }
            "#,
            accepted: false,
        },
        AddCase {
            validator: "valid self-reference normalization",
            sdl: "type InvalidSelfReference { parent: InvalidSelfReference @primary }",
            accepted: true,
        },
        AddCase {
            validator: "validateCollectionMaterialized",
            sdl: "type DematerializedCollection @materialized(if: false) { name: String }",
            accepted: false,
        },
        AddCase {
            validator: "validateCollectionFieldDefaultValue",
            sdl: r#"type InvalidDefault { score: Int @default(value: "bad") }"#,
            accepted: false,
        },
        AddCase {
            validator: "validateEmbeddingAndKindCompatible",
            sdl: r#"
                type InvalidEmbeddingKind {
                    source: String
                    vector: String @embedding(provider: "openai", model: "ada", fields: ["source"])
                }
            "#,
            accepted: false,
        },
        AddCase {
            validator: "validateEmbeddingFieldsForGeneration",
            sdl: r#"
                type InvalidEmbeddingField {
                    vector: [Float32!] @embedding(provider: "openai", model: "ada", fields: ["missing"])
                }
            "#,
            accepted: false,
        },
        AddCase {
            validator: "validateEmbeddingProviderAndModel",
            sdl: r#"
                type InvalidEmbeddingProvider {
                    source: String
                    vector: [Float32!] @embedding(provider: "unknown", model: "ada", fields: ["source"])
                }
            "#,
            accepted: false,
        },
    ];

    for case in cases {
        let rust_result = rust.schema_add(case.sdl);
        let go_result = go.schema_add(case.sdl);
        assert_outcome(case.validator, case.accepted, &rust_result, &go_result);
        purge_both(&rust, &go);
    }
}

#[tokio::test]
async fn go_schema_patch_validation_parity() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);

    let cases = [
        PatchCase {
            validator: "valid nullable field addition",
            sdl: "type PatchValid { name: String }",
            patch: r#"[{"op":"add","path":"/PatchValid/Fields/-","value":{"Name":"extra","Kind":"String"}}]"#,
            accepted: true,
        },
        PatchCase {
            validator: "validateSourcesNotRedefined",
            sdl: "type PatchSources { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchSources/PreviousVersion","value":{"SourceCollectionID":"bafkreibifvyfr6qvb6wx4v4cogvcdksb3v7vniaon7hdzzqb62cotpmlc4"}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateIndexesNotModified",
            sdl: "type PatchIndexes { name: String @index }",
            patch: r#"[{"op":"replace","path":"/PatchIndexes/Indexes","value":[]}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateEncryptedIndexesNotModified",
            sdl: "type PatchEncryptedIndexes { secret: String @encryptedIndex }",
            patch: r#"[{"op":"replace","path":"/PatchEncryptedIndexes/EncryptedIndexes","value":[]}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validatePolicyNotModified",
            sdl: "type PatchPolicy { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchPolicy/Policy","value":{}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateIDNotEmpty",
            sdl: "type PatchEmptyID { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchEmptyID/CollectionID","value":""}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionIDNotMutated",
            sdl: "type PatchCollectionID { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchCollectionID/CollectionID","value":"bafkreibifvyfr6qvb6wx4v4cogvcdksb3v7vniaon7hdzzqb62cotpmlc4"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateFieldNotMutated",
            sdl: "type PatchFieldMutation { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchFieldMutation/Fields/1/Kind","value":"Int"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateFieldNotMoved",
            sdl: "type PatchFieldMove { first: String second: Int }",
            patch: r#"[{"op":"move","from":"/PatchFieldMove/Fields/1","path":"/PatchFieldMove/Fields/2"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionNameNotMutated",
            sdl: "type PatchName { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchName/Name","value":"Renamed"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionNameNotEmpty",
            sdl: "type PatchEmptyName { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchEmptyName/Name","value":""}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateNonNillableFieldNotAdded",
            sdl: "type PatchRequired { name: String }",
            patch: r#"[{"op":"add","path":"/PatchRequired/Fields/-","value":{"Name":"required","Kind":23}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionNotAdded",
            sdl: "type PatchCollectionAdd { name: String }",
            patch: r#"[{"op":"add","path":"/-","value":{"Name":"Added"}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionVersionIDNotMutated",
            sdl: "type PatchVersionID { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchVersionID/VersionID","value":"bafkreibifvyfr6qvb6wx4v4cogvcdksb3v7vniaon7hdzzqb62cotpmlc4"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionIsBranchableNotMutated",
            sdl: "type PatchBranchable { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchBranchable/IsBranchable","value":true}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateRelationNameSet",
            sdl: "type PatchRelationName { name: String }",
            patch: r#"[{"op":"add","path":"/PatchRelationName/Fields/-","value":{"Name":"children","Kind":"[PatchRelationName]"}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateRelationalFieldIDType",
            sdl:
                "type PatchRelationTarget { name: String } type PatchRelationHost { name: String }",
            patch: r#"[
                {"op":"add","path":"/PatchRelationHost/Fields/-","value":{"Name":"target","Kind":"PatchRelationTarget","RelationName":"patch_relation","IsPrimary":true}},
                {"op":"add","path":"/PatchRelationHost/Fields/-","value":{"Name":"_targetID","Kind":2}}
            ]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateTypeSupported",
            sdl: "type PatchUnsupportedType { name: String }",
            patch: r#"[{"op":"add","path":"/PatchUnsupportedType/Fields/-","value":{"Name":"invalid","Kind":111}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateTypeAndKindCompatible",
            sdl: "type PatchIncompatibleType { name: String }",
            patch: r#"[{"op":"add","path":"/PatchIncompatibleType/Fields/-","value":{"Name":"invalid","Kind":"Boolean","Typ":4}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateFieldNotDuplicated",
            sdl: "type PatchDuplicateField { name: String }",
            patch: r#"[{"op":"add","path":"/PatchDuplicateField/Fields/-","value":{"Name":"name","Kind":"String"}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionMaterialized",
            sdl: "type PatchMaterialized { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchMaterialized/IsMaterialized","value":false}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionFieldDefaultValue",
            sdl: "type PatchDefault { name: String }",
            patch: r#"[{"op":"add","path":"/PatchDefault/Fields/-","value":{"Name":"score","Kind":"Int","DefaultValue":"bad"}}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateEncryptedIndexes",
            sdl: "type PatchInvalidEncryptedIndex { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchInvalidEncryptedIndex/EncryptedIndexes","value":[{"FieldName":"missing"}]}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateVersionID",
            sdl: "type PatchInvalidVersionCID { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchInvalidVersionCID/VersionID","value":"invalid"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionID",
            sdl: "type PatchInvalidCollectionCID { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchInvalidCollectionCID/CollectionID","value":"invalid"}]"#,
            accepted: false,
        },
        PatchCase {
            validator: "validateCollectionSourceFromSameCollection",
            sdl: "type PatchInvalidSource { name: String }",
            patch: r#"[{"op":"replace","path":"/PatchInvalidSource/PreviousVersion","value":{"SourceCollectionID":"bafkreibifvyfr6qvb6wx4v4cogvcdksb3v7vniaon7hdzzqb62cotpmlc4"}}]"#,
            accepted: false,
        },
    ];

    for case in cases {
        rust.schema_add(case.sdl)
            .unwrap_or_else(|error| panic!("{} Rust setup failed: {error}", case.validator));
        go.schema_add(case.sdl)
            .unwrap_or_else(|error| panic!("{} Go setup failed: {error}", case.validator));

        let rust_result = rust.collection_patch(case.patch);
        let go_result = go.collection_patch(case.patch);
        assert_outcome(case.validator, case.accepted, &rust_result, &go_result);
        purge_both(&rust, &go);
    }
}
