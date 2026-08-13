use db::database::DB;
use lens::{LensConfig, LensModule, TransformId};
use schema::{FieldKind, ScalarArrayKind};
use storage::backends::MemoryStore;
use storage::corekv::Key;
use storage::keys::systemstore::LensConfigKey;

async fn agent_response_db() -> DB<MemoryStore> {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let collections = query::parse_sdl(
        r#"
        type AgentResponse {
            message: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();
    db
}

async fn users_db() -> DB<MemoryStore> {
    let db = DB::new(MemoryStore::new()).unwrap();
    let collections = query::parse_sdl("type Users { name: String }").unwrap();
    db.create_collections_atomic(collections).await.unwrap();
    db
}

const ADD_AGE_PATCH: &str = r#"
    [{
        "op": "add",
        "path": "/Users/Fields/-",
        "value": {"Name": "age", "Kind": "Int"}
    }]
"#;

#[tokio::test]
async fn patch_collection_registers_migration_atomically() {
    let db = users_db().await;
    let original = db.get_collection("Users").unwrap().unwrap();
    let migration = LensConfig::new(
        "ignored-source",
        "ignored-destination",
        LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
    );

    let patched = db
        .patch_collection_with_migration("Users", ADD_AGE_PATCH, Some(migration), None)
        .await
        .unwrap();
    let previous = patched.previous_version.as_ref().unwrap();
    let transform_id = previous.transform.as_ref().unwrap();

    assert_eq!(previous.source_collection_id, original.version_id());
    assert!(db.has_migration(&TransformId::new(transform_id)));

    let txn = db.new_txn(true).await.unwrap();
    let persisted = txn
        .systemstore()
        .unwrap()
        .get(&LensConfigKey::new(transform_id).bytes())
        .await
        .unwrap()
        .unwrap();
    let persisted: LensConfig = serde_json::from_slice(&persisted).unwrap();
    assert_eq!(persisted.source_schema_version_id, original.version_id());
    assert_eq!(persisted.destination_schema_version_id, patched.version_id);
    assert!(db
        .get_all_collection_versions()
        .await
        .unwrap()
        .iter()
        .any(|version| version.version_id == patched.version_id
            && version.previous_version == patched.previous_version));
}

#[tokio::test]
async fn patch_collection_rolls_back_when_migration_is_invalid() {
    let expected_db = users_db().await;
    let expected = expected_db
        .patch_collection("Users", ADD_AGE_PATCH, None)
        .await
        .unwrap();

    let db = users_db().await;
    let original = db.get_collection("Users").unwrap().unwrap();
    let invalid_migration = LensConfig::new("", "", LensModule::from_bytes(vec![0]));

    db.patch_collection_with_migration("Users", ADD_AGE_PATCH, Some(invalid_migration), None)
        .await
        .unwrap_err();

    assert_eq!(
        db.get_collection("Users").unwrap().unwrap().version_id(),
        original.version_id()
    );
    assert_eq!(db.get_all_collection_versions().await.unwrap().len(), 1);

    let retried = db
        .patch_collection("Users", ADD_AGE_PATCH, None)
        .await
        .unwrap();
    assert_eq!(retried.version_id, expected.version_id);
}

#[tokio::test]
async fn patch_collection_preserves_runtime_root_id() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let collections = query::parse_sdl(
        r#"
        type Users {
            name: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    let original = db.get_collection("Users").unwrap().unwrap();
    let root_id = original.resolved_root_id();
    assert_ne!(root_id, 0);

    let patched = db
        .patch_collection(
            "Users",
            r#"
            [
                { "op": "add", "path": "/Users/Fields/-", "value": {
                    "Name": "age", "Kind": "Int"
                }}
            ]
            "#,
            None,
        )
        .await
        .unwrap();

    assert_eq!(patched.root_id, root_id);
    assert_eq!(
        db.get_collection("Users")
            .unwrap()
            .unwrap()
            .resolved_root_id(),
        root_id
    );
}

#[tokio::test]
async fn patch_collection_accepts_numeric_go_kind() {
    let db = agent_response_db().await;

    let patched = db
        .patch_collection(
            "AgentResponse",
            r#"
            [
                { "op": "add", "path": "/AgentResponse/Fields/-", "value": {
                    "Name": "reasoning_progress_seq", "Kind": 5
                }}
            ]
            "#,
            None,
        )
        .await
        .unwrap();

    let reasoning_progress_seq = patched
        .fields
        .iter()
        .find(|field| field.name == "reasoning_progress_seq")
        .unwrap();
    assert_eq!(
        reasoning_progress_seq.kind,
        FieldKind::ScalarArray(ScalarArrayKind::IntArray)
    );
}

#[tokio::test]
async fn patch_collection_rejects_new_non_nillable_field() {
    let db = agent_response_db().await;
    let err = db
        .patch_collection(
            "AgentResponse",
            r#"
            [{
                "op": "add",
                "path": "/AgentResponse/Fields/-",
                "value": {"Name": "score", "Kind": 23}
            }]
            "#,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid patch: adding a non-nillable field to an existing collection is not supported"
    );
}

#[tokio::test]
async fn patch_collection_reports_unknown_numeric_kind_like_go() {
    let db = agent_response_db().await;
    let err = db
        .patch_collection(
            "AgentResponse",
            r#"
            [{
                "op": "add",
                "path": "/AgentResponse/Fields/-",
                "value": {"Name": "score", "Kind": 111}
            }]
            "#,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid patch: no type found for given name. Type: 111"
    );
}

#[tokio::test]
async fn patch_view_query_creates_new_version() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let users = query::parse_sdl("type Users { name: String }").unwrap();
    db.create_collections_atomic(users).await.unwrap();

    let mut views = query::parse_sdl("type UserView { name: String fullName: String }").unwrap();
    let select = query::parse_query("query { Users { name } }").unwrap();
    views[0].query = Some(schema::QuerySource::new(query::select_to_go_json(
        &select[0],
    )));
    db.create_collections_atomic(views).await.unwrap();
    let original = db.get_collection("UserView").unwrap().unwrap();

    let patched = db
        .patch_collection(
            "UserView",
            r#"
            [{
                "op": "replace",
                "path": "/UserView/Query/Query",
                "value": {"Name": "Users", "Fields": [{"Name": "name", "Alias": "fullName"}]}
            }]
            "#,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        patched.version_id,
        "bafyreihekbkpnimeo5g5tkv5a3urtcalx2qd447tyhhptjunb7vvpdyvue"
    );
    assert_eq!(
        patched
            .previous_version
            .as_ref()
            .map(|source| source.source_collection_id.as_str()),
        Some(original.version_id())
    );
    assert_eq!(db.get_all_collection_versions().await.unwrap().len(), 3);
}

#[tokio::test]
async fn patch_collection_accepts_numeric_kind_in_whole_fields_replacement() {
    let db = agent_response_db().await;
    let collection = db.get_collection("AgentResponse").unwrap().unwrap();
    let mut fields = serde_json::to_value(&collection.schema().fields).unwrap();

    for field in fields.as_array_mut().unwrap() {
        let map = field.as_object_mut().unwrap();
        let canonical = match map.get("Name").and_then(|name| name.as_str()).unwrap() {
            "_docID" => "ID",
            "message" => "String",
            name => panic!("unexpected field: {name}"),
        };
        map.insert("Kind".to_string(), serde_json::json!(canonical));
    }
    fields.as_array_mut().unwrap().push(serde_json::json!({
        "FieldID": "9",
        "Name": "reasoning_progress_seq",
        "Kind": 5
    }));

    let patch = serde_json::json!([{
        "op": "replace",
        "path": "/AgentResponse/Fields",
        "value": fields
    }]);
    let patched = db
        .patch_collection("AgentResponse", &patch.to_string(), None)
        .await
        .unwrap();
    assert_eq!(
        patched
            .fields
            .iter()
            .find(|field| field.name == "reasoning_progress_seq")
            .unwrap()
            .kind,
        FieldKind::ScalarArray(ScalarArrayKind::IntArray)
    );
}

#[tokio::test]
async fn patch_collection_rejects_numeric_kind_in_direct_kind_replacement() {
    let db = agent_response_db().await;
    let err = db
        .patch_collection(
            "AgentResponse",
            r#"
            [{
                "op": "replace",
                "path": "/AgentResponse/Fields/message/Kind",
                "value": 3
            }]
            "#,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid patch: mutating an existing field is not supported. ProposedName: "
    );
}

#[tokio::test]
async fn patch_relation_version_switching_preserves_go_canonical_versions() {
    const AUTHOR_V1: &str = "bafyreibvcavbxqwimz5vdxe5q5href63g3skc6ytg45hm4fqh6wsx57wmq";
    const AUTHOR_V2: &str = "bafyreihv2jdbz3sipc7tqdoycerkcjn6gehr5aleiroqlewvsmjd26unfq";

    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let collections = query::parse_sdl(
        r#"
        type Author {
            name: String
        }
        type Book {
            title: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    let author = db.get_collection("Author").unwrap().unwrap();
    assert_eq!(author.version_id(), AUTHOR_V1);

    db.patch_collection(
        "Author",
        r#"
        [
            { "op": "add", "path": "/Author/Fields/-", "value": {
                "Name": "published", "Kind": "Book", "RelationName": "author_book", "IsPrimary": true
            }},
            { "op": "add", "path": "/Author/Fields/-", "value": {
                "Name": "_publishedID", "Kind": "ID", "RelationName": "author_book", "IsPrimary": true
            }}
        ]
        "#,
        None,
    )
    .await
    .unwrap();

    db.patch_collection(
        "Book",
        r#"
        [
            { "op": "add", "path": "/Book/Fields/-", "value": {
                "Name": "author", "Kind": "Author", "RelationName": "author_book"
            }}
        ]
        "#,
        None,
    )
    .await
    .unwrap();

    let author_v2 = db
        .get_collection_by_version_id_full(AUTHOR_V2)
        .await
        .unwrap();
    assert!(author_v2.is_some());

    db.set_active_collection_version(AUTHOR_V1).await.unwrap();
    assert!(db
        .get_collection("Author")
        .unwrap()
        .unwrap()
        .get_indexes()
        .is_empty());

    db.set_active_collection_version(AUTHOR_V2).await.unwrap();
    let author = db.get_collection("Author").unwrap().unwrap();
    assert_eq!(author.version_id(), AUTHOR_V2);
    assert_eq!(author.get_indexes().len(), 1);
    assert_eq!(author.get_indexes()[0].name, "Author__publishedID_ASC");
}
